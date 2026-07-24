package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"distcache/internal/cache"
	"distcache/internal/cluster"
	"distcache/internal/locks"
	"distcache/internal/metrics"
	"distcache/internal/pubsub"
	"distcache/internal/replication"
	"distcache/internal/resp"
	"distcache/internal/txn"
)

type Config struct {
	Addr string

	MetricsAddr string

	Password string

	NodeID string
}

type Server struct {
	cfg     Config
	cache   *cache.Cache
	broker  *pubsub.Broker
	locks   *locks.Manager
	txn     *txn.Coordinator
	metrics *metrics.Registry

	ln        net.Listener
	httpSrv   *http.Server
	mu        sync.Mutex
	conns     map[*conn]struct{}
	quit      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once

	registry *cluster.Registry
	primary  *replication.Primary
	replica  *replication.Replica
}

func New(c *cache.Cache, cfg Config) *Server {
	if cfg.NodeID == "" {
		cfg.NodeID = "node-1"
	}
	return &Server{
		cfg:     cfg,
		cache:   c,
		broker:  pubsub.NewBroker(),
		locks:   locks.New(),
		txn:     txn.NewCoordinator(c),
		metrics: metrics.New(cfg.NodeID),
		conns:   make(map[*conn]struct{}),
		quit:    make(chan struct{}),
	}
}

func (s *Server) Metrics() *metrics.Registry { return s.metrics }

func (s *Server) Broker() *pubsub.Broker { return s.broker }

func (s *Server) Locks() *locks.Manager { return s.locks }

func (s *Server) AttachCluster(r *cluster.Registry) { s.registry = r }

func (s *Server) AttachPrimary(p *replication.Primary) { s.primary = p }

func (s *Server) AttachReplica(r *replication.Replica) { s.replica = r }

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	if s.cfg.MetricsAddr != "" {
		s.startMetricsHTTP()
	}
	return s.Serve(ln)
}

func (s *Server) startMetricsHTTP() {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		s.metrics.WritePrometheus(w, s.cache.Stats())
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK\n"))
	})
	s.httpSrv = &http.Server{Addr: s.cfg.MetricsAddr, Handler: mux}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {

		}
	}()
}

func (s *Server) Serve(ln net.Listener) error {
	s.ln = ln
	for {
		nc, err := ln.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return nil
			default:
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					continue
				}
				return err
			}
		}
		c := newConn(s, nc)
		s.trackConn(c, true)
		s.metrics.ConnOpened()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.metrics.ConnClosed()
			defer s.trackConn(c, false)
			c.serve()
		}()
	}
}

func (s *Server) trackConn(c *conn, add bool) {
	s.mu.Lock()
	if add {
		s.conns[c] = struct{}{}
	} else {
		delete(s.conns, c)
	}
	s.mu.Unlock()
}

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		close(s.quit)
		if s.ln != nil {
			_ = s.ln.Close()
		}
		s.mu.Lock()
		for c := range s.conns {
			_ = c.nc.Close()
		}
		s.mu.Unlock()
		if s.httpSrv != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = s.httpSrv.Shutdown(ctx)
		}
	})
	s.wg.Wait()
	return nil
}

type conn struct {
	s      *Server
	nc     net.Conn
	r      *resp.Reader
	w      *resp.Writer
	wmu    sync.Mutex
	authed bool

	tx      *txn.Tx
	inMulti bool

	subMu sync.Mutex
	subs  map[string]*pubsub.Subscription
}

func newConn(s *Server, nc net.Conn) *conn {
	return &conn{
		s:    s,
		nc:   nc,
		r:    resp.NewReader(nc),
		w:    resp.NewWriter(nc),
		subs: make(map[string]*pubsub.Subscription),
	}
}

func (c *conn) serve() {
	defer c.closeSubs()
	defer c.nc.Close()
	for {
		select {
		case <-c.s.quit:
			return
		default:
		}
		args, err := c.r.ReadCommand()
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		start := time.Now()
		c.s.metrics.IncCommands()
		if !c.dispatch(args) {
			return
		}
		c.s.metrics.Observe(time.Since(start))
	}
}

func (c *conn) closeSubs() {
	c.subMu.Lock()
	for _, sub := range c.subs {
		sub.Close()
	}
	c.subs = map[string]*pubsub.Subscription{}
	c.subMu.Unlock()
}

func (c *conn) flushLocked() { _ = c.w.Flush() }
