package replication

import (
	"bufio"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"distcache/internal/cache"
)

type Primary struct {
	cache  *cache.Cache
	addr   string
	logger *log.Logger

	ln net.Listener

	mu       sync.RWMutex
	replicas map[*replicaConn]struct{}

	quit      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

type replicaConn struct {
	nc        net.Conn
	addr      string
	ch        chan cache.Event
	startSeq  uint64
	lastSent  uint64
	lastAck   uint64
	connected int64
	done      chan struct{}
	once      sync.Once
}

func (rc *replicaConn) fail() { rc.once.Do(func() { close(rc.done) }) }

type ReplicaState struct {
	Addr        string
	LastSentSeq uint64
	LastAckSeq  uint64
	ConnectedAt time.Time
}

func NewPrimary(c *cache.Cache, addr string, logger *log.Logger) *Primary {
	p := &Primary{
		cache:    c,
		addr:     addr,
		logger:   logger,
		replicas: make(map[*replicaConn]struct{}),
		quit:     make(chan struct{}),
	}
	c.Subscribe(p.onEvent)
	return p
}

func (p *Primary) onEvent(ev cache.Event) {
	p.mu.RLock()
	for rc := range p.replicas {
		select {
		case rc.ch <- ev:
		default:
			rc.fail()
		}
	}
	p.mu.RUnlock()
}

func (p *Primary) ListenAndServe() error {
	ln, err := net.Listen("tcp", p.addr)
	if err != nil {
		return err
	}
	return p.Serve(ln)
}

func (p *Primary) Serve(ln net.Listener) error {
	p.mu.Lock()
	p.ln = ln
	p.mu.Unlock()
	for {
		nc, err := ln.Accept()
		if err != nil {
			select {
			case <-p.quit:
				return nil
			default:
				return err
			}
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.handle(nc)
		}()
	}
}

func (p *Primary) Addr() string {
	p.mu.RLock()
	ln := p.ln
	p.mu.RUnlock()
	if ln != nil {
		return ln.Addr().String()
	}
	return p.addr
}

func (p *Primary) handle(nc net.Conn) {
	defer nc.Close()
	r := bufio.NewReaderSize(nc, 32*1024)
	w := bufio.NewWriterSize(nc, 64*1024)

	magic := make([]byte, len(replMagic))
	if _, err := io.ReadFull(r, magic); err != nil || string(magic) != replMagic {
		return
	}

	rc := &replicaConn{
		nc:        nc,
		addr:      nc.RemoteAddr().String(),
		ch:        make(chan cache.Event, 8192),
		done:      make(chan struct{}),
		connected: time.Now().UnixNano(),
	}

	p.mu.Lock()
	p.replicas[rc] = struct{}{}
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.replicas, rc)
		p.mu.Unlock()
	}()

	if err := p.sendSnapshot(rc, w); err != nil {
		p.logf("replica %s snapshot failed: %v", rc.addr, err)
		return
	}
	p.logf("replica %s synced at seq %d", rc.addr, rc.startSeq)

	go p.readAcks(rc, r)
	p.stream(rc, w)
}

func (p *Primary) sendSnapshot(rc *replicaConn, w *bufio.Writer) error {
	p.cache.Sync()
	rc.startSeq = p.cache.Seq()

	if err := writeFrame(w, frameEvent, encodeEvent(cache.Event{Op: cache.OpFlush})); err != nil {
		return err
	}
	var sendErr error
	p.cache.Export(func(key string, val []byte, exp int64) {
		if sendErr != nil {
			return
		}
		ev := cache.Event{Op: cache.OpSet, Key: key, Value: val, ExpireAt: exp}
		sendErr = writeFrame(w, frameEvent, encodeEvent(ev))
	})
	if sendErr != nil {
		return sendErr
	}
	if err := writeFrame(w, frameSynced, u64Bytes(rc.startSeq)); err != nil {
		return err
	}
	return w.Flush()
}

func (p *Primary) stream(rc *replicaConn, w *bufio.Writer) {
	for {
		select {
		case <-p.quit:
			return
		case <-rc.done:
			return
		case ev := <-rc.ch:
			if ev.Seq <= rc.startSeq {
				continue
			}
			if err := writeFrame(w, frameEvent, encodeEvent(ev)); err != nil {
				return
			}

			if len(rc.ch) == 0 {
				if err := w.Flush(); err != nil {
					return
				}
			}
			atomic.StoreUint64(&rc.lastSent, ev.Seq)
		}
	}
}

func (p *Primary) readAcks(rc *replicaConn, r *bufio.Reader) {
	defer rc.fail()
	for {
		typ, payload, err := readFrame(r)
		if err != nil {
			return
		}
		if typ == frameAck {
			if seq, ok := readU64(payload); ok {
				atomic.StoreUint64(&rc.lastAck, seq)
			}
		}
	}
}

func (p *Primary) Replicas() []ReplicaState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]ReplicaState, 0, len(p.replicas))
	for rc := range p.replicas {
		out = append(out, ReplicaState{
			Addr:        rc.addr,
			LastSentSeq: atomic.LoadUint64(&rc.lastSent),
			LastAckSeq:  atomic.LoadUint64(&rc.lastAck),
			ConnectedAt: time.Unix(0, atomic.LoadInt64(&rc.connected)),
		})
	}
	return out
}

func (p *Primary) NumReplicas() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.replicas)
}

func (p *Primary) Close() error {
	p.closeOnce.Do(func() {
		close(p.quit)
		p.mu.RLock()
		ln := p.ln
		p.mu.RUnlock()
		if ln != nil {
			_ = ln.Close()
		}
		p.mu.RLock()
		for rc := range p.replicas {
			rc.fail()
			_ = rc.nc.Close()
		}
		p.mu.RUnlock()
	})
	p.wg.Wait()
	return nil
}

func (p *Primary) logf(format string, args ...any) {
	if p.logger != nil {
		p.logger.Printf(format, args...)
	}
}
