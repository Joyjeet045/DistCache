package replication

import (
	"bufio"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"distcache/internal/cache"
)

const ackInterval = time.Second

const reconnectBackoff = time.Second

type Replica struct {
	cache       *cache.Cache
	primaryAddr string
	logger      *log.Logger

	synced      atomic.Bool
	lastApplied uint64

	quit      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func NewReplica(c *cache.Cache, primaryAddr string, logger *log.Logger) *Replica {
	return &Replica{
		cache:       c,
		primaryAddr: primaryAddr,
		logger:      logger,
		quit:        make(chan struct{}),
	}
}

func (r *Replica) Start() {
	r.wg.Add(1)
	go r.run()
}

func (r *Replica) run() {
	defer r.wg.Done()
	for {
		select {
		case <-r.quit:
			return
		default:
		}
		if err := r.replicate(); err != nil {
			r.synced.Store(false)
			r.logf("replication to %s ended: %v", r.primaryAddr, err)
		}
		select {
		case <-r.quit:
			return
		case <-time.After(reconnectBackoff):
		}
	}
}

func (r *Replica) replicate() error {
	nc, err := net.Dial("tcp", r.primaryAddr)
	if err != nil {
		return err
	}
	defer nc.Close()

	connDone := make(chan struct{})
	defer close(connDone)
	go func() {
		select {
		case <-r.quit:
			_ = nc.Close()
		case <-connDone:
		}
	}()

	w := bufio.NewWriterSize(nc, 32*1024)
	if _, err := w.WriteString(replMagic); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}

	var ackWG sync.WaitGroup
	ackWG.Add(1)
	go func() {
		defer ackWG.Done()
		r.ackLoop(w, connDone)
	}()
	defer ackWG.Wait()

	rd := bufio.NewReaderSize(nc, 64*1024)
	for {
		typ, payload, err := readFrame(rd)
		if err != nil {
			return err
		}
		switch typ {
		case frameEvent:
			ev, derr := decodeEvent(payload)
			if derr != nil {
				return derr
			}
			r.cache.ApplyEvent(ev)
			if ev.Seq > 0 {
				atomic.StoreUint64(&r.lastApplied, ev.Seq)
			}
		case frameSynced:
			if seq, ok := readU64(payload); ok {
				atomic.StoreUint64(&r.lastApplied, seq)
			}
			r.synced.Store(true)
			r.logf("replica synced with %s", r.primaryAddr)
		}
	}
}

func (r *Replica) ackLoop(w *bufio.Writer, done <-chan struct{}) {
	t := time.NewTicker(ackInterval)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-r.quit:
			return
		case <-t.C:
			seq := atomic.LoadUint64(&r.lastApplied)
			if err := writeFrame(w, frameAck, u64Bytes(seq)); err != nil {
				return
			}
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}

func (r *Replica) Synced() bool { return r.synced.Load() }

func (r *Replica) LastApplied() uint64 { return atomic.LoadUint64(&r.lastApplied) }

func (r *Replica) PrimaryAddr() string { return r.primaryAddr }

func (r *Replica) Close() error {
	r.closeOnce.Do(func() { close(r.quit) })
	r.wg.Wait()
	return nil
}

func (r *Replica) logf(format string, args ...any) {
	if r.logger != nil {
		r.logger.Printf(format, args...)
	}
}
