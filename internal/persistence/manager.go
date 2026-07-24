package persistence

import (
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"distcache/internal/cache"
)

const (
	snapshotFile = "snapshot.db"
	aofFile      = "appendonly.aof"
)

type Config struct {
	Dir string

	Sync SyncPolicy

	SnapshotInterval time.Duration

	Logger *log.Logger
}

type Manager struct {
	cfg      Config
	snapPath string
	aofPath  string

	mu    sync.Mutex
	aof   *AOF
	cache *cache.Cache

	stop       chan struct{}
	wg         sync.WaitGroup
	appendErrs uint64
}

func Open(cfg Config) (*Manager, error) {
	if cfg.Sync == "" {
		cfg.Sync = SyncEverySec
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	return &Manager{
		cfg:      cfg,
		snapPath: filepath.Join(cfg.Dir, snapshotFile),
		aofPath:  filepath.Join(cfg.Dir, aofFile),
		stop:     make(chan struct{}),
	}, nil
}

func (m *Manager) Recover(c *cache.Cache) error {
	startSeq, err := ReadSnapshot(m.snapPath, c.ApplyEvent)
	if err != nil {
		return err
	}
	if err := ReadAOF(m.aofPath, func(ev cache.Event) {
		if ev.Seq > startSeq {
			c.ApplyEvent(ev)
		}
	}); err != nil {
		return err
	}

	aof, err := OpenAOF(m.aofPath, m.cfg.Sync)
	if err != nil {
		return err
	}
	m.aof = aof
	m.cache = c
	c.Subscribe(func(ev cache.Event) {
		if err := m.aof.Append(ev); err != nil {
			atomic.AddUint64(&m.appendErrs, 1)
			m.logf("aof append: %v", err)
		}
	})

	if m.cfg.SnapshotInterval > 0 {
		m.wg.Add(1)
		go m.snapshotLoop(m.cfg.SnapshotInterval)
	}
	return nil
}

func (m *Manager) Snapshot() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cache == nil {
		return nil
	}

	m.cache.Sync()
	startSeq, err := WriteSnapshot(m.snapPath, m.cache)
	if err != nil {
		return err
	}
	return m.aof.Compact(startSeq)
}

func (m *Manager) snapshotLoop(interval time.Duration) {
	defer m.wg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := m.Snapshot(); err != nil {
				m.logf("snapshot: %v", err)
			}
		case <-m.stop:
			return
		}
	}
}

func (m *Manager) AppendErrors() uint64 { return atomic.LoadUint64(&m.appendErrs) }

func (m *Manager) Close() error {
	close(m.stop)
	m.wg.Wait()
	if m.aof != nil {
		return m.aof.Close()
	}
	return nil
}

func (m *Manager) logf(format string, args ...any) {
	if m.cfg.Logger != nil {
		m.cfg.Logger.Printf(format, args...)
	}
}
