package txn

import (
	"errors"
	"sync"

	"distcache/internal/cache"
)

var ErrAborted = errors.New("txn: aborted, watched key modified")

var ErrNoMulti = errors.New("txn: EXEC/queue without MULTI")

type Coordinator struct {
	cache *cache.Cache

	mu       sync.Mutex
	execMu   sync.Mutex
	versions map[string]uint64
	watchers map[string]int
}

func NewCoordinator(c *cache.Cache) *Coordinator {
	co := &Coordinator{
		cache:    c,
		versions: make(map[string]uint64),
		watchers: make(map[string]int),
	}
	c.Subscribe(co.onEvent)
	return co
}

func (co *Coordinator) onEvent(ev cache.Event) {
	co.mu.Lock()
	defer co.mu.Unlock()
	if ev.Op == cache.OpFlush {
		for k := range co.watchers {
			co.versions[k]++
		}
		return
	}
	if ev.Key != "" && co.watchers[ev.Key] > 0 {
		co.versions[ev.Key]++
	}
}

func (co *Coordinator) Begin() *Tx {
	return &Tx{co: co, watched: make(map[string]uint64)}
}

type Tx struct {
	co      *Coordinator
	watched map[string]uint64
	queued  []func(*cache.Cache) any
	inMulti bool
}

func (tx *Tx) Watch(keys ...string) error {
	if tx.inMulti {
		return errors.New("txn: WATCH not allowed inside MULTI")
	}

	tx.co.cache.Sync()
	tx.co.mu.Lock()
	defer tx.co.mu.Unlock()
	for _, k := range keys {
		if _, already := tx.watched[k]; already {
			continue
		}
		tx.co.watchers[k]++
		tx.watched[k] = tx.co.versions[k]
	}
	return nil
}

func (tx *Tx) Multi() error {
	if tx.inMulti {
		return errors.New("txn: MULTI already called")
	}
	tx.inMulti = true
	return nil
}

func (tx *Tx) Queue(cmd func(*cache.Cache) any) error {
	if !tx.inMulti {
		return ErrNoMulti
	}
	tx.queued = append(tx.queued, cmd)
	return nil
}

func (tx *Tx) Exec() ([]any, error) {
	if !tx.inMulti {
		return nil, ErrNoMulti
	}
	tx.co.execMu.Lock()
	defer tx.co.execMu.Unlock()
	defer tx.release()

	tx.co.mu.Lock()
	changed := false
	for k, snap := range tx.watched {
		if tx.co.versions[k] != snap {
			changed = true
			break
		}
	}
	tx.co.mu.Unlock()
	if changed {
		return nil, ErrAborted
	}

	results := make([]any, 0, len(tx.queued))
	for _, cmd := range tx.queued {
		results = append(results, cmd(tx.co.cache))
	}
	tx.inMulti = false
	tx.queued = nil
	return results, nil
}

func (tx *Tx) Discard() {
	tx.release()
	tx.inMulti = false
	tx.queued = nil
}

func (tx *Tx) release() {
	if len(tx.watched) == 0 {
		return
	}
	tx.co.mu.Lock()
	for k := range tx.watched {
		if tx.co.watchers[k] > 0 {
			tx.co.watchers[k]--
			if tx.co.watchers[k] == 0 {
				delete(tx.co.watchers, k)
				delete(tx.co.versions, k)
			}
		}
	}
	tx.co.mu.Unlock()
	tx.watched = make(map[string]uint64)
}
