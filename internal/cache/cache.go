package cache

import (
	"errors"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"distcache/internal/eviction"
)

var (
	ErrNotInteger = errors.New("cache: value is not an integer")

	ErrClosed = errors.New("cache: closed")
)

type Config struct {
	Shards int

	MaxEntries int64

	MaxMemory int64

	Policy eviction.Kind

	ActiveExpiry time.Duration

	EventBuffer int
}

func (c *Config) withDefaults() {
	if c.Shards <= 0 {
		c.Shards = 256
	}
	c.Shards = nextPow2(c.Shards)
	if c.Policy == "" {
		c.Policy = eviction.NoEviction
	}
	if c.EventBuffer <= 0 {
		c.EventBuffer = 4096
	}
}

type Stats struct {
	Hits        uint64
	Misses      uint64
	Sets        uint64
	Deletes     uint64
	Evictions   uint64
	Expirations uint64
	Keys        int64
	MemoryBytes int64
}

type Cache struct {
	cfg    Config
	shards []*shard
	mask   uint64
	seed   maphash.Seed

	seq uint64

	hits, misses, sets, dels, evictions, expirations uint64

	subMu       sync.RWMutex
	subscribers []func(Event)
	events      chan Event
	done        chan struct{}
	wg          sync.WaitGroup
	closeOnce   sync.Once
	closed      atomic.Bool

	clock func() int64
}

func New(cfg Config) (*Cache, error) {
	cfg.withDefaults()
	c := &Cache{
		cfg:    cfg,
		shards: make([]*shard, cfg.Shards),
		mask:   uint64(cfg.Shards - 1),
		seed:   maphash.MakeSeed(),
		events: make(chan Event, cfg.EventBuffer),
		done:   make(chan struct{}),
		clock:  func() int64 { return time.Now().UnixNano() },
	}
	perShardEntries := int64(0)
	if cfg.MaxEntries > 0 {
		perShardEntries = cfg.MaxEntries / int64(cfg.Shards)
		if perShardEntries < 1 {
			perShardEntries = 1
		}
	}
	perShardMem := int64(0)
	if cfg.MaxMemory > 0 {
		perShardMem = cfg.MaxMemory / int64(cfg.Shards)
		if perShardMem < 1 {
			perShardMem = 1
		}
	}
	for i := range c.shards {
		pol, err := eviction.New(cfg.Policy)
		if err != nil {
			return nil, err
		}
		c.shards[i] = newShard(pol, perShardEntries, perShardMem, c.emit)
	}

	c.wg.Add(1)
	go c.dispatch()
	if cfg.ActiveExpiry > 0 {
		c.wg.Add(1)
		go c.activeExpiry(cfg.ActiveExpiry)
	}
	return c, nil
}

func (c *Cache) now() int64 { return c.clock() }

func (c *Cache) shardFor(key string) *shard {
	var h maphash.Hash
	h.SetSeed(c.seed)
	_, _ = h.WriteString(key)
	return c.shards[h.Sum64()&c.mask]
}

func (c *Cache) emit(ev Event) {
	if c.closed.Load() {
		return
	}
	select {
	case c.events <- ev:
	case <-c.done:
	}
}

func (c *Cache) dispatch() {
	defer c.wg.Done()
	var seq uint64
	fan := func(ev Event) {
		if ev.ack != nil {
			close(ev.ack)
			return
		}
		seq++
		ev.Seq = seq
		c.subMu.RLock()
		for _, s := range c.subscribers {
			s(ev)
		}
		c.subMu.RUnlock()
		atomic.StoreUint64(&c.seq, seq)
	}
	for {
		select {
		case ev := <-c.events:
			fan(ev)
		case <-c.done:
			for {
				select {
				case ev := <-c.events:
					fan(ev)
				default:
					return
				}
			}
		}
	}
}

func (c *Cache) Subscribe(fn func(Event)) {
	c.subMu.Lock()
	c.subscribers = append(c.subscribers, fn)
	c.subMu.Unlock()
}

func (c *Cache) Sync() {
	if c.closed.Load() {
		return
	}
	ack := make(chan struct{})
	select {
	case c.events <- Event{ack: ack}:
	case <-c.done:
		return
	}
	select {
	case <-ack:
	case <-c.done:
	}
}

func (c *Cache) activeExpiry(interval time.Duration) {
	defer c.wg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	const sample = 32
	for {
		select {
		case <-t.C:
			now := c.now()
			for _, s := range c.shards {
				expired := s.sweepExpired(now, sample)
				if n := len(expired); n > 0 {
					atomic.AddUint64(&c.expirations, uint64(n))
				}
			}
		case <-c.done:
			return
		}
	}
}

func (c *Cache) Set(key string, value []byte) {
	c.SetTTL(key, value, 0)
}

func (c *Cache) SetTTL(key string, value []byte, ttl time.Duration) {
	var expireAt int64
	if ttl > 0 {
		expireAt = c.now() + int64(ttl)
	}
	evicted := c.shardFor(key).set(key, value, expireAt)
	atomic.AddUint64(&c.sets, 1)
	if n := len(evicted); n > 0 {
		atomic.AddUint64(&c.evictions, uint64(n))
	}
}

func (c *Cache) Get(key string) ([]byte, bool) {
	val, found, expiredKey := c.shardFor(key).get(key, c.now())
	if expiredKey != "" {
		atomic.AddUint64(&c.expirations, 1)
	}
	if found {
		atomic.AddUint64(&c.hits, 1)
	} else {
		atomic.AddUint64(&c.misses, 1)
	}
	return val, found
}

func (c *Cache) Delete(key string) bool {
	ok := c.shardFor(key).del(key)
	if ok {
		atomic.AddUint64(&c.dels, 1)
	}
	return ok
}

func (c *Cache) Exists(key string) bool {
	ok, expiredKey := c.shardFor(key).exists(key, c.now())
	if expiredKey != "" {
		atomic.AddUint64(&c.expirations, 1)
	}
	return ok
}

func (c *Cache) IncrBy(key string, delta int64) (int64, error) {
	v, evicted, err := c.shardFor(key).incrBy(key, delta, c.now())
	if err != nil {
		return 0, err
	}
	atomic.AddUint64(&c.sets, 1)
	if n := len(evicted); n > 0 {
		atomic.AddUint64(&c.evictions, uint64(n))
	}
	return v, nil
}

func (c *Cache) Incr(key string) (int64, error)                { return c.IncrBy(key, 1) }
func (c *Cache) Decr(key string) (int64, error)                { return c.IncrBy(key, -1) }
func (c *Cache) DecrBy(key string, delta int64) (int64, error) { return c.IncrBy(key, -delta) }

func (c *Cache) Expire(key string, ttl time.Duration) bool {
	var expireAt int64
	if ttl > 0 {
		expireAt = c.now() + int64(ttl)
	}
	ok, expiredKey := c.shardFor(key).expire(key, expireAt, c.now())
	if expiredKey != "" {
		atomic.AddUint64(&c.expirations, 1)
	}
	return ok
}

func (c *Cache) TTL(key string) (remaining time.Duration, found, persists bool) {
	now := c.now()
	expireAt, ok, expiredKey := c.shardFor(key).ttl(key, now)
	if expiredKey != "" {
		atomic.AddUint64(&c.expirations, 1)
	}
	if !ok {
		return 0, false, false
	}
	if expireAt == 0 {
		return 0, true, true
	}
	return time.Duration(expireAt - now), true, false
}

type KV struct {
	Key   string
	Value []byte
	TTL   time.Duration
}

func (c *Cache) MSet(pairs []KV) {
	for _, p := range pairs {
		c.SetTTL(p.Key, p.Value, p.TTL)
	}
}

func (c *Cache) MGet(keys []string) (values [][]byte, found []bool) {
	values = make([][]byte, len(keys))
	found = make([]bool, len(keys))
	for i, k := range keys {
		values[i], found[i] = c.Get(k)
	}
	return values, found
}

func (c *Cache) Keys() []string {
	now := c.now()
	var out []string
	for _, s := range c.shards {
		out = append(out, s.keys(now)...)
	}
	return out
}

func (c *Cache) Len() int {
	n := 0
	for _, s := range c.shards {
		n += s.len()
	}
	return n
}

func (c *Cache) FlushAll() {
	for _, s := range c.shards {
		s.flush()
	}
}

func (c *Cache) Stats() Stats {
	var keys, mem int64
	for _, s := range c.shards {
		keys += int64(s.len())
		mem += s.memBytes()
	}
	return Stats{
		Hits:        atomic.LoadUint64(&c.hits),
		Misses:      atomic.LoadUint64(&c.misses),
		Sets:        atomic.LoadUint64(&c.sets),
		Deletes:     atomic.LoadUint64(&c.dels),
		Evictions:   atomic.LoadUint64(&c.evictions),
		Expirations: atomic.LoadUint64(&c.expirations),
		Keys:        keys,
		MemoryBytes: mem,
	}
}

func (s Stats) HitRatio() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

func (c *Cache) Seq() uint64 { return atomic.LoadUint64(&c.seq) }

func (c *Cache) Export(fn func(key string, value []byte, expireAt int64)) {
	now := c.now()
	for _, s := range c.shards {
		s.export(now, fn)
	}
}

func (c *Cache) ApplyEvent(ev Event) {
	switch ev.Op {
	case OpSet:
		c.shardFor(ev.Key).applySet(ev.Key, ev.Value, ev.ExpireAt)
	case OpDel:
		c.shardFor(ev.Key).applyDel(ev.Key)
	case OpExpire:
		c.shardFor(ev.Key).applyExpire(ev.Key, ev.ExpireAt)
	case OpFlush:
		for _, s := range c.shards {
			s.flushSilent()
		}
	}
}

func (c *Cache) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.done)
		c.wg.Wait()
	})
	return nil
}

func nextPow2(n int) int {
	if n < 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}
