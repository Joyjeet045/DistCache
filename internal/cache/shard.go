package cache

import (
	"strconv"
	"sync"

	"distcache/internal/eviction"
)

type shard struct {
	mu          sync.Mutex
	items       map[string]*item
	policy      eviction.Policy
	expiryAware eviction.ExpiryAware
	mem         int64
	maxEntries  int64
	maxMemory   int64
	sink        func(Event)
}

func newShard(policy eviction.Policy, maxEntries, maxMemory int64, sink func(Event)) *shard {
	ea, _ := policy.(eviction.ExpiryAware)
	return &shard{
		items:       make(map[string]*item),
		policy:      policy,
		expiryAware: ea,
		maxEntries:  maxEntries,
		maxMemory:   maxMemory,
		sink:        sink,
	}
}

func (s *shard) emit(ev Event) {
	if s.sink != nil {
		s.sink(ev)
	}
}

func (s *shard) get(key string, now int64) (val []byte, found bool, expiredKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	it, ok := s.items[key]
	if !ok {
		return nil, false, ""
	}
	if it.expired(now) {
		s.removeLocked(key, it)
		s.emit(Event{Op: OpDel, Key: key})
		return nil, false, key
	}
	s.policy.Access(key)
	return it.value, true, ""
}

func (s *shard) set(key string, value []byte, expireAt int64) (evicted []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setLocked(key, value, expireAt)
	s.emit(Event{Op: OpSet, Key: key, Value: value, ExpireAt: expireAt})
	evicted = s.enforceLimitsLocked(key)
	for _, k := range evicted {
		s.emit(Event{Op: OpDel, Key: k})
	}
	return evicted
}

func (s *shard) setLocked(key string, value []byte, expireAt int64) {
	v := append([]byte(nil), value...)
	if old, ok := s.items[key]; ok {
		s.mem += int64(len(v)) - int64(len(old.value))
		old.value = v
		old.expireAt = expireAt
		s.policy.Access(key)
	} else {
		s.items[key] = &item{value: v, expireAt: expireAt}
		s.mem += approxSize(key, v)
		s.policy.Add(key)
	}
	if s.expiryAware != nil {
		s.expiryAware.Note(key, expireAt)
	}
}

func (s *shard) del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	it, ok := s.items[key]
	if !ok {
		return false
	}
	s.removeLocked(key, it)
	s.emit(Event{Op: OpDel, Key: key})
	return true
}

func (s *shard) exists(key string, now int64) (ok bool, expiredKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	it, found := s.items[key]
	if !found {
		return false, ""
	}
	if it.expired(now) {
		s.removeLocked(key, it)
		s.emit(Event{Op: OpDel, Key: key})
		return false, key
	}
	return true, ""
}

func (s *shard) expire(key string, expireAt, now int64) (ok bool, expiredKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	it, found := s.items[key]
	if !found {
		return false, ""
	}
	if it.expired(now) {
		s.removeLocked(key, it)
		s.emit(Event{Op: OpDel, Key: key})
		return false, key
	}
	it.expireAt = expireAt
	if s.expiryAware != nil {
		s.expiryAware.Note(key, expireAt)
	}
	s.emit(Event{Op: OpExpire, Key: key, ExpireAt: expireAt})
	return true, ""
}

func (s *shard) ttl(key string, now int64) (expireAt int64, found bool, expiredKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	it, ok := s.items[key]
	if !ok {
		return 0, false, ""
	}
	if it.expired(now) {
		s.removeLocked(key, it)
		s.emit(Event{Op: OpDel, Key: key})
		return 0, false, key
	}
	return it.expireAt, true, ""
}

func (s *shard) incrBy(key string, delta, now int64) (newVal int64, evicted []string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var base int64
	if it, ok := s.items[key]; ok && !it.expired(now) {
		base, err = strconv.ParseInt(string(it.value), 10, 64)
		if err != nil {
			return 0, nil, ErrNotInteger
		}
	} else if ok {

		s.removeLocked(key, it)
	}
	newVal = base + delta
	nv := []byte(strconv.FormatInt(newVal, 10))
	s.setLocked(key, nv, 0)
	s.emit(Event{Op: OpSet, Key: key, Value: nv})
	evicted = s.enforceLimitsLocked(key)
	for _, k := range evicted {
		s.emit(Event{Op: OpDel, Key: k})
	}
	return newVal, evicted, nil
}

func (s *shard) keys(now int64) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.items))
	for k, it := range s.items {
		if !it.expired(now) {
			out = append(out, k)
		}
	}
	return out
}

func (s *shard) flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.items {
		s.policy.Remove(k)
	}
	s.items = make(map[string]*item)
	s.mem = 0
	s.emit(Event{Op: OpFlush})
}

func (s *shard) sweepExpired(now int64, sample int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var expired []string
	n := 0
	for k, it := range s.items {
		if n >= sample {
			break
		}
		n++
		if it.expired(now) {
			s.removeLocked(k, it)
			s.emit(Event{Op: OpDel, Key: k})
			expired = append(expired, k)
		}
	}
	return expired
}

func (s *shard) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

func (s *shard) export(now int64, fn func(key string, value []byte, expireAt int64)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, it := range s.items {
		if !it.expired(now) {
			fn(k, it.value, it.expireAt)
		}
	}
}

func (s *shard) applySet(key string, value []byte, expireAt int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setLocked(key, value, expireAt)
	s.enforceLimitsLocked(key)
}

func (s *shard) applyDel(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if it, ok := s.items[key]; ok {
		s.removeLocked(key, it)
	}
}

func (s *shard) applyExpire(key string, expireAt int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if it, ok := s.items[key]; ok {
		it.expireAt = expireAt
		if s.expiryAware != nil {
			s.expiryAware.Note(key, expireAt)
		}
	}
}

func (s *shard) flushSilent() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.items {
		s.policy.Remove(k)
	}
	s.items = make(map[string]*item)
	s.mem = 0
}

func (s *shard) memBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mem
}

func (s *shard) removeLocked(key string, it *item) {
	delete(s.items, key)
	s.policy.Remove(key)
	s.mem -= approxSize(key, it.value)
	if s.mem < 0 {
		s.mem = 0
	}
}

func (s *shard) enforceLimitsLocked(protected string) []string {
	var evicted []string
	for s.overCapacityLocked() {
		victim, ok := s.policy.Evict()
		if !ok {
			break
		}
		if victim == protected {

			s.policy.Add(victim)
			break
		}
		if it, ok := s.items[victim]; ok {
			delete(s.items, victim)
			s.mem -= approxSize(victim, it.value)
			if s.mem < 0 {
				s.mem = 0
			}
			evicted = append(evicted, victim)
		}
	}
	return evicted
}

func (s *shard) overCapacityLocked() bool {
	if s.maxEntries > 0 && int64(len(s.items)) > s.maxEntries {
		return true
	}
	if s.maxMemory > 0 && s.mem > s.maxMemory {
		return true
	}
	return false
}
