package eviction

import (
	"container/heap"
	"math"
)

type ttl struct {
	eff map[string]int64
	h   ttlHeap
}

func NewTTL() Policy {
	t := &ttl{eff: make(map[string]int64)}
	heap.Init(&t.h)
	return t
}

const ttlNever = int64(math.MaxInt64)

func (p *ttl) Add(key string) {
	if _, ok := p.eff[key]; ok {
		return
	}
	p.eff[key] = ttlNever
	heap.Push(&p.h, ttlItem{key: key, expireAt: ttlNever})
}

func (p *ttl) Note(key string, expireAt int64) {
	e := expireAt
	if e == 0 {
		e = ttlNever
	}
	p.eff[key] = e
	heap.Push(&p.h, ttlItem{key: key, expireAt: e})
}

func (p *ttl) Access(string) {}

func (p *ttl) Remove(key string) { delete(p.eff, key) }

func (p *ttl) Evict() (string, bool) {
	for p.h.Len() > 0 {
		it := heap.Pop(&p.h).(ttlItem)
		cur, ok := p.eff[it.key]
		if !ok || cur != it.expireAt {
			continue
		}
		delete(p.eff, it.key)
		return it.key, true
	}
	return "", false
}

func (p *ttl) Len() int     { return len(p.eff) }
func (p *ttl) Name() string { return string(TTL) }

type ttlItem struct {
	key      string
	expireAt int64
}

type ttlHeap []ttlItem

func (h ttlHeap) Len() int           { return len(h) }
func (h ttlHeap) Less(i, j int) bool { return h[i].expireAt < h[j].expireAt }
func (h ttlHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *ttlHeap) Push(x any)        { *h = append(*h, x.(ttlItem)) }
func (h *ttlHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}
