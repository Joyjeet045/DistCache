package eviction

import (
	"fmt"
	"strings"
)

type Policy interface {
	Add(key string)

	Access(key string)

	Remove(key string)

	Evict() (string, bool)

	Len() int

	Name() string
}

type ExpiryAware interface {
	Note(key string, expireAt int64)
}

type Kind string

const (
	NoEviction Kind = "noeviction"
	LRU        Kind = "lru"
	LFU        Kind = "lfu"
	FIFO       Kind = "fifo"
	Random     Kind = "random"
	TTL        Kind = "ttl"
)

func New(kind Kind) (Policy, error) {
	switch Kind(strings.ToLower(string(kind))) {
	case NoEviction, "":
		return NewNoEviction(), nil
	case LRU:
		return NewLRU(), nil
	case LFU:
		return NewLFU(), nil
	case FIFO:
		return NewFIFO(), nil
	case Random:
		return NewRandom(), nil
	case TTL:
		return NewTTL(), nil
	default:
		return NewNoEviction(), fmt.Errorf("eviction: unknown policy %q", kind)
	}
}

type noEviction struct{ n int }

func NewNoEviction() Policy         { return &noEviction{} }
func (p *noEviction) Add(string)    { p.n++ }
func (p *noEviction) Access(string) {}
func (p *noEviction) Remove(string) {
	if p.n > 0 {
		p.n--
	}
}
func (p *noEviction) Evict() (string, bool) { return "", false }
func (p *noEviction) Len() int              { return p.n }
func (p *noEviction) Name() string          { return string(NoEviction) }
