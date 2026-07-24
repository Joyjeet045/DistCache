package eviction

import (
	"math/rand"
	"time"
)

type random struct {
	keys []string
	idx  map[string]int
	rng  *rand.Rand
}

func NewRandom() Policy {
	return &random{
		idx: make(map[string]int),
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (p *random) Add(key string) {
	if _, ok := p.idx[key]; ok {
		return
	}
	p.idx[key] = len(p.keys)
	p.keys = append(p.keys, key)
}

func (p *random) Access(string) {}

func (p *random) Remove(key string) {
	i, ok := p.idx[key]
	if !ok {
		return
	}
	last := len(p.keys) - 1
	if i != last {
		moved := p.keys[last]
		p.keys[i] = moved
		p.idx[moved] = i
	}
	p.keys = p.keys[:last]
	delete(p.idx, key)
}

func (p *random) Evict() (string, bool) {
	if len(p.keys) == 0 {
		return "", false
	}
	i := p.rng.Intn(len(p.keys))
	key := p.keys[i]
	p.Remove(key)
	return key, true
}

func (p *random) Len() int     { return len(p.keys) }
func (p *random) Name() string { return string(Random) }
