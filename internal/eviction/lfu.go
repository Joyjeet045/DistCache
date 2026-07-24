package eviction

import "container/list"

type lfuNode struct {
	key  string
	freq int
	el   *list.Element
}

type lfu struct {
	nodes       map[string]*lfuNode
	freqBuckets map[int]*list.List
	minFreq     int
}

func NewLFU() Policy {
	return &lfu{
		nodes:       make(map[string]*lfuNode),
		freqBuckets: make(map[int]*list.List),
	}
}

func (p *lfu) bucket(freq int) *list.List {
	b := p.freqBuckets[freq]
	if b == nil {
		b = list.New()
		p.freqBuckets[freq] = b
	}
	return b
}

func (p *lfu) Add(key string) {
	if _, ok := p.nodes[key]; ok {
		p.Access(key)
		return
	}
	n := &lfuNode{key: key, freq: 1}
	n.el = p.bucket(1).PushBack(n)
	p.nodes[key] = n
	p.minFreq = 1
}

func (p *lfu) Access(key string) {
	n, ok := p.nodes[key]
	if !ok {
		return
	}
	old := n.freq
	p.freqBuckets[old].Remove(n.el)
	if p.freqBuckets[old].Len() == 0 {
		delete(p.freqBuckets, old)
		if p.minFreq == old {
			p.minFreq = old + 1
		}
	}
	n.freq++
	n.el = p.bucket(n.freq).PushBack(n)
}

func (p *lfu) Remove(key string) {
	n, ok := p.nodes[key]
	if !ok {
		return
	}
	p.freqBuckets[n.freq].Remove(n.el)
	if p.freqBuckets[n.freq].Len() == 0 {
		delete(p.freqBuckets, n.freq)
	}
	delete(p.nodes, key)
}

func (p *lfu) Evict() (string, bool) {
	if len(p.nodes) == 0 {
		return "", false
	}
	b := p.freqBuckets[p.minFreq]
	if b == nil || b.Len() == 0 {
		p.recomputeMinFreq()
		b = p.freqBuckets[p.minFreq]
		if b == nil || b.Len() == 0 {
			return "", false
		}
	}
	el := b.Front()
	n := el.Value.(*lfuNode)
	b.Remove(el)
	if b.Len() == 0 {
		delete(p.freqBuckets, n.freq)
	}
	delete(p.nodes, n.key)
	return n.key, true
}

func (p *lfu) recomputeMinFreq() {
	min := -1
	for f := range p.freqBuckets {
		if min == -1 || f < min {
			min = f
		}
	}
	if min == -1 {
		min = 0
	}
	p.minFreq = min
}

func (p *lfu) Len() int     { return len(p.nodes) }
func (p *lfu) Name() string { return string(LFU) }
