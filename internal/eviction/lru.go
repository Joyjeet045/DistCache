package eviction

import "container/list"

type lru struct {
	ll    *list.List
	nodes map[string]*list.Element
}

func NewLRU() Policy {
	return &lru{ll: list.New(), nodes: make(map[string]*list.Element)}
}

func (p *lru) Add(key string) {
	if el, ok := p.nodes[key]; ok {
		p.ll.MoveToFront(el)
		return
	}
	p.nodes[key] = p.ll.PushFront(key)
}

func (p *lru) Access(key string) {
	if el, ok := p.nodes[key]; ok {
		p.ll.MoveToFront(el)
	}
}

func (p *lru) Remove(key string) {
	if el, ok := p.nodes[key]; ok {
		p.ll.Remove(el)
		delete(p.nodes, key)
	}
}

func (p *lru) Evict() (string, bool) {
	el := p.ll.Back()
	if el == nil {
		return "", false
	}
	key := el.Value.(string)
	p.ll.Remove(el)
	delete(p.nodes, key)
	return key, true
}

func (p *lru) Len() int     { return p.ll.Len() }
func (p *lru) Name() string { return string(LRU) }
