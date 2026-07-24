package eviction

import "container/list"

type fifo struct {
	ll    *list.List
	nodes map[string]*list.Element
}

func NewFIFO() Policy {
	return &fifo{ll: list.New(), nodes: make(map[string]*list.Element)}
}

func (p *fifo) Add(key string) {
	if _, ok := p.nodes[key]; ok {
		return
	}
	p.nodes[key] = p.ll.PushBack(key)
}

func (p *fifo) Access(string) {}

func (p *fifo) Remove(key string) {
	if el, ok := p.nodes[key]; ok {
		p.ll.Remove(el)
		delete(p.nodes, key)
	}
}

func (p *fifo) Evict() (string, bool) {
	el := p.ll.Front()
	if el == nil {
		return "", false
	}
	key := el.Value.(string)
	p.ll.Remove(el)
	delete(p.nodes, key)
	return key, true
}

func (p *fifo) Len() int     { return p.ll.Len() }
func (p *fifo) Name() string { return string(FIFO) }
