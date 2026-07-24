package cluster

import (
	"sort"
	"sync"
	"time"

	"distcache/internal/hashring"
)

type Node struct {
	ID       string
	Addr     string
	LastSeen time.Time
	Alive    bool
	Self     bool
}

type Registry struct {
	mu        sync.RWMutex
	selfID    string
	selfAddr  string
	rf        int
	failAfter time.Duration
	clock     func() time.Time

	nodes map[string]*nodeState
	ring  *hashring.Ring
}

type nodeState struct {
	id       string
	addr     string
	lastSeen time.Time
}

type Config struct {
	SelfID   string
	SelfAddr string

	ReplicationFactor int

	FailAfter time.Duration

	VNodes int

	Clock func() time.Time
}

func New(cfg Config) *Registry {
	if cfg.ReplicationFactor < 1 {
		cfg.ReplicationFactor = 1
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	r := &Registry{
		selfID:    cfg.SelfID,
		selfAddr:  cfg.SelfAddr,
		rf:        cfg.ReplicationFactor,
		failAfter: cfg.FailAfter,
		clock:     cfg.Clock,
		nodes:     make(map[string]*nodeState),
		ring:      hashring.New(cfg.VNodes),
	}
	r.addLocked(cfg.SelfID, cfg.SelfAddr)
	return r
}

func (r *Registry) addLocked(id, addr string) {
	if _, ok := r.nodes[id]; !ok {
		r.ring.Add(id)
	}
	r.nodes[id] = &nodeState{id: id, addr: addr, lastSeen: r.clock()}
}

func (r *Registry) Add(id, addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addLocked(id, addr)
}

func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[id]; ok {
		delete(r.nodes, id)
		r.ring.Remove(id)
	}
}

func (r *Registry) Heartbeat(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.nodes[id]; ok {
		n.lastSeen = r.clock()
	}
}

func (r *Registry) alive(n *nodeState, now time.Time) bool {
	if r.failAfter <= 0 || n.id == r.selfID {
		return true
	}
	return now.Sub(n.lastSeen) <= r.failAfter
}

func (r *Registry) Owner(key string) (Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.ring.Get(key)
	if !ok {
		return Node{}, false
	}
	return r.nodeView(id), true
}

func (r *Registry) Replicas(key string) []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := r.ring.GetN(key, r.rf)
	out := make([]Node, 0, len(ids))
	for _, id := range ids {
		out = append(out, r.nodeView(id))
	}
	return out
}

func (r *Registry) IsLocal(key string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.ring.Get(key)
	return ok && id == r.selfID
}

func (r *Registry) nodeView(id string) Node {
	now := r.clock()
	n := r.nodes[id]
	if n == nil {
		return Node{ID: id}
	}
	return Node{
		ID:       n.id,
		Addr:     n.addr,
		LastSeen: n.lastSeen,
		Alive:    r.alive(n, now),
		Self:     n.id == r.selfID,
	}
}

func (r *Registry) Nodes() []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Node, 0, len(r.nodes))
	for id := range r.nodes {
		out = append(out, r.nodeView(id))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *Registry) Self() Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nodeView(r.selfID)
}

func (r *Registry) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}
