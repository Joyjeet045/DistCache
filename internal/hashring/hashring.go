package hashring

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type Ring struct {
	mu       sync.RWMutex
	replicas int
	ring     []uint32
	owners   map[uint32]string
	nodes    map[string]bool
}

func New(replicas int) *Ring {
	if replicas <= 0 {
		replicas = 100
	}
	return &Ring{
		replicas: replicas,
		owners:   make(map[uint32]string),
		nodes:    make(map[string]bool),
	}
}

func hashKey(s string) uint32 { return crc32.ChecksumIEEE([]byte(s)) }

func vnodeKey(node string, i int) string { return node + "#" + strconv.Itoa(i) }

func (r *Ring) Add(nodes ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, node := range nodes {
		if r.nodes[node] {
			continue
		}
		r.nodes[node] = true
		for i := 0; i < r.replicas; i++ {
			h := hashKey(vnodeKey(node, i))
			r.owners[h] = node
			r.ring = append(r.ring, h)
		}
	}
	sort.Slice(r.ring, func(i, j int) bool { return r.ring[i] < r.ring[j] })
}

func (r *Ring) Remove(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.nodes[node] {
		return
	}
	delete(r.nodes, node)
	for i := 0; i < r.replicas; i++ {
		delete(r.owners, hashKey(vnodeKey(node, i)))
	}
	kept := r.ring[:0]
	for _, h := range r.ring {
		if _, ok := r.owners[h]; ok {
			kept = append(kept, h)
		}
	}
	r.ring = kept
}

func (r *Ring) Get(key string) (node string, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.ring) == 0 {
		return "", false
	}
	return r.owners[r.ring[r.search(hashKey(key))]], true
}

func (r *Ring) GetN(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.ring) == 0 || n <= 0 {
		return nil
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}
	out := make([]string, 0, n)
	seen := make(map[string]bool, n)
	start := r.search(hashKey(key))
	for i := 0; i < len(r.ring) && len(out) < n; i++ {
		node := r.owners[r.ring[(start+i)%len(r.ring)]]
		if !seen[node] {
			seen[node] = true
			out = append(out, node)
		}
	}
	return out
}

func (r *Ring) search(h uint32) int {
	i := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] >= h })
	if i == len(r.ring) {
		return 0
	}
	return i
}

func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}
