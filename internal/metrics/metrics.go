package metrics

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"distcache/internal/cache"
)

type Histogram struct {
	bounds  []float64
	buckets []uint64
	sum     uint64
	count   uint64
}

var DefaultLatencyBounds = []float64{
	0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
}

func newHistogram(bounds []float64) *Histogram {
	return &Histogram{bounds: bounds, buckets: make([]uint64, len(bounds))}
}

func (h *Histogram) Observe(d time.Duration) {
	sec := d.Seconds()
	for i, b := range h.bounds {
		if sec <= b {
			atomic.AddUint64(&h.buckets[i], 1)
		}
	}
	atomic.AddUint64(&h.sum, uint64(d.Microseconds()))
	atomic.AddUint64(&h.count, 1)
}

type Registry struct {
	CommandsTotal     uint64
	ConnectionsOpened uint64
	ConnectionsActive int64
	PubSubMessages    uint64
	NetInBytes        uint64
	NetOutBytes       uint64

	Latency *Histogram

	mu           sync.RWMutex
	gauges       map[string]float64
	labeledNodes map[string]string
	start        time.Time
	nodeID       string
}

func New(nodeID string) *Registry {
	return &Registry{
		Latency:      newHistogram(DefaultLatencyBounds),
		gauges:       make(map[string]float64),
		labeledNodes: make(map[string]string),
		start:        time.Now(),
		nodeID:       nodeID,
	}
}

func (r *Registry) IncCommands()    { atomic.AddUint64(&r.CommandsTotal, 1) }
func (r *Registry) IncPubSub()      { atomic.AddUint64(&r.PubSubMessages, 1) }
func (r *Registry) AddNetIn(n int)  { atomic.AddUint64(&r.NetInBytes, uint64(n)) }
func (r *Registry) AddNetOut(n int) { atomic.AddUint64(&r.NetOutBytes, uint64(n)) }
func (r *Registry) ConnOpened() {
	atomic.AddUint64(&r.ConnectionsOpened, 1)
	atomic.AddInt64(&r.ConnectionsActive, 1)
}
func (r *Registry) ConnClosed()             { atomic.AddInt64(&r.ConnectionsActive, -1) }
func (r *Registry) Observe(d time.Duration) { r.Latency.Observe(d) }

func (r *Registry) SetGauge(name string, v float64) {
	r.mu.Lock()
	r.gauges[name] = v
	r.mu.Unlock()
}

func (r *Registry) SetNodeState(nodeID, state string) {
	r.mu.Lock()
	r.labeledNodes[nodeID] = state
	r.mu.Unlock()
}

func (r *Registry) WritePrometheus(w io.Writer, cs cache.Stats) {
	up := time.Since(r.start).Seconds()

	writeCounter(w, "distcache_commands_total", "Total commands processed.", atomic.LoadUint64(&r.CommandsTotal))
	writeCounter(w, "distcache_connections_opened_total", "Total client connections opened.", atomic.LoadUint64(&r.ConnectionsOpened))
	writeGauge(w, "distcache_connections_active", "Currently open client connections.", float64(atomic.LoadInt64(&r.ConnectionsActive)))
	writeCounter(w, "distcache_pubsub_messages_total", "Total pub/sub messages published.", atomic.LoadUint64(&r.PubSubMessages))
	writeCounter(w, "distcache_net_input_bytes_total", "Total bytes read from clients.", atomic.LoadUint64(&r.NetInBytes))
	writeCounter(w, "distcache_net_output_bytes_total", "Total bytes written to clients.", atomic.LoadUint64(&r.NetOutBytes))
	writeGauge(w, "distcache_uptime_seconds", "Process uptime in seconds.", up)

	writeCounter(w, "distcache_keyspace_hits_total", "Cache read hits.", cs.Hits)
	writeCounter(w, "distcache_keyspace_misses_total", "Cache read misses.", cs.Misses)
	writeCounter(w, "distcache_sets_total", "Total SET-like writes.", cs.Sets)
	writeCounter(w, "distcache_deletes_total", "Total explicit deletes.", cs.Deletes)
	writeCounter(w, "distcache_evictions_total", "Keys removed by eviction.", cs.Evictions)
	writeCounter(w, "distcache_expirations_total", "Keys removed by TTL expiry.", cs.Expirations)
	writeGauge(w, "distcache_keys", "Live keys stored.", float64(cs.Keys))
	writeGauge(w, "distcache_memory_bytes", "Approximate bytes held.", float64(cs.MemoryBytes))
	writeGauge(w, "distcache_hit_ratio", "Cache hit ratio [0,1].", cs.HitRatio())

	r.mu.RLock()
	names := make([]string, 0, len(r.gauges))
	for n := range r.gauges {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		writeGauge(w, "distcache_"+n, "", r.gauges[n])
	}

	if len(r.labeledNodes) > 0 {
		fmt.Fprintln(w, "# HELP distcache_cluster_node Cluster node state (1=present).")
		fmt.Fprintln(w, "# TYPE distcache_cluster_node gauge")
		ids := make([]string, 0, len(r.labeledNodes))
		for id := range r.labeledNodes {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			fmt.Fprintf(w, "distcache_cluster_node{node=%q,state=%q} 1\n", id, r.labeledNodes[id])
		}
	}
	r.mu.RUnlock()

	writeHistogram(w, "distcache_command_latency_seconds", "Command handling latency.", r.Latency)
}

func writeCounter(w io.Writer, name, help string, v uint64) {
	if help != "" {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
	}
	fmt.Fprintf(w, "%s %d\n", name, v)
}

func writeGauge(w io.Writer, name, help string, v float64) {
	if help != "" {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name)
	}
	fmt.Fprintf(w, "%s %g\n", name, v)
}

func writeHistogram(w io.Writer, name, help string, h *Histogram) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
	for i, b := range h.bounds {
		fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", name, b, atomic.LoadUint64(&h.buckets[i]))
	}
	count := atomic.LoadUint64(&h.count)
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", name, count)
	sumSec := float64(atomic.LoadUint64(&h.sum)) / 1e6
	fmt.Fprintf(w, "%s_sum %g\n", name, sumSec)
	fmt.Fprintf(w, "%s_count %d\n", name, count)
}
