package metrics

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"distcache/internal/cache"
)

func TestHistogramObserve(t *testing.T) {
	h := newHistogram([]float64{0.001, 0.01, 0.1})

	h.Observe(500 * time.Microsecond)
	h.Observe(5 * time.Millisecond)
	h.Observe(50 * time.Millisecond)
	h.Observe(500 * time.Millisecond)

	if h.count != 4 {
		t.Fatalf("count = %d, want 4", h.count)
	}

	if h.buckets[0] != 1 || h.buckets[1] != 2 || h.buckets[2] != 3 {
		t.Fatalf("buckets = %v, want [1 2 3]", h.buckets)
	}

	if h.sum != 555500 {
		t.Fatalf("sum = %d us, want 555500", h.sum)
	}
}

func TestWritePrometheusOutput(t *testing.T) {
	r := New("node-a")
	r.IncCommands()
	r.IncCommands()
	r.IncCommands()
	r.ConnOpened()
	r.ConnOpened()
	r.ConnClosed()
	r.IncPubSub()
	r.AddNetIn(100)
	r.AddNetOut(250)
	r.Observe(2 * time.Millisecond)
	r.SetGauge("replication_lag_seconds", 1.5)
	r.SetNodeState("node-a", "alive")
	r.SetNodeState("node-b", "dead")

	cs := cache.Stats{
		Hits: 8, Misses: 2, Sets: 5, Deletes: 1,
		Evictions: 3, Expirations: 4, Keys: 6, MemoryBytes: 1024,
	}

	var buf bytes.Buffer
	r.WritePrometheus(&buf, cs)
	out := buf.String()

	wantSubstrings := []string{
		"distcache_commands_total 3",
		"distcache_connections_opened_total 2",
		"distcache_connections_active 1",
		"distcache_pubsub_messages_total 1",
		"distcache_net_input_bytes_total 100",
		"distcache_net_output_bytes_total 250",
		"distcache_keyspace_hits_total 8",
		"distcache_keyspace_misses_total 2",
		"distcache_sets_total 5",
		"distcache_deletes_total 1",
		"distcache_evictions_total 3",
		"distcache_expirations_total 4",
		"distcache_keys 6",
		"distcache_memory_bytes 1024",
		"distcache_hit_ratio 0.8",
		"distcache_replication_lag_seconds 1.5",
		`distcache_cluster_node{node="node-a",state="alive"} 1`,
		`distcache_cluster_node{node="node-b",state="dead"} 1`,
		"distcache_command_latency_seconds_bucket",
		`distcache_command_latency_seconds_bucket{le="+Inf"} 1`,
		"distcache_command_latency_seconds_sum",
	}
	for _, sub := range wantSubstrings {
		if !strings.Contains(out, sub) {
			t.Fatalf("metrics output missing %q\n---\n%s", sub, out)
		}
	}

	if !strings.Contains(out, "distcache_command_latency_seconds_sum 0.002") {
		t.Fatalf("latency sum not rendered as 0.002s:\n%s", out)
	}
}

func TestWritePrometheusValidExposition(t *testing.T) {

	r := New("n1")
	r.IncCommands()
	var buf bytes.Buffer
	r.WritePrometheus(&buf, cache.Stats{})
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if fields := strings.Fields(line); len(fields) < 2 {
			t.Fatalf("malformed exposition line: %q", line)
		}
	}
}
