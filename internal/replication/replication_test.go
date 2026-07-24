package replication

import (
	"net"
	"testing"
	"time"

	"distcache/internal/cache"
)

func newCache(t *testing.T) *cache.Cache {
	t.Helper()
	c, err := cache.New(cache.Config{Shards: 8})
	if err != nil {
		t.Fatalf("cache: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func startPrimary(t *testing.T, c *cache.Cache) (*Primary, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := NewPrimary(c, ln.Addr().String(), nil)
	go func() { _ = p.Serve(ln) }()
	t.Cleanup(func() { _ = p.Close() })
	return p, ln.Addr().String()
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

func TestReplicationInitialSyncAndLive(t *testing.T) {
	primaryCache := newCache(t)

	primaryCache.Set("a", []byte("1"))
	primaryCache.Set("b", []byte("2"))
	primaryCache.Sync()

	p, addr := startPrimary(t, primaryCache)

	replicaCache := newCache(t)
	r := NewReplica(replicaCache, addr, nil)
	r.Start()
	t.Cleanup(func() { _ = r.Close() })

	waitFor(t, func() bool { return r.Synced() }, "initial sync")
	waitFor(t, func() bool {
		v, ok := replicaCache.Get("a")
		return ok && string(v) == "1"
	}, "snapshot key a")
	if v, ok := replicaCache.Get("b"); !ok || string(v) != "2" {
		t.Fatalf("replica missing b: %q ok=%v", v, ok)
	}

	primaryCache.Set("c", []byte("3"))
	primaryCache.Delete("a")
	waitFor(t, func() bool {
		v, ok := replicaCache.Get("c")
		return ok && string(v) == "3"
	}, "live key c")
	waitFor(t, func() bool {
		_, ok := replicaCache.Get("a")
		return !ok
	}, "deletion of a")

	waitFor(t, func() bool {
		reps := p.Replicas()
		return len(reps) == 1 && reps[0].LastAckSeq >= primaryCache.Seq()-1
	}, "replica ack progress")
}

func TestReplicationFlushPropagates(t *testing.T) {
	primaryCache := newCache(t)
	primaryCache.Set("x", []byte("1"))
	_, addr := startPrimary(t, primaryCache)

	replicaCache := newCache(t)
	r := NewReplica(replicaCache, addr, nil)
	r.Start()
	t.Cleanup(func() { _ = r.Close() })

	waitFor(t, func() bool {
		_, ok := replicaCache.Get("x")
		return ok
	}, "initial key x")

	primaryCache.FlushAll()
	waitFor(t, func() bool { return replicaCache.Len() == 0 }, "flush propagation")
}
