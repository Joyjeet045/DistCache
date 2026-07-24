package cache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"distcache/internal/eviction"
)

func newTestCache(t *testing.T, cfg Config) (*Cache, *int64) {
	t.Helper()
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var now int64 = 1_000_000
	c.clock = func() int64 { return atomic.LoadInt64(&now) }
	t.Cleanup(func() { _ = c.Close() })
	return c, &now
}

func TestSetGetDeleteExists(t *testing.T) {
	c, _ := newTestCache(t, Config{Shards: 4})
	if _, ok := c.Get("missing"); ok {
		t.Fatal("expected miss")
	}
	c.Set("k", []byte("v"))
	got, ok := c.Get("k")
	if !ok || string(got) != "v" {
		t.Fatalf("got %q ok=%v", got, ok)
	}
	if !c.Exists("k") {
		t.Fatal("expected exists")
	}
	if !c.Delete("k") {
		t.Fatal("expected delete to report existed")
	}
	if c.Exists("k") || c.Delete("k") {
		t.Fatal("key should be gone")
	}
}

func TestBinarySafeValues(t *testing.T) {
	c, _ := newTestCache(t, Config{Shards: 2})
	val := []byte{0x00, 0xff, 0x00, 'a', 0x10}
	c.Set("bin", val)
	got, _ := c.Get("bin")
	if string(got) != string(val) {
		t.Fatalf("binary value corrupted: %v", got)
	}

	val[0] = 0x42
	got2, _ := c.Get("bin")
	if got2[0] != 0x00 {
		t.Fatal("stored value was not defensively copied")
	}
}

func TestTTLExpiry(t *testing.T) {
	c, now := newTestCache(t, Config{Shards: 2})
	c.SetTTL("k", []byte("v"), 100*time.Nanosecond)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("should be live before expiry")
	}
	atomic.AddInt64(now, 200)
	if _, ok := c.Get("k"); ok {
		t.Fatal("should be expired")
	}
	if c.Stats().Expirations == 0 {
		t.Fatal("expiration not counted")
	}
}

func TestTTLReporting(t *testing.T) {
	c, _ := newTestCache(t, Config{Shards: 2})
	c.Set("persist", []byte("v"))
	if _, found, persists := c.TTL("persist"); !found || !persists {
		t.Fatal("persistent key should report persists=true")
	}
	c.SetTTL("temp", []byte("v"), time.Hour)
	rem, found, persists := c.TTL("temp")
	if !found || persists || rem <= 0 {
		t.Fatalf("temp TTL wrong: rem=%v found=%v persists=%v", rem, found, persists)
	}
	if _, found, _ := c.TTL("nope"); found {
		t.Fatal("missing key should not be found")
	}
}

func TestIncrDecr(t *testing.T) {
	c, _ := newTestCache(t, Config{Shards: 2})
	if v, err := c.Incr("n"); err != nil || v != 1 {
		t.Fatalf("incr got %d err %v", v, err)
	}
	if v, _ := c.IncrBy("n", 9); v != 10 {
		t.Fatalf("incrby got %d", v)
	}
	if v, _ := c.Decr("n"); v != 9 {
		t.Fatalf("decr got %d", v)
	}
	if v, _ := c.DecrBy("n", 4); v != 5 {
		t.Fatalf("decrby got %d", v)
	}
	c.Set("s", []byte("notint"))
	if _, err := c.Incr("s"); err != ErrNotInteger {
		t.Fatalf("want ErrNotInteger, got %v", err)
	}
}

func TestBatchOps(t *testing.T) {
	c, _ := newTestCache(t, Config{Shards: 4})
	c.MSet([]KV{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
		{Key: "c", Value: []byte("3")},
	})
	vals, found := c.MGet([]string{"a", "x", "c"})
	if !found[0] || found[1] || !found[2] {
		t.Fatalf("found bitmap wrong: %v", found)
	}
	if string(vals[0]) != "1" || string(vals[2]) != "3" {
		t.Fatalf("values wrong: %q %q", vals[0], vals[2])
	}
}

func TestLRUEviction(t *testing.T) {
	c, _ := newTestCache(t, Config{Shards: 1, MaxEntries: 3, Policy: eviction.LRU})
	c.Set("a", []byte("1"))
	c.Set("b", []byte("2"))
	c.Set("c", []byte("3"))
	c.Get("a")
	c.Set("d", []byte("4"))
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	for _, k := range []string{"a", "c", "d"} {
		if _, ok := c.Get(k); !ok {
			t.Fatalf("%s should still be present", k)
		}
	}
	if c.Stats().Evictions == 0 {
		t.Fatal("eviction not counted")
	}
}

func TestFlushAll(t *testing.T) {
	c, _ := newTestCache(t, Config{Shards: 4})
	c.Set("a", []byte("1"))
	c.Set("b", []byte("2"))
	c.FlushAll()
	if c.Len() != 0 {
		t.Fatalf("expected empty after flush, got %d", c.Len())
	}
}

func TestEventStreamAndReplay(t *testing.T) {
	c, _ := newTestCache(t, Config{Shards: 4})
	var mu sync.Mutex
	var events []Event
	c.Subscribe(func(ev Event) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	})
	c.Set("a", []byte("1"))
	c.Set("b", []byte("2"))
	c.Delete("a")
	c.Set("b", []byte("22"))
	_ = c.Close()

	replay, err := New(Config{Shards: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	mu.Lock()
	captured := append([]Event(nil), events...)
	mu.Unlock()
	if len(captured) < 4 {
		t.Fatalf("expected >=4 events, got %d", len(captured))
	}
	for _, ev := range captured {
		replay.ApplyEvent(ev)
	}
	if _, ok := replay.Get("a"); ok {
		t.Fatal("a should be deleted in replay")
	}
	if v, ok := replay.Get("b"); !ok || string(v) != "22" {
		t.Fatalf("b replay wrong: %q ok=%v", v, ok)
	}
}

func TestHitRatio(t *testing.T) {
	c, _ := newTestCache(t, Config{Shards: 2})
	c.Set("k", []byte("v"))
	c.Get("k")
	c.Get("missing")
	if r := c.Stats().HitRatio(); r != 0.5 {
		t.Fatalf("hit ratio = %v, want 0.5", r)
	}
}

func TestConcurrentAccess(t *testing.T) {
	c, _ := newTestCache(t, Config{Shards: 16})
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 1000 {
				k := fmt.Sprintf("k%d", (g*1000+i)%200)
				c.Set(k, []byte("v"))
				c.Get(k)
				if i%7 == 0 {
					c.Delete(k)
				}
			}
		}(g)
	}
	wg.Wait()

}
