package cache

import (
	"fmt"
	"testing"
	"time"

	"distcache/internal/eviction"
)

func TestEntryCapEviction(t *testing.T) {
	c, _ := newTestCache(t, Config{Shards: 1, MaxEntries: 5, Policy: eviction.FIFO})
	for i := 0; i < 50; i++ {
		c.Set(fmt.Sprintf("k%d", i), []byte("v"))
	}
	if got := c.Len(); got != 5 {
		t.Fatalf("Len = %d, want 5 (cap enforced)", got)
	}
	if ev := c.Stats().Evictions; ev != 45 {
		t.Fatalf("Evictions = %d, want 45", ev)
	}

	if _, ok := c.Get("k0"); ok {
		t.Fatal("k0 should have been evicted")
	}
	if _, ok := c.Get("k49"); !ok {
		t.Fatal("k49 (most recent) should survive")
	}
}

func TestMemoryCapEviction(t *testing.T) {
	c, _ := newTestCache(t, Config{Shards: 1, MaxMemory: 1024, Policy: eviction.LRU})
	big := make([]byte, 256)
	for i := 0; i < 50; i++ {
		c.Set(fmt.Sprintf("k%d", i), big)
	}
	st := c.Stats()
	if st.Evictions == 0 {
		t.Fatal("expected evictions under a memory cap")
	}

	if st.MemoryBytes > 2048 {
		t.Fatalf("MemoryBytes = %d, want <= 2048", st.MemoryBytes)
	}
	if c.Len() >= 50 {
		t.Fatalf("Len = %d, expected far fewer than 50 under cap", c.Len())
	}
}

func TestActiveExpirySweep(t *testing.T) {

	c, err := New(Config{Shards: 2, ActiveExpiry: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	for i := 0; i < 10; i++ {
		c.SetTTL(fmt.Sprintf("k%d", i), []byte("v"), 20*time.Millisecond)
	}

	deadline := time.Now().Add(2 * time.Second)
	for c.Len() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("active expiry did not sweep keys; Len=%d", c.Len())
		}
		time.Sleep(2 * time.Millisecond)
	}
	if c.Stats().Expirations == 0 {
		t.Fatal("active sweep did not count expirations")
	}
}

func TestKeysAndIncrErrors(t *testing.T) {
	c, _ := newTestCache(t, Config{Shards: 4})
	c.Set("alpha", []byte("1"))
	c.Set("beta", []byte("2"))
	c.Set("gamma", []byte("3"))

	keys := c.Keys()
	if len(keys) != 3 {
		t.Fatalf("Keys len = %d, want 3", len(keys))
	}
	set := map[string]bool{}
	for _, k := range keys {
		set[k] = true
	}
	for _, want := range []string{"alpha", "beta", "gamma"} {
		if !set[want] {
			t.Fatalf("Keys missing %q: %v", want, keys)
		}
	}

	c.Set("str", []byte("notanumber"))
	if _, err := c.IncrBy("str", 5); err != ErrNotInteger {
		t.Fatalf("IncrBy err = %v, want ErrNotInteger", err)
	}
	if v, _ := c.Get("str"); string(v) != "notanumber" {
		t.Fatalf("value mutated on failed IncrBy: %q", v)
	}

	vals, found := c.MGet([]string{"x", "y"})
	if len(vals) != 2 || found[0] || found[1] {
		t.Fatalf("MGet all-missing: vals=%v found=%v", vals, found)
	}
}

func TestExportApplyEventTTL(t *testing.T) {
	src, _ := newTestCache(t, Config{Shards: 2})
	src.Set("persist", []byte("p"))
	src.SetTTL("temp", []byte("t"), time.Hour)

	exported := map[string]int64{}
	vals := map[string]string{}
	src.Export(func(k string, v []byte, expireAt int64) {
		exported[k] = expireAt
		vals[k] = string(v)
	})
	if len(exported) != 2 {
		t.Fatalf("exported %d keys, want 2", len(exported))
	}
	if exported["persist"] != 0 {
		t.Fatalf("persist expireAt = %d, want 0", exported["persist"])
	}
	if exported["temp"] <= 0 {
		t.Fatalf("temp expireAt = %d, want > 0", exported["temp"])
	}

	dst, _ := newTestCache(t, Config{Shards: 2})
	for k, exp := range exported {
		dst.ApplyEvent(Event{Op: OpSet, Key: k, Value: []byte(vals[k]), ExpireAt: exp})
	}
	if _, found, persists := dst.TTL("persist"); !found || !persists {
		t.Fatal("persist should replay as persistent")
	}
	rem, found, persists := dst.TTL("temp")
	if !found || persists || rem <= 0 {
		t.Fatalf("temp replay TTL wrong: rem=%v found=%v persists=%v", rem, found, persists)
	}
}

func TestFlushStatsAndHitRatioZero(t *testing.T) {
	c, _ := newTestCache(t, Config{Shards: 4})

	if r := c.Stats().HitRatio(); r != 0 {
		t.Fatalf("HitRatio with no reads = %v, want 0", r)
	}

	c.Set("a", []byte("1"))
	c.Set("b", []byte("2"))
	c.Get("a")
	if c.Stats().Keys != 2 {
		t.Fatalf("Keys before flush = %d, want 2", c.Stats().Keys)
	}

	c.FlushAll()
	st := c.Stats()
	if st.Keys != 0 || st.MemoryBytes != 0 {
		t.Fatalf("after flush Keys=%d Mem=%d, want 0/0", st.Keys, st.MemoryBytes)
	}
	if c.Len() != 0 {
		t.Fatalf("Len after flush = %d, want 0", c.Len())
	}
}
