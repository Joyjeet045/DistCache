package txn

import (
	"testing"

	"distcache/internal/cache"
)

func newCache(t *testing.T) *cache.Cache {
	t.Helper()
	c, err := cache.New(cache.Config{Shards: 4})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestExecAppliesQueuedCommands(t *testing.T) {
	c := newCache(t)
	co := NewCoordinator(c)

	tx := co.Begin()
	tx.Multi()
	tx.Queue(func(c *cache.Cache) any { c.Set("a", []byte("1")); return "OK" })
	tx.Queue(func(c *cache.Cache) any { v, _ := c.Incr("counter"); return v })
	results, err := tx.Exec()
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if len(results) != 2 || results[0] != "OK" || results[1] != int64(1) {
		t.Fatalf("unexpected results: %v", results)
	}
	if v, ok := c.Get("a"); !ok || string(v) != "1" {
		t.Fatalf("a = %q ok=%v", v, ok)
	}
}

func TestWatchAbortsOnConcurrentModification(t *testing.T) {
	c := newCache(t)
	co := NewCoordinator(c)

	c.Set("watched", []byte("v0"))

	tx := co.Begin()
	if err := tx.Watch("watched"); err != nil {
		t.Fatal(err)
	}

	c.Set("watched", []byte("v1"))
	c.Sync()

	tx.Multi()
	tx.Queue(func(c *cache.Cache) any { c.Set("watched", []byte("txn")); return nil })
	if _, err := tx.Exec(); err != ErrAborted {
		t.Fatalf("expected ErrAborted, got %v", err)
	}

	if v, _ := c.Get("watched"); string(v) != "v1" {
		t.Fatalf("watched = %q, want v1 (txn should not have run)", v)
	}
}

func TestWatchSucceedsWhenUnchanged(t *testing.T) {
	c := newCache(t)
	co := NewCoordinator(c)
	c.Set("k", []byte("v0"))

	tx := co.Begin()
	tx.Watch("k")
	c.Sync()
	tx.Multi()
	tx.Queue(func(c *cache.Cache) any { c.Set("k", []byte("v1")); return nil })
	if _, err := tx.Exec(); err != nil {
		t.Fatalf("exec should succeed: %v", err)
	}
	if v, _ := c.Get("k"); string(v) != "v1" {
		t.Fatalf("k = %q, want v1", v)
	}
}

func TestDiscardReleasesWatches(t *testing.T) {
	c := newCache(t)
	co := NewCoordinator(c)
	c.Set("k", []byte("v0"))

	tx := co.Begin()
	tx.Watch("k")
	tx.Discard()

	co.mu.Lock()
	_, tracked := co.watchers["k"]
	co.mu.Unlock()
	if tracked {
		t.Fatal("watch should be released after discard")
	}
}

func TestQueueRequiresMulti(t *testing.T) {
	c := newCache(t)
	co := NewCoordinator(c)
	tx := co.Begin()
	if err := tx.Queue(func(*cache.Cache) any { return nil }); err != ErrNoMulti {
		t.Fatalf("expected ErrNoMulti, got %v", err)
	}
	if _, err := tx.Exec(); err != ErrNoMulti {
		t.Fatalf("expected ErrNoMulti on exec, got %v", err)
	}
}
