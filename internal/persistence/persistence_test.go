package persistence

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"distcache/internal/cache"
)

func newCache(t *testing.T) *cache.Cache {
	t.Helper()
	c, err := cache.New(cache.Config{Shards: 8})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	return c
}

func TestAOFRecovery(t *testing.T) {
	dir := t.TempDir()

	c1 := newCache(t)
	m1, err := Open(Config{Dir: dir, Sync: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := m1.Recover(c1); err != nil {
		t.Fatal(err)
	}
	c1.Set("a", []byte("1"))
	c1.Set("b", []byte("2"))
	c1.Set("c", []byte("3"))
	c1.Delete("b")
	c1.Set("a", []byte("11"))

	c1.Close()
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}
	if m1.AppendErrors() != 0 {
		t.Fatalf("append errors: %d", m1.AppendErrors())
	}

	c2 := newCache(t)
	defer c2.Close()
	m2, err := Open(Config{Dir: dir, Sync: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.Recover(c2); err != nil {
		t.Fatal(err)
	}
	defer m2.Close()

	if v, ok := c2.Get("a"); !ok || string(v) != "11" {
		t.Fatalf("a = %q ok=%v, want 11", v, ok)
	}
	if _, ok := c2.Get("b"); ok {
		t.Fatal("b should be deleted after recovery")
	}
	if v, ok := c2.Get("c"); !ok || string(v) != "3" {
		t.Fatalf("c = %q ok=%v, want 3", v, ok)
	}
}

func TestSnapshotAndCompactionRecovery(t *testing.T) {
	dir := t.TempDir()

	c1 := newCache(t)
	m1, err := Open(Config{Dir: dir, Sync: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := m1.Recover(c1); err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		c1.Set("k"+strconv.Itoa(i), []byte("v"+strconv.Itoa(i)))
	}

	if err := m1.Snapshot(); err != nil {
		t.Fatal(err)
	}

	c1.Set("k0", []byte("updated"))
	c1.Delete("k50")

	c1.Close()
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	aofInfo, err := os.Stat(filepath.Join(dir, aofFile))
	if err != nil {
		t.Fatal(err)
	}
	if aofInfo.Size() > 1024 {
		t.Fatalf("compacted AOF unexpectedly large: %d bytes", aofInfo.Size())
	}

	c2 := newCache(t)
	defer c2.Close()
	m2, err := Open(Config{Dir: dir, Sync: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.Recover(c2); err != nil {
		t.Fatal(err)
	}
	defer m2.Close()

	if v, ok := c2.Get("k0"); !ok || string(v) != "updated" {
		t.Fatalf("k0 = %q ok=%v, want updated", v, ok)
	}
	if _, ok := c2.Get("k50"); ok {
		t.Fatal("k50 should be deleted")
	}
	if v, ok := c2.Get("k99"); !ok || string(v) != "v99" {
		t.Fatalf("k99 = %q ok=%v, want v99", v, ok)
	}
	if got := c2.Len(); got != 99 {
		t.Fatalf("recovered %d keys, want 99", got)
	}
}

func TestTornTrailingRecordIsIgnored(t *testing.T) {
	dir := t.TempDir()

	c1 := newCache(t)
	m1, err := Open(Config{Dir: dir, Sync: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := m1.Recover(c1); err != nil {
		t.Fatal(err)
	}
	c1.Set("good", []byte("value"))
	c1.Close()
	m1.Close()

	aofPath := filepath.Join(dir, aofFile)
	partial := encodeRecord(cache.Event{Op: cache.OpSet, Seq: 999, Key: "torn", Value: []byte("half")})
	f, err := os.OpenFile(aofPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(partial[:len(partial)-3]); err != nil {
		t.Fatal(err)
	}
	f.Close()

	c2 := newCache(t)
	defer c2.Close()
	m2, err := Open(Config{Dir: dir, Sync: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.Recover(c2); err != nil {
		t.Fatalf("recover should tolerate torn tail: %v", err)
	}
	defer m2.Close()

	if v, ok := c2.Get("good"); !ok || string(v) != "value" {
		t.Fatalf("good = %q ok=%v, want value", v, ok)
	}
	if _, ok := c2.Get("torn"); ok {
		t.Fatal("torn record must not be applied")
	}
}
