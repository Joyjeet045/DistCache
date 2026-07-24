package persistence

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestAOFSyncPoliciesRecover(t *testing.T) {
	for _, policy := range []SyncPolicy{SyncAlways, SyncEverySec} {
		t.Run(string(policy), func(t *testing.T) {
			dir := t.TempDir()

			c1 := newCache(t)
			m1, err := Open(Config{Dir: dir, Sync: policy})
			if err != nil {
				t.Fatal(err)
			}
			if err := m1.Recover(c1); err != nil {
				t.Fatal(err)
			}
			c1.Set("k", []byte("v"))
			c1.Set("n", []byte("42"))
			c1.Close()
			if err := m1.Close(); err != nil {
				t.Fatal(err)
			}

			c2 := newCache(t)
			defer c2.Close()
			m2, err := Open(Config{Dir: dir, Sync: policy})
			if err != nil {
				t.Fatal(err)
			}
			if err := m2.Recover(c2); err != nil {
				t.Fatal(err)
			}
			defer m2.Close()
			if v, ok := c2.Get("k"); !ok || string(v) != "v" {
				t.Fatalf("k = %q ok=%v, want v", v, ok)
			}
		})
	}
}

func TestAOFSyncNoFlushesOnClose(t *testing.T) {
	dir := t.TempDir()

	c1 := newCache(t)
	m1, err := Open(Config{Dir: dir, Sync: SyncNo})
	if err != nil {
		t.Fatal(err)
	}
	if err := m1.Recover(c1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		c1.Set("k"+strconv.Itoa(i), []byte("v"))
	}
	c1.Close()

	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	c2 := newCache(t)
	defer c2.Close()
	m2, err := Open(Config{Dir: dir, Sync: SyncNo})
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.Recover(c2); err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	if got := c2.Len(); got != 20 {
		t.Fatalf("recovered %d keys, want 20", got)
	}
}

func TestSnapshotShrinksAOF(t *testing.T) {
	dir := t.TempDir()

	c := newCache(t)
	defer c.Close()
	m, err := Open(Config{Dir: dir, Sync: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Recover(c); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 200; i++ {
		c.Set("k"+strconv.Itoa(i), []byte("value-"+strconv.Itoa(i)))
	}
	c.Sync()

	aofPath := filepath.Join(dir, aofFile)
	before, err := os.Stat(aofPath)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() == 0 {
		t.Fatal("expected a non-empty AOF before snapshot")
	}

	if err := m.Snapshot(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(aofPath)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size() {
		t.Fatalf("AOF did not shrink: before=%d after=%d", before.Size(), after.Size())
	}

	snapInfo, err := os.Stat(filepath.Join(dir, snapshotFile))
	if err != nil || snapInfo.Size() == 0 {
		t.Fatalf("snapshot missing or empty: %v", err)
	}
}
