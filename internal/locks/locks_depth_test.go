package locks

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestAcquireBlockingWaitsThenSucceeds(t *testing.T) {
	m := New()

	tok, ok := m.AcquireBlocking("r", "a", time.Minute, time.Second, 5*time.Millisecond)
	if !ok || tok == "" {
		t.Fatalf("AcquireBlocking on free lock = %q ok=%v", tok, ok)
	}

	start := time.Now()
	if _, ok := m.AcquireBlocking("r", "b", time.Minute, 40*time.Millisecond, 5*time.Millisecond); ok {
		t.Fatal("AcquireBlocking should fail while held by another owner")
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("returned too early: %v", elapsed)
	}

	if !m.Release("r", tok) {
		t.Fatal("release failed")
	}
	if _, ok := m.AcquireBlocking("r", "b", time.Minute, time.Second, 5*time.Millisecond); !ok {
		t.Fatal("AcquireBlocking should succeed after release")
	}
}

func TestAcquireBlockingReclaimsExpiredLease(t *testing.T) {
	m := New()
	var now int64 = 1_000
	m.clock = func() int64 { return atomic.LoadInt64(&now) }

	if _, ok := m.Acquire("r", "crashed", 100); !ok {
		t.Fatal("initial acquire failed")
	}
	atomic.StoreInt64(&now, 2_000)

	if _, ok := m.AcquireBlocking("r", "next", 100, time.Second, 5*time.Millisecond); !ok {
		t.Fatal("expired lease should be reclaimable via AcquireBlocking")
	}
}
