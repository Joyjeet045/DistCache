package locks

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestAcquireReleaseCycle(t *testing.T) {
	m := New()
	tok, ok := m.Acquire("resource", "clientA", time.Minute)
	if !ok || tok == "" {
		t.Fatal("clientA should acquire the free lock")
	}
	if _, ok := m.Acquire("resource", "clientB", time.Minute); ok {
		t.Fatal("clientB must not acquire a held lock")
	}
	if !m.IsLocked("resource") {
		t.Fatal("resource should be locked")
	}
	if !m.Release("resource", tok) {
		t.Fatal("clientA should release with a valid token")
	}
	if _, ok := m.Acquire("resource", "clientB", time.Minute); !ok {
		t.Fatal("clientB should acquire after release")
	}
}

func TestReleaseWithWrongTokenFails(t *testing.T) {
	m := New()
	tok, _ := m.Acquire("r", "a", time.Minute)
	if m.Release("r", "not-the-token") {
		t.Fatal("release must fail with a stale/incorrect token")
	}
	if !m.Release("r", tok) {
		t.Fatal("release must succeed with the correct token")
	}
}

func TestLeaseExpiryReclaim(t *testing.T) {
	m := New()
	var now int64 = 1_000
	m.clock = func() int64 { return atomic.LoadInt64(&now) }

	tok, ok := m.Acquire("r", "crashed", 100)
	if !ok {
		t.Fatal("should acquire")
	}
	atomic.StoreInt64(&now, 1_200)
	if m.IsLocked("r") {
		t.Fatal("expired lease should not count as locked")
	}

	if _, ok := m.Acquire("r", "recovered", 100); !ok {
		t.Fatal("expired lock should be reclaimable")
	}

	if m.Renew("r", tok, 100) {
		t.Fatal("stale token must not renew")
	}
	if m.Release("r", tok) {
		t.Fatal("stale token must not release")
	}
}

func TestRenewExtendsLease(t *testing.T) {
	m := New()
	var now int64 = 1_000
	m.clock = func() int64 { return atomic.LoadInt64(&now) }
	tok, _ := m.Acquire("r", "a", 100)
	atomic.StoreInt64(&now, 1_050)
	if !m.Renew("r", tok, 100) {
		t.Fatal("renew should succeed within the lease")
	}
	atomic.StoreInt64(&now, 1_120)
	if !m.IsLocked("r") {
		t.Fatal("renewed lease should still be held")
	}
}

func TestReentrantAcquireBySameOwner(t *testing.T) {
	m := New()
	t1, _ := m.Acquire("r", "a", time.Minute)
	t2, ok := m.Acquire("r", "a", time.Minute)
	if !ok || t2 == "" {
		t.Fatal("same owner should refresh its own lock")
	}

	if m.Release("r", t1) {
		t.Fatal("superseded token should not release")
	}
	if !m.Release("r", t2) {
		t.Fatal("current token should release")
	}
}
