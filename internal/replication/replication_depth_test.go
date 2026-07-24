package replication

import (
	"testing"
)

func TestSecondReplicaFullSnapshot(t *testing.T) {
	pc := newCache(t)
	pc.Set("a", []byte("1"))
	pc.Set("b", []byte("2"))
	pc.Sync()

	p, addr := startPrimary(t, pc)

	rc1 := newCache(t)
	r1 := NewReplica(rc1, addr, nil)
	r1.Start()
	t.Cleanup(func() { _ = r1.Close() })
	waitFor(t, r1.Synced, "first replica sync")

	pc.Set("c", []byte("3"))
	waitFor(t, func() bool {
		v, ok := rc1.Get("c")
		return ok && string(v) == "3"
	}, "first replica live key c")

	rc2 := newCache(t)
	r2 := NewReplica(rc2, addr, nil)
	r2.Start()
	t.Cleanup(func() { _ = r2.Close() })
	waitFor(t, r2.Synced, "second replica sync")

	for _, kv := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		waitFor(t, func() bool {
			v, ok := rc2.Get(kv.k)
			return ok && string(v) == kv.v
		}, "second replica snapshot key "+kv.k)
	}
	waitFor(t, func() bool { return p.NumReplicas() == 2 }, "two replicas registered")
}

func TestReplicaDisconnectAndResync(t *testing.T) {
	pc := newCache(t)
	p, addr := startPrimary(t, pc)

	rc1 := newCache(t)
	r1 := NewReplica(rc1, addr, nil)
	r1.Start()
	waitFor(t, func() bool { return r1.Synced() && p.NumReplicas() == 1 }, "initial replica")

	_ = r1.Close()
	waitFor(t, func() bool { return p.NumReplicas() == 0 }, "replica removed on disconnect")

	pc.Set("late", []byte("v"))
	pc.Sync()

	rc2 := newCache(t)
	r2 := NewReplica(rc2, addr, nil)
	r2.Start()
	t.Cleanup(func() { _ = r2.Close() })
	waitFor(t, r2.Synced, "resync")
	waitFor(t, func() bool {
		v, ok := rc2.Get("late")
		return ok && string(v) == "v"
	}, "resynced late key")
	waitFor(t, func() bool { return p.NumReplicas() == 1 }, "reconnected replica registered")
}

func TestPrimaryReplicaStateTracking(t *testing.T) {
	pc := newCache(t)
	p, addr := startPrimary(t, pc)

	rc := newCache(t)
	r := NewReplica(rc, addr, nil)
	r.Start()
	t.Cleanup(func() { _ = r.Close() })
	waitFor(t, r.Synced, "sync")

	for i := 0; i < 20; i++ {
		pc.Set("k"+string(rune('a'+i%26)), []byte("v"))
	}
	pc.Sync()

	waitFor(t, func() bool { return p.NumReplicas() == 1 }, "one replica")
	waitFor(t, func() bool {
		reps := p.Replicas()
		if len(reps) != 1 {
			return false
		}
		st := reps[0]
		return st.Addr != "" && !st.ConnectedAt.IsZero() &&
			st.LastSentSeq >= pc.Seq() && st.LastAckSeq >= pc.Seq()
	}, "replica state fully caught up")

	waitFor(t, func() bool { return r.LastApplied() >= pc.Seq() }, "replica LastApplied")
	if r.PrimaryAddr() != addr {
		t.Fatalf("PrimaryAddr = %q, want %q", r.PrimaryAddr(), addr)
	}
}
