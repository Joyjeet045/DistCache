package cluster

import (
	"testing"
	"time"
)

func TestRegistryOwnershipAndReplicas(t *testing.T) {
	r := New(Config{
		SelfID:            "n1",
		SelfAddr:          "127.0.0.1:6380",
		ReplicationFactor: 2,
		VNodes:            64,
	})
	r.Add("n2", "127.0.0.1:6381")
	r.Add("n3", "127.0.0.1:6382")

	if r.Size() != 3 {
		t.Fatalf("size = %d, want 3", r.Size())
	}

	seen := map[string]bool{}
	for _, k := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		owner, ok := r.Owner(k)
		if !ok {
			t.Fatalf("no owner for %q", k)
		}
		if owner.ID != "n1" && owner.ID != "n2" && owner.ID != "n3" {
			t.Fatalf("unexpected owner %q for %q", owner.ID, k)
		}
		seen[owner.ID] = true

		reps := r.Replicas(k)
		if len(reps) != 2 {
			t.Fatalf("replicas for %q = %d, want 2", k, len(reps))
		}
		if reps[0].ID != owner.ID {
			t.Fatalf("first replica %q != owner %q", reps[0].ID, owner.ID)
		}
		if reps[0].ID == reps[1].ID {
			t.Fatalf("replica set has duplicate node for %q", k)
		}
	}

	if len(seen) < 2 {
		t.Fatalf("keys concentrated on %d node(s)", len(seen))
	}
}

func TestRegistryLiveness(t *testing.T) {
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	r := New(Config{
		SelfID:    "n1",
		SelfAddr:  "a",
		FailAfter: 5 * time.Second,
		Clock:     clock,
	})
	r.Add("n2", "b")

	if !nodeByID(r.Nodes(), "n2").Alive {
		t.Fatalf("n2 should be alive right after join")
	}

	now = now.Add(10 * time.Second)
	if nodeByID(r.Nodes(), "n2").Alive {
		t.Fatalf("n2 should be marked dead")
	}

	r.Heartbeat("n2")
	if !nodeByID(r.Nodes(), "n2").Alive {
		t.Fatalf("n2 should be alive after heartbeat")
	}

	if !r.Self().Alive {
		t.Fatalf("self should always be alive")
	}
}

func TestRegistryRemove(t *testing.T) {
	r := New(Config{SelfID: "n1", SelfAddr: "a", ReplicationFactor: 1})
	r.Add("n2", "b")
	r.Remove("n2")
	if r.Size() != 1 {
		t.Fatalf("size = %d, want 1 after remove", r.Size())
	}
	for _, k := range []string{"x", "y", "z", "w"} {
		owner, _ := r.Owner(k)
		if owner.ID != "n1" {
			t.Fatalf("owner of %q = %q, want n1 (only node)", k, owner.ID)
		}
	}
}

func nodeByID(nodes []Node, id string) Node {
	for _, n := range nodes {
		if n.ID == id {
			return n
		}
	}
	return Node{}
}
