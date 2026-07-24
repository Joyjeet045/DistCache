package hashring

import (
	"fmt"
	"testing"
)

func TestGetIsDeterministicAndCoversNodes(t *testing.T) {
	r := New(50)
	r.Add("n1", "n2", "n3")
	if r.Len() != 3 {
		t.Fatalf("expected 3 nodes, got %d", r.Len())
	}
	seen := map[string]bool{}
	for i := range 1000 {
		node, ok := r.Get(fmt.Sprintf("key-%d", i))
		if !ok {
			t.Fatal("expected an owner")
		}
		seen[node] = true
	}
	if len(seen) != 3 {
		t.Fatalf("all nodes should own some keys, got %v", seen)
	}

	a, _ := r.Get("stable")
	b, _ := r.Get("stable")
	if a != b {
		t.Fatalf("non-deterministic mapping: %s vs %s", a, b)
	}
}

func TestEmptyRing(t *testing.T) {
	r := New(10)
	if _, ok := r.Get("k"); ok {
		t.Fatal("empty ring should have no owner")
	}
	if r.GetN("k", 3) != nil {
		t.Fatal("empty ring GetN should be nil")
	}
}

func TestMinimalKeyMovementOnAdd(t *testing.T) {
	r := New(200)
	r.Add("n1", "n2", "n3")

	const N = 10000
	before := make(map[string]string, N)
	for i := range N {
		k := fmt.Sprintf("key-%d", i)
		node, _ := r.Get(k)
		before[k] = node
	}

	r.Add("n4")

	moved := 0
	for k, oldNode := range before {
		newNode, _ := r.Get(k)
		if newNode != oldNode {
			moved++
		}
	}

	frac := float64(moved) / float64(N)
	if frac > 0.40 {
		t.Fatalf("moved %.1f%% of keys, expected well under 40%%", frac*100)
	}
	if frac == 0 {
		t.Fatal("adding a node should move some keys")
	}
}

func TestGetNReturnsDistinctNodes(t *testing.T) {
	r := New(100)
	r.Add("n1", "n2", "n3", "n4", "n5")
	nodes := r.GetN("mykey", 3)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d: %v", len(nodes), nodes)
	}
	seen := map[string]bool{}
	for _, n := range nodes {
		if seen[n] {
			t.Fatalf("duplicate node in replica set: %v", nodes)
		}
		seen[n] = true
	}

	all := r.GetN("mykey", 99)
	if len(all) != 5 {
		t.Fatalf("expected all 5 nodes, got %d", len(all))
	}
}

func TestRemoveNode(t *testing.T) {
	r := New(50)
	r.Add("n1", "n2", "n3")
	r.Remove("n2")
	if r.Len() != 2 {
		t.Fatalf("expected 2 nodes after remove, got %d", r.Len())
	}
	for i := range 500 {
		node, _ := r.Get(fmt.Sprintf("k-%d", i))
		if node == "n2" {
			t.Fatal("removed node should never be returned")
		}
	}
}
