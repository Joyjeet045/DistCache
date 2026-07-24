package hashring

import (
	"sort"
	"testing"
)

func TestNodesListingAndIdempotentAdd(t *testing.T) {
	r := New(0)
	r.Add("n2", "n1", "n3")
	r.Add("n1")

	if r.Len() != 3 {
		t.Fatalf("Len = %d, want 3 (idempotent add)", r.Len())
	}

	nodes := r.Nodes()
	if !sort.StringsAreSorted(nodes) {
		t.Fatalf("Nodes not sorted: %v", nodes)
	}
	if len(nodes) != 3 || nodes[0] != "n1" || nodes[2] != "n3" {
		t.Fatalf("Nodes = %v", nodes)
	}

	single := New(10)
	single.Add("only")
	for _, k := range []string{"a", "b", "zzz"} {
		if n, ok := single.Get(k); !ok || n != "only" {
			t.Fatalf("Get(%q) = %q ok=%v, want only", k, n, ok)
		}
	}
}
