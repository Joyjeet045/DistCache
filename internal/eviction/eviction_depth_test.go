package eviction

import "testing"

func TestNoEvictionNeverEvicts(t *testing.T) {
	p := NewNoEviction()
	p.Add("a")
	p.Add("b")
	p.Access("a")
	if p.Len() != 2 {
		t.Fatalf("Len = %d, want 2", p.Len())
	}
	if _, ok := p.Evict(); ok {
		t.Fatal("noeviction must never yield a victim")
	}
	if Kind(p.Name()) != NoEviction {
		t.Fatalf("Name = %q, want %q", p.Name(), NoEviction)
	}
}

func TestRemoveExcludesKeyFromEviction(t *testing.T) {
	build := map[string]func() Policy{
		"lru":    func() Policy { return NewLRU() },
		"fifo":   func() Policy { return NewFIFO() },
		"random": func() Policy { return NewRandom() },
	}
	for name, mk := range build {
		t.Run(name, func(t *testing.T) {
			p := mk()
			p.Add("a")
			p.Add("b")
			p.Add("c")
			p.Remove("b")
			if p.Len() != 2 {
				t.Fatalf("Len after remove = %d, want 2", p.Len())
			}
			for _, k := range drain(p) {
				if k == "b" {
					t.Fatal("removed key must not be evicted")
				}
			}
		})
	}
}
