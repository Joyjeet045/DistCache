package eviction

import "testing"

func drain(p Policy) []string {
	var out []string
	for {
		k, ok := p.Evict()
		if !ok {
			break
		}
		out = append(out, k)
	}
	return out
}

func TestLRU_EvictsLeastRecentlyUsed(t *testing.T) {
	p := NewLRU()
	p.Add("a")
	p.Add("b")
	p.Add("c")
	p.Access("a")
	if k, ok := p.Evict(); !ok || k != "b" {
		t.Fatalf("want b, got %q ok=%v", k, ok)
	}
	if k, _ := p.Evict(); k != "c" {
		t.Fatalf("want c, got %q", k)
	}
	if k, _ := p.Evict(); k != "a" {
		t.Fatalf("want a, got %q", k)
	}
	if _, ok := p.Evict(); ok {
		t.Fatal("expected empty policy")
	}
}

func TestFIFO_EvictsInsertionOrder(t *testing.T) {
	p := NewFIFO()
	p.Add("a")
	p.Add("b")
	p.Add("c")
	p.Access("a")
	got := drain(p)
	want := []string{"a", "b", "c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("fifo order = %v, want %v", got, want)
		}
	}
}

func TestLFU_EvictsLeastFrequent(t *testing.T) {
	p := NewLFU()
	p.Add("a")
	p.Add("b")
	p.Add("c")

	p.Access("a")
	p.Access("a")
	p.Access("a")
	p.Access("b")
	p.Access("b")
	if k, _ := p.Evict(); k != "c" {
		t.Fatalf("want c (least frequent), got %q", k)
	}
	if k, _ := p.Evict(); k != "b" {
		t.Fatalf("want b, got %q", k)
	}
	if k, _ := p.Evict(); k != "a" {
		t.Fatalf("want a, got %q", k)
	}
}

func TestLFU_TieBreakByLRU(t *testing.T) {
	p := NewLFU()
	p.Add("a")
	p.Add("b")

	if k, _ := p.Evict(); k != "a" {
		t.Fatalf("want a (oldest at min freq), got %q", k)
	}
}

func TestLFU_RemoveThenEvict(t *testing.T) {
	p := NewLFU()
	p.Add("a")
	p.Add("b")
	p.Access("b")
	p.Remove("a")
	if k, ok := p.Evict(); !ok || k != "b" {
		t.Fatalf("want b, got %q ok=%v", k, ok)
	}
}

func TestRandom_EvictsAll(t *testing.T) {
	p := NewRandom()
	for _, k := range []string{"a", "b", "c", "d"} {
		p.Add(k)
	}
	seen := map[string]bool{}
	for range 4 {
		k, ok := p.Evict()
		if !ok {
			t.Fatal("unexpected empty")
		}
		if seen[k] {
			t.Fatalf("duplicate eviction %q", k)
		}
		seen[k] = true
	}
	if _, ok := p.Evict(); ok {
		t.Fatal("expected empty after draining")
	}
	if len(seen) != 4 {
		t.Fatalf("want 4 distinct, got %d", len(seen))
	}
}

func TestTTL_EvictsNearestExpiryFirst(t *testing.T) {
	p := NewTTL().(*ttl)
	p.Add("never")
	p.Add("far")
	p.Note("far", 1000)
	p.Add("soon")
	p.Note("soon", 10)
	p.Add("mid")
	p.Note("mid", 100)

	order := drain(p)
	want := []string{"soon", "mid", "far", "never"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("ttl eviction order = %v, want %v", order, want)
		}
	}
}

func TestTTL_UpdatedExpiryUsesLatest(t *testing.T) {
	p := NewTTL().(*ttl)
	p.Add("k")
	p.Note("k", 500)
	p.Note("k", 50)
	p.Add("j")
	p.Note("j", 100)
	if k, _ := p.Evict(); k != "k" {
		t.Fatalf("want k (updated to 50), got %q", k)
	}
}

func TestNew_FactoryAndUnknown(t *testing.T) {
	for _, kind := range []Kind{NoEviction, LRU, LFU, FIFO, Random, TTL} {
		p, err := New(kind)
		if err != nil {
			t.Fatalf("New(%s) err: %v", kind, err)
		}
		if Kind(p.Name()) != kind {
			t.Fatalf("Name mismatch: got %s want %s", p.Name(), kind)
		}
	}
	if _, err := New("bogus"); err == nil {
		t.Fatal("expected error for unknown policy")
	}
}
