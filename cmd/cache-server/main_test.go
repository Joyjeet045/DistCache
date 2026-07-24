package main

import "testing"

func TestBuildRegistryParsesSpec(t *testing.T) {
	reg, err := buildRegistry("self", "127.0.0.1:7000", 2, "n1=127.0.0.1:7001,n2=127.0.0.1:7002")
	if err != nil {
		t.Fatalf("buildRegistry: %v", err)
	}
	if reg.Size() != 3 {
		t.Fatalf("Size = %d, want 3 (self + 2)", reg.Size())
	}
	ids := map[string]bool{}
	for _, n := range reg.Nodes() {
		ids[n.ID] = true
	}
	for _, want := range []string{"self", "n1", "n2"} {
		if !ids[want] {
			t.Fatalf("registry missing node %q: have %v", want, ids)
		}
	}
	if reg.Self().ID != "self" {
		t.Fatalf("Self ID = %q, want self", reg.Self().ID)
	}
}

func TestBuildRegistryEmptySpecIsSelfOnly(t *testing.T) {
	reg, err := buildRegistry("solo", "127.0.0.1:7000", 1, "")
	if err != nil {
		t.Fatalf("buildRegistry: %v", err)
	}
	if reg.Size() != 1 {
		t.Fatalf("Size = %d, want 1", reg.Size())
	}

	reg2, err := buildRegistry("solo", "127.0.0.1:7000", 1, "  , n1=127.0.0.1:7001 ,")
	if err != nil {
		t.Fatalf("buildRegistry with padding: %v", err)
	}
	if reg2.Size() != 2 {
		t.Fatalf("Size = %d, want 2", reg2.Size())
	}
}

func TestBuildRegistryRejectsBadEntry(t *testing.T) {
	if _, err := buildRegistry("self", "127.0.0.1:7000", 1, "missing-equals"); err == nil {
		t.Fatal("expected an error for an entry without '='")
	}
}
