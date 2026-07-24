package pubsub

import (
	"sort"
	"testing"
)

func TestTopicsListing(t *testing.T) {
	b := NewBroker()
	if len(b.Topics()) != 0 {
		t.Fatal("new broker should have no topics")
	}
	s1 := b.Subscribe("alpha", 4)
	s2 := b.Subscribe("beta", 4)
	defer s1.Close()
	defer s2.Close()

	topics := b.Topics()
	sort.Strings(topics)
	if len(topics) != 2 || topics[0] != "alpha" || topics[1] != "beta" {
		t.Fatalf("Topics = %v, want [alpha beta]", topics)
	}
	if b.NumSubscribers("unknown") != 0 {
		t.Fatal("unknown topic should report 0 subscribers")
	}
}

func TestSubscriptionCloseIsIdempotent(t *testing.T) {
	b := NewBroker()
	s := b.Subscribe("t", 4)
	if s.Topic() != "t" {
		t.Fatalf("Topic = %q, want t", s.Topic())
	}

	s.Close()

	s.Close()
	if b.NumSubscribers("t") != 0 {
		t.Fatal("topic should be empty after close")
	}

	if _, open := <-s.C(); open {
		t.Fatal("channel should be closed after Close")
	}
}

func TestDroppedCounterCountsExactly(t *testing.T) {
	b := NewBroker()
	s := b.Subscribe("t", 1)
	defer s.Close()

	for i := 0; i < 5; i++ {
		b.Publish("t", []byte("x"))
	}
	if got := s.Dropped(); got != 4 {
		t.Fatalf("Dropped = %d, want 4 (1 buffered, 4 dropped)", got)
	}
}
