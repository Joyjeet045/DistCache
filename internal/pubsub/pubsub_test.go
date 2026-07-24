package pubsub

import (
	"testing"
	"time"
)

func recv(t *testing.T, sub *Subscription) Message {
	t.Helper()
	select {
	case m := <-sub.C():
		return m
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
		return Message{}
	}
}

func TestPublishToMultipleSubscribers(t *testing.T) {
	b := NewBroker()
	s1 := b.Subscribe("news", 4)
	s2 := b.Subscribe("news", 4)
	defer s1.Close()
	defer s2.Close()

	if n := b.Publish("news", []byte("hello")); n != 2 {
		t.Fatalf("published to %d subscribers, want 2", n)
	}
	if m := recv(t, s1); string(m.Payload) != "hello" || m.Topic != "news" {
		t.Fatalf("s1 got %+v", m)
	}
	if m := recv(t, s2); string(m.Payload) != "hello" {
		t.Fatalf("s2 got %+v", m)
	}
}

func TestTopicIsolation(t *testing.T) {
	b := NewBroker()
	sports := b.Subscribe("sports", 4)
	defer sports.Close()
	weather := b.Subscribe("weather", 4)
	defer weather.Close()

	b.Publish("sports", []byte("goal"))
	if n := b.NumSubscribers("weather"); n != 1 {
		t.Fatalf("weather subs = %d", n)
	}
	select {
	case <-weather.C():
		t.Fatal("weather subscriber should not receive sports message")
	case <-time.After(50 * time.Millisecond):
	}
	if m := recv(t, sports); string(m.Payload) != "goal" {
		t.Fatalf("sports got %+v", m)
	}
}

func TestUnsubscribeCleansUpTopic(t *testing.T) {
	b := NewBroker()
	s := b.Subscribe("t", 1)
	if b.NumSubscribers("t") != 1 {
		t.Fatal("expected 1 subscriber")
	}
	s.Close()
	if b.NumSubscribers("t") != 0 {
		t.Fatal("expected topic cleaned up after close")
	}

	if n := b.Publish("t", []byte("x")); n != 0 {
		t.Fatalf("published to %d, want 0", n)
	}
}

func TestSlowSubscriberDropsInsteadOfBlocking(t *testing.T) {
	b := NewBroker()
	s := b.Subscribe("t", 1)
	defer s.Close()
	for range 10 {
		b.Publish("t", []byte("x"))
	}
	if s.Dropped() == 0 {
		t.Fatal("expected some dropped messages for the slow subscriber")
	}
}
