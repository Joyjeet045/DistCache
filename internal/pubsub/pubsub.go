package pubsub

import (
	"sync"
	"sync/atomic"
)

type Message struct {
	Topic   string
	Payload []byte
}

type Subscription struct {
	topic   string
	ch      chan Message
	broker  *Broker
	once    sync.Once
	dropped uint64
}

func (s *Subscription) C() <-chan Message { return s.ch }

func (s *Subscription) Topic() string { return s.topic }

func (s *Subscription) Dropped() uint64 { return atomic.LoadUint64(&s.dropped) }

func (s *Subscription) Close() {
	s.once.Do(func() {
		s.broker.remove(s)
		close(s.ch)
	})
}

type Broker struct {
	mu     sync.RWMutex
	topics map[string]map[*Subscription]struct{}
}

func NewBroker() *Broker {
	return &Broker{topics: make(map[string]map[*Subscription]struct{})}
}

func (b *Broker) Subscribe(topic string, buffer int) *Subscription {
	if buffer <= 0 {
		buffer = 16
	}
	sub := &Subscription{
		topic:  topic,
		ch:     make(chan Message, buffer),
		broker: b,
	}
	b.mu.Lock()
	subs := b.topics[topic]
	if subs == nil {
		subs = make(map[*Subscription]struct{})
		b.topics[topic] = subs
	}
	subs[sub] = struct{}{}
	b.mu.Unlock()
	return sub
}

func (b *Broker) Publish(topic string, payload []byte) int {
	b.mu.RLock()
	subs := b.topics[topic]
	targets := make([]*Subscription, 0, len(subs))
	for s := range subs {
		targets = append(targets, s)
	}
	b.mu.RUnlock()

	msg := Message{Topic: topic, Payload: payload}
	for _, s := range targets {
		select {
		case s.ch <- msg:
		default:
			atomic.AddUint64(&s.dropped, 1)
		}
	}
	return len(targets)
}

func (b *Broker) NumSubscribers(topic string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.topics[topic])
}

func (b *Broker) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, 0, len(b.topics))
	for t := range b.topics {
		out = append(out, t)
	}
	return out
}

func (b *Broker) remove(s *Subscription) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if subs, ok := b.topics[s.topic]; ok {
		delete(subs, s)
		if len(subs) == 0 {
			delete(b.topics, s.topic)
		}
	}
}
