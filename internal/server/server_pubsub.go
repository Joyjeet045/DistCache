package server

import "distcache/internal/pubsub"

func (c *conn) doSubscribe(args [][]byte) bool {
	if len(args) < 2 {
		c.reply(rErr("ERR wrong number of arguments for 'subscribe'"))
		return true
	}
	for _, t := range args[1:] {
		topic := string(t)
		c.subMu.Lock()
		if _, exists := c.subs[topic]; !exists {
			sub := c.s.broker.Subscribe(topic, 256)
			c.subs[topic] = sub
			go c.forward(sub)
		}
		count := len(c.subs)
		c.subMu.Unlock()
		c.confirm("subscribe", topic, count)
	}
	return true
}

func (c *conn) doUnsubscribe(args [][]byte) bool {
	var topics []string
	if len(args) >= 2 {
		for _, t := range args[1:] {
			topics = append(topics, string(t))
		}
	} else {
		c.subMu.Lock()
		for t := range c.subs {
			topics = append(topics, t)
		}
		c.subMu.Unlock()
	}
	if len(topics) == 0 {
		c.confirm("unsubscribe", "", 0)
		return true
	}
	for _, topic := range topics {
		c.subMu.Lock()
		if sub, ok := c.subs[topic]; ok {
			sub.Close()
			delete(c.subs, topic)
		}
		count := len(c.subs)
		c.subMu.Unlock()
		c.confirm("unsubscribe", topic, count)
	}
	return true
}

func (c *conn) cmdPublish(args [][]byte) reply {
	if len(args) != 3 {
		return rErr("ERR wrong number of arguments for 'publish'")
	}
	n := c.s.broker.Publish(string(args[1]), args[2])
	c.s.metrics.IncPubSub()
	return rInt(int64(n))
}

func (c *conn) forward(sub *pubsub.Subscription) {
	for msg := range sub.C() {
		c.wmu.Lock()
		_ = c.w.WriteArrayHeader(3)
		_ = c.w.WriteBulkString("message")
		_ = c.w.WriteBulkString(msg.Topic)
		_ = c.w.WriteBulk(msg.Payload)
		_ = c.w.Flush()
		c.wmu.Unlock()
	}
}

func (c *conn) confirm(kind, topic string, count int) {
	c.wmu.Lock()
	_ = c.w.WriteArrayHeader(3)
	_ = c.w.WriteBulkString(kind)
	if topic == "" {
		_ = c.w.WriteNil()
	} else {
		_ = c.w.WriteBulkString(topic)
	}
	_ = c.w.WriteInt(int64(count))
	_ = c.w.Flush()
	c.wmu.Unlock()
}
