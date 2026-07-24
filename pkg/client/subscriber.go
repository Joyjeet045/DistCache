package client

import (
	"net"

	"distcache/internal/resp"
)

type Message struct {
	Topic   string
	Payload []byte
}

type Subscriber struct {
	conn net.Conn
	r    *resp.Reader
	msgs chan Message
	done chan struct{}
}

func Subscribe(addr, password string, topics ...string) (*Subscriber, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)
	if password != "" {
		if err := w.WriteCommandStrings("AUTH", password); err != nil {
			conn.Close()
			return nil, err
		}
		if _, err := r.ReadReply(); err != nil {
			conn.Close()
			return nil, err
		}
	}
	args := append([]string{"SUBSCRIBE"}, topics...)
	if err := w.WriteCommandStrings(args...); err != nil {
		conn.Close()
		return nil, err
	}
	s := &Subscriber{
		conn: conn,
		r:    r,
		msgs: make(chan Message, 64),
		done: make(chan struct{}),
	}

	for range topics {
		if _, err := r.ReadReply(); err != nil {
			conn.Close()
			return nil, err
		}
	}
	go s.loop()
	return s, nil
}

func (s *Subscriber) Messages() <-chan Message { return s.msgs }

func (s *Subscriber) Close() error {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	return s.conn.Close()
}

func (s *Subscriber) loop() {
	defer close(s.msgs)
	for {
		v, err := s.r.ReadReply()
		if err != nil {
			return
		}
		if v.Type != '*' || len(v.Array) != 3 {
			continue
		}
		if string(v.Array[0].Bulk) != "message" {
			continue
		}
		msg := Message{Topic: string(v.Array[1].Bulk), Payload: v.Array[2].Bulk}
		select {
		case s.msgs <- msg:
		case <-s.done:
			return
		}
	}
}
