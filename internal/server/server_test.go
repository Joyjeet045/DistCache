package server

import (
	"net"
	"testing"
	"time"

	"distcache/internal/cache"
	"distcache/pkg/client"
)

func newTestServer(t *testing.T, cfg Config) string {
	t.Helper()
	c, err := cache.New(cache.Config{Shards: 8})
	if err != nil {
		t.Fatalf("cache: %v", err)
	}
	s := New(c, cfg)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = s.Serve(ln) }()
	t.Cleanup(func() {
		_ = s.Close()
		_ = c.Close()
	})
	return ln.Addr().String()
}

func dial(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestServerBasicCommands(t *testing.T) {
	addr := newTestServer(t, Config{NodeID: "test"})
	c := dial(t, addr)

	if err := c.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if err := c.Set("k", []byte("hello")); err != nil {
		t.Fatalf("set: %v", err)
	}
	v, ok, err := c.Get("k")
	if err != nil || !ok || string(v) != "hello" {
		t.Fatalf("get = %q ok=%v err=%v", v, ok, err)
	}
	if _, ok, _ := c.Get("missing"); ok {
		t.Fatalf("missing key should not exist")
	}
	if n, _ := c.Exists("k", "missing"); n != 1 {
		t.Fatalf("exists = %d, want 1", n)
	}
	if n, _ := c.Del("k"); n != 1 {
		t.Fatalf("del = %d, want 1", n)
	}
	if _, ok, _ := c.Get("k"); ok {
		t.Fatalf("key should be deleted")
	}
}

func TestServerCounters(t *testing.T) {
	addr := newTestServer(t, Config{})
	c := dial(t, addr)

	if n, err := c.Incr("ctr"); err != nil || n != 1 {
		t.Fatalf("incr = %d err=%v", n, err)
	}
	if n, _ := c.IncrBy("ctr", 10); n != 11 {
		t.Fatalf("incrby = %d, want 11", n)
	}
}

func TestServerTTL(t *testing.T) {
	addr := newTestServer(t, Config{})
	c := dial(t, addr)

	_ = c.Set("k", []byte("v"))
	ok, err := c.Expire("k", 100*time.Second)
	if err != nil || !ok {
		t.Fatalf("expire ok=%v err=%v", ok, err)
	}
	ttl, err := c.TTL("k")
	if err != nil || ttl <= 0 || ttl > 100*time.Second {
		t.Fatalf("ttl = %v err=%v", ttl, err)
	}
	if ttl, _ := c.TTL("missing"); ttl != -2 {
		t.Fatalf("missing ttl = %v, want -2", ttl)
	}
}

func TestServerAuth(t *testing.T) {
	addr := newTestServer(t, Config{Password: "secret"})

	bad := dial(t, addr)
	if err := bad.Set("k", []byte("v")); err == nil {
		t.Fatalf("expected NOAUTH error without AUTH")
	}

	good, err := client.DialPassword(addr, "secret")
	if err != nil {
		t.Fatalf("dial+auth: %v", err)
	}
	defer good.Close()
	if err := good.Set("k", []byte("v")); err != nil {
		t.Fatalf("set after auth: %v", err)
	}
}

func TestServerMultiExec(t *testing.T) {
	addr := newTestServer(t, Config{})
	c := dial(t, addr)

	if v, err := c.Do("MULTI"); err != nil || v.Str != "OK" {
		t.Fatalf("multi = %+v err=%v", v, err)
	}
	if v, _ := c.Do("SET", "a", "1"); v.Str != "QUEUED" {
		t.Fatalf("queued set = %+v", v)
	}
	if v, _ := c.Do("INCR", "a"); v.Str != "QUEUED" {
		t.Fatalf("queued incr = %+v", v)
	}
	v, err := c.Do("EXEC")
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if v.Type != '*' || len(v.Array) != 2 {
		t.Fatalf("exec reply = %+v", v)
	}
	if v.Array[1].Int != 2 {
		t.Fatalf("incr result = %d, want 2", v.Array[1].Int)
	}
}

func TestServerPubSub(t *testing.T) {
	addr := newTestServer(t, Config{})

	sub, err := client.Subscribe(addr, "", "news")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	pub := dial(t, addr)

	deadline := time.Now().Add(time.Second)
	for {
		n, _ := pub.Publish("news", []byte("hi"))
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no subscriber saw the publish")
		}
		time.Sleep(5 * time.Millisecond)
	}

	select {
	case msg := <-sub.Messages():
		if msg.Topic != "news" || string(msg.Payload) != "hi" {
			t.Fatalf("msg = %+v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for message")
	}
}

func TestServerLocks(t *testing.T) {
	addr := newTestServer(t, Config{})
	c := dial(t, addr)

	token, ok, err := c.Lock("resource", 30*time.Second, "owner-1")
	if err != nil || !ok || token == "" {
		t.Fatalf("lock ok=%v token=%q err=%v", ok, token, err)
	}

	if _, ok, _ := c.Lock("resource", 30*time.Second, "owner-2"); ok {
		t.Fatalf("lock should be held")
	}
	if released, err := c.Unlock("resource", token); err != nil || !released {
		t.Fatalf("unlock released=%v err=%v", released, err)
	}
}
