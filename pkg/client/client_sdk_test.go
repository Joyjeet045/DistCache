package client_test

import (
	"errors"
	"net"
	"testing"
	"time"

	"distcache/pkg/client"
)

func TestClientSetEXAndTTL(t *testing.T) {
	addr := startNode(t)
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if err := c.SetEX("k", []byte("v"), 100*time.Second); err != nil {
		t.Fatalf("SetEX: %v", err)
	}
	ttl, err := c.TTL("k")
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl <= 0 || ttl > 100*time.Second {
		t.Fatalf("TTL = %v, want (0,100s]", ttl)
	}

	if err := c.SetEX("short", []byte("v"), 30*time.Millisecond); err != nil {
		t.Fatalf("SetEX short: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, ok, err := c.Get("short")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("SetEX key did not expire")
		}
	}
}

func TestClientExpireAndTTLSentinels(t *testing.T) {
	addr := startNode(t)
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ok, err := c.Expire("missing", 10*time.Second)
	if err != nil || ok {
		t.Fatalf("Expire missing = %v err=%v, want false", ok, err)
	}
	if ttl, _ := c.TTL("missing"); ttl != time.Duration(-2) {
		t.Fatalf("TTL missing = %v, want -2ns", ttl)
	}

	if err := c.Set("k", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if ttl, _ := c.TTL("k"); ttl != time.Duration(-1) {
		t.Fatalf("TTL no-expiry = %v, want -1ns", ttl)
	}

	ok, err = c.Expire("k", 50*time.Second)
	if err != nil || !ok {
		t.Fatalf("Expire = %v err=%v, want true", ok, err)
	}
	if ttl, _ := c.TTL("k"); ttl <= 0 || ttl > 50*time.Second {
		t.Fatalf("TTL after expire = %v", ttl)
	}
}

func TestClientLocks(t *testing.T) {
	addr := startNode(t)
	a, _ := client.Dial(addr)
	b, _ := client.Dial(addr)
	defer a.Close()
	defer b.Close()

	token, ok, err := a.Lock("res", 30*time.Second, "owner-a")
	if err != nil || !ok || token == "" {
		t.Fatalf("Lock a = %q ok=%v err=%v", token, ok, err)
	}

	if _, ok, _ := b.Lock("res", 30*time.Second, "owner-b"); ok {
		t.Fatal("owner-b acquired a held lock")
	}

	token2, ok, err := a.Lock("res", 30*time.Second, "owner-a")
	if err != nil || !ok || token2 == token {
		t.Fatalf("reentrant Lock = %q ok=%v err=%v", token2, ok, err)
	}
	if released, _ := a.Unlock("res", token); released {
		t.Fatal("stale token released the lock")
	}

	if released, _ := a.Unlock("res", "not-a-token"); released {
		t.Fatal("wrong token released the lock")
	}
	if released, err := a.Unlock("res", token2); err != nil || !released {
		t.Fatalf("Unlock fresh token = %v err=%v", released, err)
	}
}

func TestClientPublishSubscribe(t *testing.T) {
	addr := startNode(t)

	sub, err := client.Subscribe(addr, "", "a", "b")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	pub, err := client.Dial(addr)
	if err != nil {
		t.Fatalf("dial pub: %v", err)
	}
	defer pub.Close()

	n, err := pub.Publish("a", []byte("m-a"))
	if err != nil || n != 1 {
		t.Fatalf("Publish a = %d err=%v, want 1", n, err)
	}
	n, err = pub.Publish("b", []byte("m-b"))
	if err != nil || n != 1 {
		t.Fatalf("Publish b = %d err=%v, want 1", n, err)
	}

	if n, _ := pub.Publish("c", []byte("x")); n != 0 {
		t.Fatalf("Publish c = %d, want 0", n)
	}

	got := map[string]string{}
	for i := 0; i < 2; i++ {
		select {
		case msg := <-sub.Messages():
			got[msg.Topic] = string(msg.Payload)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for message %d (got %v)", i, got)
		}
	}
	if got["a"] != "m-a" || got["b"] != "m-b" {
		t.Fatalf("received = %v", got)
	}
}

func TestClientDoAndError(t *testing.T) {
	addr := startNode(t)
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if err := c.Set("k", []byte("notint")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	_, err = c.Do("INCR", "k")
	if err == nil {
		t.Fatal("INCR on non-integer should error")
	}
	var rerr *client.Error
	if !errors.As(err, &rerr) {
		t.Fatalf("error type = %T, want *client.Error", err)
	}
	if rerr.Msg == "" {
		t.Fatal("Error.Msg is empty")
	}

	if _, err := c.Do("NOPESUCHCMD"); !errors.As(err, &rerr) {
		t.Fatalf("unknown command error = %v", err)
	}
}

func TestClusterClientOps(t *testing.T) {
	nodes := map[string]string{
		"n1": startNode(t),
		"n2": startNode(t),
		"n3": startNode(t),
	}
	cc, err := client.NewCluster(nodes, "")
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer cc.Close()

	got := cc.Nodes()
	if len(got) != 3 || got[0] != "n1" || got[1] != "n2" || got[2] != "n3" {
		t.Fatalf("Nodes = %v", got)
	}

	owner, ok := cc.OwnerOf("some-key")
	if !ok || (owner != "n1" && owner != "n2" && owner != "n3") {
		t.Fatalf("OwnerOf = %q ok=%v", owner, ok)
	}

	if err := cc.SetEX("ek", []byte("v"), 100*time.Second); err != nil {
		t.Fatalf("SetEX: %v", err)
	}
	v, found, err := cc.Get("ek")
	if err != nil || !found || string(v) != "v" {
		t.Fatalf("Get = %q found=%v err=%v", v, found, err)
	}
	if n, err := cc.Del("ek"); err != nil || n != 1 {
		t.Fatalf("Del = %d err=%v, want 1", n, err)
	}
	if n, err := cc.Incr("counter"); err != nil || n != 1 {
		t.Fatalf("Incr = %d err=%v, want 1", n, err)
	}
}

func TestClusterClientDialError(t *testing.T) {

	if _, err := client.NewCluster(nil, ""); err == nil {
		t.Fatal("NewCluster(nil) should error")
	}

	cc, err := client.NewCluster(map[string]string{"dead": freeAddr(t)}, "")
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer cc.Close()
	if err := cc.Set("k", []byte("v")); err == nil {
		t.Fatal("Set to dead node should error")
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}
