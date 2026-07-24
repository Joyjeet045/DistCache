package client_test

import (
	"fmt"
	"net"
	"testing"

	"distcache/internal/cache"
	"distcache/internal/server"
	"distcache/pkg/client"
)

func startNode(t *testing.T) string {
	t.Helper()
	c, err := cache.New(cache.Config{Shards: 8})
	if err != nil {
		t.Fatalf("cache: %v", err)
	}
	s := server.New(c, server.Config{})
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

func TestClusterClientRoutesAndDistributes(t *testing.T) {
	nodes := map[string]string{
		"n1": startNode(t),
		"n2": startNode(t),
		"n3": startNode(t),
	}
	cc, err := client.NewCluster(nodes, "")
	if err != nil {
		t.Fatalf("cluster: %v", err)
	}
	defer cc.Close()

	const total = 300
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := cc.Set(key, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}

	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		v, ok, err := cc.Get(key)
		if err != nil || !ok {
			t.Fatalf("get %s: ok=%v err=%v", key, ok, err)
		}
		if want := fmt.Sprintf("v%d", i); string(v) != want {
			t.Fatalf("get %s = %q, want %q", key, v, want)
		}
	}

	sum := 0
	nonEmpty := 0
	for _, addr := range nodes {
		c, err := client.Dial(addr)
		if err != nil {
			t.Fatalf("dial %s: %v", addr, err)
		}
		v, err := c.Do("DBSIZE")
		c.Close()
		if err != nil {
			t.Fatalf("dbsize: %v", err)
		}
		sum += int(v.Int)
		if v.Int > 0 {
			nonEmpty++
		}
	}
	if sum != total {
		t.Fatalf("total keys across nodes = %d, want %d", sum, total)
	}
	if nonEmpty < 2 {
		t.Fatalf("keys landed on only %d node(s); expected distribution", nonEmpty)
	}
}
