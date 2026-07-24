package client

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"distcache/internal/hashring"
)

type ClusterClient struct {
	password string

	mu      sync.Mutex
	ring    *hashring.Ring
	addrs   map[string]string
	clients map[string]*Client
}

func NewCluster(nodes map[string]string, password string) (*ClusterClient, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("client: cluster requires at least one node")
	}
	cc := &ClusterClient{
		password: password,
		ring:     hashring.New(0),
		addrs:    make(map[string]string, len(nodes)),
		clients:  make(map[string]*Client, len(nodes)),
	}
	for id, addr := range nodes {
		cc.addrs[id] = addr
		cc.ring.Add(id)
	}
	return cc, nil
}

func (cc *ClusterClient) Nodes() []string {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	ids := make([]string, 0, len(cc.addrs))
	for id := range cc.addrs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (cc *ClusterClient) OwnerOf(key string) (string, bool) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.ring.Get(key)
}

func (cc *ClusterClient) clientFor(key string) (*Client, error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	id, ok := cc.ring.Get(key)
	if !ok {
		return nil, fmt.Errorf("client: empty cluster")
	}
	if c, ok := cc.clients[id]; ok {
		return c, nil
	}
	c, err := DialPassword(cc.addrs[id], cc.password)
	if err != nil {
		return nil, fmt.Errorf("client: dial %s (%s): %w", id, cc.addrs[id], err)
	}
	cc.clients[id] = c
	return c, nil
}

func (cc *ClusterClient) Set(key string, value []byte) error {
	c, err := cc.clientFor(key)
	if err != nil {
		return err
	}
	return c.Set(key, value)
}

func (cc *ClusterClient) SetEX(key string, value []byte, ttl time.Duration) error {
	c, err := cc.clientFor(key)
	if err != nil {
		return err
	}
	return c.SetEX(key, value, ttl)
}

func (cc *ClusterClient) Get(key string) ([]byte, bool, error) {
	c, err := cc.clientFor(key)
	if err != nil {
		return nil, false, err
	}
	return c.Get(key)
}

func (cc *ClusterClient) Del(key string) (int64, error) {
	c, err := cc.clientFor(key)
	if err != nil {
		return 0, err
	}
	return c.Del(key)
}

func (cc *ClusterClient) Incr(key string) (int64, error) {
	c, err := cc.clientFor(key)
	if err != nil {
		return 0, err
	}
	return c.Incr(key)
}

func (cc *ClusterClient) Close() error {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	for _, c := range cc.clients {
		_ = c.Close()
	}
	cc.clients = map[string]*Client{}
	return nil
}
