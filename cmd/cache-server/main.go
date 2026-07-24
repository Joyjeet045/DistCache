package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"distcache/internal/cache"
	"distcache/internal/cluster"
	"distcache/internal/eviction"
	"distcache/internal/persistence"
	"distcache/internal/replication"
	"distcache/internal/server"
)

func main() {
	var (
		addr        = flag.String("addr", ":6380", "RESP listen address")
		metricsAddr = flag.String("metrics-addr", ":9121", "Prometheus /metrics HTTP address (empty to disable)")
		password    = flag.String("password", "", "require AUTH with this password (empty = no auth)")
		nodeID      = flag.String("node-id", "node-1", "node identifier for INFO/metrics")
		shards      = flag.Int("shards", 256, "number of cache shards")
		maxMemMB    = flag.Int64("max-memory-mb", 0, "soft memory cap in MiB (0 = unlimited)")
		maxEntries  = flag.Int64("max-entries", 0, "soft cap on live keys (0 = unlimited)")
		policy      = flag.String("policy", "lru", "eviction policy: noeviction|lru|lfu|fifo|random|ttl")
		dataDir     = flag.String("data-dir", "", "directory for persistence (empty = in-memory only)")
		aofSync     = flag.String("aof-sync", "everysec", "AOF fsync policy: no|everysec|always")
		snapEvery   = flag.Duration("snapshot-interval", 5*time.Minute, "background snapshot interval (0 = disabled)")
		activeExp   = flag.Duration("active-expiry", time.Second, "background expired-key sweep interval (0 = disabled)")
		replListen  = flag.String("repl-listen", "", "address to accept replica connections (empty = disabled)")
		replicaOf   = flag.String("replicaof", "", "primary replication address; makes this node a replica")
		clusterSpec = flag.String("cluster", "", "cluster topology as id=addr[,id=addr...] (enables CLUSTER)")
		replFactor  = flag.Int("replication-factor", 1, "cluster replication factor")
	)
	flag.Parse()

	logger := log.New(os.Stderr, "[distcache] ", log.LstdFlags)

	c, err := cache.New(cache.Config{
		Shards:       *shards,
		MaxEntries:   *maxEntries,
		MaxMemory:    *maxMemMB * 1024 * 1024,
		Policy:       eviction.Kind(*policy),
		ActiveExpiry: *activeExp,
	})
	if err != nil {
		logger.Fatalf("init cache: %v", err)
	}

	var pm *persistence.Manager
	if *dataDir != "" {
		pm, err = persistence.Open(persistence.Config{
			Dir:              *dataDir,
			Sync:             persistence.SyncPolicy(*aofSync),
			SnapshotInterval: *snapEvery,
			Logger:           logger,
		})
		if err != nil {
			logger.Fatalf("init persistence: %v", err)
		}
		if err := pm.Recover(c); err != nil {
			logger.Fatalf("recover: %v", err)
		}
		logger.Printf("recovered from %s (%d keys)", *dataDir, c.Len())
	}

	srv := server.New(c, server.Config{
		Addr:        *addr,
		MetricsAddr: *metricsAddr,
		Password:    *password,
		NodeID:      *nodeID,
	})

	if *clusterSpec != "" {
		reg, err := buildRegistry(*nodeID, *addr, *replFactor, *clusterSpec)
		if err != nil {
			logger.Fatalf("cluster: %v", err)
		}
		srv.AttachCluster(reg)
		logger.Printf("cluster topology: %d nodes, replication-factor %d", reg.Size(), *replFactor)
	}

	var primary *replication.Primary
	if *replListen != "" {
		primary = replication.NewPrimary(c, *replListen, logger)
		srv.AttachPrimary(primary)
		go func() {
			if err := primary.ListenAndServe(); err != nil {
				logger.Printf("replication primary: %v", err)
			}
		}()
		logger.Printf("replication primary listening on %s", *replListen)
	}
	var replica *replication.Replica
	if *replicaOf != "" {
		replica = replication.NewReplica(c, *replicaOf, logger)
		srv.AttachReplica(replica)
		replica.Start()
		logger.Printf("replicating from primary %s", *replicaOf)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	logger.Printf("node %s listening on %s (metrics %s, policy %s)", *nodeID, *addr, *metricsAddr, *policy)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if err != nil {
			logger.Fatalf("serve: %v", err)
		}
	case s := <-sig:
		logger.Printf("received %s, shutting down", s)
	}

	_ = srv.Close()
	if replica != nil {
		_ = replica.Close()
	}
	if primary != nil {
		_ = primary.Close()
	}
	if pm != nil {
		if err := pm.Snapshot(); err != nil {
			logger.Printf("final snapshot: %v", err)
		}
		_ = pm.Close()
	}
	_ = c.Close()
	logger.Printf("bye")
}

func buildRegistry(selfID, selfAddr string, rf int, spec string) (*cluster.Registry, error) {
	reg := cluster.New(cluster.Config{
		SelfID:            selfID,
		SelfAddr:          selfAddr,
		ReplicationFactor: rf,
		FailAfter:         10 * time.Second,
	})
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("invalid cluster entry %q (want id=addr)", part)
		}
		reg.Add(strings.TrimSpace(id), strings.TrimSpace(addr))
	}
	return reg, nil
}
