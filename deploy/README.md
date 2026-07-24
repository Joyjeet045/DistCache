# Integration & scenario testing (`deploy/`)

These scenarios exercise the **distributed** behaviours end to end against the
live Docker cluster — the things unit tests can only approximate: replication,
crash recovery, cluster routing, and monitoring.

## Bring the stack up

```powershell
docker compose -f deploy/docker-compose.yml up --build -d
docker compose -f deploy/docker-compose.yml ps
```

| Service | Client port | Metrics | Role |
| --- | --- | --- | --- |
| `distcache-1` | `6380` | `9121` | primary (persistent, `-data-dir=/data`) |
| `distcache-2` | `6381` | `9122` | replica of cache-1 |
| `distcache-3` | `6382` | `9123` | replica of cache-1 |
| `distcache-prometheus` | `9090` | — | scrapes all nodes |
| `distcache-grafana` | `3000` | — | dashboard (admin / admin) |

Any client works: the bundled CLI (`go run ./cmd/cache-cli -addr 127.0.0.1:PORT`),
`redis-cli -p PORT`, or `docker exec -it distcache-1 wget -qO- localhost:9121/metrics`.
Examples below use the bundled CLI in one-shot mode.

---

## Scenario 1 — Replication propagation

Write on the primary, read from a replica:

```powershell
go run ./cmd/cache-cli -addr 127.0.0.1:6380 SET user:1 "alice"
go run ./cmd/cache-cli -addr 127.0.0.1:6381 GET user:1   # replica cache-2
go run ./cmd/cache-cli -addr 127.0.0.1:6382 GET user:1   # replica cache-3
```

**Expect:** both replicas return `"alice"` within a few milliseconds.

## Scenario 2 — Replication lag & role reporting

```powershell
go run ./cmd/cache-cli -addr 127.0.0.1:6380 INFO      # role:master, connected_slaves:2
go run ./cmd/cache-cli -addr 127.0.0.1:6381 INFO      # role:replica, master_link_status:up
```

Drive some writes, then re-check `INFO` on a replica — `slave_repl_offset`
should advance and stay close to the primary's sequence.

## Scenario 3 — Replica resync after restart

```powershell
# Kill a replica, write while it is down, then bring it back
docker compose -f deploy/docker-compose.yml stop cache-2
go run ./cmd/cache-cli -addr 127.0.0.1:6380 SET while:down "written"
docker compose -f deploy/docker-compose.yml start cache-2

# After it reconnects it performs a fresh full snapshot sync
go run ./cmd/cache-cli -addr 127.0.0.1:6381 GET while:down
```

**Expect:** `"written"` — the replica caught up on the data written while it was
offline. On the primary, `INFO` briefly shows `connected_slaves:1` then `2`.

## Scenario 4 — Persistence & crash recovery

cache-1 mounts a named volume and writes an AOF + snapshots, so its data
survives a restart:

```powershell
go run ./cmd/cache-cli -addr 127.0.0.1:6380 SET durable "survives"
docker compose -f deploy/docker-compose.yml restart cache-1
# give it a second to recover from disk, then:
go run ./cmd/cache-cli -addr 127.0.0.1:6380 GET durable
```

**Expect:** `"survives"`. The startup log shows a `recovered from /data (N keys)`
line — inspect it with:

```powershell
docker compose -f deploy/docker-compose.yml logs cache-1 | Select-String recovered
```

## Scenario 5 — Cluster routing & ownership

```powershell
go run ./cmd/cache-cli -addr 127.0.0.1:6380 CLUSTER NODES
go run ./cmd/cache-cli -addr 127.0.0.1:6380 CLUSTER INFO   # cluster_enabled:1, cluster_known_nodes:3
```

From Go, the routing client sends each key to its owning node:

```go
cc, _ := client.NewCluster(map[string]string{
    "cache-1": "127.0.0.1:6380",
    "cache-2": "127.0.0.1:6381",
    "cache-3": "127.0.0.1:6382",
}, "")
owner, _ := cc.OwnerOf("user:42")   // deterministic owner via consistent hashing
```

## Scenario 6 — Pub/Sub

In one terminal, subscribe; in another, publish:

```powershell
# Terminal A (interactive REPL)
go run ./cmd/cache-cli -addr 127.0.0.1:6380
127.0.0.1:6380> SUBSCRIBE news

# Terminal B
go run ./cmd/cache-cli -addr 127.0.0.1:6380 PUBLISH news "breaking"
```

**Expect:** terminal A prints a `message news breaking` push. `PUBLISH` returns
the number of subscribers reached.

## Scenario 7 — Distributed locks (fencing tokens)

```powershell
go run ./cmd/cache-cli -addr 127.0.0.1:6380 LOCK job 30 worker-1   # -> a token
# A different owner is refused while held:
go run ./cmd/cache-cli -addr 127.0.0.1:6380 LOCK job 30 worker-2   # -> (nil)
go run ./cmd/cache-cli -addr 127.0.0.1:6380 UNLOCK job <token>     # -> (integer) 1
```

**Expect:** the second `LOCK` returns `(nil)`; `RENEWLOCK` / `UNLOCK` only succeed
with the exact fencing token.

## Scenario 8 — Transactions (optimistic WATCH)

```powershell
go run ./cmd/cache-cli -addr 127.0.0.1:6380
127.0.0.1:6380> SET balance 100
127.0.0.1:6380> WATCH balance
127.0.0.1:6380> MULTI
127.0.0.1:6380> INCRBY balance 50
127.0.0.1:6380> EXEC        # -> array of results, unless balance changed meanwhile
```

To see an **abort**, change `balance` from a second connection after `WATCH` but
before `EXEC`; `EXEC` then returns `(nil)` and applies nothing.

## Scenario 9 — Eviction under a cap

Restart one node with a tiny cap to watch eviction happen (or add `-max-entries`
to the compose command):

```powershell
go run ./cmd/cache-server -addr :6390 -max-entries 100 -policy lru
# hammer it with >100 keys, then:
go run ./cmd/cache-cli -addr 127.0.0.1:6390 DBSIZE          # stays ~100
```

`/metrics` on that node shows `distcache_evictions_total` climbing.

## Scenario 10 — Monitoring

```powershell
# Raw metrics
(Invoke-WebRequest http://localhost:9121/metrics).Content
# Health
(Invoke-WebRequest http://localhost:9121/health).Content     # ok
```

- **Prometheus** — <http://localhost:9090> → try queries like
  `rate(distcache_commands_total[1m])`, `distcache_hit_ratio`,
  `distcache_keys`, `distcache_cluster_node`.
- **Grafana** — <http://localhost:3000> (admin / admin) → the auto-provisioned
  *distcache* dashboard shows throughput, latency, hit ratio, memory, evictions,
  and replication topology per node.

## Scenario 11 — AUTH gating

Add `-password=secret` to a node's command, then:

```powershell
go run ./cmd/cache-cli -addr 127.0.0.1:6380 GET k          # -> (error) NOAUTH ...
go run ./cmd/cache-cli -addr 127.0.0.1:6380 AUTH secret    # -> OK, then commands work
```

## Load / smoke test

`redis-cli` compatibility means `redis-benchmark` works too:

```powershell
redis-benchmark -p 6380 -t set,get -n 100000 -q
```

Watch the effect live in Grafana or via `/metrics` (`distcache_command_latency_seconds`).

---

## Tear down

```powershell
docker compose -f deploy/docker-compose.yml down          # keep volumes
docker compose -f deploy/docker-compose.yml down -v       # also wipe persisted data
```
