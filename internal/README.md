# Unit-test map (`internal/`)

This guide maps every package under `internal/` (plus `pkg/client`) to its test
file and the scenarios that file asserts. Use it to find the right place to add
a test, or to understand what a package already guarantees.

Run any single package with the race detector:

```powershell
go test -race ./internal/<package>/
go test -race -run <TestName> -v ./internal/<package>/
```

---

## `cache/` — sharded engine + event stream

| File | Scenarios |
| --- | --- |
| `cache_test.go` | GET/SET/DEL/EXISTS, binary-safe values + defensive copy, TTL expiry, TTL reporting, INCR/DECR, MSET/MGET, LRU eviction, FlushAll, event replay via `ApplyEvent`, hit ratio, concurrent access (race) |
| `cache_depth_test.go` | Entry-count cap eviction (FIFO), memory-cap eviction (LRU), **active background expiry sweep**, `Keys()`, IncrBy on non-integer, MGet all-missing, `Export`/`ApplyEvent` TTL round-trip, FlushAll stats reset, zero-ops hit ratio |

> The active-expiry test uses the **real clock** (`cache.New` directly), not the
> injected-clock helper — injecting a clock after `New` starts the sweeper
> goroutine would race on the `clock` field under `-race`.

## `eviction/` — pluggable policies

| File | Scenarios |
| --- | --- |
| `eviction_test.go` | LRU order, FIFO insertion order, LFU least-frequent + LRU tie-break + remove, Random drains all distinctly, TTL nearest-expiry-first + updated expiry, `New()` factory + unknown-kind error |
| `eviction_depth_test.go` | `NoEviction` never yields a victim, `Remove` excludes a key on LRU/FIFO/Random |

## `persistence/` — AOF + snapshot + recovery

| File | Scenarios |
| --- | --- |
| `persistence_test.go` | AOF recovery (`always`), snapshot + compaction recovery, torn trailing record ignored |
| `persistence_depth_test.go` | `always` **and** `everysec` recover, `no` policy flushes on `Close`, snapshot boundary compaction shrinks the AOF (before/after `os.Stat`) |

## `replication/` — primary/replica async

| File | Scenarios |
| --- | --- |
| `replication_test.go` | Initial snapshot sync + live propagation + ack progress, FlushAll propagation |
| `replication_depth_test.go` | A **second replica joining later** gets a full snapshot, replica **disconnect → resync** (primary `NumReplicas` drops then recovers), per-replica `Replicas()` state (`Addr`, `ConnectedAt`, `LastSentSeq`, `LastAckSeq`) + `LastApplied`/`PrimaryAddr` |

> Helpers `startPrimary` returns `ln.Addr().String()` (never `p.Addr()`), because
> `p.ln` is set inside the `Serve` goroutine and would race. `waitFor` polls a
> condition with a 3s deadline.

## `cluster/` — registry + ownership/liveness

| File | Scenarios |
| --- | --- |
| `registry_test.go` | Ownership + replica set, liveness with an injected clock, node removal |

## `hashring/` — consistent hashing

| File | Scenarios |
| --- | --- |
| `hashring_test.go` | Deterministic mapping + full node coverage, empty ring, **minimal key movement** on add (< 40% remap 3→4 nodes), `GetN` distinct + more-than-available, node removal |
| `hashring_depth_test.go` | `Nodes()` sorted, idempotent `Add`, single-node routing, default vnode count (`New(0)`) |

## `locks/` — lease-based distributed locks

| File | Scenarios |
| --- | --- |
| `locks_test.go` | Acquire/release cycle, wrong-token release fails, **lease expiry reclaim**, renew extends lease, reentrant acquire supersedes old token |
| `locks_depth_test.go` | `AcquireBlocking` (free / held-then-wait / after-release / expired-reclaim) |

## `txn/` — MULTI/EXEC/WATCH

| File | Scenarios |
| --- | --- |
| `txn_test.go` | Queue + EXEC applies in order, WATCH conflict aborts, DISCARD, watch release |

## `pubsub/` — topic broker

| File | Scenarios |
| --- | --- |
| `pubsub_test.go` | Fan-out to multiple subscribers, topic isolation, unsubscribe cleanup, slow subscriber **drops** instead of blocking |
| `pubsub_depth_test.go` | `Topics()` listing, idempotent `Close` (double close safe), exact `Dropped` count |

## `resp/` — RESP2 codec

| File | Scenarios |
| --- | --- |
| `resp_test.go` | Array command parsing + binary-safe bulk, inline/telnet parsing, blank-line skipping, protocol errors, writer reply round-trip, `ReadReply` nested/nil arrays, `WriteCommand` encoding |

## `metrics/` — Prometheus exposition

| File | Scenarios |
| --- | --- |
| `metrics_test.go` | Histogram bucket placement + sum/count, full `WritePrometheus` output (counters, gauges, `cluster_node`, latency histogram, merged cache stats), valid exposition format |

## `server/` — RESP front-end

| File | Scenarios |
| --- | --- |
| `server_test.go` | Basic commands, counters, TTL, AUTH gating, MULTI/EXEC, pub/sub, locks |
| `server_commands_test.go` | ECHO/PERSIST/PTTL/PEXPIRE/DECR/DECRBY, MSET/MGET/KEYS/TYPE/DBSIZE/FLUSHALL, `SET EX/PX` expiry, **WATCH/EXEC abort + success over the wire**, SUBSCRIBE/UNSUBSCRIBE raw-wire routing, INFO/CLUSTER/COMMAND/RENEWLOCK/AUTH-wrong-password |

> The WATCH/EXEC abort test uses a `WATCH __barrier__` on the conflicting
> connection to force a `cache.Sync()` so the transaction coordinator observes
> the competing write **before** `EXEC` runs (`Exec` does not self-sync).

## `pkg/client/` — Go SDK

| File | Scenarios |
| --- | --- |
| `cluster_test.go` | 3-node routing distributes 300 keys, all readable, spread verified via per-node `DBSIZE` |
| `client_sdk_test.go` | `SetEX`/`TTL`, `Expire` on missing key + TTL `-1`/`-2` sentinels, `Lock` reentrancy + wrong token, `Publish`/multi-topic `Subscribe`, raw `Do` + `*client.Error` via `errors.As`, `ClusterClient` `SetEX`/`Del`/`Incr`/`OwnerOf`/`Nodes` + dial error |
