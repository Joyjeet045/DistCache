# Manual Test Scenarios (Step-by-step)

This guide captures the scenarios we ran together, with exact commands and expected output.

## Preconditions

1. Open PowerShell in the repository root.
2. Ensure Go is available:

```powershell
go version
```

If `go` is not found, use this in your session:

```powershell
$env:Path += ";C:\Program Files\Go\bin"
```

3. Build CLI once (recommended for timing-sensitive TTL tests):

```powershell
go build -o .\bin\cache-cli.exe ./cmd/cache-cli
```

## Port note

Port `6380` may already be in use on your machine. If so, run these local scenarios on `6390`.

Start server:

```powershell
go run ./cmd/cache-server -addr :6390 -metrics-addr :9130 -policy lru
```

If you want to check port conflicts:

```powershell
Get-NetTCPConnection -LocalPort 6380 -ErrorAction SilentlyContinue | Select-Object -First 5 LocalAddress,LocalPort,State,OwningProcess | Format-Table -AutoSize
```

## Scenario 1: Basic read/write correctness

Purpose: verify `SET/GET/EXISTS/DEL` behavior.

Run:

```powershell
.\bin\cache-cli.exe -addr 127.0.0.1:6390 SET k1 hello
.\bin\cache-cli.exe -addr 127.0.0.1:6390 GET k1
.\bin\cache-cli.exe -addr 127.0.0.1:6390 EXISTS k1
.\bin\cache-cli.exe -addr 127.0.0.1:6390 DEL k1
.\bin\cache-cli.exe -addr 127.0.0.1:6390 GET k1
```

Expected:

1. `SET k1 hello` -> `OK`
2. `GET k1` -> `"hello"`
3. `EXISTS k1` -> `(integer) 1`
4. `DEL k1` -> `(integer) 1`
5. `GET k1` -> `(nil)`

## Scenario 2: TTL and expiry behavior

Purpose: verify key expiry and TTL reporting.

### Important caveat

Using `go run` for each command can introduce compile delay that makes short TTL keys expire before the next command. Use the prebuilt CLI (`.\bin\cache-cli.exe`) for accurate TTL testing.

Run:

```powershell
.\bin\cache-cli.exe -addr 127.0.0.1:6390 SET session30 abc EX 30
.\bin\cache-cli.exe -addr 127.0.0.1:6390 TTL session30
.\bin\cache-cli.exe -addr 127.0.0.1:6390 SET sessionShort xyz PX 10
.\bin\cache-cli.exe -addr 127.0.0.1:6390 GET sessionShort
.\bin\cache-cli.exe -addr 127.0.0.1:6390 TTL sessionShort
.\bin\cache-cli.exe -addr 127.0.0.1:6390 GET session30
```

Expected:

1. `SET session30 abc EX 30` -> `OK`
2. `TTL session30` -> positive integer (for example `29`)
3. `SET sessionShort xyz PX 10` -> `OK`
4. `GET sessionShort` -> `(nil)` after quick expiry
5. `TTL sessionShort` -> `(integer) -2` (key not found)
6. `GET session30` -> `"abc"` (still alive)

## Scenario 3+: Distributed/integration scenarios

Purpose: replication, resync, persistence, pub/sub, locks, transactions, cluster info, and monitoring.

Use the existing integration playbook:

- `deploy/README.md`

It includes:

1. Replication propagation
2. Replication lag and role reporting
3. Replica restart and resync
4. Persistence and crash recovery
5. Cluster routing and ownership
6. Pub/Sub fan-out
7. Distributed locks and fencing tokens
8. WATCH/MULTI/EXEC optimistic transactions
9. Eviction under caps
10. Metrics/Prometheus/Grafana checks
11. AUTH gating

## Quick teardown

If you started local server in a terminal, stop it with `Ctrl+C`.

If you started Docker stack:

```powershell
docker compose -f deploy/docker-compose.yml down
```
