# QuorumDB

A replicated key-value store in Go built on a from-scratch Raft implementation. Writes and reads are linearizable, retried writes apply exactly once, and the cluster keeps serving through node crashes and network partitions as long as a majority is up and connected.

Peers talk over gRPC with Protobuf messages. Clients use a small HTTP/JSON API.

## Run a 3-node cluster

Requires Go 1.26+ on macOS or Linux.

```sh
go build -o bin/quorumdb ./cmd/quorumdb
go build -o bin/qdbctl ./cmd/qdbctl
CLUSTER=1=127.0.0.1:9001/127.0.0.1:8001,2=127.0.0.1:9002/127.0.0.1:8002,3=127.0.0.1:9003/127.0.0.1:8003
bin/quorumdb -id 1 -cluster $CLUSTER -data data/node1 &
bin/quorumdb -id 2 -cluster $CLUSTER -data data/node2 &
bin/quorumdb -id 3 -cluster $CLUSTER -data data/node3 &
```

Each member is `id=raftAddr/httpAddr`. Every node needs its own data directory, which is locked while the node runs.

```sh
bin/qdbctl put greeting hello            # finds the leader, retries through failover
bin/qdbctl get greeting
curl -L localhost:8001/v1/kv/greeting    # followers answer 307 to the leader
curl -L -X PUT localhost:8001/v1/kv/greeting -d '{"value":"hi"}'
curl localhost:8002/v1/status            # term, role, leader, commit/applied/snapshot index
```

`scripts/failover-demo.sh` builds the binaries and starts a cluster. While a client writes 150 keys, it sends SIGKILL to the leader. It then restarts the dead node, waits for it to catch up via snapshot, and reads back every acknowledged key. A typical run looks like this:

```
== node 3 killed (SIGKILL)
write 11: served by 127.0.0.1:8002 (took 1.141s)
150 writes acknowledged; 6 retries; slowest write 1.141s
== node 3 caught up: {"id":3,"term":2,"state":"follower","leader":2,"commit":152,"applied":152,...,"snapshot_index":150}
== all 150 acknowledged writes read back linearizably; demo passed
```

## Architecture

```
HTTP client API ─┐                         ┌─ gRPC stream to each peer (internal/transport)
                 ▼                         │
     internal/server: one event-loop goroutine owns everything below
                 │
     internal/node: persist → send → apply → answer requests
        │               │                 │
 internal/raft    internal/wal      internal/kv
 (pure consensus  (term/vote,       (map + client
  state machine)   log, snapshot)    session table)
```

- **`internal/raft`** is a deterministic Raft state machine with no I/O, goroutines, or clocks. Callers feed it `Tick`, `Step`, `Propose`, and `ReadIndex`, then handle a `Ready` and call `Advance`. It implements:
  - leader election with randomized timeouts and the up-to-date-log vote restriction
  - log replication with consistency checks and conflict truncation, using probe/pipelined modes and term-based backoff hints
  - commitment only of current-term entries (Raft §5.4.2), helped by a no-op entry from each new leader
  - **pre-vote**, so a node rejoining after a partition can't depose a healthy leader
  - **check-quorum**, so a leader cut off from a majority steps down
  - **ReadIndex** linearizable reads (thesis §6.4)
  - log compaction, and **InstallSnapshot** for followers behind the compacted log
- **`internal/wal`** stores Raft's durable state:
  - `state` holds the term and vote, replaced atomically
  - `snapshot` holds the latest snapshot, replaced atomically
  - `wal.log` is an append-only, CRC32C-framed entry log that records the snapshot it follows
  - every change is fsynced before any message that depends on it is sent
  - on restart, a torn final record is truncated, while damage earlier in the log refuses startup
  - a crash between writing a snapshot and rewriting the log is reconciled on load
- **`internal/node`** connects the pieces. It persists each `Ready`, sends messages, applies committed entries, and completes requests. A write succeeds only when its entry is applied at the index and term where it was proposed. Losing leadership fails pending requests as *outcome unknown*, which is safe to retry. It snapshots the state machine every `-snapshot-entries` applied entries.
- **`internal/kv`** is the replicated state machine. A session table records the last applied sequence number for each client ID, so a delayed or retried write never applies twice.
- **`internal/transport`** keeps one long-lived gRPC client stream (`proto/raft.proto`) to each peer and batches queued messages. Delivery is best effort: it drops messages on overload or disconnect, and Raft retransmits.
- **`internal/server`** runs a single event loop. Each loop iteration folds every queued message and request into one fsync. The HTTP API redirects to the leader, maps errors to retryable statuses, and accepts `X-Client-Id` / `X-Request-Seq` headers for exactly-once writes.
- **`internal/client`** (used by `qdbctl`) finds the leader, follows redirects, and retries with backoff. It reuses the same sequence number across retries.

## Guarantees

- **Linearizable writes and reads.** Reads go through ReadIndex on the leader: before answering, it confirms with a quorum that it is still leader and waits until it has applied the commit index. Safety never depends on clock accuracy.
- **Exactly-once writes** for clients that send a client ID and increasing sequence numbers, one request at a time. `qdbctl` and `internal/client` do this. Plain `curl` writes have no session, so a retry may apply twice.
- **Durability.** A write is acknowledged only after a majority has fsynced it.
- **Availability.** With `2f+1` nodes, the cluster tolerates `f` crashed or partitioned nodes. With the default 50ms tick, failover takes roughly 0.5–1.5s.

## Testing

```sh
go vet ./...
go test -race ./...
QUORUMDB_SEEDS=1000 go test ./internal/sim -run Linearizable   # larger seed sweep
scripts/failover-demo.sh
```

- **Raft unit tests** (`internal/raft`) cover:
  - elections and replication on 1, 3, and 5 nodes
  - failover and check-quorum
  - pre-vote preventing disruption
  - Figure 7 divergent logs converging
  - the Figure 8 commit rule
  - the vote restriction, snapshot catch-up, and ReadIndex quorum
- **WAL tests** (`internal/wal`) cover:
  - truncation and reload
  - a torn tail
  - corrupt interior records
  - compaction and snapshot install
  - a crash between the snapshot write and the log rewrite
  - the directory lock
- **Deterministic simulation** (`internal/sim`) runs 3- and 5-node clusters in a single-threaded, seeded discrete-event simulator with 5 concurrent clients.
  - Faults: random partitions, leader isolation, crash/restart with recovery from persisted state, process pauses (e.g. GC or SIGSTOP), message loss up to 25%, duplication, long delays that reorder messages, and clock drift.
  - Every run checks four things:
    - at most one leader per term
    - all nodes apply identical entries at each index
    - after healing, the cluster recovers and replicas converge
    - the full client history is linearizable according to [Porcupine](https://github.com/anishathalye/porcupine)
  - A failing seed reproduces exactly and writes a Porcupine visualization.
  - Results so far: 22,000+ seed runs passed (20,000 in one sweep, 600 under `-race`).
- **Real-cluster tests** (`internal/server`) start 3 nodes with the gRPC transport and on-disk WAL:
  - stop the leader mid-write; the stopped node restarts and catches up via snapshot
  - restart the whole cluster
  - check that a leader without its peers refuses reads and writes
  - check a Porcupine history of ~1,100 operations while nodes are repeatedly stopped and restarted
- **Mutation check.** To show the checks have teeth, each bug below was injected and run against 200 simulation seeds. Every one was caught:

  | Injected bug | Seeds failing |
  |---|---|
  | serve reads locally without ReadIndex | 200 |
  | heartbeat advances follower commit past its known match | 200 |
  | disable session deduplication | 170 |
  | don't persist term/vote | 24 |
  | serve reads before the leader's no-op commits | 18 |
  | confirm reads without a quorum | 14 |
  | commit prior-term entries by counting replicas (Figure 8) | 3 |
  | grant votes without the up-to-date-log check | 1 |

  Rare bugs such as the last two are caught by the dedicated unit tests regardless.

## Limitations

- **Static membership.** There is no joint-consensus reconfiguration or leader transfer.
- **No security.** There is no TLS or authentication on either port. The defaults bind to loopback.
- **Whole-state snapshots.** The state machine lives in memory, and each snapshot is a full JSON image. InstallSnapshot sends it as one gRPC message, capped at 256 MiB, rather than in chunks.
- **Session table never shrinks.** It keeps one entry per client ID forever; there is no expiry.
- **Reads go through the leader.** Each costs a heartbeat round. There are no lease-based or follower reads.
- **Simulation storage is in memory.** It is crash-consistent but doesn't model torn or lost disk writes. Those are covered by the WAL tests. Power-loss durability ultimately depends on the filesystem and hardware honouring fsync.
- **Not benchmarked.** There are no throughput or latency figures beyond the failover timing shown above.

Reference: [Raft extended paper](https://raft.github.io/raft.pdf), [Raft thesis](https://github.com/ongardie/dissertation).
