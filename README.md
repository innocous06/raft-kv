# Raft-KV — Distributed Consensus Key-Value Store in Go

> A clean, tested implementation of the Raft consensus algorithm in Go, featuring Write-Ahead Logging (WAL) with CRC32 torn-write recovery, snapshot compaction, state-machine deduplication, fault injection testing, and in-repo linearizability checking.

[![Interactive Simulator](https://img.shields.io/badge/Interactive%20Simulator-GitHub%20Pages-c28f2c.svg)](https://innocous06.github.io/raft-kv/)
[![Go Version](https://img.shields.io/badge/Go-1.22+-2b2823.svg?logo=go)](https://golang.org)
[![Test Suite](https://img.shields.io/badge/Tests-22%2F22%20Passing%20(1%2C000%20Seeded%20Runs)-2e4c23.svg)](#6-verification-matrix--chaos-testing)
[![License: MIT](https://img.shields.io/badge/License-MIT-8c857b.svg)](LICENSE)

---

## 1. Overview & Problem Statement

Building replicated distributed systems requires coordinating state across nodes in the face of network latency, dropped packets, arbitrary message reordering, network partitions, and node crashes.

Raft-KV implements the Raft consensus protocol ([Ongaro & Ousterhout, 2014](https://raft.github.io/raft.pdf)) in Go, providing strongly consistent key-value storage.

### Consistency Hazards Addressed
* **Split-Brain under Partitions:** When network partitions divide a cluster, uncoordinated groups could each elect a leader and accept conflicting mutations. Raft prevents this by requiring a strict majority quorum ($Q = \lfloor N/2 \rfloor + 1$) for both elections and log replication.
* **Commit Index Bounds Clamping:** Followers clamp `commitIndex = min(leaderCommit, indexOfLastNewEntry)` and enforce monotonic advancement to prevent out-of-order heartbeats or truncated logs from corrupting follower commit state.
* **Torn Writes on Crash:** Disk writes interrupted by power loss or process crashes produce partial records at the log tail. The storage engine uses framed records with IEEE CRC32 checksums to detect and truncate torn tail bytes upon restart.
* **Non-Linearizable Concurrency:** Network retries and out-of-order message delivery risk duplicate executions or stale reads. Client deduplication tables (`ClientID` + `SeqNum`) and monotonic commit application ensure sequential consistency.

```
       [ Client Request ]
               │
               ▼
┌───────────────────────────────┐
│     HTTP / REST API Layer     │  ◄── Deduplication (ClientID + SeqNum) & Quorum Routing
└──────────────┬────────────────┘
               │
               ▼
┌───────────────────────────────┐
│    Raft Actor Core Loop       │  ◄── Single-Threaded Mutex-Free State Guard
│  (Election, Replication, WAL) │  ◄── Strict Monotonic Commit Index Progression
└──────────────┬────────────────┘
               │
               ▼
┌───────────────────────────────┐
│    Replicated State Machine   │  ◄── Sequential FIFO Commit Applier
│       (In-Memory KV Store)    │  ◄── Synchronous State Cloning with Background Disk Snapshotting
└───────────────────────────────┘
```

---

## 2. Safety Invariants & Consensus Theory

Raft guarantees correctness by maintaining five core invariants across all execution paths:
1. **Election Safety:** At most one leader can be elected in a given term.
2. **Leader Append-Only:** A leader never overwrites or truncates its own log entries; it only appends new entries.
3. **Log Matching:** If two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index.
4. **Leader Completeness:** If a log entry is committed in a given term, that entry is present in the logs of the leaders for all higher-numbered terms.
5. **State Machine Safety:** If a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index.

For the formal mathematical specifications, Quorum Intersection mechanics, and the inductive proof of Leader Completeness, see [`docs/CONSENSUS_SPEC.md`](docs/CONSENSUS_SPEC.md).

### Linearizability Checking (Wing & Gong 1993)
Raft-KV includes an **in-repo implementation of the Wing & Gong (1993) sequential consistency search algorithm** (`harness/linearizability.go`), inspired by Porcupine.

*Complexity Note:* Linearizability checking is NP-complete in general (Gibbons & Korach, 1997). The checker explores valid execution paths for concurrent operations up to 100 to 300 operations during chaos scenarios, ensuring that observed read/write orders match sequential register semantics.

---

## 3. Architecture & Design Decisions

### 1. Actor Event Loop
All mutable Raft consensus state (term, votedFor, log entries, commit index, role transitions) resides inside a single goroutine event loop (`n.run()` in `internal/raft/node.go`). Outside RPC calls and client requests communicate via Go channels, avoiding lock inversion and mutex deadlocks.

### 2. Sequential FIFO Log Application
To ensure that entries are applied strictly in log order, commits are pushed to a synchronized FIFO queue (`applyQueue`) governed by `sync.Cond` and consumed by a sequential applier goroutine. Client waiter channels are backed by a ring cache of recently applied results to handle fast commits that complete before waiter registration.

### 3. Write-Ahead Log (WAL) & Crash Recovery
* **Framing Format:** `[Length: 4B][CRC32: 4B][Payload: NB]`
* **Torn-Write Recovery:** On restart, `storage.DiskStorage` reads framed records and validates IEEE CRC32 checksums. If a crash produced a partial header (< 8 bytes) or corrupt payload at the tail, the file is safely truncated to the last valid boundary.
* **Allocation Bounds:** Single WAL record lengths are capped at 32 MB, snapshot headers at 16 MB, and snapshot payloads at 256 MB to prevent corrupted length fields from triggering out-of-memory panics.

### 4. Snapshot Compaction with State Isolation
When the log reaches the compaction threshold:
1. The state machine acquires its read/write lock and creates a point-in-time clone of its in-memory key-value map and deduplication table (`SnapshotData`).
2. The lock is immediately released, allowing new client requests to proceed.
3. A background goroutine marshals the cloned state and writes it to disk, notifying the Raft core to advance `lastIncludedIndex` and truncate discarded log entries.

### 5. Pluggable Transport Subsystem
* **`SimNet` (Testing & Chaos):** In-memory simulated network supporting packet drops, message duplication, network partitions, and per-node asymmetric delay injection without OS network overhead.
* **HTTP Transport:** REST RPC layer running over HTTP/1.1 with connection reuse and `http.MaxBytesReader` protection against oversized payloads.

---

## 4. HTTP REST API Specification

All endpoints communicate via standard JSON over HTTP.

| Method | Endpoint | Description | Request Body / Parameters | Response Status |
| :--- | :--- | :--- | :--- | :--- |
| `POST` | `/api/v1/kv/put` | Submit replicated key-value mutation | `{"key":"k","value":"v","clientId":"c1","seqNum":1}` | `200 OK` / `400 Bad Request` / `409 Conflict` |
| `GET` | `/api/v1/kv/get` | Retrieve value for key | Query: `?key=name` | `200 OK` / `400 Bad Request` / `404 Not Found` |
| `POST` / `DELETE` | `/api/v1/kv/delete` | Delete key from state machine | `{"key":"k","clientId":"c1","seqNum":2}` | `200 OK` / `400 Bad Request` |
| `GET` | `/api/v1/kv/all` | Retrieve all committed key-value pairs | None | `200 OK` / `405 Method Not Allowed` |
| `GET` | `/api/v1/cluster/status` | Introspect telemetry for all cluster nodes | None | `200 OK` / `405 Method Not Allowed` |
| `GET` | `/api/v1/events/stream` | Real-time SSE telemetry event stream | None | `200 OK` (`text/event-stream`) |
| `POST` | `/api/v1/chaos/isolate_leader` | Disconnect current leader into minority | None | `200 OK` / `405 Method Not Allowed` |
| `POST` | `/api/v1/chaos/partition` | Create asymmetric network partition (2 vs 3) | None | `200 OK` / `405 Method Not Allowed` |
| `POST` | `/api/v1/chaos/heal` | Heal network and restore full connectivity | None | `200 OK` / `405 Method Not Allowed` |
| `POST` | `/api/v1/chaos/kill` | Crash a target node process | Query: `?node=node-1` or JSON `{"node":"node-1"}` | `200 OK` / `404 Not Found` |
| `POST` | `/api/v1/chaos/restart` | Restart a stopped node from disk state | Query: `?node=node-1` or JSON `{"node":"node-1"}` | `200 OK` / `400 Bad Request` |

---

## 5. Build & Execution

### Prerequisites
* Go 1.22+ (tested on Go 1.26)

### Building Binaries

**On Linux / macOS:**
```bash
go build -o raft-node ./cmd/node
go build -o raft-chaos ./cmd/chaos
```

**On Windows (PowerShell):**
```powershell
go build -o raft-node.exe ./cmd/node
go build -o raft-chaos.exe ./cmd/chaos
```

### Running an In-Process 5-Node Cluster

Launch a 5-node cluster running inside a single process with embedded web telemetry:

```bash
# Linux / macOS
./raft-node -cluster=5 -port=8001

# Windows
.\raft-node.exe -cluster=5 -port=8001
```

Access the local dashboard at `http://127.0.0.1:8001`.

### Running Standalone Multi-Process Nodes

Run independent OS processes communicating over HTTP transport:

```bash
# Terminal 1 (Node 1)
./raft-node -id=node-1 -port=8001 -peers=node-2=http://127.0.0.1:8002,node-3=http://127.0.0.1:8003 -data=./data/n1

# Terminal 2 (Node 2)
./raft-node -id=node-2 -port=8002 -peers=node-1=http://127.0.0.1:8001,node-3=http://127.0.0.1:8003 -data=./data/n2

# Terminal 3 (Node 3)
./raft-node -id=node-3 -port=8003 -peers=node-1=http://127.0.0.1:8001,node-2=http://127.0.0.1:8002 -data=./data/n3
```

---

## 6. Verification Matrix & Chaos Testing

The test suite runs with zero external dependencies via `go test`:

```bash
go test -v -count=1 ./harness
```

### Test Suite Results (22 / 22 Passing)

| Test Identifier | Category | Scenario & Invariants Checked | Duration |
| :--- | :--- | :--- | :--- |
| `TestScenario1_ElectionStability` | Core Consensus | Solitary leader elected within election timeout; Election Safety preserved | 0.44s |
| `TestScenario2_ReelectionOnLeaderFailure` | Fault Tolerance | Leader killed; replacement leader elected by surviving quorum | 0.26s |
| `TestScenario3_LogReplication` | Replication | Entry replication and commit progression across 3 nodes | 0.16s |
| `TestScenario4_KillLeaderUnderWriteLoad` | Churn | Consecutive leader crashes under continuous write traffic | 0.45s |
| `TestScenario5_NetworkPartitionPartitionLeader` | Network Partition | Majority/minority split; isolated leader cannot commit; healed and unified | 0.70s |
| `TestScenario6_SlowFollowerSnapshotCatchUp` | Log Compaction | Disconnected follower catches up via `InstallSnapshot` RPC | 0.63s |
| `TestScenario7_RollingRestart` | Disk Persistence | Sequential restart of all nodes; state restored from WAL with zero data loss | 1.24s |
| `TestScenario8_ChaosTestingAndLinearizability` | Verification | 90 chaotic operations verified against Wing & Gong sequential specification | 6.16s |
| `TestEdgeCase_EmptyKeyHandling` | Boundary | Rejection of empty keys on PUT, GET, and DELETE | 0.14s |
| `TestEdgeCase_DeduplicationAndIdempotency` | State Machine | Duplicate sequence numbers return cached results without re-execution | 0.14s |
| `TestEdgeCase_MalformedCommandHandling` | Resilience | Corrupt log payloads handled without stalling `lastApplied` progression | 0.46s |
| `TestEdgeCase_NonExistentNodeChaos` | Error Handling | Safe rejection of invalid node crash/restart requests | 0.00s |
| `TestEdgeCase_AsymmetricPartition` | Network Fault | One-way link drops resolved without split brain or invariant violation | 1.10s |
| `TestEdgeCase_HTTPAPIRoutes` | HTTP Protocol | Complete route suite testing methods, 404s, and malformed bodies | 0.19s |
| `TestEdgeCase_HeavyConcurrentWritesAndReads` | Concurrency | 200 concurrent read/write ops across 8 workers; thread safety verified | 0.14s |
| `TestGrill_MassivePayloadStress` | Stress | Replicating and snapshotting 286 KB payloads; byte-level matching | 0.18s |
| `TestGrill_CorruptWALRecovery` | Crash Recovery | Truncating torn 3-byte headers, corrupt CRC entries, and 4 GB claimed lengths | 0.02s |
| `TestGrill_HTTPRouteBoundaryAndOversizedRejection` | Boundary | 5 MB body rejection (`http.MaxBytesReader`), whitespace keys, method checks | 0.69s |
| `TestGrill_ConcurrentDeduplicationRace` | Race Condition | 25 concurrent requests with identical `(ClientID, Seq)` yield 1 execution | 0.12s |
| `TestGrill_NetworkFlappingUnderWriteStorm` | Chaos | 164 writes during rapid split-brain flapping every 45ms; zero invariant breaks | 1.49s |
| `TestGrill_CascadingNodeCrashAndRecovery` | Quorum Loss | 3 nodes killed, proposals blocked; 2 nodes revived, quorum recovered | 0.87s |
| `TestChaos_MultiSeedFuzzing` | Multi-Seed | 30 deterministic seeds with artificial delays and invariant verification | 6.13s |
| `TestChaos_1000SeededRuns` | Scale Chaos | 1,000 parallel seeded chaos runs under randomized delays and churn | 6.57s |

**Result:** Passes 1,000 seeded chaos runs with 0 safety invariant violations.

---

## 7. Performance Benchmarks & Empirical Measurements

Benchmarks were executed on an AMD Ryzen 5 5600H (12 vCPUs) running Go 1.26 on Windows 11.

### A. Throughput & Allocation Matrix (`go test -bench=. -benchmem ./harness`)

| Benchmark Scenario | Iterations | Latency (ns/op) | Throughput | Memory (B/op) | Allocations |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `WriteThroughput_3Nodes` | 500 ops | 42,895 ns/op | ~23,312 writes/sec | 15,682 B/op | 131 allocs/op |
| `WriteThroughput_5Nodes` | 500 ops | 71,282 ns/op | ~14,028 writes/sec | 26,596 B/op | 215 allocs/op |
| `ReadThroughput` | 1,000 ops | 47,881 ns/op | ~20,885 reads/sec | 14,213 B/op | 113 allocs/op |

### B. Write Throughput vs. Quorum Scale

```
Cluster Size    Throughput (ops/sec)
3 Nodes         [========================================] 23,312 ops/sec (42.9 µs/op)
5 Nodes         [========================]                 14,028 ops/sec (71.3 µs/op)
```

### C. Election Convergence Latency
Measured across 5 randomized election cycles per cluster topology:
* **3-Node Cluster Average:** `122.0 ms`
* **5-Node Cluster Average:** `122.2 ms`

---

## 8. Formal TLA+ Model Specifications

The repository includes a formal TLA+ specification of the Raft consensus safety kernel:

* **Core Protocol Specification:** [`specs/Raft.tla`](specs/Raft.tla)
  * Defines state spaces: `currentTerm`, `state`, `votedFor`, `log`, `commitIndex`, and in-transit `messages`.
  * Specifies state actions: `Timeout`, `HandleRequestVoteRequest`, `BecomeLeader`, `ClientRequest`, `HandleAppendEntriesRequest`, and `AdvanceCommitIndex`.
  * Encodes safety invariants: `ElectionSafety`, `LogMatching`, and `LeaderCompleteness`.
* **TLC Model Checking Configuration:** [`specs/MC.tla`](specs/MC.tla) & [`specs/MC.cfg`](specs/MC.cfg)
  * Model-checked with the TLC Model Checker across 31,645 states to depth 8 with 0 invariant violations.

To verify with TLC:
```bash
cd specs
java -cp tla2tools.jar tlc2.TLC -dfid 8 -config MC.cfg MC.tla
```

---

## 9. Bug Log & Root Cause Analysis

A central part of engineering consensus protocols is surfacing and documenting edge cases found during testing. The full history of 18 bugs identified by the test harness and resolved is documented in [`docs/bug-log.md`](docs/bug-log.md).

### Notable Bugs Caught by Harness
1. **Commit Index Bounds on Heartbeats (§5.3):** Follower `commitIndex` computed as `min(leaderCommit, len(entries))` on empty heartbeats could regress backwards if `PrevLogIndex < commitIndex`. Resolved by clamping heartbeats to `n.log.LastIndex()` and enforcing monotonic advancement.
2. **Out-of-Order Entry Application via Detached Goroutines:** Saturated `applyCh` spawned background goroutines that reordered applied entries under OS thread scheduling. Resolved with a thread-safe FIFO queue governed by `sync.Cond`.
3. **Duplicate Vote Counting on Retransmissions (§5.2):** `votesReceived` was an integer counter, allowing retransmitted votes to elect a leader with a minority. Replaced with `votesGranted map[string]bool` for idempotent vote counting.
4. **Snapshot Boundary Term Check (`>` vs `>=`):** AppendEntries skipped `prevLogTerm` check when `PrevLogIndex` landed exactly on `lastIncludedIndex`. Resolved by extending check to `>=`.
5. **Proposal Waiter Registration Race:** Fast commits completed before `Execute()` registered its channel in `waiters`. Resolved with an applied-results ring cache.
6. **Async Snapshot State Divergence:** Background goroutine acquired `RLock()` asynchronously, allowing subsequent commands to mutate state before serialization. Resolved with synchronous state cloning under lock.
7. **Torn WAL Header Truncation Leak:** Partial header reads (< 8 bytes) broke the read loop without truncating trailing corrupt bytes from disk. Resolved by truncating `wal.log` to `startOffset` upon any partial read.

See [`docs/bug-log.md`](docs/bug-log.md) for full reproduction steps and commit references.

---

## 10. Limitations & Non-Goals

To maintain clarity of scope, this implementation intentionally omits several features required for multi-tenant production deployments:

* **No Dynamic Membership Changes (Raft §6):** Cluster membership is fixed at configuration time. Joint consensus and single-server reconfiguration are not implemented.
* **No Pre-Vote Protocol (§9.6):** Partitioned nodes that reconnect with higher terms can force unnecessary re-elections upon rejoining the cluster.
* **No Network Encryption / Authentication:** RPC communication uses plain JSON over HTTP and does not include TLS/mTLS or token-based authentication.
* **In-Memory State Machine:** Key-value pairs are stored in memory with periodic disk snapshots, rather than on-disk B-trees or LSM-trees. Dataset size is bounded by available RAM.
* **Linearizability Checker Scalability:** The Wing & Gong search algorithm is NP-complete. Checking histories is practical for ~100 to 300 operations during chaos tests; verifying millions of operations requires pruning heuristics or interval tree approximations.
* **Single-Host Network Emulation for Testing:** The chaos harness uses in-process goroutines and memory channels (`SimNet`) to simulate partitions and delays; real multi-host deployments run via the HTTP binary.

---

## 11. Interactive Browser Simulator

An interactive visualization of Raft consensus is hosted via GitHub Pages:

* **Live Simulator:** [https://innocous06.github.io/raft-kv/](https://innocous06.github.io/raft-kv/)

*Clarification:* This is a visualization written in JavaScript that simulates the protocol; the real implementation is the Go code.

---

## 12. License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
