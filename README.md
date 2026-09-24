# Raft-KV - Distributed Consensus Key-Value Store in Go

> An implementation of the Raft consensus algorithm in Go with disk persistence, CRC32 torn-write recovery, snapshot compaction, client request deduplication, randomized fault injection testing, bounded TLA+ model checking, and an in-repo linearizability checker.

[![Go Version](https://img.shields.io/badge/Go-1.22+-2b2823.svg?logo=go)](https://golang.org)
[![Test Suite](https://img.shields.io/badge/Tests-25%2F25%20Passing%20(1%2C000%20Randomized%20Runs)-2e4c23.svg)](#5-testing-approach--chaos-matrix)
[![Interactive Simulator](https://img.shields.io/badge/Simulator-GitHub%20Pages-c28f2c.svg)](https://innocous06.github.io/raft-kv/)
[![License: MIT](https://img.shields.io/badge/License-MIT-8c857b.svg)](LICENSE)

---

## 1. Real Cluster Execution Output

```text
$ ./raft-node -cluster=3 -port=8001

[INFO] Initializing In-Process Raft KV Cluster (3 nodes)...
[LEADER] Initial leader elected: node-1 (term=1)
[HTTP] Live Dashboard and REST API listening at: http://127.0.0.1:8001

$ curl -X POST http://127.0.0.1:8001/api/v1/kv/put \
    -H "Content-Type: application/json" \
    -d '{"key":"alpha","value":"consensus-value","clientId":"c1","seqNum":1}'
{"success":true,"term":1,"index":1,"leaderId":"node-1","data":"consensus-value"}

$ curl http://127.0.0.1:8001/api/v1/kv/get?key=alpha
{"success":true,"key":"alpha","value":"consensus-value"}
```

---

## 2. Overview & Problem Statement

Distributed key-value stores must maintain consistent state across multiple servers despite network latency, dropped packets, arbitrary message reordering, partitions, and process crashes.

Raft-KV implements the Raft consensus protocol (Ongaro & Ousterhout, 2014) in Go, providing linearizable key-value storage with atomic disk state replacement, bounded memory allocations, and automated crash recovery.

---

## 3. Quick Start

### Build Binaries

```bash
# Linux / macOS
go build -o raft-node ./cmd/node
go build -o raft-chaos ./cmd/chaos

# Windows (PowerShell)
go build -o raft-node.exe ./cmd/node
go build -o raft-chaos.exe ./cmd/chaos
```

### Run an In-Process Cluster

Launch a 3-node or 5-node cluster inside a single process with embedded web telemetry:

```bash
# Linux / macOS
./raft-node -cluster=3 -port=8001

# Windows
.\raft-node.exe -cluster=3 -port=8001
```

Access the dashboard and REST API at `http://127.0.0.1:8001`.

### Run Standalone Multi-Process Nodes

Run independent operating system processes communicating over HTTP transport:

```bash
# Node 1
./raft-node -id=node-1 -port=8001 -peers=node-2=http://127.0.0.1:8002,node-3=http://127.0.0.1:8003 -data=./data/n1

# Node 2
./raft-node -id=node-2 -port=8002 -peers=node-1=http://127.0.0.1:8001,node-3=http://127.0.0.1:8003 -data=./data/n2

# Node 3
./raft-node -id=node-3 -port=8003 -peers=node-1=http://127.0.0.1:8001,node-2=http://127.0.0.1:8002 -data=./data/n3
```

---

## 4. How It Works: Architecture & Key Decisions

For comprehensive architecture diagrams, data flows, and failure matrices, see [`docs/design.md`](docs/design.md). Architectural alternatives and trade-offs are logged in [`docs/decisions.md`](docs/decisions.md).

```text
       [ Client Request ]
               │
               ▼
┌───────────────────────────────┐
│     HTTP / REST API Layer     │  <-- Deduplication (ClientID + SeqNum) & Quorum Routing
└──────────────┬────────────────┘
               │
               ▼
┌───────────────────────────────┐
│    Raft Actor Core Loop       │  <-- Single-Threaded Mutex-Free State Guard
│  (Election, Replication, WAL) │  <-- Strict Monotonic Commit Index Progression
└──────────────┬────────────────┘
               │
               ▼
┌───────────────────────────────┐
│    Replicated State Machine   │  <-- Sequential FIFO Commit Applier (sync.Cond)
│       (In-Memory KV Store)    │  <-- Synchronous State Cloning with Background Disk Snapshotting
└───────────────────────────────┘
```

### Core Design Decisions

1. **Actor Event Loop:** All mutable Raft state resides inside a single goroutine event loop (`n.run()`), eliminating lock-ordering deadlocks and mutex contention. Outside RPC calls communicate strictly via typed Go channels.
2. **Sequential FIFO Log Application:** Commits are pushed to a synchronized FIFO queue governed by `sync.Cond` and consumed by a dedicated applier goroutine, preventing out-of-order execution under thread scheduling.
3. **Persistence with Torn-Write Recovery:** Log entries use framed records `[Length: 4B][CRC32: 4B][Payload: NB]`. On startup, corrupt headers (<8B) or CRC mismatches are truncated to the last valid boundary.
4. **Crash-Stop Storage Semantics:** Disk write errors trigger immediate node termination and in-memory log rollback, preventing nodes from operating with unpersisted state.
5. **Linearizable Deduplication:** The state machine tracks client sequence numbers `(ClientID -> {LastSeq, Response})`, ensuring exact-once execution for retried mutations.
6. **In-Repo Linearizability Checker:** Includes a pure-Go implementation of the Wing & Gong (1993) search algorithm (`harness/linearizability.go`) to verify sequential register consistency during chaos tests.

---

## 5. Testing Approach & Chaos Matrix

The test suite runs with zero external dependencies via standard `go test`:

```bash
go test -v -count=1 ./harness
```

### Verification Matrix (25 / 25 Passing)

| Test Identifier | Category | Scenario & Invariants Checked | Duration |
| :--- | :--- | :--- | :--- |
| `TestScenario1_ElectionStability` | Core Consensus | Solitary leader elected within election timeout; Election Safety preserved | 0.44s |
| `TestScenario2_ReelectionOnLeaderFailure` | Fault Tolerance | Leader killed; replacement leader elected by surviving quorum | 0.26s |
| `TestScenario3_LogReplication` | Replication | Entry replication and commit progression across 3 nodes | 0.16s |
| `TestScenario4_KillLeaderUnderWriteLoad` | Churn | Consecutive leader crashes under continuous write traffic | 0.45s |
| `TestScenario5_NetworkPartitionPartitionLeader` | Network Partition | Majority/minority split; isolated leader cannot commit; healed and unified | 0.70s |
| `TestScenario6_SlowFollowerSnapshotCatchUp` | Log Compaction | Disconnected follower catches up via InstallSnapshot RPC | 0.63s |
| `TestScenario7_RollingRestart` | Disk Persistence | Sequential restart of all nodes; state restored from disk with zero data loss | 1.24s |
| `TestScenario8_ChaosTestingAndLinearizability` | Linearizability | 90 chaotic operations checked against Wing & Gong sequential specification | 6.16s |
| `TestEdgeCase_EmptyKeyHandling` | Boundary | Rejection of empty keys on PUT, GET, and DELETE | 0.14s |
| `TestEdgeCase_DeduplicationAndIdempotency` | State Machine | Duplicate sequence numbers return cached results without re-execution | 0.14s |
| `TestEdgeCase_MalformedCommandHandling` | Resilience | Corrupt log payloads handled without stalling lastApplied progression | 0.46s |
| `TestEdgeCase_NonExistentNodeChaos` | Error Handling | Safe rejection of invalid node crash/restart requests | 0.00s |
| `TestEdgeCase_AsymmetricPartition` | Network Fault | One-way link drops resolved without split brain or invariant violation | 1.10s |
| `TestEdgeCase_HTTPAPIRoutes` | HTTP Protocol | Complete route suite testing methods, 404s, and malformed bodies | 0.19s |
| `TestEdgeCase_HeavyConcurrentWritesAndReads` | Concurrency | 200 concurrent read/write ops across 8 workers; thread safety confirmed | 0.14s |
| `TestEdgeCase_StorageFailureProtection` | Storage Failure | Vote grant refusal, append refusal, and immediate crash-stop on I/O error | 0.10s |
| `TestGrill_MassivePayloadStress` | Stress | Replicating and snapshotting 286 KB payloads; byte-level matching | 0.18s |
| `TestGrill_CorruptWALRecovery` | Crash Recovery | Truncating torn 3-byte headers, corrupt CRC entries, and 4 GB claimed lengths | 0.02s |
| `TestGrill_HTTPRouteBoundaryAndOversizedRejection` | Boundary | 5 MB body rejection (http.MaxBytesReader), whitespace keys, method checks | 0.69s |
| `TestGrill_ConcurrentDeduplicationRace` | Race Condition | 25 concurrent requests with identical (ClientID, Seq) yield 1 execution | 0.12s |
| `TestGrill_NetworkFlappingUnderWriteStorm` | Chaos | 164 writes during rapid split-brain flapping every 45ms; zero invariant breaks | 1.49s |
| `TestGrill_CascadingNodeCrashAndRecovery` | Quorum Loss | 3 nodes killed, proposals blocked; 2 nodes revived, quorum recovered | 0.87s |
| `TestChaos_MultiSeedFuzzing` | Multi-Seed | 30 deterministic seeds with artificial delays and invariant verification | 6.13s |
| `TestChaos_1000SeededRuns` | Scale Chaos | 1,000 parallel randomized chaos runs under randomized delays and churn | 6.57s |

**Result:** Passes 1,000 randomized chaos runs with 0 safety invariant violations under real timers and wall-clock RNG. Full failure handling behavior is detailed in [`docs/design.md`](docs/design.md).

---

## 6. Performance Benchmarks

Benchmarks were measured on an AMD Ryzen 5 5600H (12 vCPUs) running Go 1.22 on Windows 11 (`go test -bench=. -benchmem ./harness`).

| Benchmark Scenario | Storage Mode | Iterations | Latency (ns/op) | Throughput | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `WriteThroughput_3Nodes` | In-Memory (SimNet) | 500 ops | 48,721 ns/op | ~20,525 writes/sec | Pure actor loop and consensus overhead |
| `WriteThroughput_5Nodes` | In-Memory (SimNet) | 500 ops | 67,218 ns/op | ~14,876 writes/sec | 5-node actor loop overhead |
| `WriteThroughput_Disk_3Nodes` | Disk WAL (fsync) | 100 ops | 8,394,062 ns/op | ~119 writes/sec | Synchronous disk fsync with CRC32 verification |
| `ReadThroughput` | In-Memory (SimNet) | 1,000 ops | 47,881 ns/op | ~20,885 reads/sec | Linearizable read path |

*Context:* In-memory benchmarks measure pure event loop scheduling and consensus message passing. Disk-backed benchmarks reflect synchronous per-commit fsync overhead on local physical storage.

---

## 7. Bounded TLA+ Model Specifications

The formal TLA+ protocol safety kernel is evaluated via bounded model checking:

* **Core Protocol Specification:** [`specs/Raft.tla`](specs/Raft.tla)
* **Bounded Model Configuration:** [`specs/MC.tla`](specs/MC.tla) & [`specs/MC.cfg`](specs/MC.cfg)
  * Explored with the TLC Model Checker across 31,645 states to depth 8 with 0 invariant violations (constants: 3 servers, max term 3, max log length 3).

---

## 8. Bug Log & Root Cause Analysis

A central part of engineering consensus protocols is surfacing and documenting edge cases found during testing and review. All 27 bugs are documented in [`docs/bug-log.md`](docs/bug-log.md).

Key issues identified and fixed include:
1. **Heartbeat commitIndex bounds:** Prevented commit index backwards regression on empty AppendEntries.
2. **FIFO entry applier:** Resolved out-of-order state application under channel backpressure with `sync.Cond`.
3. **Idempotent vote accounting:** Replaced counter with map to prevent duplicate vote counting on RPC retransmission.
4. **votedFor preservation:** Fixed premature vote clearing during same-term step-downs.
5. **Fail-stop persistence:** Enforced immediate crash-stop and log rollback upon disk write errors.
6. **Compaction boundary filtering:** Pruned obsolete compacted entries on startup to prevent slice offset skew.
7. **POSIX directory fsync:** Added parent directory fsync after atomic file renames.

---

## 9. Limitations & Non-Goals

Operational boundaries and non-goals are documented in [`docs/limitations.md`](docs/limitations.md):

* **Storage Architecture:** Uses atomic snapshot-rewrite per persist rather than an append-only segmented log, trading disk throughput (~119 ops/sec) for simpler recovery.
* **No Dynamic Membership Changes:** Cluster membership is fixed at configuration time; joint consensus is not implemented.
* **No Pre-Vote Protocol:** Partitioned nodes reconnecting with incremented terms can trigger disruptive elections.
* **Plaintext Transport:** Inter-node RPCs use plain JSON over HTTP without TLS/mTLS encryption.
* **In-Memory State Machine:** State is held in RAM with periodic snapshotting; dataset size is bounded by memory.
* **Linearizability Checker Bound:** The Wing & Gong search is NP-complete, practical for 100 to 300 operations in chaos tests.

---

## 10. Interactive Browser Simulator

An interactive visualization of Raft consensus is hosted via GitHub Pages:

* **Live Simulator:** [https://innocous06.github.io/raft-kv/](https://innocous06.github.io/raft-kv/)

*Clarification:* This is a client-side visualization written in JavaScript that simulates protocol concepts; the real consensus implementation is the Go engine in this repository.

---

## 11. References & Prior Work

* **Ongaro & Ousterhout (2014):** In Search of an Understandable Consensus Algorithm (USENIX ATC '14)
* **Wing & Gong (1993):** Testing and Verifying Concurrent Objects (JPDC)
* **Gibbons & Korach (1997):** Testing Shared Memories (SIAM J. Comput.)
* **Lamport (2002):** Specifying Systems: The TLA+ Language and Tools
* Full bibliography: [`docs/references.md`](docs/references.md)

---

## 12. License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
