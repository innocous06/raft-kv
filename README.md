# Raft KV: Distributed Replicated Key-Value Consensus Cluster

[![Go](https://img.shields.io/badge/Go-1.26+-1c1917?style=flat-square&logo=go)](https://golang.org)
[![Tests](https://img.shields.io/badge/Tests-8%2F8%20Passing-27451e?style=flat-square)]()
[![Invariants](https://img.shields.io/badge/Raft%20Safety-5%2F5%20Verified-44403c?style=flat-square)]()
[![Linearizability](https://img.shields.io/badge/Consistency-Strictly%20Linearizable-785110?style=flat-square)]()
[![License](https://img.shields.io/badge/License-MIT-a8a29e?style=flat-square)]()

A production-grade, fault-tolerant distributed key-value store built in Go implementing the **Raft Consensus Algorithm** (Ongaro & Ousterhout, 2014). It features a pure transport-agnostic core, pluggable simulated network (`SimNet`) and production RPC transports, write-ahead log (WAL) with CRC32 torn-write recovery, continuous safety invariant verification, Porcupine-style linearizability checking, and an embedded Warm Editorial web dashboard.

---

## 1. System Architecture

```
+-------------------------------------------------------------+
|         Warm Editorial Web Dashboard (SSE Stream + REST)    |
+-------------------------------------------------------------+
|        Client API Layer (Deduplication + Redirection)       |
+-------------------------------------------------------------+
|      KV State Machine (Committed Log Entry Application)     |
+-------------------------------------------------------------+
|      Raft Consensus Core (Pure Actor Event Loop)            |
+-------------------------------------------------------------+
|  Transport Layer (SimNet In-Memory OR Real TCP / HTTP RPC)  |
+-------------------------------------------------------------+
|       Storage Engine (Atomic Metadata + CRC32 WAL)          |
+-------------------------------------------------------------+
```

### Visual Walkthrough: Leader Crash, Election & Recovery Sequence

```
   +----------+                 +----------+                 +----------+
   |  Node 1  |                 |  Node 2  |                 |  Node 3  |
   | (Leader) |                 |(Follower)|                 |(Follower)|
   +----+-----+                 +----+-----+                 +----+-----+
        |                            |                            |
        |--- Heartbeat (Term 1) ---->|                            |
        |--- Heartbeat (Term 1) --------------------------------->|
        |                            |                            |
     [CRASH]                         |                            |
        X                            |                            |
                                     | (Election timer fires)     |
                                     |                            |
                                     |-- RequestVote (Term 2) --->|
                                     |<-- VoteGranted (Term 2) ---|
                                     |                            |
                                [BECOMES LEADER]                  |
                                     |                            |
                                     |--- Heartbeat (Term 2) ---->|
                                     |                            |
     [RECOVERS]                      |                            |
        |                            |                            |
        |<-- Heartbeat (Term 2) -----|                            |
        | (Discovers Term 2 > 1)     |                            |
        | (Steps down to Follower)   |                            |
        |                            |                            |
```

The system is structured into five modular layers:

1. **Raft Core (`internal/raft/`)**: Transport-agnostic consensus engine adhering strictly to Raft Paper Figure 2. Implements a single-threaded Actor event loop that owns all mutable state, guaranteeing zero mutex race conditions.
2. **Pluggable Transports (`internal/transport/`)**:
   - `SimNet`: In-memory network simulator with deterministic PRNG seeding, configurable packet loss, latency jitter, and symmetric/asymmetric network partitions.
   - `GRPCTransport` & `HTTPTransport`: Production transports over TCP/RPC and HTTP/JSON for multi-process deployments.
3. **Persistent Storage (`internal/storage/`)**:
   - Write-Ahead Log (WAL) with CRC32-IEEE checksums per record.
   - Tail torn-write detection and recovery.
   - Atomic metadata persistence (`currentTerm`, `votedFor`) via fsync and atomic rename.
   - State machine snapshots with log prefix compaction.
4. **Replicated KV State Machine (`internal/kv/`)**:
   - Client deduplication table `map[string]ClientRecord` ensuring exactly-once execution.
   - Read/write execution with leader redirection hints.
5. **Warm Editorial Dashboard (`web/`)**:
   - Zero-dependency embedded web UI (`embed.FS`).
   - Server-Sent Events (SSE) streaming live cluster events (`RoleChanged`, `EntryCommitted`, `PartitionCreated`, etc.).
   - Interactive cluster topology cards, KV studio, and chaos triggers with literary typography and neutral light-mode palette.

---

## 2. Quickstart & Execution

### 1. Run the Full Chaos & Scenario Test Suite
Run all 8 distributed fault-injection tests with 100% pass rate:
```bash
go test -v ./harness/...
```

### 2. Launch the Standalone Chaos Engine CLI
Execute randomized chaos testing with concurrent clients, continuous invariant checking, and Porcupine linearizability verification:
```bash
go run ./cmd/chaos -nodes=5 -duration=5s -seed=42
```

### 3. Launch the Live Web Dashboard Cluster
Start an in-process 3-node or 5-node cluster with one command:
```bash
go run ./cmd/node -cluster=5 -port=8001
```
Open **[http://127.0.0.1:8001](http://127.0.0.1:8001)** in your browser to inspect cluster nodes in real time, submit key-value operations, trigger network partitions, and observe live SSE traces.

---

## 3. Comprehensive Scenario Test Suite

The test harness (`harness/scenarios_test.go`) validates the cluster against complex distributed failure modes:

| Test Scenario | Description | Result |
|---|---|---|
| `TestScenario1_ElectionStability` | 3 nodes elect exactly one leader with stable heartbeats | PASS |
| `TestScenario2_ReelectionOnLeaderFailure` | Leader killed; replacement elected within timeouts | PASS |
| `TestScenario3_LogReplication` | Writes replicated to followers and committed across quorums | PASS |
| `TestScenario4_KillLeaderUnderWriteLoad` | Leader repeatedly crashed during active client write workload | PASS |
| `TestScenario5_NetworkPartitionPartitionLeader` | Leader isolated into minority partition; majority elects new leader; uncommitted minority writes discarded on heal | PASS |
| `TestScenario6_SlowFollowerSnapshotCatchUp` | Disconnected follower catches up via `InstallSnapshot` log compaction | PASS |
| `TestScenario7_RollingRestart` | Sequential kill and restart of all nodes with persistent disk WAL | PASS |
| `TestScenario8_ChaosTestingAndLinearizability` | Concurrent clients under packet loss, partitions, and jitter; validates all 5 invariants & Porcupine linearizability | PASS |

---

## 4. The Five Core Safety Invariants

Checked continuously by `harness/invariants.go`:
1. **Election Safety**: At most one leader can be elected in any given term.
2. **Leader Append-Only**: A leader never overwrites or truncates its own log entries.
3. **Log Matching**: If two logs contain an entry with the same index and term, all preceding entries are identical.
4. **Leader Completeness**: Any entry committed in term $T$ appears in the logs of all future leaders ($> T$).
5. **State Machine Safety**: No two nodes ever apply differing commands at the same log index.

---

## 5. REST Interface Reference

| Endpoint | Method | Payload / Parameters | Description |
|---|---|---|---|
| `/api/v1/kv/put` | `POST` | `{"key": "k", "value": "v", "clientId": "c1", "seqNum": 1}` | Replicate and commit write |
| `/api/v1/kv/get` | `GET` | `?key=k` | Linearizable read through consensus log |
| `/api/v1/kv/delete` | `POST` | `{"key": "k", "clientId": "c1", "seqNum": 2}` | Replicate and commit deletion |
| `/api/v1/kv/all` | `GET` | &mdash; | Return all key-value entries across cluster |
| `/api/v1/cluster/status` | `GET` | &mdash; | Return status of all nodes in cluster |
| `/api/v1/events/stream` | `GET` | &mdash; | Server-Sent Events (SSE) telemetry stream |
| `/api/v1/chaos/isolate_leader` | `POST` | &mdash; | Isolate active leader into 1-node partition |
| `/api/v1/chaos/partition` | `POST` | &mdash; | Split cluster into majority vs minority |
| `/api/v1/chaos/heal` | `POST` | &mdash; | Restore all network partitions |
| `/api/v1/chaos/kill` | `POST` | `?node=node-1` | Crash an individual node process |
| `/api/v1/chaos/restart` | `POST` | `?node=node-1` | Restart a crashed node and reload WAL |

---

## 6. Engineering Documentation

- **[Architecture Specification](file:///C:/Users/dharm/.gemini/antigravity/scratch/raft-kv/docs/ARCHITECTURE.md)**
- **[Fault-Injection Harness](file:///C:/Users/dharm/.gemini/antigravity/scratch/raft-kv/docs/FAULT_INJECTION.md)**
- **[Chaos Bug Log & Analysis](file:///C:/Users/dharm/.gemini/antigravity/scratch/raft-kv/docs/BUG_LOG.md)**
