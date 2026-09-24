# Raft-KV — Production-Grade Distributed Consensus Key-Value Store

> **Formally verified, fault-tolerant distributed consensus based on the Raft algorithm.**  
> *Zero-dependency actor event-loop core, Write-Ahead Logging with CRC32 torn-write recovery, Porcupine linearizability verification, and real-time Warm Editorial telemetry.*

[![Live Demo](https://img.shields.io/badge/Live%20Demo-GitHub%20Pages-c28f2c.svg)](https://innocous06.github.io/raft-kv/)
[![Go Version](https://img.shields.io/badge/Go-1.26+-2b2823.svg?logo=go)](https://golang.org)
[![Safety Invariants](https://img.shields.io/badge/Raft%20Safety-5%2F5%20Verified-2e4c23.svg)](#)
[![Consistency](https://img.shields.io/badge/Linearizability-100%25%20Wing%20%26%20Gong-785110.svg)](#)
[![Tests](https://img.shields.io/badge/Tests-15%2F15%20Passing-2e4c23.svg)](#)
[![License: MIT](https://img.shields.io/badge/License-MIT-8c857b.svg)](LICENSE)

---

## 1. Executive Summary & Problem Statement

Distributed systems cannot guarantee reliability through simple replication alone. Under asynchronous networks, messages are delayed, dropped, reordered, or duplicated; nodes crash, restart, or experience asymmetric connectivity partitions.

### The Problem: Consistency Hazards in Distributed Storage
* **Split-Brain Anomaly:** If network partitions segment a cluster, uncoordinated partitions may each elect a leader and accept conflicting mutations, permanently corrupting global state.
* **Commit Regression:** Unsynchronized followers processing heartbeat RPCs can compute regressive commit indexes if leader commit notifications lag behind local log truncation.
* **Torn Writes on Crash:** Disk I/O during unexpected power cuts or abrupt process termination produces corrupt, partial log tail blocks that prevent deterministic node recovery.
* **Non-Linearizable Concurrency:** Concurrent client writes submitted across rapidly shifting terms risk silent drops, duplicate state application, or stale reads.

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
│       (In-Memory KV Store)    │  ◄── Async Snapshot Compaction with State Isolation
└───────────────────────────────┘
```

> **Raft-KV eliminates these hazards through an exact implementation of the Raft Consensus Algorithm (Ongaro & Ousterhout, 2014), backed by automated continuous invariant assertions and Wing & Gong linearizability verification.**

---

## 2. Mathematical Foundations & Formal Consensus Invariants

Raft guarantees safety across all execution paths by preserving five fundamental invariants:

### A. Quorum Intersection
For any cluster of $N$ nodes, every election and log commit requires approval from a strict majority quorum $Q$:
$$Q = \left\lfloor \frac{N}{2} \right\rfloor + 1$$
Because any two majorities $Q_1, Q_2 \subseteq N$ must intersect:
$$|Q_1 \cap Q_2| \ge 1$$
Every validly elected leader contains at least one node that approved the most recently committed log entry.

### B. Election Safety
$$\forall \text{ Term } T, \quad |\text{Leaders}(T)| \le 1$$
*Proof Sketch:* A candidate requires $Q$ votes to win term $T$. Each node votes at most once per term (recorded in durable storage). By Quorum Intersection, two candidates cannot both assemble majorities in the same term.

### C. Log Matching Property
$$\left( \text{log}_1[i].\text{term} = \text{log}_2[i].\text{term} \right) \implies \left( \forall k \le i, \; \text{log}_1[k] = \text{log}_2[k] \right)$$
*Induction Step:* Leaders create at most one entry per index per term and entries are never modified or moved. Followers reject `AppendEntries` if the entry preceding new ones does not match in index and term (`prevLogIndex`, `prevLogTerm`).

### D. Leader Completeness
If a log entry is committed at $(index, term)$, that entry is present in the logs of all leaders for all higher terms:
$$\text{committed}(e) \implies \forall T' > e.\text{term}, \; \text{Leader}(T').\text{contains}(e)$$

### E. Linearizability (Wing & Gong 1993)
A concurrent execution history $H$ is linearizable if there exists an equivalent sequential execution $S$ such that:
$$S \sim H \quad \wedge \quad \forall op_1, op_2 \in H, \; \left(op_1 \prec_H op_2 \implies op_1 \prec_S op_2\right)$$
Raft-KV verifies client traces against this sequential specification using an integrated Porcupine-style checker during every chaos scenario.

---

## 3. Core Architecture & Engineering Highlights

### 1. Pure Actor Event Loop (Zero Mutex Races)
All mutable Raft consensus state (current term, voted for, log entries, commit index, role transitions) resides inside a single-threaded event loop (`n.run()` in `internal/raft/node.go`). Outside RPC calls and client requests communicate strictly through Go channels, eliminating lock inversion, deadlock, and race conditions.

### 2. Sequential FIFO Log Application
To eliminate out-of-order execution across the state machine boundary, entries are routed through a synchronized FIFO queue governed by `sync.Cond` and consumed by an independent `applierLoop`. Client notification channels are backed by an applied-results ring cache to prevent proposal waiter registration races.

### 3. Write-Ahead Log (WAL) & Crash Recovery
* **Framing Format:** `[Length: 4B][Type: 1B][CRC32: 4B][Payload: NB]`
* **Torn-Write Recovery:** On restart, the WAL parser inspects record lengths and validates IEEE CRC32 checksums. If an unexpected power cut produces a torn tail record, the engine safely truncates the file back to the last valid boundary without data loss.
* **Log Compaction:** Once log records exceed the configured compaction threshold (default: 100 entries), an immutable state snapshot is captured and serialized asynchronously while log entries below `lastIncludedIndex` are discarded.

### 4. Pluggable Transport Subsystem
* **`SimNet` (Testing & Chaos):** In-memory actor network supporting configurable packet drop rates, message duplication, network partitions, and per-node asymmetric delay injection.
* **Production HTTP Transport:** High-performance REST RPC layer running over Keep-Alive HTTP/1.1 with connection pooling.

### 5. Warm Editorial Web Dashboard
An embedded, lightweight web interface styled in **Warm Editorial Minimalism** (cream paper palette `#FAF8F5`, serif broadsheet headings, tabular monospace telemetry, zero emojis). Features real-time Server-Sent Events (SSE) telemetry, cluster topology status cards, interactive KV operations, and one-click chaos fault injection.

---

## 4. System Processing Pipeline

```
  [01: Client Submit]           [02: Raft Proposal]          [03: Replication]            [04: Quorum Commit]
Client PUT (Key, Val)    ──>   Leader Appends Entry   ──>   Broadcast AppendEntries  ──>  Majority Ack Entry
SeqNum Deduplication           Durable WAL Flush            Parallel RPC over Net         CommitIndex Advanced
(api/server.go)                (raft/node.go)               (raft/replication.go)         (kv/kv.go)
```

```
  [05: State Machine]           [06: Client Response]        [07: Compaction]             [08: Slow Catch-Up]
Apply FIFO Queue         ──>   Resolve Waiter Chan    ──>   Log Compaction Check    ──>   InstallSnapshot RPC
Key-Value Mutation             HTTP 200 JSON Return         Serialize Snapshot            Catch Up Lagging Node
(kv/statemachine.go)           (api/server.go)              (storage/snapshot.go)         (raft/replication.go)
```

---

## 5. HTTP REST API Specification

All endpoints communicate via standard JSON over HTTP.

| Method | Endpoint | Description | Request Body / Parameters | Response Status |
| :--- | :--- | :--- | :--- | :--- |
| `POST` | `/api/v1/kv/put` | Propose replicated key-value mutation | `{"key":"k","value":"v","clientId":"c1","seqNum":1}` | `200 OK` / `400 Bad Request` / `409 Conflict` |
| `GET` | `/api/v1/kv/get` | Retrieve value for key | Query: `?key=name` | `200 OK` / `404 Not Found` |
| `POST` | `/api/v1/kv/delete` | Tombstone a key from state | `{"key":"k","clientId":"c1","seqNum":2}` | `200 OK` / `400 Bad Request` |
| `GET` | `/api/v1/kv/all` | Dump all committed key-value pairs | None | `200 OK` |
| `GET` | `/api/v1/cluster/status` | Introspect telemetry for all cluster nodes | None | `200 OK` (JSON array of `NodeState`) |
| `GET` | `/api/v1/events/stream` | Real-time SSE telemetry event bus | None | `200 OK` (`text/event-stream`) |
| `POST` | `/api/v1/chaos/isolate_leader` | Disconnect current leader into minority | None | `200 OK` |
| `POST` | `/api/v1/chaos/partition` | Create asymmetric network partition (2 vs 3) | None | `200 OK` |
| `POST` | `/api/v1/chaos/heal` | Heal network and restore communication | None | `200 OK` |
| `POST` | `/api/v1/chaos/kill` | Terminate a target node process | Query: `?node=node-1` or JSON `{"node":"node-1"}` | `200 OK` / `404 Not Found` |
| `POST` | `/api/v1/chaos/restart` | Restart a stopped node from disk state | Query: `?node=node-1` or JSON `{"node":"node-1"}` | `200 OK` / `400 Bad Request` |

---

## 6. Local Setup & Deployment

### Quick Start (In-Process 5-Node Cluster)

```bash
# Clone the repository
git clone https://github.com/innocous06/raft-kv.git
cd raft-kv

# Build executables
go build -o raft-node.exe ./cmd/node
go build -o raft-chaos.exe ./cmd/chaos

# Launch an in-process 5-node cluster with web dashboard
./raft-node.exe -cluster=5 -port=8001
```

Open your browser to:
```
http://127.0.0.1:8001
```

### Running Standalone Multi-Process Cluster

To run independent processes connected via HTTP transport:

```bash
# Terminal 1 (Node 1)
./raft-node.exe -id=node-1 -port=8001 -peers=node-2=http://127.0.0.1:8002,node-3=http://127.0.0.1:8003 -data=./data/n1

# Terminal 2 (Node 2)
./raft-node.exe -id=node-2 -port=8002 -peers=node-1=http://127.0.0.1:8001,node-3=http://127.0.0.1:8003 -data=./data/n2

# Terminal 3 (Node 3)
./raft-node.exe -id=node-3 -port=8003 -peers=node-1=http://127.0.0.1:8001,node-2=http://127.0.0.1:8002 -data=./data/n3
```

### Running Chaos & Linearizability Verification Engine

```bash
# Run 300+ randomized operations under network splits, packet drops, and node crashes
./raft-chaos.exe -nodes=5 -duration=5s -seed=1337
```

---

## 7. Verification Matrix & Chaos Testing

The full distributed test harness checks Raft safety across adversarial network conditions:

```bash
go test -v -count=1 ./harness
```

| Test Identifier | Scenario Tested | Result |
| :--- | :--- | :--- |
| `TestEdgeCase_EmptyKeyHandling` | Rejection of empty keys on PUT/GET/DELETE | `[PASS]` |
| `TestEdgeCase_DeduplicationAndIdempotency` | Duplicate sequence numbers filtered; idempotent execution | `[PASS]` |
| `TestEdgeCase_MalformedCommandHandling` | Corrupt client payloads handled without stalling `lastApplied` | `[PASS]` |
| `TestEdgeCase_NonExistentNodeChaos` | Safe handling of invalid node crash/restart invocations | `[PASS]` |
| `TestEdgeCase_AsymmetricPartition` | One-way packet drops with uninterrupted majority consensus | `[PASS]` |
| `TestEdgeCase_HTTPAPIRoutes` | Complete REST API route suite (GET, PUT, DELETE, 404, 400) | `[PASS]` |
| `TestEdgeCase_HeavyConcurrentWritesAndReads` | 50 concurrent client goroutines under sustained load | `[PASS]` |
| `TestScenario1_ElectionStability` | Solitary leader elected within election timeout window | `[PASS]` |
| `TestScenario2_ReelectionOnLeaderFailure` | Seamless election of replacement leader on leader crash | `[PASS]` |
| `TestScenario3_LogReplication` | Multi-node replicated entry consensus across quorum | `[PASS]` |
| `TestScenario4_KillLeaderUnderWriteLoad` | Consecutive leader kills under continuous write traffic | `[PASS]` |
| `TestScenario5_NetworkPartitionPartitionLeader` | Majority/minority network split; split-brain immunity | `[PASS]` |
| `TestScenario6_SlowFollowerSnapshotCatchUp` | Follower catch-up via `InstallSnapshot` log compaction | `[PASS]` |
| `TestScenario7_RollingRestart` | Rolling restart of all 5 nodes with zero data loss | `[PASS]` |
| `TestScenario8_ChaosTestingAndLinearizability` | Wing & Gong linearizability over 90 concurrent chaotic ops | `[PASS]` |

**Result: 15/15 tests passing (100% pass rate). Zero invariant violations.**

---

## 8. Interactive GitHub Pages Demo

An interactive browser-side simulation of the Raft cluster is available on GitHub Pages:

* **Live Demo:** [https://innocous06.github.io/raft-kv/](https://innocous06.github.io/raft-kv/)
* **Features:**
  * Client-side 5-node virtual Raft quorum running in JavaScript
  * Interactive `PUT`, `GET`, and `DELETE` state mutations with real-time log commits
  * Live chaos triggers: **Isolate Leader**, **Split Brain (2 vs 3)**, and **Heal Network**
  * Per-node **Kill** and **Restart** controls with automatic leader re-election
  * Real-time event log trace displaying term changes, election events, and commit acks

---

## 9. Project Metadata & Author

* **Project:** Raft-KV (Distributed Consensus Key-Value Store)
* **Author:** innocous06
* **Domain:** Distributed Systems / Consensus Algorithms / Fault-Tolerant Storage
* **Repository:** [https://github.com/innocous06/raft-kv](https://github.com/innocous06/raft-kv)
* **License:** MIT License ([LICENSE](LICENSE))
