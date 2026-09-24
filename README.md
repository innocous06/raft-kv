# ⚡ Raft KV: Distributed Replicated Key-Value Consensus Cluster

[![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go)](https://golang.org)
[![Tests](https://img.shields.io/badge/Tests-8%2F8%20Passing-10b981?style=flat)]()
[![Invariants](https://img.shields.io/badge/Raft%20Safety-5%2F5%20Verified-6366f1?style=flat)]()
[![Linearizability](https://img.shields.io/badge/Consistency-Strictly%20Linearizable-f59e0b?style=flat)]()
[![License](https://img.shields.io/badge/License-MIT-gray?style=flat)]()

A production-grade, fault-tolerant distributed key-value store built in Go implementing the **Raft Consensus Algorithm** (Ongaro & Ousterhout, 2014). It features a pure transport-agnostic core, pluggable simulated network (`SimNet`) & HTTP transport, write-ahead log (WAL) with CRC32 torn write recovery, continuous safety invariant verification, Porcupine-style linearizability checking, and a live web dashboard.

---

## 🏛️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│          Live Web Dashboard (SSE Stream + REST Studio)      │
├─────────────────────────────────────────────────────────────┤
│         Client API Layer (Deduplication + Redirection)      │
├─────────────────────────────────────────────────────────────┤
│       KV State Machine (Committed Log Entry Application)    │
├─────────────────────────────────────────────────────────────┤
│       Raft Consensus Core (Pure Actor Event Loop)           │
├─────────────────────────────────────────────────────────────┤
│   Transport Layer (SimNet In-Memory OR Real HTTP / gRPC)    │
├─────────────────────────────────────────────────────────────┤
│        Storage Engine (Atomic Metadata + CRC32 WAL)         │
└─────────────────────────────────────────────────────────────┘
```

The system is architected into 5 modular layers:
1. **Raft Core (`internal/raft/`)**: Transport-agnostic consensus engine adhering strictly to Raft Paper Figure 2. Implements a single-threaded Actor event loop that owns all mutable state, guaranteeing zero mutex race conditions.
2. **Pluggable Transports (`internal/transport/`)**:
   - `SimNet`: In-memory network simulator with deterministic PRNG seeding, configurable packet loss, latency jitter, and symmetric/asymmetric network partitions.
   - `HTTPTransport`: Production transport over HTTP/JSON for multi-process cluster deployments.
3. **Persistent Storage (`internal/storage/`)**:
   - Write-Ahead Log (WAL) with CRC32-IEEE checksums per record.
   - Tail torn-write detection and recovery.
   - Atomic metadata persistence (`currentTerm`, `votedFor`) via fsync and atomic rename.
   - State machine snapshots with log prefix compaction.
4. **Replicated KV State Machine (`internal/kv/`)**:
   - Client deduplication table `map[string]ClientRecord` ensuring exactly-once execution.
   - Read/write execution with leader redirection hints.
5. **Full-Stack Dashboard (`web/`)**:
   - Zero-dependency embedded web UI (`embed.FS`).
   - Server-Sent Events (SSE) streaming live cluster events (`RoleChanged`, `EntryCommitted`, `PartitionCreated`, etc.).
   - Interactive cluster topology cards, KV studio, and chaos triggers.

---

## 🚀 Quick Start

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

### 3. Launch the Live Web Dashboard & Cluster
Start an in-process 3-node or 5-node cluster with one command:
```bash
go run ./cmd/node -cluster=3 -port=8001
```
Open **[http://127.0.0.1:8001](http://127.0.0.1:8001)** in your browser to inspect cluster nodes in real time, submit key-value operations, trigger network partitions, and observe live SSE traces!

---

## 🧪 Comprehensive Scenario Test Suite

The test harness (`harness/scenarios_test.go`) validates the cluster against complex distributed failure modes:

| Test Scenario | Description | Status |
|---|---|---|
| `TestScenario1_ElectionStability` | 3 nodes elect exactly one leader with stable heartbeats | **PASS** |
| `TestScenario2_ReelectionOnLeaderFailure` | Leader killed; replacement elected within timeouts | **PASS** |
| `TestScenario3_LogReplication` | Writes replicated to followers and committed across quorums | **PASS** |
| `TestScenario4_KillLeaderUnderWriteLoad` | Leader repeatedly crashed during active client write workload | **PASS** |
| `TestScenario5_NetworkPartitionPartitionLeader` | Leader isolated into minority partition; majority elects new leader; uncommitted minority writes discarded on heal | **PASS** |
| `TestScenario6_SlowFollowerSnapshotCatchUp` | Disconnected follower catches up via `InstallSnapshot` log compaction | **PASS** |
| `TestScenario7_RollingRestart` | Sequential kill and restart of all nodes with persistent disk WAL | **PASS** |
| `TestScenario8_ChaosTestingAndLinearizability` | Concurrent clients under packet loss, partitions, and jitter; validates all 5 invariants & Porcupine linearizability | **PASS** |

---

## 🛡️ The 5 Raft Safety Invariants

Checked continuously by `harness/invariants.go`:
1. **Election Safety**: At most one leader can be elected in any given term.
2. **Leader Append-Only**: A leader never overwrites or truncates its own log entries.
3. **Log Matching**: If two logs contain an entry with the same index and term, all preceding entries are identical.
4. **Leader Completeness**: Any entry committed in term $T$ appears in the logs of all future leaders ($> T$).
5. **State Machine Safety**: No two nodes ever apply differing commands at the same log index.

---

## 📡 REST API Reference

| Endpoint | Method | Description |
|---|---|---|
| `/api/v1/kv/put` | `POST` | `{"key": "k", "value": "v", "clientId": "c1", "seqNum": 1}` |
| `/api/v1/kv/get?key=k` | `GET` | Read value of key `k` |
| `/api/v1/kv/delete` | `POST` | Delete key `k` |
| `/api/v1/kv/all` | `GET` | Return all key-value entries across the cluster |
| `/api/v1/cluster/status` | `GET` | Returns node role, term, commitIndex, appliedIndex |
| `/api/v1/events/stream` | `GET` | Server-Sent Events (SSE) live telemetry stream |

---

## 📖 Detailed Documentation

- **[Architecture Specification](file:///C:/Users/dharm/.gemini/antigravity/scratch/raft-kv/docs/ARCHITECTURE.md)**
- **[Fault-Injection Harness](file:///C:/Users/dharm/.gemini/antigravity/scratch/raft-kv/docs/FAULT_INJECTION.md)**
- **[Chaos Bug Log & Analysis](file:///C:/Users/dharm/.gemini/antigravity/scratch/raft-kv/docs/BUG_LOG.md)**
