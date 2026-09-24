# Raft KV Architecture & Specification

`raft-kv` is an educational, fault-tolerant distributed Key-Value store prototype implementing the Raft consensus algorithm (Ongaro & Ousterhout, 2014) in Go. It is designed around a strictly decoupled, transport-agnostic core capable of running seamlessly over simulated networks for deterministic chaos testing and real HTTP/gRPC networks for multi-process clusters.

---

## 1. System Layers

```
┌─────────────────────────────────────────────────────────────┐
│           Dashboard Web UI (SSE Stream + REST)              │
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

### Layer 1: Dashboard Web UI (`web/`)
- Zero-dependency embedded static frontend served via Go `embed.FS`.
- Real-time telemetry via Server-Sent Events (SSE) from the internal event bus.
- Interactive cluster node cards reflecting roles (Leader, Candidate, Follower), terms, commit indices, and log lengths.
- Real-time client studio for Put, Get, Delete queries and interactive fault-injection triggers.

### Layer 2: Client API Layer (`internal/api/`)
- Public REST API supporting `PUT`, `GET`, and `DELETE`.
- Transparent leader failover and redirection ("not leader" responses indicate leader hint).
- Exactly-once semantics via unique `ClientID` and monotonically increasing `SeqNum`.

### Layer 3: KV State Machine (`internal/kv/`)
- In-memory key-value state machine driven by committed entries delivered via `ApplyCh`.
- Client deduplication table `map[string]ClientRecord` recording `(LastSeq, LastResult)`.
- Snapshot serialization and restoration compacting state machine state and deduplication records.

### Layer 4: Raft Consensus Core (`internal/raft/`)
- Pure, transport-agnostic consensus engine adhering strictly to Raft Paper Figure 2.
- Single-threaded actor event loop per node owning all mutable state: guarantees zero lock contention and no internal deadlocks.
- Implements:
  - Randomized election timeouts (150-300ms).
  - Leader election & heartbeats.
  - Log replication with fast conflict-term backoff optimization.
  - Figure 8 commit rule (leaders only commit entries from their current term).
  - Snapshot compaction and `InstallSnapshot` RPC.

### Layer 5: Transport Layer (`internal/transport/`)
- `SimNet`: In-memory network simulator with deterministic PRNG seeding, configurable packet loss, latency jitter, and symmetric/asymmetric network partitions.
- `HTTPTransport`: Production transport communicating over HTTP/JSON for multi-process deployments.

### Layer 6: Storage Engine (`internal/storage/`)
- `MemoryStorage`: High-speed thread-safe storage for in-memory testing.
- `DiskStorage`:
  - WAL with CRC32 IEEE checksums per record.
  - Automatic detection and truncation of torn writes at file tail upon crash recovery.
  - Atomic metadata persistence (`currentTerm` and `votedFor`) using temp file fsync and atomic rename.
  - Snapshot persistence and log prefix truncation.

---

## 2. Raft Paper Compliance (Figure 2)

| Component | Paper Specification | Implementation |
|---|---|---|
| **Persistent State** | `currentTerm`, `votedFor`, `log[]` | Saved via `storage.SaveState()` before replying to any RPC |
| **Volatile State** | `commitIndex`, `lastApplied` | Maintained in `raft.Node` event loop |
| **Leader Volatile State** | `nextIndex[]`, `matchIndex[]` | Reinitialized upon election in `becomeLeader()` |
| **Election Safety** | At most one leader per term | Verified continuously in `InvariantChecker` |
| **Leader Append-Only** | Leader never overwrites own log | Enforced in `node.run()` and verified by invariant checker |
| **Log Matching** | Same (index, term) implies identical prefixes | Enforced via `PrevLogIndex`/`PrevLogTerm` checks and conflict backoff |
| **Leader Completeness** | Committed entries exist in all future leaders | Guaranteed by Figure 8 commit rule and up-to-date vote restriction |
| **State Machine Safety** | Identical commands applied at same index | Verified across all nodes via event bus telemetry |

---

## 3. Concurrency Architecture

Instead of fine-grained mutexes that risk lock inversions and deadlocks under concurrent RPCs, `raft-kv` utilizes a **single-threaded Actor event loop**:
- All node state (`currentTerm`, `role`, `log`, `commitIndex`, etc.) is private to the node's `run()` goroutine.
- Incoming RPCs (`RequestVote`, `AppendEntries`, `InstallSnapshot`) and client proposals are submitted via typed Go channels.
- Outbound RPCs are dispatched in ephemeral goroutines with contexts and deliver results back to the event loop via dedicated response channels.
- State inspections (`GetState`, `GetNodeState`) execute synchronously within the event loop via internal closures.
