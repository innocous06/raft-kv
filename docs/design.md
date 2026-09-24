# System Design & Failure Specification

This document details the architectural design, end-to-end data flow, consensus safety invariants, and the failure handling matrix for Raft-KV.

---

## 1. System Architecture

The node architecture is divided into decoupled, transport-agnostic layers:

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

### Key Components

1. **Raft Consensus Core (`internal/raft/`):**
   * Single-threaded actor event loop (`n.run()`) owning all mutable consensus state (`currentTerm`, `votedFor`, `log`, `commitIndex`, `role`).
   * No mutex deadlocks or lock inversion: outside callers interact exclusively through typed channels (`rpcCh`, `proposeCh`).
2. **State Machine (`internal/kv/`):**
   * Key-value map driven by committed entries delivered via a dedicated FIFO apply queue governed by `sync.Cond`.
   * Client deduplication table (`map[string]ClientRecord`) mapping `ClientID` to `(LastSeq, LastResult)` to guarantee exact-once linearizable semantics.
3. **Storage Engine (`internal/storage/`):**
   * `DiskStorage` uses atomic snapshot-rewrite per persist with framed CRC32 records (`[Length: 4B][CRC32: 4B][Payload: NB]`).
   * Recovers torn writes on restart by truncating corrupt tail bytes.
4. **Transport Layer (`internal/transport/`):**
   * Abstract `Transport` interface supporting `SimNet` for deterministic, zero-socket in-memory fault injection, and `HTTPTransport` for multi-process distributed clusters.

---

## 2. End-to-End Data Flow

### Write Path (Client Mutation: PUT / DELETE)

```
Client             Leader API          Leader Actor Loop       Follower Actor Loop     State Machine
  │                     │                      │                       │                     │
  │─── POST /put ──────>│                      │                       │                     │
  │   (ClientID,Seq)    │── Propose(cmd) ─────>│                       │                     │
  │                     │                      │── Append to Log       │                     │
  │                     │                      │── Persist to Disk     │                     │
  │                     │                      │── Send AppendEntries ─┼────────────────────>│
  │                     │                      │   (PrevLog, Entries)  │                     │
  │                     │                      │                       │── Append & Persist  │
  │                     │                      │<── AppendSuccess ─────│                     │
  │                     │                      │                       │                     │
  │                     │                      │── Advance CommitIndex │                     │
  │                     │                      │── Enqueue ApplyMsg ───┼────────────────────>│
  │                     │                      │                       │                     │── Apply mutation
  │                     │                      │                       │                     │── Record (ClientID,Seq)
  │                     │<── ProposeResult ────│                       │                     │── Resolve waiter
  │<── 200 OK (val) ────│                      │                       │                     │
```

### Read Path (Linearizable Read: GET)

* Reads verify leadership state and return current committed values from the in-memory state machine.
* Non-existent keys return 404; stale followers redirect clients to the known leader hint.

---

## 3. Consensus Safety Invariants

The implementation enforces the five fundamental Raft safety invariants:

1. **Election Safety:** At most one leader can be elected in any given term. Enforced by requiring a strict majority quorum ($Q = \lfloor N/2 \rfloor + 1$) and one vote per node per term.
2. **Leader Append-Only:** A leader never overwrites or truncates its own log entries; it only appends new entries.
3. **Log Matching:** If two logs contain an entry with the same index and term, the logs are identical in all entries up to that index. Enforced by inductive `PrevLogIndex` and `PrevLogTerm` checks in AppendEntries.
4. **Leader Completeness:** If a log entry is committed in a given term, that entry will be present in the logs of the leaders for all higher terms. Enforced by candidate log up-to-date checks during election.
5. **State Machine Safety:** If a server applies a log entry at a given index to its state machine, no other server will ever apply a different entry for that index.

---

## 4. Failure Handling: "What Happens If X Fails?"

| Component / Event | Failure Scenario | Immediate System Response | Recovery & Safety Guarantee |
| :--- | :--- | :--- | :--- |
| **Leader Node** | Leader process crashes or hangs | Election timers tick down on followers; election triggered within 150-300ms | Quorum elects a new leader with the most up-to-date log; Election Safety preserved |
| **Network Partition** | 5-node cluster splits into 2 vs 3 nodes | Minority partition cannot achieve quorum ($2 < 3$); mutations reject or stall | Majority partition continues operating; on heal, followers back off and reconcile logs |
| **Asymmetric Drop** | Link drops messages in one direction | Leader heartbeats fail; recipient initiates election or drops stale connection | High-term step-down rule forces resolution; Leader Completeness preserved |
| **Disk Write (Election)** | `persist()` fails during `startElection` | Term and vote rolled back in memory, election aborted | Node does not solicit votes without durable state; double-vote prevented |
| **Disk Write (Vote Grant)** | `persist()` fails during `RequestVote` | Previous `votedFor` restored, returns `VoteGranted: false` | Candidate refused; node cannot grant vote without durable record |
| **Disk Write (Replication)** | `persist()` fails during `AppendEntries` | In-memory log restored via `RestoreEntries()`, `StorageFatal` emitted, node halted | Follower terminates immediately; unpersisted entries never acknowledged |
| **Disk Write (Step Down)** | `persist()` fails during `becomeFollower` | Emits `StorageFatal` event, node halts immediately (`go n.Stop()`) | Node halts rather than running in memory with compromised term |
| **Disk Write (Client Propose)**| `persist()` fails during client propose | Appended entry truncated from log, returns `isLeader: false` to client | Phantom unpersisted entries discarded; client receives failure and retries |
| **Snapshot Compaction** | Disk fails during `Snapshot` | Returns error without compacting in-memory log or advancing markers | Compaction safely aborts; existing log entries remain valid |
| **Power Loss / Torn Write** | Process killed mid-write to log | Tail record has incomplete header (<8B) or invalid CRC32 checksum | Startup scan truncates file to last valid boundary; prior records intact |
| **Client Duplicate** | Network retry re-sends same sequence | Deduplication table detects cached `(ClientID, SeqNum)` | State machine returns cached response without re-executing mutation |
| **Oversized Request** | Malicious / corrupt 5 MB payload | `http.MaxBytesReader` rejects body before memory exhaustion | HTTP server returns 400 Bad Request; actor event loop unaffected |
