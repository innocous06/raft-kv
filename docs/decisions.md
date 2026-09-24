# Architecture & Design Decision Log

This document records the architectural and engineering decisions made during the development of Raft-KV. Each entry captures the context, options considered, trade-offs, and conditions under which the decision should be revisited.

---

## D-001: Actor Event Loop over Mutex-Guarded State
Date: 2026-09-24
Context: Raft core involves complex, interleaved inputs: concurrent RPC handlers, periodic heartbeats, randomized election timeouts, and client read/write calls.
Options considered:
- (a) Mutex-guarded struct: Protecting Node fields with a coarse-grained or fine-grained sync.RWMutex.
- (b) Single-threaded actor loop: Channel-driven event loop (n.run()) owning all mutable consensus state.
Decision: (b) Single-threaded actor loop.
Why: Eliminates lock-ordering deadlocks, race conditions, and lock-inversion hazards between nested RPC calls and internal state updates. Protocol rules (Figures 2 and 8) are reasoned about sequentially.
Trade-off: State machine entry application cannot block inside the event loop without stalling protocol heartbeats, requiring an asynchronous FIFO queue and sync.Cond apply worker.
Revisit if: Single-core CPU scheduling of the event loop becomes the primary throughput bottleneck under high write concurrency.

---

## D-002: Rewrite-per-Persist Storage over Append-Only Segmented WAL
Date: 2026-09-24
Context: Persistent storage is required to store currentTerm, votedFor, and log entries across crashes.
Options considered:
- (a) Segmented append-only log with compaction pointers: Maintaining append streams with index tombstones and background segment compaction.
- (b) Atomic snapshot-rewrite per persist: Writing uncompacted entries to a temporary file, fsyncing, and atomically renaming.
Decision: (b) Atomic snapshot-rewrite per persist with CRC32 verification.
Why: Dramatically simpler to verify and validate against torn writes and power loss. Atomic os.Rename guarantees that partial writes never corrupt prior state.
Trade-off: Each persist is O(log size) in write volume, yielding ~119 writes/sec under synchronous disk fsync.
Revisit if: Workload requires high sustained disk write throughput (>1,000 fsyncs/sec) without relying on batching or group commits.

---

## D-003: Linearizable Client Request Deduplication over At-Least-Once Execution
Date: 2026-09-24
Context: Network packet loss or client retries can cause identical commands to be replicated multiple times.
Options considered:
- (a) At-least-once execution: Applying every received command without tracking client identity.
- (b) State machine deduplication table: Tracking (ClientID -> {LastSeq, Response}) in the state machine.
Decision: (b) State machine deduplication table.
Why: Ensures exact-once execution semantics (linearizability) for state mutations. Repeated client retries return the cached result without re-executing non-idempotent operations.
Trade-off: Memory overhead proportional to active clients.
Revisit if: Client count scales into millions, requiring LRU eviction policies or expiring session leases.

---

## D-004: In-Repo Wing & Gong Linearizability Checker over External Dependencies
Date: 2026-09-24
Context: Verifying linearizability during randomized fault injection and chaos testing.
Options considered:
- (a) External tool dependency: Requiring external test binaries or Porcupine CGO packages.
- (b) In-repo Wing & Gong (1993) search algorithm: Self-contained pure-Go implementation (harness/linearizability.go).
Decision: (b) In-repo implementation.
Why: Zero external CGO or tool dependencies. Runs natively across all platforms (Windows, Linux, macOS) via standard go test.
Trade-off: Wing & Gong search is NP-complete; history size must be bounded (~100 to 300 operations per chaos scenario).
Revisit if: Fuzzing histories exceed 1,000 concurrent operations, requiring interval-tree pruning or WGL heuristic solvers.

---

## D-005: Crash-Stop Persistence Semantics over In-Memory Degradation
Date: 2026-09-25
Context: What should a node do when disk persistence (SaveState / SaveSnapshot) fails due to I/O error or full disk?
Options considered:
- (a) Degraded in-memory execution: Swallowing the disk error, continuing in memory, and hoping subsequent writes succeed.
- (b) Crash-stop semantics: Immediately halting the node (go n.Stop()), refusing vote grants, and rolling back unpersisted log entries.
Decision: (b) Crash-stop semantics.
Why: Running in memory with unpersisted state violates Raft safety. A node that acknowledges entries or increments terms in memory and crashes will double-vote or lose committed entries upon restart.
Trade-off: A transient disk error halts the node rather than retrying in-place.
Revisit if: Transient disk errors can be safely retried in a dedicated staging buffer before exposing state to the cluster.

---

## D-006: FIFO sync.Cond Apply Queue over Unbuffered Worker Channels
Date: 2026-09-24
Context: Applying committed entries to the state machine in strict index order.
Options considered:
- (a) Spawning goroutines per commit or unbuffered channels: Relying on Go runtime goroutine scheduler.
- (b) Dedicated sequential apply queue with sync.Cond: A single dedicated applier thread consuming from a FIFO queue.
Decision: (b) Dedicated FIFO queue with sync.Cond.
Why: In early testing (Bug 2), concurrent goroutines applied commits out of order under thread scheduling. A sequential queue guarantees monotonic lastApplied progression.
Trade-off: Small lock contention between actor loop enqueuing and worker dequeuing.
Revisit if: Pipelined state machine execution is introduced for non-conflicting keys.

---

## D-007: Event-Driven Commit Tracking over Periodic State Polling
Date: 2026-09-25
Context: Invariant harness checking Leader Completeness (Property 4: committed entries must appear in future leaders).
Options considered:
- (a) Periodic state polling: Sampling leader logs every N milliseconds.
- (b) Instantaneous event bus notification: Emitting committed LogEntry records on EntryCommitted events.
Decision: (b) Instantaneous event bus notification.
Why: Periodic polling could miss entries committed by a leader that crashes immediately before the next sampling tick. Event emission captures 100% of commits permanently.
Trade-off: Retaining event history in the test harness bus consumes memory.
Revisit if: Test harness runs indefinitely, requiring bounded event sliding windows.
