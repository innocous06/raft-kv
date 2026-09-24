# Chaos Bug Log & Root Cause Analysis

This document records bugs discovered during testing, fuzzing, and adversarial review. Each entry documents the trigger, symptom, root cause, fix, verification status, and architectural lesson learned.

---

### Bug 1: Named Struct Type Mismatch in Internal Actor Loop
- Found by: test (initial cluster bootstrap)
- Trigger: Launching a cluster and awaiting leadership election via TestScenario1_ElectionStability
- Symptom: c.WaitLeader() deadlocked and hung indefinitely
- Root cause: GetState() defined an inner struct type stateResp struct { term uint64; ... }. The actor loop dispatch used a type switch switch c := replyCh.(type) with case chan struct { ... }. In Go, named types and anonymous struct types are distinct; the type switch failed silently without sending a response, causing channel receive to block indefinitely
- Fix: Replaced dynamic interface casting with package-level StateSummary and explicit typed channels, eliminating reflection and type mismatches
- Regression test: TestScenario1_ElectionStability
- Lesson: Avoid interface{} and dynamic type-switches in high-throughput actor message routing; use explicit typed channels

---

### Bug 2: Indeterminate Unacknowledged Writes in Linearizability Checking
- Found by: fuzzing (chaos test suite)
- Trigger: Running randomized chaos under network latency with Seed 42 and Seed 707
- Symptom: Linearizability violation reported on key 'k-0': no sequential history matches real-time intervals
- Root cause: A client submitted Put(k, v) and timed out after 1500ms due to transient network latency, marking Success = false. However, the quorum committed the entry right after the client gave up. When a subsequent Get(k) read v, the checker discarded the unacknowledged write, assuming v appeared without a preceding write
- Fix: Enhanced harness/linearizability.go to treat unacknowledged operations as indeterminate: if an unacknowledged write payload is observed by a subsequent read, it is included in the sequential search space
- Regression test: TestScenario8_ChaosTestingAndLinearizability
- Lesson: In distributed consensus, client timeouts do not equal operation abortion; unacknowledged operations are indeterminate

---

### Bug 3: Embedded Web FileServer Canonical Path Redirection Loop
- Found by: review (dashboard manual verification)
- Trigger: Launching node with embedded web dashboard and accessing root URL /
- Symptom: HTTP client returned Too many automatic redirections were attempted
- Root cause: http.FileServer mounted over fs.Sub(staticFiles, "static") expects paths relative to the sub-filesystem root. Setting r.URL.Path = "/static/index.html" caused the standard library FileServer to issue HTTP 301 canonical redirects in an infinite loop
- Fix: Used http.StripPrefix("/static/", web.Handler()) for assets and routed / directly to web.Handler().ServeHTTP(w, r)
- Regression test: TestEdgeCase_HTTPAPIRoutes
- Lesson: Verify net/http path prefix stripping against canonical path redirects when using embed.FS

---

### Bug 4: Slow Follower Backoff Roundtrips During Partition Healing
- Found by: test (partition scenario)
- Trigger: Healing an isolated node with a diverging log in TestScenario5_NetworkPartitionPartitionLeader
- Symptom: Log replication required dozens of AppendEntries roundtrips before successfully synchronizing
- Root cause: Leader backed off nextIndex by a single entry per rejected AppendEntries RPC
- Fix: Implemented the Raft paper Section 5.3 fast conflict-term backoff optimization. Follower returns ConflictTerm and ConflictIndex; leader searches its log for that term and skips conflicting blocks in one roundtrip
- Regression test: TestScenario5_NetworkPartitionPartitionLeader
- Lesson: Naive linear backoff nextIndex-- causes quadratic roundtrips during cluster divergence; use term-based skipping

---

### Bug 5: Client Duplicate Execution on Retries
- Found by: test (packet loss stress)
- Trigger: Network packet loss where client mutation commits but RPC response is dropped
- Symptom: Client retry re-executed non-idempotent operations twice against state machine
- Root cause: State machine did not track client request identifiers or sequence numbers
- Fix: Introduced client deduplication table map[string]ClientRecord (LastSeq, LastResult) in internal/kv/kv.go, serialized into snapshots to survive compaction
- Regression test: TestEdgeCase_DeduplicationAndIdempotency
- Lesson: Replicated state machines require request deduplication to bridge at-least-once transport to exact-once linearizable semantics

---

### Bug 6: commitIndex Regression on Empty AppendEntries (Heartbeats)
- Found by: test (replication stress)
- Trigger: Heartbeat arrival on follower with PrevLogIndex < commitIndex
- Symptom: Follower regressed commitIndex backwards
- Root cause: On empty heartbeats, lastNewIndex computed as req.PrevLogIndex + uint64(len(req.Entries)) equaled req.PrevLogIndex. Setting commitIndex = min(leaderCommit, lastNewIndex) pulled commitIndex backward
- Fix: Differentiated empty heartbeats from log replication. For heartbeats, upper bound is n.log.LastIndex(). Enforced strict monotonic advancement if newCommit > n.commitIndex
- Regression test: TestScenario3_LogReplication
- Lesson: Never allow state machine markers or commit indices to regress; enforce strict monotonicity

---

### Bug 7: Out-of-Order Entry Application via Detached Goroutines
- Found by: test (concurrency stress)
- Trigger: Rapid commit bursts saturating applyCh channel buffer
- Symptom: State machine applied entries out of log index order
- Root cause: Channel saturation spawned detached goroutines go func(m ApplyMsg) { n.applyCh <- m }(msg), allowing OS thread scheduling to reorder applied entries
- Fix: Replaced detached goroutines with a thread-safe FIFO queue guarded by sync.Cond and consumed by a single sequential worker goroutine
- Regression test: TestEdgeCase_HeavyConcurrentWritesAndReads
- Lesson: Unbounded concurrency to relieve channel backpressure destroys FIFO ordering guarantees

---

### Bug 8: Duplicate Vote Counting on RPC Retransmissions
- Found by: test (network churn)
- Trigger: Candidate receiving duplicate RequestVote responses from the same peer
- Symptom: Candidate claimed election victory with minority quorum
- Root cause: votesReceived was an integer counter incremented on every granted vote response
- Fix: Replaced integer counter with votesGranted map[string]bool initialized with [n.id] = true, making vote aggregation strictly idempotent
- Regression test: TestScenario1_ElectionStability
- Lesson: Vote counting must be idempotent across packet duplications and retries

---

### Bug 9: Snapshot Boundary Term Check Bypass
- Found by: test (snapshot boundary verification)
- Trigger: AppendEntries RPC where req.PrevLogIndex == n.log.LastIncludedIndex()
- Symptom: Follower skipped term validation when PrevLogIndex landed exactly on snapshot boundary
- Root cause: Guard was written as req.PrevLogIndex > n.log.LastIncludedIndex(), omitting boundary equality
- Fix: Extended check to req.PrevLogIndex >= n.log.LastIncludedIndex() and verified term against LastIncludedTerm()
- Regression test: TestScenario6_SlowFollowerSnapshotCatchUp
- Lesson: Boundary conditions at log compaction boundaries must include equality checks

---

### Bug 10: Proposal Waiter Registration Race on Fast Commits
- Found by: test (low-latency benchmark)
- Trigger: Rapid client proposal completing before waiter registration in StateMachine
- Symptom: Client timed out despite proposal successfully committing
- Root cause: Propose() was invoked prior to waiter channel registration under lock
- Fix: Added appliedResults ring cache in StateMachine. If apply finishes before waiter registration, result is cached and returned immediately
- Regression test: TestGrill_ConcurrentDeduplicationRace
- Lesson: Asynchronous commit completion can outrun caller waiter registration; decouple with a ring buffer of applied results

---

### Bug 11: Async Snapshot State Divergence
- Found by: test (concurrent write stress)
- Trigger: High write throughput concurrent with state machine snapshot compaction
- Symptom: Snapshots captured mutations applied after the snapshot boundary index
- Root cause: Background snapshot goroutine acquired read lock asynchronously, allowing intervening commands to mutate state
- Fix: Deep-cloned state maps synchronously under lock at the moment threshold was reached, passing the immutable clone to the serialization worker
- Regression test: TestGrill_MassivePayloadStress
- Lesson: Background persistence must operate on point-in-time immutable state clones

---

### Bug 12: Cluster Panic on Restarting Unknown Node ID
- Found by: fuzzing (edge case chaos)
- Trigger: Invoking RestartNode with an unregistered node ID
- Symptom: Process panicked with nil pointer dereference on cfg.Storage.LoadState()
- Root cause: RestartNode lacked existence check for target ID in cluster configuration
- Fix: Added presence validation in RestartNode and added nil check for Storage in NewNode
- Regression test: TestEdgeCase_NonExistentNodeChaos
- Lesson: Validate all IDs and external inputs at cluster boundary interfaces

---

### Bug 13: Stalled lastApplied Progression on Deduplicated and Malformed Commands
- Found by: test (edge case suite)
- Trigger: Submitting duplicate or malformed JSON payloads to the state machine
- Symptom: lastApplied index lagged behind committed log index
- Root cause: Early returns in applyCommand skipped lastApplied advancement and snapshot evaluation
- Fix: Advanced lastApplied unconditionally upon dequeuing each commit message, notifying waiters on malformed JSON
- Regression test: TestEdgeCase_DeduplicationAndIdempotency
- Lesson: Commit stream consumption must always advance application markers regardless of command execution outcome

---

### Bug 14: HTTP 409 vs 404 Status Misclassification on Missing Keys
- Found by: test (API boundary suite)
- Trigger: GET /api/v1/kv/get for a nonexistent key
- Symptom: REST API returned 409 Conflict instead of 404 Not Found
- Root cause: Handler grouped all non-nil errors from Execute() under http.StatusConflict before inspecting res.Found
- Fix: Added explicit inspection for ErrKeyNotFound, mapping missing keys to 404 Not Found
- Regression test: TestEdgeCase_HTTPAPIRoutes
- Lesson: Differentiate application-level missing data from consensus-level replication conflict

---

### Bug 15: Empty Key Submissions Leading to State Inconsistencies
- Found by: test (boundary testing)
- Trigger: Submitting PUT / GET / DELETE with empty key ""
- Symptom: State machine accepted empty keys, polluting internal storage
- Root cause: Missing validation for key != "" in Execute() and HTTP handlers
- Fix: Added strict input validation returning 400 Bad Request on empty keys
- Regression test: TestEdgeCase_EmptyKeyHandling
- Lesson: Validate external input boundaries before passing requests to consensus

---

### Bug 16: Nil Pointer Panic on Empty Client Endpoints
- Found by: test (unit boundary)
- Trigger: Initializing api.NewClient with an empty endpoint list
- Symptom: Index out of range panic in retryLoop
- Root cause: Client accessed c.endpoints[idx] without verifying slice length > 0
- Fix: Added early length check returning an error if no endpoints are provided
- Regression test: TestEdgeCase_HTTPAPIRoutes
- Lesson: Never access slice indices without length guards, especially on public client constructors

---

### Bug 17: Torn Write Tail Truncation Leak on Partial EOF Header
- Found by: fuzzing (fault injection)
- Trigger: Terminating process while writing the 8-byte WAL record header, leaving <8 bytes at EOF
- Symptom: Fewer than 8 bytes broke the read loop without truncating, leaving torn bytes permanently on disk
- Root cause: LoadState() handled EOF by breaking out without truncating file to startOffset
- Fix: Enforced os.Truncate(walFile, startOffset) whenever read returned an error or incomplete header
- Regression test: TestGrill_CorruptWALRecovery
- Lesson: Framed binary storage must actively truncate partial tail headers on recovery

---

### Bug 18: Unbounded Slice Allocation Hazard on Corrupted WAL Length
- Found by: fuzzing (fault injection)
- Trigger: Injecting corrupted length field claiming 4 GB payload in WAL header
- Symptom: Process panicked with out-of-memory error during crash recovery
- Root cause: LoadState allocated make([]byte, length) directly based on untrusted disk bytes before checksum verification
- Fix: Capped single record allocation to 32 MB, snapshot headers to 16 MB, and snapshot payloads to 256 MB
- Regression test: TestGrill_CorruptWALRecovery
- Lesson: Bound all memory allocations derived from external or untrusted binary headers

---

### Bug 19: Unconditional votedFor Reset on Step Down in Same Term
- Found by: review (code audit)
- Trigger: Candidate receiving AppendEntries from legitimate leader in the same term
- Symptom: Potential double-voting if another candidate solicited a vote in that term
- Root cause: becomeFollower cleared votedFor unconditionally even when term did not increment
- Fix: Restricted votedFor clearing to strict term increments (term > n.currentTerm)
- Regression test: TestScenario2_ReelectionOnLeaderFailure
- Lesson: Only clear election vote records when the term strictly advances

---

### Bug 20: Swallowed Persistence Errors in Vote Granting and Replication
- Found by: review (code audit)
- Trigger: Disk write failure during RequestVote or AppendEntries
- Symptom: Node granted votes or acknowledged entries without durable persistence
- Root cause: persist() discarded errors silently from SaveState()
- Fix: Refactored persist() to return error and updated RPC handlers to refuse requests on disk error
- Regression test: TestEdgeCase_StorageFailureProtection
- Lesson: Never swallow storage errors in consensus state transitions

---

### Bug 21: Obsolete Compacted Log Entry Infiltration in NewRaftLog
- Found by: test (compaction recovery)
- Trigger: Node restart when WAL contains entries prior to snapshot compaction boundary
- Symptom: Slice index skew causing out-of-bounds log access or panic
- Root cause: NewRaftLog loaded all WAL entries without pruning entries with Index <= LastIncludedIndex
- Fix: Filtered loaded entries in NewRaftLog to prune any entry where Index <= LastIncludedIndex
- Regression test: TestScenario7_RollingRestart
- Lesson: Storage log recovery must reconcile raw WAL entries against snapshot compaction boundaries

---

### Bug 22: Premature lastApplied Advancement on Missing Log Entry
- Found by: review (code audit)
- Trigger: Entry retrieval failure during state machine log application
- Symptom: lastApplied incremented past missing entries, skipping mutations
- Root cause: lastApplied++ executed before verifying successful entry retrieval
- Fix: Staged next index, verified entry retrieval first, and broke out of apply loop on failure
- Regression test: TestScenario3_LogReplication
- Lesson: Never increment application state counters before successfully reading the underlying entry

---

### Bug 23: Leader Completeness Compaction Heuristic Underflow Hazard
- Found by: review (code audit)
- Trigger: Verifying Leader Completeness invariant during active compaction
- Symptom: uint64 arithmetic underflow in slice boundary estimation
- Root cause: Heuristic calculation using slice length difference underflowed when log was trimmed
- Fix: Added LastIncludedIndex to NodeState and checked index boundaries directly
- Regression test: TestScenario8_ChaosTestingAndLinearizability
- Lesson: Use explicit boundary markers rather than deriving offsets from slice lengths

---

### Bug 24: Unhandled Persistence Error in becomeFollower
- Found by: review (adversarial code audit)
- Trigger: Disk failure when node steps down to a higher term upon receiving higher-term RPC
- Symptom: Node updated term and cleared votedFor in memory without disk persistence, risking double-voting after restart
- Root cause: becomeFollower discarded persist() error with _ =
- Fix: Evaluated persist() error in becomeFollower. On failure, emitted StorageFatal event and halted node (go n.Stop())
- Regression test: TestEdgeCase_StorageFailureProtection
- Lesson: Crash-stop immediately when consensus state cannot be made durable

---

### Bug 25: Follower Uncommitted Log Desynchronization on Persist Failure in processAppendEntries
- Found by: review (adversarial code audit)
- Trigger: Disk write failure during follower AppendEntries processing after in-memory append
- Symptom: Follower kept unpersisted entries in memory while returning Success: false, risking acknowledging them on subsequent heartbeats
- Root cause: Entries were appended to in-memory log prior to persist() with no rollback on error
- Fix: Staged pre-append log snapshot. On persist failure, restored in-memory log via RestoreEntries, emitted StorageFatal, and halted node
- Regression test: TestEdgeCase_StorageFailureProtection
- Lesson: Roll back all in-memory mutations when disk persistence fails before halting

---

### Bug 26: Missing Directory Metadata fsync After File Rename
- Found by: review (adversarial code audit)
- Trigger: Sudden power loss immediately following atomic file replacement via os.Rename
- Symptom: File contents were flushed via Sync(), but parent directory metadata was not fsynced on POSIX file systems
- Root cause: Absence of directory handle sync after os.Rename calls in SaveState and SaveSnapshot
- Fix: Added syncDir(dir string) helper executing Sync() on parent directory handle after rename operations (enforced on POSIX, no-op on Windows)
- Regression test: not testable without fault-injecting the filesystem
- Lesson: On POSIX filesystems, directory entry durability requires fsync on the parent directory descriptor

---

### Bug 27: Silently Discarded Snapshot Error in checkSnapshotThreshold
- Found by: review (adversarial code audit)
- Trigger: Snapshot compaction failure in state machine background goroutine
- Symptom: Error from sm.raftNode.Snapshot() was discarded with _ =, hiding compaction failures
- Root cause: Unchecked error return in asynchronous goroutine
- Fix: Evaluated error from sm.raftNode.Snapshot() and emitted structured SnapshotError event to cluster event bus
- Regression test: not testable without fault-injecting the filesystem
- Lesson: Asynchronous background operations must emit diagnostic events on failure rather than silently discarding errors
