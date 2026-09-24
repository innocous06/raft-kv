# Chaos Bug Log & Root Cause Analysis

As outlined in the design blueprint, real distributed systems testing surfaces subtle race conditions, type mismatches, and boundary edge cases. Below is the record of bugs identified by the harness, their root causes, and their fixes.

---

### Bug 1: Named Struct Type Mismatch in Internal Actor Loop
* **Trigger/Test**: Initial cluster bootstrap (`TestScenario1_ElectionStability`)
* **Symptom**: `c.WaitLeader()` deadlocked and hung indefinitely.
* **Root Cause**: `GetState()` defined an inner struct `type stateResp struct { term uint64; ... }`. The dispatch helper used a type switch `switch c := replyCh.(type)` with `case chan struct { ... }`. In Go, named types and anonymous struct types are distinct types; the type switch failed silently without sending a response, causing `<-ch` to wait forever.
* **Fix**: Replaced dynamic interface casting with package-level `StateSummary` and explicit typed channels, eliminating all reflection overhead and preventing type-mismatch deadlocks.

---

### Bug 2: Indeterminate Unacknowledged Writes in Linearizability Checking
* **Trigger/Test**: Chaos test suite (Seed 42 / Seed 707, `TestScenario8_ChaosTestingAndLinearizability`)
* **Symptom**: `Linearizability violation on key 'k-0': no sequential history matches real-time intervals`.
* **Root Cause**: During chaos injection, a client submitted `Put(k, v)`. While Raft was replicating the entry, network partition or latency caused the client's timeout (1500ms) to expire. The client marked `Success = false`. However, the quorum had already committed the entry! When a subsequent `Get(k)` read `v`, the naive linearizability checker had discarded the unacknowledged `Put`, falsely concluding that `v` appeared without an initiating write.
* **Fix**: In distributed systems (Porcupine / Jepsen theory), unacknowledged operations are indeterminate. Enhanced `harness/linearizability.go` so that if an unacknowledged write's payload is observed by a subsequent read, it is included in the linearization search space.

---

### Bug 3: Embedded Web FileServer Canonical Path Redirection Loop
* **Trigger/Test**: Embedded web dashboard launch (`raft-node.exe -cluster=3`)
* **Symptom**: HTTP client returned `Too many automatic redirections were attempted`.
* **Root Cause**: `http.FileServer` mounted over `fs.Sub(staticFiles, "static")` expects paths relative to the sub-filesystem root. Setting `r.URL.Path = "/static/index.html"` inside a root handler caused the standard library's `http.FileServer` to issue HTTP 301 canonical redirects in an infinite loop.
* **Fix**: Used `http.StripPrefix("/static/", web.Handler())` for static assets and routed `/` directly to `web.Handler().ServeHTTP(w, r)`, serving `index.html` with zero redirects.

---

### Bug 4: Slow Follower Backoff Roundtrips During Partition Healing
* **Trigger/Test**: Scenario 5 (`TestScenario5_NetworkPartitionPartitionLeader`)
* **Symptom**: Reconnecting an isolated node with a diverging log required dozens of AppendEntries roundtrips before log replication succeeded.
* **Root Cause**: Leader backed off `nextIndex` by 1 entry per rejected AppendEntries RPC.
* **Fix**: Implemented the Raft paper §5.3 fast conflict-term optimization in `internal/raft/replication.go`. The follower returns `ConflictTerm` and `ConflictIndex`; the leader searches its own log for that term and skips the entire block of conflicting entries in one roundtrip.

---

### Bug 5: Client Duplicate Execution on Retries
* **Trigger/Test**: Scenario 3 & 4 under network packet loss
* **Symptom**: If an RPC response was dropped after committing, a client retry could execute the operation twice (e.g. non-idempotent operations).
* **Root Cause**: State machine did not track client request sequence numbers.
* **Fix**: Introduced client deduplication table `map[string]ClientRecord` (`LastSeq`, `LastResult`) in `internal/kv/kv.go`. If `SeqNum <= LastSeq`, the cached result is returned immediately without re-executing. The deduplication table is also serialized into snapshots to survive log compaction.

---

### Bug 6: commitIndex Regression on Empty AppendEntries (Heartbeats)
* **Trigger/Test**: Heartbeat processing in `internal/raft/replication.go`
* **Symptom**: Followers could regress `commitIndex` backwards if `PrevLogIndex < commitIndex` when receiving an empty heartbeat.
* **Root Cause**: On empty `AppendEntries`, `lastNewIndex` computed as `req.PrevLogIndex + uint64(len(req.Entries))` equaled `req.PrevLogIndex`. Setting `commitIndex = min(leaderCommit, lastNewIndex)` regressed `commitIndex`.
* **Fix**: Differentiated empty heartbeats from log entry replication. For heartbeats, the upper bound is `n.log.LastIndex()`. Enforced strict monotonic non-decreasing advancement `if newCommit > n.commitIndex { n.commitIndex = newCommit; n.applyEntries() }`.

---

### Bug 7: Out-of-Order Entry Application via Detached Goroutines
* **Trigger/Test**: Channel backpressure during rapid commit bursts
* **Symptom**: `applyCh` channel saturation spawned detached goroutines `go func(m ApplyMsg) { n.applyCh <- m }(msg)`, allowing OS thread scheduling to reorder applied commands.
* **Root Cause**: Unbounded concurrency on channel overflow discarded FIFO ordering guarantees.
* **Fix**: Implemented a dedicated thread-safe FIFO `applyQueue []ApplyMsg` guarded by `sync.Cond` and processed by a single sequential worker goroutine (`applierLoop`), preserving strict monotonic log order.

---

### Bug 8: Duplicate Vote Counting on RPC Retransmissions
* **Trigger/Test**: Unreliable transport with packet duplication or client retransmissions
* **Symptom**: Candidate node could count multiple vote responses from the same peer, potentially winning leadership with a minority quorum.
* **Root Cause**: `votesReceived` was an integer counter incremented upon each incoming granted vote response.
* **Fix**: Replaced integer counter with `votesGranted map[string]bool` initialized with `[n.id] = true`. Inbound votes set `votesGranted[v.peer] = true`, making vote aggregation strictly idempotent.

---

### Bug 9: Snapshot Boundary Term Check Bypass
* **Trigger/Test**: AppendEntries RPC where `req.PrevLogIndex == n.log.LastIncludedIndex()`
* **Symptom**: Follower skipped `prevLogTerm` validation when `req.PrevLogIndex` landed exactly on the snapshot boundary.
* **Root Cause**: Guard was written as `req.PrevLogIndex > n.log.LastIncludedIndex()`, omitting equality.
* **Fix**: Extended check to `req.PrevLogIndex >= n.log.LastIncludedIndex()` and verified `term == req.PrevLogTerm` against `n.log.LastIncludedTerm()`. If `PrevLogIndex < LastIncludedIndex`, returns fast conflict backoff to trigger `InstallSnapshot`.

---

### Bug 10: Proposal Waiter Registration Race on Fast Commits
* **Trigger/Test**: In-memory and low-latency environments under concurrent proposals
* **Symptom**: Proposal completed and applied before `Execute()` registered its channel in `sm.waiters`, causing client timeouts despite successful commits.
* **Root Cause**: `Propose()` was invoked prior to waiter channel registration under lock.
* **Fix**: Added `appliedResults map[uint64]OpResult` cache in `StateMachine`. If `applyCommand()` finishes before a waiter is registered, the result is cached. `Execute()` checks the cache under lock before waiting.

---

### Bug 11: Async Snapshot State Divergence
* **Trigger/Test**: High write throughput concurrent with snapshot compaction
* **Symptom**: State machine snapshots captured mutations applied *after* the snapshot boundary index.
* **Root Cause**: `go sm.takeSnapshot(sm.lastApplied)` acquired `RLock()` asynchronously, allowing intervening commands to mutate `sm.data` before serialization.
* **Fix**: Synchronously deep-cloned state maps under `sm.mu.Lock()` at the moment threshold was reached, passing the immutable clone to the asynchronous serialization goroutine.

---

### Bug 12: Cluster Panic on Restarting Unknown Node ID
* **Trigger/Test**: `TestEdgeCase_NonExistentNodeChaos` / POST `/api/v1/chaos/restart?node=unknown`
* **Symptom**: Server panicked with nil pointer dereference on `cfg.Storage.LoadState()`.
* **Root Cause**: `RestartNode(id)` lacked existence validation on `c.storages[id]`. Attempting to restart an unconfigured node passed `nil` storage into `NewNode`, crashing the process.
* **Fix**: Added validation in `RestartNode` ensuring `id` is present in cluster configuration before proceeding, and added an explicit `cfg.Storage == nil` guard in `NewNode`.

---

### Bug 13: Stalled `lastApplied` Progression on Deduplicated and Malformed Commands
* **Trigger/Test**: `TestEdgeCase_DeduplicationAndIdempotency` & `TestEdgeCase_MalformedCommandHandling`
* **Symptom**: State machine `lastApplied` diverged from Raft log index when receiving duplicate or malformed client commands.
* **Root Cause**: Early returns in `applyCommand` omitted `sm.lastApplied = msg.CommandIndex` and skipped `checkSnapshotThreshold()`.
* **Fix**: Advanced `sm.lastApplied = msg.CommandIndex` unconditionally at the entrance of `applyCommand`, notified waiters on malformed JSON, and factored snapshot evaluation into `checkSnapshotThreshold()`.

---

### Bug 14: HTTP 409 vs 404 Status Misclassification on Missing Keys
* **Trigger/Test**: `TestEdgeCase_HTTPAPIRoutes` (GET `/api/v1/kv/get?key=nonexistent`)
* **Symptom**: REST API returned `409 Conflict` instead of `404 Not Found` when a queried key did not exist.
* **Root Cause**: `handleGet` grouped all non-nil errors from `Execute()` under `http.StatusConflict` before checking `res.Found`.
* **Fix**: Added explicit inspection for `kv.ErrKeyNotFound` in `handleGet`, mapping missing keys to `404 Not Found` and replication/leadership failures to `409 Conflict`.

---

### Bug 15: Empty Key Submissions Leading to State Inconsistencies
* **Trigger/Test**: `TestEdgeCase_EmptyKeyHandling` (Put/Get/Delete with `""` key)
* **Symptom**: State machine accepted zero-length keys, polluting internal maps and generating ambiguous API paths.
* **Root Cause**: Lack of boundary validation for `op.Key == ""` in `StateMachine.Execute()` and HTTP API handlers.
* **Fix**: Enforced strict validation across `Execute()` and API routing, returning `400 Bad Request` with structured JSON error `{"success": false, "error": "key cannot be empty"}`.

---

### Bug 16: Nil Pointer Panic on Empty Client Endpoints
* **Trigger/Test**: Initializing `api.NewClient([]string{})`
* **Symptom**: Index out of range panic in `retryLoop`.
* **Root Cause**: `c.endpoints[idx]` accessed elements without verifying `len(c.endpoints) > 0`.
* **Fix**: Added early return in `retryLoop` if `len(c.endpoints) == 0`, returning `fmt.Errorf("no cluster endpoints configured")`.

---

### Bug 17: Torn Write Tail Truncation Leak on Partial EOF Header
* **Trigger/Test**: `TestGrill_CorruptWALRecovery`
* **Symptom**: Fewer than 8 bytes at EOF broke the WAL read loop without truncating `wal.log`, leaving torn partial header bytes permanently on disk across reboots.
* **Root Cause**: `LoadState()` checked `if err == io.EOF || err == io.ErrUnexpectedEOF` and broke out without truncating to `startOffset`.
* **Fix**: Enforced `_ = os.Truncate(d.walFile, startOffset)` whenever `err != nil || n < 8`, restoring `wal.log` cleanly to the last verified record boundary.

---

### Bug 18: Unbounded Slice Allocation Hazard on Corrupted WAL Length
* **Trigger/Test**: `TestGrill_CorruptWALRecovery`
* **Symptom**: A corrupted 4-byte length field in a WAL header specifying an extreme value (e.g. 4 GB) crashed the process with out-of-memory allocation panics.
* **Root Cause**: Unbounded `make([]byte, length)` allocated directly based on unchecked disk bytes before checksum evaluation.
* **Fix**: Capped single record allocation to 32 MB, snapshot headers to 16 MB, and snapshot payloads to 256 MB, safely truncating corrupted logs before memory allocation.


