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
