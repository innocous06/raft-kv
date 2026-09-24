# Mathematical Foundations & Consensus Specification

This document details the safety properties, quorum mechanics, and inductive proof of the Raft consensus algorithm as implemented in this repository.

---

## 1. Quorum Intersection

For any cluster of $N$ nodes, every election and log commit requires approval from a strict majority quorum $Q$:

$$Q = \left\lfloor \frac{N}{2} \right\rfloor + 1$$

Because any two majorities $Q_1, Q_2 \subseteq N$ must intersect:

$$|Q_1 \cap Q_2| \ge 1$$

Every validly elected leader contains at least one node that approved the most recently committed log entry.

---

## 2. Core Safety Invariants

### A. Election Safety
$$\forall \text{ Term } T, \quad |\text{Leaders}(T)| \le 1$$
A candidate requires $Q$ votes to win term $T$. Each node persists its vote (`votedFor`, `currentTerm`) to disk and grants at most one vote per term. By Quorum Intersection, two candidates cannot both assemble majorities in the same term.

### B. Log Matching Property
$$\left( \text{log}_1[i].\text{term} = \text{log}_2[i].\text{term} \right) \implies \left( \forall k \le i, \; \text{log}_1[k] = \text{log}_2[k] \right)$$
Leaders append at most one entry per index per term and entries are never mutated or reordered. Followers reject `AppendEntries` if the entry preceding new ones does not match in index and term (`prevLogIndex`, `prevLogTerm`).

### C. Leader Completeness (Inductive Proof)
If a log entry is committed at $(index, term = T)$, that entry is present in the logs of the leaders for all higher terms $T' > T$:

$$\text{committed}(e) \implies \forall T' > e.\text{term}, \; \text{Leader}(T').\text{contains}(e)$$

**Proof by Induction (Ongaro & Ousterhout §5.4.1, §5.4.2):**
1. **Base Case:** Leader $L_T$ commits entry $e$ in term $T$. By definition, $e$ is replicated on a majority of nodes $Q_{\text{commit}}$.
2. **Inductive Hypothesis:** Assume entry $e$ is present in the logs of all leaders from term $T$ through term $U-1$.
3. **Inductive Step:** Consider candidate $C_U$ elected leader for term $U$.
   * $C_U$ must receive votes from a majority quorum $Q_{\text{vote}}$.
   * By Quorum Intersection, $Q_{\text{commit}} \cap Q_{\text{vote}} \ne \emptyset$. There exists at least one node $v \in Q_{\text{commit}} \cap Q_{\text{vote}}$.
   * Voter $v$ accepted $e$ during term $T$, so its log contains $e$.
   * Under Raft's voting rule (§5.4.1), voter $v$ grants its vote to candidate $C_U$ only if $C_U$'s log is at least as up-to-date as $v$'s log.
   * If $C_U$ and $v$ share the same last term, $C_U$'s log is at least as long as $v$'s log; if $C_U$'s last term is greater, it must contain entries from terms $\ge T$.
   * In either case, by the Log Matching Property, candidate $C_U$'s log must contain entry $e$.
   * Therefore, $C_U$ contains entry $e$ when becoming leader in term $U$. By induction, all leaders in terms $T' > T$ contain $e$.

---

## 3. Linearizability (Wing & Gong 1993)

A concurrent execution history $H$ is linearizable if there exists an equivalent sequential execution $S$ such that:

$$S \sim H \quad \wedge \quad \forall op_1, op_2 \in H, \; \left(op_1 \prec_H op_2 \implies op_1 \prec_S op_2\right)$$

Raft-KV verifies client traces against this sequential specification using an in-repo implementation of the Wing & Gong (1993) search algorithm (`harness/linearizability.go`).

**Complexity Note:** Linearizability checking is NP-complete in general (Gibbons & Korach, 1997). The checker searches across execution graphs of concurrent operations, verifying valid sequential orderings that respect real-time invocation and response intervals.

---

## 4. Bounded Model Checking with TLC

The formal TLA+ specification in `specs/Raft.tla` and configuration `specs/MC.cfg` evaluates these properties via bounded model checking:

```bash
cd specs
java -cp tla2tools.jar tlc2.TLC -dfid 8 -config MC.cfg MC.tla
```

Bounded model checking to depth 8 explores 31,645 states with 0 invariant violations of `ElectionSafety`, `LogMatching`, and `LeaderCompleteness` under the model constants (3 servers, max term 3, max log length 3).

---

## 5. Storage Durability & Atomicity Boundaries

* **No full append-only WAL:** Rather than maintaining a continuously appended file with inline compaction/truncation records, `DiskStorage` uses atomic snapshot-rewrite per persist. Each persist writes all uncompacted entries to a temporary file, fsyncs, and atomically renames it.
* **Sequential rather than jointly atomic file replacement:** Metadata (`metadata.json`) and log entries (`wal.log`) are replaced sequentially rather than within a single multi-file transaction. Each file is individually fsynced and replaced atomically via `os.Rename`, followed by a directory `Sync()`. Directory fsync is enforced on POSIX platforms (ext4/xfs), and is best-effort and a no-op on Windows. No crash-safe directory-level joint atomicity across power loss is claimed for the pair.

---

## 6. Storage Failure Matrix ("What happens if the disk fails?")

The Raft implementation enforces crash-stop semantics on persistent storage failure:

| Code Path | Operation / Context | Failure Handling | State Machine / Cluster Impact |
| :--- | :--- | :--- | :--- |
| `startElection` | Increment term & vote self | Reverts role to Follower, clears `votedFor`, decrements term, aborts election | Candidate does not solicit votes without durable term/vote record |
| `processRequestVote` | Grant vote to candidate | Restores previous `votedFor`, returns `VoteGranted: false` | Candidate is refused; node cannot grant vote without durable record |
| `processAppendEntries` | Append replicated entries | Restores pre-append log via `RestoreEntries()`, emits `StorageFatal`, returns `Success: false`, calls `go n.Stop()` | Follower halts immediately; unpersisted entries never acknowledged |
| `becomeFollower` | Step down on higher term | Emits `StorageFatal` event, halts node via `go n.Stop()` | Node halts immediately rather than running with unpersisted term |
| `handlePropose` | Leader client command | Truncates newly appended entry from in-memory log, replies `isLeader: false` | Phantom entries discarded; client receives failure and retries |
| `Snapshot` (`processSnapshot`) | Compact log with snapshot | Returns error without advancing snapshot markers or truncating log | Compaction aborts safely; previous log entries remain valid |
| `checkSnapshotThreshold` | Async state machine snapshot | Evaluates error and emits structured `SnapshotError` event to event bus | Monitored via cluster event stream without silent error discarding |

