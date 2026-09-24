# Fault-Injection Harness & Verification Engine

The fault-injection harness (`harness/`) is the core differentiator of `raft-kv`. Rather than relying on simple unit tests, the system is subjected to simulated real-world failures while continuously verifying distributed consensus invariants and strict linearizability.

---

## 1. Simulated Failures (`harness/faults.go`)

The harness supports dynamic, programmatic injection of the following failure scenarios:
1. **Node Crashes & Restarts**: Sudden process termination without unmounting or flushing; verifies crash recovery with persistent WAL and snapshot restoration.
2. **Network Partitions**:
   - `IsolateLeader`: Isolates the current leader into a 1-node partition.
   - `PartitionMajorityMinority`: Splits an $N$-node cluster into a majority quorum and minority island.
   - `Partition3Way`: Splits the cluster into three disjoint sets to simulate severe split-brain conditions.
3. **Packet Loss**: Configurable drop probability ($p \in [0.0, 1.0]$) randomly discarding RPC requests or replies.
4. **Latency Jitter & Delays**: Simulates GC pauses, network bufferbloat, and slow links by delaying packet delivery by randomized durations.
5. **Rolling Restarts**: Sequentially terminates and restarts every node in the cluster under continuous client write load.

---

## 2. The 5 Core Raft Safety Invariants (`harness/invariants.go`)

The `InvariantChecker` executes concurrently during all chaos tests, asserting that none of Raft's fundamental safety guarantees are ever violated:

1. **Election Safety**: At most one leader can be elected in any given term. The checker tracks historical and active leaders across all terms.
2. **Leader Append-Only**: A leader never overwrites, truncates, or deletes its own log entries; log length must be monotonically non-decreasing during leadership.
3. **Log Matching**: If two nodes contain a log entry with the same index and term, their logs are identical in all entries up to that index.
4. **Leader Completeness**: If a log entry is committed in a given term, that entry will be present in the logs of the leaders for all higher terms.
5. **State Machine Safety**: If a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index.

---

## 3. Strict Linearizability Verification (`harness/linearizability.go`)

To provide absolute proof of correctness, every client operation is logged with:
- Operation Type (`Put`, `Get`, `Delete`)
- Invocation start timestamp ($T_{start}$)
- Return completion timestamp ($T_{end}$)
- Arguments and returned values

The linearizability verification engine implements the **Wing & Gong DFS algorithm**:
- Operations on independent keys are partitioned and checked independently.
- Candidate linear execution paths must satisfy real-time precedence constraints ($op_1.End < op_2.Start \implies op_1 \prec op_2$).
- Unacknowledged operations that timed out during partitions are treated as indeterminate: if an unacknowledged write's value was observed by a subsequent read, it is verified in the valid linearized history.
