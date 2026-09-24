# References & Prior Work

This project builds upon foundational research in distributed systems, consensus theory, formal verification, and linearizability checking.

---

## Academic Papers

1. **Ongaro, D., & Ousterhout, J. (2014).**
   *In Search of an Understandable Consensus Algorithm.*
   USENIX Annual Technical Conference (ATC '14), Philadelphia, PA.
   [https://raft.github.io/raft.pdf](https://raft.github.io/raft.pdf)
   *Core reference for leader election, log replication, commit rules, safety invariants, and cluster configuration.*

2. **Ongaro, D. (2014).**
   *Consensus: Bridging Theory and Practice.*
   Ph.D. dissertation, Stanford University.
   [https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
   *Reference for log compaction, snapshot transfer, client interaction, linearizable semantics, and pre-vote mechanics.*

3. **Wing, J. M., & Gong, C. (1993).**
   *Testing and Verifying Concurrent Objects.*
   Journal of Parallel and Distributed Computing, 17(1-2), 164–182.
   *Reference for the sequential consistency / linearizability search algorithm implemented in `harness/linearizability.go`.*

4. **Gibbons, P. B., & Korach, E. (1997).**
   *Testing Shared Memories.*
   SIAM Journal on Computing, 26(4), 1208–1244.
   *Established that verifying linearizability for arbitrary concurrent executions is NP-complete.*

5. **Lamport, L. (2002).**
   *Specifying Systems: The TLA+ Language and Tools for Hardware and Software Engineers.*
   Addison-Wesley.
   [https://lamport.azurewebsites.net/tla/book.html](https://lamport.azurewebsites.net/tla/book.html)
   *Foundational reference for the TLA+ specification and TLC model checker configurations in `specs/`.*

---

## Open Source Implementations Studied

1. **HashiCorp Raft (`hashicorp/raft`)**
   Production Go implementation of Raft consensus used in Consul and Nomad.
   Studied for actor channel boundaries and state machine applier decoupling.

2. **etcd Raft (`etcd-io/raft`)**
   Production Go consensus engine used in Kubernetes and etcd.
   Studied for non-blocking state machine separation and message framing conventions.

3. **Porcupine (`anishathalye/porcupine`)**
   Fast linearizability checker in Go based on Wing & Gong.
   Inspired the pure-Go in-repo verification engine in `harness/linearizability.go`.
