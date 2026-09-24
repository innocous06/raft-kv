# Limitations & Non-Goals

This document plainly records the architectural boundaries, known trade-offs, and deliberate non-goals of Raft-KV.

---

## 1. Storage Engine Boundaries

* **No Append-Only Segmented WAL:** Rather than maintaining continuous append logs with inline compaction pointers, `DiskStorage` uses atomic snapshot-rewrite per persist: all uncompacted entries are written to a temp file, fsynced, and renamed.
  * *Impact:* $O(\text{log size})$ write volume per commit; disk-backed throughput is ~119 writes/sec under synchronous fsync.
* **Sequential Two-File Replacement:** Metadata (`metadata.json`) and log entries (`wal.log`) are fsynced and renamed sequentially rather than within a joint multi-file filesystem transaction. No crash-safe directory-level joint atomicity is claimed across power loss.
* **Directory fsync OS Differences:** `syncDir()` flushes directory entry metadata on POSIX platforms (Linux, macOS), but is a best-effort no-op on Windows because Windows NTFS directory handles do not support `FlushFileBuffers`.

---

## 2. Protocol Non-Goals

* **Static Cluster Membership:** Cluster topology is fixed at initialization time. Dynamic membership reconfiguration (Raft paper §6: joint consensus and single-server transitions) is not implemented.
* **No Pre-Vote Protocol (§9.6):** Partitioned nodes reconnecting with incremented terms can trigger election cycles and force temporary leadership transitions.
* **No Raft Log Compaction Streaming:** Log compaction uses point-in-time state machine cloning and single snapshot chunks via HTTP, suitable for prototypes but not for terabyte-scale state machines.
* **Plaintext Transport:** Inter-node RPCs and client endpoints communicate via plain HTTP JSON without TLS/mTLS encryption or cryptographic mutual authentication.

---

## 3. Verification Boundaries

* **Bounded Model Checking:** The TLA+ formal specification (`specs/Raft.tla`) is verified via bounded model checking with TLC to depth 8 (constants: 3 servers, max term 3, max log length 3; 31,645 states explored with 0 violations). It is not a machine-checked mathematical proof (e.g. TLAPS or Coq).
* **Randomized Chaos Testing:** Chaos simulations use real timers and wall-clock RNG; runs cannot be strictly replayed from a deterministic seed due to OS thread scheduling and timer resolution.
* **Linearizability Checker Bound:** The in-repo Wing & Gong search algorithm is NP-complete. Checking histories is practical for ~100 to 300 operations during chaos runs; verifying millions of concurrent operations requires interval-tree pruning or WGL heuristic solvers.
