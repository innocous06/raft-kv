--------------------------------- MODULE MC ---------------------------------
(*
 * TLC Model Checking Configuration Module for Raft.tla
 * Bounds parameters to verify ElectionSafety, LogMatching, and LeaderCompleteness.
 *)

EXTENDS Raft

CONSTANTS
    MCServer,
    MCValue

MCSpec == Init /\ [][Next /\ \A s \in Server : currentTerm[s] <= 3 /\ LastLogIndex(s) <= 3]_vars

=============================================================================
