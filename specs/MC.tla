--------------------------------- MODULE MC ---------------------------------
(*
 * TLC Model Checking Configuration Module for Raft.tla
 * Bounds parameters to verify ElectionSafety, LogMatching, and LeaderCompleteness.
 *)

EXTENDS Raft

CONSTANTS
    MCServer,
    MCValue

MCSpec == Init /\ [][Next /\ \A s \in Server : currentTerm[s] <= 2 /\ LastLogIndex(s) <= 2]_vars

=============================================================================
