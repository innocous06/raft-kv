-------------------------------- MODULE Raft --------------------------------
(*
 * Formal TLA+ specification of the Raft Consensus Protocol
 * Based on Ongaro & Ousterhout (2014) "In Search of an Understandable Consensus Algorithm"
 *
 * Focuses on core safety properties:
 * - Election Safety: At most one leader can be elected in a given term.
 * - Log Matching: If two logs contain an entry with the same index and term, then the logs
 *   are identical in all entries up through the given index.
 * - Leader Completeness: If a log entry is committed in a given term, then that entry will
 *   be present in the logs of the leaders for all higher-numbered terms.
 *)

EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS 
    Server,          \* Set of servers in the cluster
    Value,           \* Set of possible data values
    Nil              \* Null placeholder

VARIABLES
    currentTerm,     \* [Server -> Nat]: latest term server has seen
    state,           \* [Server -> {"Follower", "Candidate", "Leader"}]
    votedFor,        \* [Server -> Server \union {Nil}]: candidate server voted for in current term
    log,             \* [Server -> Seq([term: Nat, val: Value])]: log entries
    commitIndex,     \* [Server -> Nat]: index of highest log entry known to be committed
    messages         \* Set of network messages in transit

vars == <<currentTerm, state, votedFor, log, commitIndex, messages>>

\* Quorum size (strict majority)
Quorum == {i \in SUBSET Server : Cardinality(i) * 2 > Cardinality(Server)}

\* Helper: Last log term and index
LastLogIndex(s) == Len(log[s])
LastLogTerm(s) == IF LastLogIndex(s) = 0 THEN 0 ELSE log[s][LastLogIndex(s)].term

-----------------------------------------------------------------------------
\* Initial states

Init ==
    /\ currentTerm = [s \in Server |-> 0]
    /\ state       = [s \in Server |-> "Follower"]
    /\ votedFor    = [s \in Server |-> Nil]
    /\ log         = [s \in Server |-> <<>>]
    /\ commitIndex = [s \in Server |-> 0]
    /\ messages    = {}

-----------------------------------------------------------------------------
\* State Transitions

\* 1. Follower or Candidate election timeout
Timeout(s) ==
    /\ state[s] \in {"Follower", "Candidate"}
    /\ state' = [state EXCEPT ![s] = "Candidate"]
    /\ currentTerm' = [currentTerm EXCEPT ![s] = currentTerm[s] + 1]
    /\ votedFor' = [votedFor EXCEPT ![s] = s]
    /\ messages' = messages \cup {[type        |-> "RequestVoteRequest",
                                  term        |-> currentTerm[s] + 1,
                                  candidateId |-> s,
                                  lastLogIndex|-> LastLogIndex(s),
                                  lastLogTerm |-> LastLogTerm(s)]}
    /\ UNCHANGED <<log, commitIndex>>

\* 2. Server handles RequestVote request
HandleRequestVoteRequest(s, m) ==
    /\ m.type = "RequestVoteRequest"
    /\ LET logOk == \/ m.lastLogTerm > LastLogTerm(s)
                    \/ (m.lastLogTerm = LastLogTerm(s) /\ m.lastLogIndex >= LastLogIndex(s))
           grant == /\ m.term >= currentTerm[s]
                    /\ (votedFor[s] = Nil \/ votedFor[s] = m.candidateId)
                    /\ logOk
       IN
       /\ IF m.term > currentTerm[s]
          THEN /\ currentTerm' = [currentTerm EXCEPT ![s] = m.term]
               /\ state' = [state EXCEPT ![s] = "Follower"]
          ELSE /\ UNCHANGED <<currentTerm, state>>
       /\ IF grant
          THEN /\ votedFor' = [votedFor EXCEPT ![s] = m.candidateId]
               /\ messages' = (messages \ {m}) \cup {[type        |-> "RequestVoteResponse",
                                                     term        |-> m.term,
                                                     voter       |-> s,
                                                     candidateId |-> m.candidateId,
                                                     voteGranted |-> TRUE]}
          ELSE /\ votedFor' = votedFor
               /\ messages' = (messages \ {m}) \cup {[type        |-> "RequestVoteResponse",
                                                     term        |-> currentTerm[s],
                                                     voter       |-> s,
                                                     candidateId |-> m.candidateId,
                                                     voteGranted |-> FALSE]}
    /\ UNCHANGED <<log, commitIndex>>

\* 3. Candidate processes vote responses and becomes Leader on majority
BecomeLeader(s) ==
    /\ state[s] = "Candidate"
    /\ LET grantedVotes == {m.voter : m \in {msg \in messages : 
                                /\ msg.type = "RequestVoteResponse"
                                /\ msg.candidateId = s
                                /\ msg.term = currentTerm[s]
                                /\ msg.voteGranted = TRUE}} \cup {s}
       IN
       /\ grantedVotes \in Quorum
       /\ state' = [state EXCEPT ![s] = "Leader"]
       /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, messages>>

\* 4. Leader accepts new client proposal
ClientRequest(s, v) ==
    /\ state[s] = "Leader"
    /\ LET entry == [term |-> currentTerm[s], val |-> v]
           newLog == Append(log[s], entry)
       IN
       /\ log' = [log EXCEPT ![s] = newLog]
       /\ messages' = messages \cup {[type         |-> "AppendEntriesRequest",
                                      term         |-> currentTerm[s],
                                      leaderId     |-> s,
                                      prevLogIndex |-> LastLogIndex(s),
                                      prevLogTerm  |-> LastLogTerm(s),
                                      entries      |-> <<entry>>,
                                      leaderCommit |-> commitIndex[s]]}
       /\ UNCHANGED <<currentTerm, state, votedFor, commitIndex>>

\* 5. Follower processes AppendEntries request
HandleAppendEntriesRequest(s, m) ==
    /\ m.type = "AppendEntriesRequest"
    /\ IF m.term > currentTerm[s]
       THEN /\ currentTerm' = [currentTerm EXCEPT ![s] = m.term]
            /\ state' = [state EXCEPT ![s] = "Follower"]
            /\ votedFor' = [votedFor EXCEPT ![s] = Nil]
       ELSE /\ UNCHANGED <<currentTerm, state, votedFor>>
    /\ IF m.term < currentTerm[s]
       THEN /\ messages' = (messages \ {m}) \cup {[type    |-> "AppendEntriesResponse",
                                                  term    |-> currentTerm[s],
                                                  from    |-> s,
                                                  to      |-> m.leaderId,
                                                  success |-> FALSE]}
            /\ UNCHANGED <<log, commitIndex>>
       ELSE IF m.prevLogIndex > 0 /\ (m.prevLogIndex > LastLogIndex(s) \/ log[s][m.prevLogIndex].term # m.prevLogTerm)
       THEN /\ messages' = (messages \ {m}) \cup {[type    |-> "AppendEntriesResponse",
                                                  term    |-> currentTerm[s],
                                                  from    |-> s,
                                                  to      |-> m.leaderId,
                                                  success |-> FALSE]}
            /\ UNCHANGED <<log, commitIndex>>
       ELSE
            /\ LET newEntries == m.entries
                   commonLog  == SubSeq(log[s], 1, m.prevLogIndex)
                   mergedLog  == commonLog \o newEntries
                   newCommit  == IF m.leaderCommit > commitIndex[s]
                                 THEN IF m.leaderCommit < Len(mergedLog)
                                      THEN m.leaderCommit
                                      ELSE Len(mergedLog)
                                 ELSE commitIndex[s]
               IN
               /\ log' = [log EXCEPT ![s] = mergedLog]
               /\ commitIndex' = [commitIndex EXCEPT ![s] = newCommit]
               /\ messages' = (messages \ {m}) \cup {[type    |-> "AppendEntriesResponse",
                                                          term    |-> currentTerm[s],
                                                          from    |-> s,
                                                          to      |-> m.leaderId,
                                                          matchIdx|-> Len(mergedLog),
                                                          success |-> TRUE]}

\* 6. Leader advances commitIndex on majority match
AdvanceCommitIndex(s) ==
    /\ state[s] = "Leader"
    /\ \E idx \in (commitIndex[s]+1)..LastLogIndex(s) :
        /\ log[s][idx].term = currentTerm[s]  \* Raft Section 5.4.2 rule: only commit current term by counting
        /\ LET matchServers == {s} \cup {m.from : m \in {msg \in messages :
                                    /\ msg.type = "AppendEntriesResponse"
                                    /\ msg.to = s
                                    /\ msg.term = currentTerm[s]
                                    /\ msg.success = TRUE
                                    /\ msg.matchIdx >= idx}}
           IN
           /\ matchServers \in Quorum
           /\ commitIndex' = [commitIndex EXCEPT ![s] = idx]
           /\ UNCHANGED <<currentTerm, state, votedFor, log, messages>>

-----------------------------------------------------------------------------
\* Next state relation

Next ==
    \/ \E s \in Server : Timeout(s)
    \/ \E s \in Server, m \in messages : HandleRequestVoteRequest(s, m)
    \/ \E s \in Server : BecomeLeader(s)
    \/ \E s \in Server, v \in Value : ClientRequest(s, v)
    \/ \E s \in Server, m \in messages : HandleAppendEntriesRequest(s, m)
    \/ \E s \in Server : AdvanceCommitIndex(s)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Safety Invariants

\* 1. Election Safety: At most one leader per term
ElectionSafety ==
    \A s1, s2 \in Server :
        (state[s1] = "Leader" /\ state[s2] = "Leader" /\ currentTerm[s1] = currentTerm[s2])
        => s1 = s2

\* 2. Log Matching: Matching index and term implies matching prefix
LogMatching ==
    \A s1, s2 \in Server :
        \A idx \in 1..LastLogIndex(s1) :
            (idx <= LastLogIndex(s2) /\ log[s1][idx].term = log[s2][idx].term)
            => SubSeq(log[s1], 1, idx) = SubSeq(log[s2], 1, idx)

\* 3. Leader Completeness: Committed entries survive into all future leaders' logs
LeaderCompleteness ==
    \A s \in Server :
        (state[s] = "Leader") =>
            \A other \in Server :
                \A idx \in 1..commitIndex[other] :
                    idx <= LastLogIndex(s) /\ log[s][idx] = log[other][idx]

=============================================================================
