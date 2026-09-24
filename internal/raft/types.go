package raft

import (
	"fmt"
)

// Role represents the role of a Raft node.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry is a single replicated log entry.
type LogEntry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"data"`
}

// RequestVoteRequest arguments (Paper §5.2).
type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidateId"`
	LastLogIndex uint64 `json:"lastLogIndex"`
	LastLogTerm  uint64 `json:"lastLogTerm"`
}

// RequestVoteResponse results (Paper §5.2).
type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"voteGranted"`
}

// AppendEntriesRequest arguments (Paper §5.3).
type AppendEntriesRequest struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leaderId"`
	PrevLogIndex uint64     `json:"prevLogIndex"`
	PrevLogTerm  uint64     `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leaderCommit"`
}

// AppendEntriesResponse results (Paper §5.3) with conflict-term optimization.
type AppendEntriesResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflictIndex,omitempty"`
	ConflictTerm  uint64 `json:"conflictTerm,omitempty"`
}

// InstallSnapshotRequest arguments (Paper §7).
type InstallSnapshotRequest struct {
	Term              uint64 `json:"term"`
	LeaderID          string `json:"leaderId"`
	LastIncludedIndex uint64 `json:"lastIncludedIndex"`
	LastIncludedTerm  uint64 `json:"lastIncludedTerm"`
	Data              []byte `json:"data"`
}

// InstallSnapshotResponse results (Paper §7).
type InstallSnapshotResponse struct {
	Term uint64 `json:"term"`
}

// ApplyMsg is sent on the apply channel to the state machine when entries are committed or snapshots applied.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
	CommandTerm  uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  uint64
	SnapshotIndex uint64
}

// NodeState is a public snapshot of a node's volatile/persistent status for dashboard/harness inspection.
type NodeState struct {
	ID          string `json:"id"`
	Role        string `json:"role"`
	Term        uint64 `json:"term"`
	VotedFor    string `json:"votedFor"`
	LeaderID    string `json:"leaderId"`
	CommitIndex uint64 `json:"commitIndex"`
	LastApplied uint64 `json:"lastApplied"`
	LastIndex   uint64 `json:"lastIndex"`
	LastTerm    uint64 `json:"lastTerm"`
	LogLength   int    `json:"logLength"`
	IsAlive     bool   `json:"isAlive"`
}

func (s NodeState) String() string {
	return fmt.Sprintf("[%s] Role=%s Term=%d Leader=%s Commit=%d Applied=%d LogLen=%d Alive=%v",
		s.ID, s.Role, s.Term, s.LeaderID, s.CommitIndex, s.LastApplied, s.LogLength, s.IsAlive)
}
