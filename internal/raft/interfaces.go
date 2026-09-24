package raft

import (
	"context"
	"time"
)

// RPCHandler defines the interface for incoming Raft RPCs.
type RPCHandler interface {
	HandleRequestVote(req *RequestVoteRequest) (*RequestVoteResponse, error)
	HandleAppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	HandleInstallSnapshot(req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}

// Transport abstracts message delivery between Raft nodes.
// Enables transparent swapping between in-memory SimNet and real HTTP/gRPC.
type Transport interface {
	SendRequestVote(ctx context.Context, to string, req *RequestVoteRequest) (*RequestVoteResponse, error)
	SendAppendEntries(ctx context.Context, to string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	SendInstallSnapshot(ctx context.Context, to string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
	Register(nodeID string, handler RPCHandler)
	Unregister(nodeID string)
}

// Storage abstracts persistent state and snapshot storage.
type Storage interface {
	// SaveState atomically saves currentTerm, votedFor, and the log entries.
	SaveState(term uint64, votedFor string, entries []LogEntry) error
	// LoadState restores persistent state. If no state exists, returns 0, "", nil, nil.
	LoadState() (term uint64, votedFor string, entries []LogEntry, err error)
	// SaveSnapshot saves a serialized state machine snapshot and metadata.
	SaveSnapshot(snapshot []byte, lastIncludedIndex uint64, lastIncludedTerm uint64) error
	// LoadSnapshot loads the saved snapshot if any.
	LoadSnapshot() (snapshot []byte, lastIncludedIndex uint64, lastIncludedTerm uint64, err error)
	// Close flushes and closes underlying storage descriptors.
	Close() error
}

// Clock allows test suites to mock time or advance time deterministically.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	Sleep(d time.Duration)
}

// RealClock uses standard library time.
type RealClock struct{}

func (RealClock) Now() time.Time                         { return time.Now() }
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (RealClock) Sleep(d time.Duration)                  { time.Sleep(d) }
