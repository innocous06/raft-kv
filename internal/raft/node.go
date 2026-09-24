package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"raft-kv/internal/events"
)

// Config contains configuration options for a Raft node.
type Config struct {
	ID                 string
	Peers              []string
	Storage            Storage
	Transport          Transport
	Clock              Clock
	EventBus           *events.Bus
	HeartbeatInterval  time.Duration
	MinElectionTimeout time.Duration
	MaxElectionTimeout time.Duration
	ApplyCh            chan ApplyMsg
}

// DefaultConfig provides recommended settings for testing and production.
func DefaultConfig(id string, peers []string) Config {
	return Config{
		ID:                 id,
		Peers:              peers,
		HeartbeatInterval:  50 * time.Millisecond,
		MinElectionTimeout: 150 * time.Millisecond,
		MaxElectionTimeout: 300 * time.Millisecond,
		ApplyCh:            make(chan ApplyMsg, 500),
		Clock:              RealClock{},
		EventBus:           events.DefaultBus,
	}
}

// Internal messages delivered to the Raft event loop.
type rpcCall struct {
	req   any
	reply chan any
	err   chan error
}

type proposeMsg struct {
	data    []byte
	replyCh chan proposeResult
}

type proposeResult struct {
	index uint64
	term  uint64
	isLeader bool
}

type voteResponseMsg struct {
	peer string
	resp *RequestVoteResponse
	term uint64
}

type appendResponseMsg struct {
	peer        string
	req         *AppendEntriesRequest
	resp        *AppendEntriesResponse
	term        uint64
	entriesSent int
}

type installSnapshotResponseMsg struct {
	peer string
	req  *InstallSnapshotRequest
	resp *InstallSnapshotResponse
	term uint64
}

// Node represents a single Raft node executing Ongaro & Ousterhout consensus.
type Node struct {
	cfg Config
	id  string

	// Persistent state on all servers (Figure 2)
	currentTerm uint64
	votedFor    string
	log         *RaftLog

	// Volatile state on all servers
	commitIndex uint64
	lastApplied uint64
	role        Role
	leaderID    string

	// Leader-only volatile state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	votesReceived int

	// Infrastructure & channels
	storage   Storage
	transport Transport
	clock     Clock
	events    *events.Bus
	applyCh   chan ApplyMsg

	rpcCh      chan rpcCall
	proposeCh  chan proposeMsg
	voteRespCh chan voteResponseMsg
	appRespCh  chan appendResponseMsg
	snapRespCh chan installSnapshotResponseMsg

	stopCh    chan struct{}
	stopped   int32
	loopDone  chan struct{}
	rng       *rand.Rand
	rngMu     sync.Mutex

	// Timers
	electionTimer *time.Timer
	heartbeatTick *time.Ticker
}

// NewNode constructs and initializes a Raft node.
func NewNode(cfg Config) (*Node, error) {
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 50 * time.Millisecond
	}
	if cfg.MinElectionTimeout == 0 {
		cfg.MinElectionTimeout = 150 * time.Millisecond
	}
	if cfg.MaxElectionTimeout == 0 {
		cfg.MaxElectionTimeout = 300 * time.Millisecond
	}
	if cfg.Clock == nil {
		cfg.Clock = RealClock{}
	}
	if cfg.EventBus == nil {
		cfg.EventBus = events.DefaultBus
	}
	if cfg.ApplyCh == nil {
		cfg.ApplyCh = make(chan ApplyMsg, 500)
	}

	n := &Node{
		cfg:        cfg,
		id:         cfg.ID,
		role:       Follower,
		storage:    cfg.Storage,
		transport:  cfg.Transport,
		clock:      cfg.Clock,
		events:     cfg.EventBus,
		applyCh:    cfg.ApplyCh,
		rpcCh:      make(chan rpcCall, 200),
		proposeCh:  make(chan proposeMsg, 200),
		voteRespCh: make(chan voteResponseMsg, 100),
		appRespCh:  make(chan appendResponseMsg, 100),
		snapRespCh: make(chan installSnapshotResponseMsg, 50),
		stopCh:     make(chan struct{}),
		loopDone:   make(chan struct{}),
		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano() + int64(hashString(cfg.ID)))),
	}

	// Restore persistent state from storage if available
	term, votedFor, entries, err := cfg.Storage.LoadState()
	if err != nil {
		return nil, fmt.Errorf("failed to load persistent state: %w", err)
	}
	n.currentTerm = term
	n.votedFor = votedFor

	// Restore snapshot if available
	snapData, snapIndex, snapTerm, err := cfg.Storage.LoadSnapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to load snapshot: %w", err)
	}

	n.log = NewRaftLog(entries, snapIndex, snapTerm)
	n.commitIndex = snapIndex
	n.lastApplied = snapIndex

	if len(snapData) > 0 {
		// Forward snapshot restore to state machine
		go func() {
			n.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapData,
				SnapshotIndex: snapIndex,
				SnapshotTerm:  snapTerm,
			}
		}()
	}

	return n, nil
}

// Start registers the node with the transport and launches the main event loop.
func (n *Node) Start() {
	if n.transport != nil {
		n.transport.Register(n.id, n)
	}

	n.electionTimer = time.NewTimer(n.randomElectionTimeout())
	n.heartbeatTick = time.NewTicker(n.cfg.HeartbeatInterval)

	n.events.Emit(n.id, events.NodeRestarted, n.role.String(), n.currentTerm, "Node started", nil)
	go n.run()
}

// Stop cleanly terminates the node.
func (n *Node) Stop() {
	if !atomic.CompareAndSwapInt32(&n.stopped, 0, 1) {
		return
	}
	close(n.stopCh)
	if n.transport != nil {
		n.transport.Unregister(n.id)
	}
	<-n.loopDone

	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTick != nil {
		n.heartbeatTick.Stop()
	}
	n.events.Emit(n.id, events.NodeCrashed, n.role.String(), n.currentTerm, "Node stopped", nil)
}

// IsStopped returns true if node has stopped.
func (n *Node) IsStopped() bool {
	return atomic.LoadInt32(&n.stopped) == 1
}

// ID returns the node identifier.
func (n *Node) ID() string {
	return n.id
}

// ApplyCh exposes the channel where committed commands are emitted.
func (n *Node) ApplyCh() <-chan ApplyMsg {
	return n.applyCh
}

// run is the single-threaded event loop for the Raft core.
// It owns all mutable state and eliminates race conditions.
func (n *Node) run() {
	defer close(n.loopDone)

	for {
		select {
		case <-n.stopCh:
			return

		case <-n.electionTimer.C:
			if n.role != Leader {
				n.startElection()
			}

		case <-n.heartbeatTick.C:
			if n.role == Leader {
				n.broadcastAppendEntries()
			}

		case call := <-n.rpcCh:
			n.handleRPC(call)

		case p := <-n.proposeCh:
			n.handlePropose(p)

		case v := <-n.voteRespCh:
			n.handleVoteResponse(v)

		case a := <-n.appRespCh:
			n.handleAppendResponse(a)

		case s := <-n.snapRespCh:
			n.handleInstallSnapshotResponse(s)
		}
	}
}

// resetElectionTimer resets the randomized election timeout.
func (n *Node) resetElectionTimer() {
	if !n.electionTimer.Stop() {
		select {
		case <-n.electionTimer.C:
		default:
		}
	}
	n.electionTimer.Reset(n.randomElectionTimeout())
}

func (n *Node) randomElectionTimeout() time.Duration {
	n.rngMu.Lock()
	defer n.rngMu.Unlock()
	min := n.cfg.MinElectionTimeout
	max := n.cfg.MaxElectionTimeout
	if max <= min {
		return min
	}
	delta := max - min
	return min + time.Duration(n.rng.Int63n(int64(delta)))
}

// persist stores currentTerm, votedFor, and log entries to storage.
func (n *Node) persist() {
	if n.storage == nil {
		return
	}
	err := n.storage.SaveState(n.currentTerm, n.votedFor, n.log.AllEntries())
	if err != nil {
		n.events.Emit(n.id, events.EventType("StorageError"), n.role.String(), n.currentTerm,
			fmt.Sprintf("Failed to persist state: %v", err), nil)
	}
}

// becomeFollower steps down to follower and updates term.
func (n *Node) becomeFollower(term uint64, leaderID string) {
	prevRole := n.role
	prevTerm := n.currentTerm

	n.role = Follower
	n.leaderID = leaderID
	n.currentTerm = term
	n.votedFor = ""
	n.persist()

	if prevRole != Follower || prevTerm != term {
		n.events.Emit(n.id, events.RoleChanged, n.role.String(), n.currentTerm,
			fmt.Sprintf("Transitioned to Follower (term=%d, leader=%s)", term, leaderID), nil)
	}
	n.resetElectionTimer()
}

// handlePropose handles client command submissions to the leader.
func (n *Node) handlePropose(p proposeMsg) {
	if n.role != Leader {
		p.replyCh <- proposeResult{isLeader: false}
		return
	}

	newIndex := n.log.LastIndex() + 1
	newTerm := n.currentTerm
	entry := LogEntry{
		Index: newIndex,
		Term:  newTerm,
		Data:  p.data,
	}

	n.log.Append(entry)
	n.persist()

	n.events.Emit(n.id, events.EntryAppended, n.role.String(), n.currentTerm,
		fmt.Sprintf("Appended client entry index=%d term=%d", newIndex, newTerm), entry)

	// Send immediate replication
	n.broadcastAppendEntries()

	p.replyCh <- proposeResult{
		index:    newIndex,
		term:     newTerm,
		isLeader: true,
	}
}

// Propose submits a new command to the Raft cluster.
// Returns (index, term, isLeader).
func (n *Node) Propose(command []byte) (uint64, uint64, bool) {
	if atomic.LoadInt32(&n.stopped) == 1 {
		return 0, 0, false
	}
	replyCh := make(chan proposeResult, 1)
	select {
	case n.proposeCh <- proposeMsg{data: command, replyCh: replyCh}:
	case <-n.stopCh:
		return 0, 0, false
	}

	select {
	case res := <-replyCh:
		return res.index, res.term, res.isLeader
	case <-n.stopCh:
		return 0, 0, false
	}
}

// StateSummary contains brief node status.
type StateSummary struct {
	Term     uint64 `json:"term"`
	IsLeader bool   `json:"isLeader"`
	LeaderID string `json:"leaderId"`
}

// GetState returns the current term, whether this node believes it is the leader, and leader ID.
func (n *Node) GetState() (term uint64, isLeader bool, leaderID string) {
	if atomic.LoadInt32(&n.stopped) == 1 {
		return 0, false, ""
	}
	retCh := make(chan any, 1)
	call := rpcCall{
		req: internalCall(func() any {
			return StateSummary{
				Term:     n.currentTerm,
				IsLeader: n.role == Leader,
				LeaderID: n.leaderID,
			}
		}),
		reply: retCh,
		err:   make(chan error, 1),
	}
	select {
	case n.rpcCh <- call:
	case <-n.stopCh:
		return 0, false, ""
	}

	select {
	case val := <-retCh:
		s := val.(StateSummary)
		return s.Term, s.IsLeader, s.LeaderID
	case <-n.stopCh:
		return 0, false, ""
	}
}

// GetNodeState returns an immutable snapshot of internal state for diagnostics.
func (n *Node) GetNodeState() NodeState {
	if atomic.LoadInt32(&n.stopped) == 1 {
		return NodeState{ID: n.id, IsAlive: false}
	}
	retCh := make(chan any, 1)
	call := rpcCall{
		req: internalCall(func() any {
			return NodeState{
				ID:          n.id,
				Role:        n.role.String(),
				Term:        n.currentTerm,
				VotedFor:    n.votedFor,
				LeaderID:    n.leaderID,
				CommitIndex: n.commitIndex,
				LastApplied: n.lastApplied,
				LastIndex:   n.log.LastIndex(),
				LastTerm:    n.log.LastTerm(),
				LogLength:   n.log.TotalCount(),
				IsAlive:     atomic.LoadInt32(&n.stopped) == 0,
			}
		}),
		reply: retCh,
		err:   make(chan error, 1),
	}
	select {
	case n.rpcCh <- call:
	case <-n.stopCh:
		return NodeState{ID: n.id, IsAlive: false}
	}

	select {
	case val := <-retCh:
		return val.(NodeState)
	case <-n.stopCh:
		return NodeState{ID: n.id, IsAlive: false}
	}
}

type internalCall func() any

func hashString(s string) int {
	h := 0
	for i := 0; i < len(s); i++ {
		h = 31*h + int(s[i])
	}
	return h
}
