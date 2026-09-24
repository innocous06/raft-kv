package kv

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"raft-kv/internal/events"
	"raft-kv/internal/raft"
)

var (
	ErrNotLeader = errors.New("not leader")
	ErrTimeout   = errors.New("operation timed out")
	ErrKeyNotFound = errors.New("key not found")
)

type OpType string

const (
	OpPut    OpType = "Put"
	OpGet    OpType = "Get"
	OpDelete OpType = "Delete"
)

// Op represents a command replicated through the Raft log.
type Op struct {
	Type     OpType `json:"type"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	ClientID string `json:"clientId"`
	SeqNum   uint64 `json:"seqNum"`
}

// OpResult is the execution result returned to waiting clients.
type OpResult struct {
	Value string `json:"value,omitempty"`
	Found bool   `json:"found"`
	Err   string `json:"err,omitempty"`
}

// SnapshotData represents the full serialized state of the KV state machine.
type SnapshotData struct {
	Data  map[string]string        `json:"data"`
	Dedup map[string]ClientRecord  `json:"dedup"`
}

// StateMachine is the replicated key-value state machine.
type StateMachine struct {
	mu           sync.RWMutex
	nodeID       string
	data         map[string]string
	dedup        map[string]ClientRecord
	waiters      map[uint64]chan OpResult
	raftNode     *raft.Node
	applyCh      <-chan raft.ApplyMsg
	lastApplied  uint64
	snapshotSize int // Snapshot threshold (entries count); <=0 disables auto snapshot
	eventBus     *events.Bus
	stopCh       chan struct{}
	doneCh       chan struct{}
}

// NewStateMachine creates a new KV state machine backed by a Raft node.
func NewStateMachine(nodeID string, raftNode *raft.Node, snapshotThreshold int, bus *events.Bus) *StateMachine {
	if bus == nil {
		bus = events.DefaultBus
	}

	sm := &StateMachine{
		nodeID:       nodeID,
		data:         make(map[string]string),
		dedup:        make(map[string]ClientRecord),
		waiters:      make(map[uint64]chan OpResult),
		raftNode:     raftNode,
		applyCh:      raftNode.ApplyCh(),
		snapshotSize: snapshotThreshold,
		eventBus:     bus,
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}

	go sm.applyLoop()
	return sm
}

// Close terminates the state machine background loop.
func (sm *StateMachine) Close() {
	close(sm.stopCh)
	<-sm.doneCh
}

// Execute submits an operation to Raft and waits for it to commit and apply.
func (sm *StateMachine) Execute(op Op, timeout time.Duration) (OpResult, error) {
	data, err := json.Marshal(op)
	if err != nil {
		return OpResult{}, err
	}

	index, term, isLeader := sm.raftNode.Propose(data)
	if !isLeader {
		_, _, leaderID := sm.raftNode.GetState()
		return OpResult{}, fmt.Errorf("%w: leader is %s", ErrNotLeader, leaderID)
	}

	waiter := make(chan OpResult, 1)
	sm.mu.Lock()
	sm.waiters[index] = waiter
	sm.mu.Unlock()

	defer func() {
		sm.mu.Lock()
		delete(sm.waiters, index)
		sm.mu.Unlock()
	}()

	select {
	case res := <-waiter:
		// Check if term changed while waiting
		currentTerm, isStillLeader, _ := sm.raftNode.GetState()
		if !isStillLeader || currentTerm != term {
			return OpResult{}, fmt.Errorf("%w: leadership lost during replication", ErrNotLeader)
		}
		if res.Err != "" {
			return res, errors.New(res.Err)
		}
		return res, nil

	case <-time.After(timeout):
		return OpResult{}, ErrTimeout

	case <-sm.stopCh:
		return OpResult{}, errors.New("state machine closed")
	}
}

// applyLoop reads committed entries from Raft applyCh and executes them.
func (sm *StateMachine) applyLoop() {
	defer close(sm.doneCh)

	for {
		select {
		case <-sm.stopCh:
			return

		case msg, ok := <-sm.applyCh:
			if !ok {
				return
			}

			if msg.SnapshotValid {
				sm.installSnapshot(msg.Snapshot, msg.SnapshotIndex)
				continue
			}

			if msg.CommandValid {
				sm.applyCommand(msg)
			}
		}
	}
}

func (sm *StateMachine) applyCommand(msg raft.ApplyMsg) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var op Op
	if err := json.Unmarshal(msg.Command, &op); err != nil {
		return
	}

	var result OpResult

	// Check client deduplication table for mutating operations (Put, Delete)
	if op.ClientID != "" && op.SeqNum > 0 {
		rec, exists := sm.dedup[op.ClientID]
		if exists && op.SeqNum <= rec.LastSeq {
			// Duplicate request: return cached result
			result = rec.LastResult
			sm.notifyWaiter(msg.CommandIndex, result)
			return
		}
	}

	// Apply operation to state machine
	switch op.Type {
	case OpPut:
		sm.data[op.Key] = op.Value
		result = OpResult{Value: op.Value, Found: true}

	case OpGet:
		val, found := sm.data[op.Key]
		result = OpResult{Value: val, Found: found}
		if !found {
			result.Err = ErrKeyNotFound.Error()
		}

	case OpDelete:
		val, found := sm.data[op.Key]
		if found {
			delete(sm.data, op.Key)
		}
		result = OpResult{Value: val, Found: found}
	}

	// Record in deduplication table
	if op.ClientID != "" && op.SeqNum > 0 && op.Type != OpGet {
		sm.dedup[op.ClientID] = ClientRecord{
			LastSeq:    op.SeqNum,
			LastResult: result,
		}
	}

	sm.lastApplied = msg.CommandIndex
	sm.notifyWaiter(msg.CommandIndex, result)

	// Check if log compaction / snapshot threshold reached
	if sm.snapshotSize > 0 && sm.lastApplied%uint64(sm.snapshotSize) == 0 {
		go sm.takeSnapshot(sm.lastApplied)
	}
}

func (sm *StateMachine) notifyWaiter(index uint64, result OpResult) {
	if ch, ok := sm.waiters[index]; ok {
		select {
		case ch <- result:
		default:
		}
	}
}

func (sm *StateMachine) takeSnapshot(index uint64) {
	sm.mu.RLock()
	snap := SnapshotData{
		Data:  make(map[string]string, len(sm.data)),
		Dedup: make(map[string]ClientRecord, len(sm.dedup)),
	}
	for k, v := range sm.data {
		snap.Data[k] = v
	}
	for k, v := range sm.dedup {
		snap.Dedup[k] = v
	}
	sm.mu.RUnlock()

	bytes, err := json.Marshal(snap)
	if err != nil {
		return
	}

	_ = sm.raftNode.Snapshot(index, bytes)
}

func (sm *StateMachine) installSnapshot(snapData []byte, index uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var snap SnapshotData
	if err := json.Unmarshal(snapData, &snap); err != nil {
		return
	}

	sm.data = snap.Data
	if sm.data == nil {
		sm.data = make(map[string]string)
	}
	sm.dedup = snap.Dedup
	if sm.dedup == nil {
		sm.dedup = make(map[string]ClientRecord)
	}
	sm.lastApplied = index
}

// GetAll returns a copy of the key-value store for state inspection.
func (sm *StateMachine) GetAll() map[string]string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	res := make(map[string]string, len(sm.data))
	for k, v := range sm.data {
		res[k] = v
	}
	return res
}
