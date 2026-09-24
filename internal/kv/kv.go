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
	ErrNotLeader   = errors.New("not leader")
	ErrTimeout     = errors.New("operation timed out")
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
	Data  map[string]string       `json:"data"`
	Dedup map[string]ClientRecord `json:"dedup"`
}

// StateMachine is the replicated key-value state machine.
type StateMachine struct {
	mu             sync.RWMutex
	nodeID         string
	data           map[string]string
	dedup          map[string]ClientRecord
	waiters        map[uint64]chan OpResult
	appliedResults map[uint64]OpResult
	raftNode       *raft.Node
	applyCh        <-chan raft.ApplyMsg
	lastApplied    uint64
	snapshotSize   int // Snapshot threshold (entries count); <=0 disables auto snapshot
	eventBus       *events.Bus
	stopCh         chan struct{}
	doneCh         chan struct{}
}

// NewStateMachine creates a new KV state machine backed by a Raft node.
func NewStateMachine(nodeID string, raftNode *raft.Node, snapshotThreshold int, bus *events.Bus) *StateMachine {
	if bus == nil {
		bus = events.DefaultBus
	}

	sm := &StateMachine{
		nodeID:         nodeID,
		data:           make(map[string]string),
		dedup:          make(map[string]ClientRecord),
		waiters:        make(map[uint64]chan OpResult),
		appliedResults: make(map[uint64]OpResult),
		raftNode:       raftNode,
		applyCh:        raftNode.ApplyCh(),
		snapshotSize:   snapshotThreshold,
		eventBus:       bus,
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
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
	if op.Key == "" {
		return OpResult{Err: "key cannot be empty"}, errors.New("key cannot be empty")
	}

	data, err := json.Marshal(op)
	if err != nil {
		return OpResult{}, err
	}

	index, term, isLeader := sm.raftNode.Propose(data)
	if !isLeader {
		_, _, leaderID := sm.raftNode.GetState()
		return OpResult{}, fmt.Errorf("%w: leader is %s", ErrNotLeader, leaderID)
	}

	sm.mu.Lock()
	// Check if already applied before we registered waiter (fast commit path)
	if res, ok := sm.appliedResults[index]; ok {
		delete(sm.appliedResults, index)
		sm.mu.Unlock()
		if res.Err != "" {
			return res, errors.New(res.Err)
		}
		return res, nil
	}

	waiter := make(chan OpResult, 1)
	sm.waiters[index] = waiter
	sm.mu.Unlock()

	defer func() {
		sm.mu.Lock()
		delete(sm.waiters, index)
		delete(sm.appliedResults, index)
		sm.mu.Unlock()
	}()

	select {
	case res := <-waiter:
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

	sm.lastApplied = msg.CommandIndex

	var op Op
	if err := json.Unmarshal(msg.Command, &op); err != nil {
		sm.notifyWaiter(msg.CommandIndex, OpResult{Err: "malformed command payload"})
		return
	}

	var result OpResult
	if op.ClientID != "" && op.SeqNum > 0 {
		rec, exists := sm.dedup[op.ClientID]
		if exists && op.SeqNum <= rec.LastSeq {
			result = rec.LastResult
			sm.notifyWaiter(msg.CommandIndex, result)
			sm.checkSnapshotThreshold()
			return
		}
	}

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

	default:
		result = OpResult{Err: fmt.Sprintf("unknown operation type: %s", op.Type)}
	}

	if op.ClientID != "" && op.SeqNum > 0 && op.Type != OpGet {
		sm.dedup[op.ClientID] = ClientRecord{
			LastSeq:    op.SeqNum,
			LastResult: result,
		}
	}

	sm.notifyWaiter(msg.CommandIndex, result)
	sm.checkSnapshotThreshold()
}

func (sm *StateMachine) checkSnapshotThreshold() {
	if sm.snapshotSize > 0 && sm.lastApplied%uint64(sm.snapshotSize) == 0 {
		// Synchronously clone state under sm.mu.Lock() to guarantee point-in-time consistency
		snapIndex := sm.lastApplied
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

		go func(idx uint64, s SnapshotData) {
			bytes, err := json.Marshal(s)
			if err != nil {
				return
			}
			_ = sm.raftNode.Snapshot(idx, bytes)
		}(snapIndex, snap)
	}
}

func (sm *StateMachine) notifyWaiter(index uint64, result OpResult) {
	if ch, ok := sm.waiters[index]; ok {
		select {
		case ch <- result:
		default:
		}
	} else {
		// Waiter not registered yet (fast commit); cache result so Execute can pick it up
		sm.appliedResults[index] = result
		if len(sm.appliedResults) > 500 {
			for k := range sm.appliedResults {
				if k < index-100 {
					delete(sm.appliedResults, k)
				}
			}
		}
	}
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

	for k := range sm.appliedResults {
		if k <= index {
			delete(sm.appliedResults, k)
		}
	}
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
