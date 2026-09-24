package transport

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"raft-kv/internal/events"
	"raft-kv/internal/raft"
)

// SimNet is a controlled in-memory network simulator supporting:
// packet drops, latencies, reordering, symmetric and asymmetric network partitions,
// and deterministic RNG seeding for reproducible bug reproduction.
type SimNet struct {
	mu           sync.RWMutex
	handlers     map[string]raft.RPCHandler
	disconnected map[string]map[string]bool // from -> to -> blocked
	dropRate     float64
	minDelay     time.Duration
	maxDelay     time.Duration
	rng          *rand.Rand
	eventBus     *events.Bus
}

// NewSimNet creates a network simulator with a given random seed.
func NewSimNet(seed int64) *SimNet {
	return &SimNet{
		handlers:     make(map[string]raft.RPCHandler),
		disconnected: make(map[string]map[string]bool),
		rng:          rand.New(rand.NewSource(seed)),
		eventBus:     events.DefaultBus,
	}
}

// SetEventBus sets the event bus for telemetry.
func (s *SimNet) SetEventBus(b *events.Bus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eventBus = b
}

// SetDropRate sets message drop probability [0.0, 1.0].
func (s *SimNet) SetDropRate(rate float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropRate = rate
}

// SetDelays sets minimum and maximum simulated network delays.
func (s *SimNet) SetDelays(min, max time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.minDelay = min
	s.maxDelay = max
}

// Register connects a node to the simulated network.
func (s *SimNet) Register(nodeID string, handler raft.RPCHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[nodeID] = handler
}

// Unregister disconnects a node from the network.
func (s *SimNet) Unregister(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.handlers, nodeID)
}

// Disconnect breaks communication from node 'from' to node 'to' (asymmetric if desired).
func (s *SimNet) Disconnect(from, to string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.disconnected[from] == nil {
		s.disconnected[from] = make(map[string]bool)
	}
	s.disconnected[from][to] = true
}

// Connect restores communication between node 'from' and node 'to'.
func (s *SimNet) Connect(from, to string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.disconnected[from] != nil {
		delete(s.disconnected[from], to)
	}
}

// Partition creates network partitions among the provided groups.
// Nodes in different groups cannot communicate with each other.
func (s *SimNet) Partition(groups ...[]string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.disconnected = make(map[string]map[string]bool)

	// Map node -> group index
	nodeGroup := make(map[string]int)
	for gIdx, group := range groups {
		for _, node := range group {
			nodeGroup[node] = gIdx
		}
	}

	for n1, g1 := range nodeGroup {
		for n2, g2 := range nodeGroup {
			if g1 != g2 {
				if s.disconnected[n1] == nil {
					s.disconnected[n1] = make(map[string]bool)
				}
				s.disconnected[n1][n2] = true
			}
		}
	}

	if s.eventBus != nil {
		s.eventBus.Emit("cluster", events.PartitionCreated, "", 0,
			fmt.Sprintf("Created partition between %d groups", len(groups)), groups)
	}
}

// Heal removes all network partitions and restores full connectivity.
func (s *SimNet) Heal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.disconnected = make(map[string]map[string]bool)

	if s.eventBus != nil {
		s.eventBus.Emit("cluster", events.PartitionHealed, "", 0, "Network partition healed", nil)
	}
}

// IsBlocked checks if transmission between from and to is partitioned.
func (s *SimNet) IsBlocked(from, to string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.disconnected[from] != nil && s.disconnected[from][to] {
		return true
	}
	return false
}

// shouldDrop checks if a packet should be dropped based on dropRate.
func (s *SimNet) shouldDrop() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dropRate <= 0 {
		return false
	}
	return s.rng.Float64() < s.dropRate
}

// getDelay calculates simulated transit latency.
func (s *SimNet) getDelay() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.maxDelay <= 0 {
		return 0
	}
	if s.maxDelay <= s.minDelay {
		return s.minDelay
	}
	delta := s.maxDelay - s.minDelay
	return s.minDelay + time.Duration(s.rng.Int63n(int64(delta)))
}

func (s *SimNet) getHandler(to string) (raft.RPCHandler, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	handler, ok := s.handlers[to]
	if !ok {
		return nil, fmt.Errorf("node %s unreachable", to)
	}
	return handler, nil
}

// SendRequestVote delivers RequestVote RPC through the simulated network.
func (s *SimNet) SendRequestVote(ctx context.Context, to string, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	if s.IsBlocked(req.CandidateID, to) || s.shouldDrop() {
		if s.eventBus != nil {
			s.eventBus.Emit(req.CandidateID, events.MessageDropped, "", req.Term,
				fmt.Sprintf("RequestVote dropped to %s", to), nil)
		}
		return nil, fmt.Errorf("packet dropped to %s", to)
	}

	handler, err := s.getHandler(to)
	if err != nil {
		return nil, err
	}

	delay := s.getDelay()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return handler.HandleRequestVote(req)
}

// SendAppendEntries delivers AppendEntries RPC through the simulated network.
func (s *SimNet) SendAppendEntries(ctx context.Context, to string, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	if s.IsBlocked(req.LeaderID, to) || s.shouldDrop() {
		if s.eventBus != nil {
			s.eventBus.Emit(req.LeaderID, events.MessageDropped, "", req.Term,
				fmt.Sprintf("AppendEntries dropped to %s", to), nil)
		}
		return nil, fmt.Errorf("packet dropped to %s", to)
	}

	handler, err := s.getHandler(to)
	if err != nil {
		return nil, err
	}

	delay := s.getDelay()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return handler.HandleAppendEntries(req)
}

// SendInstallSnapshot delivers InstallSnapshot RPC through the simulated network.
func (s *SimNet) SendInstallSnapshot(ctx context.Context, to string, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	if s.IsBlocked(req.LeaderID, to) || s.shouldDrop() {
		return nil, fmt.Errorf("packet dropped to %s", to)
	}

	handler, err := s.getHandler(to)
	if err != nil {
		return nil, err
	}

	delay := s.getDelay()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return handler.HandleInstallSnapshot(req)
}
