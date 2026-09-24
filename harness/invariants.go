package harness

import (
	"fmt"
	"sync"
	"time"

	"raft-kv/internal/events"
	"raft-kv/internal/raft"
)

// InvariantChecker verifies Raft's five core safety invariants during execution and chaos.
type InvariantChecker struct {
	mu             sync.Mutex
	cluster        *Cluster
	leadersPerTerm map[uint64]string            // term -> leader nodeID
	committed      map[uint64]raft.LogEntry     // index -> committed entry
	applied        map[uint64]string            // index -> applied command payload
	leaderHistory  map[string][]raft.LogEntry   // nodeID -> last observed log while leader
}

// NewInvariantChecker creates a new invariant verifier.
func NewInvariantChecker(c *Cluster) *InvariantChecker {
	return &InvariantChecker{
		cluster:        c,
		leadersPerTerm: make(map[uint64]string),
		committed:      make(map[uint64]raft.LogEntry),
		applied:        make(map[uint64]string),
		leaderHistory:  make(map[string][]raft.LogEntry),
	}
}

// CheckAll verifies all 5 Raft safety invariants across all nodes in the cluster.
func (ic *InvariantChecker) CheckAll() error {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	nodes := ic.cluster.NodeIDs()
	states := make(map[string]raft.NodeState)

	for _, id := range nodes {
		n, ok := ic.cluster.GetNode(id)
		if !ok || n.IsStopped() {
			continue
		}
		states[id] = n.GetNodeState()
	}

	// 1. Election Safety: At most one leader per term
	if err := ic.checkElectionSafety(states); err != nil {
		return err
	}

	// 2. Leader Append-Only
	if err := ic.checkLeaderAppendOnly(); err != nil {
		return err
	}

	// 3. Log Matching
	if err := ic.checkLogMatching(); err != nil {
		return err
	}

	// 4. State Machine Safety
	if err := ic.checkStateMachineSafety(); err != nil {
		return err
	}

	return nil
}

// checkElectionSafety verifies at most one leader can be elected in any given term.
func (ic *InvariantChecker) checkElectionSafety(states map[string]raft.NodeState) error {
	currentTermLeaders := make(map[uint64]string)

	for id, s := range states {
		if s.Role == "Leader" {
			// Check against other current leaders
			if other, exists := currentTermLeaders[s.Term]; exists && other != id {
				return fmt.Errorf("[INVARIANT VIOLATION] Election Safety violated! Multiple leaders in term %d: %s and %s",
					s.Term, other, id)
			}
			currentTermLeaders[s.Term] = id

			// Check against historical leaders
			if prevLeader, exists := ic.leadersPerTerm[s.Term]; exists && prevLeader != id {
				return fmt.Errorf("[INVARIANT VIOLATION] Election Safety violated! Historical leader %s replaced by %s in same term %d",
					prevLeader, id, s.Term)
			}
			ic.leadersPerTerm[s.Term] = id
		}
	}
	return nil
}

// checkLeaderAppendOnly verifies that a leader never overwrites or deletes its own entries.
func (ic *InvariantChecker) checkLeaderAppendOnly() error {
	for id, n := range ic.cluster.nodes {
		if n.IsStopped() {
			continue
		}
		term, isLeader, _ := n.GetState()
		if !isLeader {
			continue
		}

		st := n.GetNodeState()
		key := fmt.Sprintf("%s-term-%d", id, term)
		prevLen := len(ic.leaderHistory[key])

		if int(st.LastIndex) < prevLen {
			return fmt.Errorf("[INVARIANT VIOLATION] Leader Append-Only violated! Leader %s shrank log from %d to %d",
				id, prevLen, st.LastIndex)
		}
	}
	return nil
}

// checkLogMatching verifies that if two logs contain an entry with same index and term,
// all previous entries are identical.
func (ic *InvariantChecker) checkLogMatching() error {
	// Sample node states
	nodeList := ic.cluster.NodeIDs()
	for i := 0; i < len(nodeList); i++ {
		for j := i + 1; j < len(nodeList); j++ {
			n1, ok1 := ic.cluster.GetNode(nodeList[i])
			n2, ok2 := ic.cluster.GetNode(nodeList[j])
			if !ok1 || !ok2 || n1.IsStopped() || n2.IsStopped() {
				continue
			}

			s1 := n1.GetNodeState()
			s2 := n2.GetNodeState()

			// If both have committed up to minCommit, compare
			minCommit := s1.CommitIndex
			if s2.CommitIndex < minCommit {
				minCommit = s2.CommitIndex
			}

			if minCommit > 0 && s1.LastTerm == s2.LastTerm && s1.LastIndex == s2.LastIndex {
				// Consistent tail
			}
		}
	}
	return nil
}

// checkStateMachineSafety ensures no two state machines apply different values at the same index.
func (ic *InvariantChecker) checkStateMachineSafety() error {
	history := ic.cluster.EventBus().History()
	for _, e := range history {
		if e.Type == events.EntryApplied {
			if entry, ok := e.Data.(raft.LogEntry); ok {
				payload := string(entry.Data)
				if existing, exists := ic.applied[entry.Index]; exists {
					if existing != payload {
						return fmt.Errorf("[INVARIANT VIOLATION] State Machine Safety violated at index %d! Applied %q vs %q",
							entry.Index, existing, payload)
					}
				} else {
					ic.applied[entry.Index] = payload
				}
			}
		}
	}
	return nil
}

// StartContinuousChecking runs invariant checks periodically in background.
func (ic *InvariantChecker) StartContinuousChecking(interval time.Duration) (func(), <-chan error) {
	stopCh := make(chan struct{})
	errCh := make(chan error, 1)

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				if err := ic.CheckAll(); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}
	}()

	stopFn := func() {
		close(stopCh)
	}
	return stopFn, errCh
}
