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
	mu                   sync.Mutex
	cluster              *Cluster
	leadersPerTerm       map[uint64]string                   // term -> leader nodeID
	committed            map[uint64]raft.LogEntry            // index -> committed entry
	applied              map[uint64]string                   // index -> applied command payload
	leaderLastIndex      map[string]uint64                   // leaderKey -> highest seen LastIndex
	leaderEntriesHistory map[string]map[uint64]raft.LogEntry // leaderKey -> index -> LogEntry
}

// NewInvariantChecker creates a new invariant verifier.
func NewInvariantChecker(c *Cluster) *InvariantChecker {
	return &InvariantChecker{
		cluster:              c,
		leadersPerTerm:       make(map[uint64]string),
		committed:            make(map[uint64]raft.LogEntry),
		applied:              make(map[uint64]string),
		leaderLastIndex:      make(map[string]uint64),
		leaderEntriesHistory: make(map[string]map[uint64]raft.LogEntry),
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

	// 2. Leader Append-Only: Leader never overwrites or truncates its log
	if err := ic.checkLeaderAppendOnly(); err != nil {
		return err
	}

	// 3. Log Matching: Matching (index, term) implies identical prefixes
	if err := ic.checkLogMatching(); err != nil {
		return err
	}

	// 4. Leader Completeness: Committed entries present in all higher-term leaders
	if err := ic.checkLeaderCompleteness(); err != nil {
		return err
	}

	// 5. State Machine Safety: At most one command applied per log index
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
			if other, exists := currentTermLeaders[s.Term]; exists && other != id {
				return fmt.Errorf("[INVARIANT VIOLATION] Election Safety violated! Multiple leaders in term %d: %s and %s",
					s.Term, other, id)
			}
			currentTermLeaders[s.Term] = id

			if prevLeader, exists := ic.leadersPerTerm[s.Term]; exists && prevLeader != id {
				return fmt.Errorf("[INVARIANT VIOLATION] Election Safety violated! Historical leader %s replaced by %s in same term %d",
					prevLeader, id, s.Term)
			}
			ic.leadersPerTerm[s.Term] = id
		}
	}
	return nil
}

// checkLeaderAppendOnly verifies that a leader never overwrites or deletes its own entries (§5.3).
func (ic *InvariantChecker) checkLeaderAppendOnly() error {
	for _, id := range ic.cluster.NodeIDs() {
		n, ok := ic.cluster.GetNode(id)
		if !ok || n.IsStopped() {
			continue
		}
		term, isLeader, _ := n.GetState()
		if !isLeader {
			continue
		}

		st := n.GetNodeState()
		key := fmt.Sprintf("%s-term-%d", id, term)

		// 1. Leader logical LastIndex must be monotonic non-decreasing
		prevLastIndex, exists := ic.leaderLastIndex[key]
		if exists && st.LastIndex < prevLastIndex {
			return fmt.Errorf("[INVARIANT VIOLATION] Leader Append-Only violated! Leader %s in term %d shrank LastIndex from %d to %d",
				id, term, prevLastIndex, st.LastIndex)
		}
		ic.leaderLastIndex[key] = st.LastIndex

		// 2. Entries previously seen for this leader must not be mutated
		currentEntries := n.GetLogEntries()
		if currentEntries != nil {
			prevEntriesMap, mapExists := ic.leaderEntriesHistory[key]
			if !mapExists {
				prevEntriesMap = make(map[uint64]raft.LogEntry)
				ic.leaderEntriesHistory[key] = prevEntriesMap
			}
			for _, e := range currentEntries {
				if prev, found := prevEntriesMap[e.Index]; found {
					if prev.Term != e.Term || string(prev.Data) != string(e.Data) {
						return fmt.Errorf("[INVARIANT VIOLATION] Leader Append-Only violated! Leader %s modified existing entry at index %d",
							id, e.Index)
					}
				}
				prevEntriesMap[e.Index] = e
			}
		}
	}
	return nil
}

// checkLogMatching verifies that if two logs contain an entry with same index and term,
// all previous entries are identical (§5.3).
func (ic *InvariantChecker) checkLogMatching() error {
	nodeList := ic.cluster.NodeIDs()
	type logMap map[uint64]raft.LogEntry
	nodeLogs := make(map[string]logMap)

	for _, id := range nodeList {
		n, ok := ic.cluster.GetNode(id)
		if !ok || n.IsStopped() {
			continue
		}
		entries := n.GetLogEntries()
		lm := make(logMap)
		for _, e := range entries {
			lm[e.Index] = e
		}
		nodeLogs[id] = lm
	}

	for i := 0; i < len(nodeList); i++ {
		for j := i + 1; j < len(nodeList); j++ {
			id1 := nodeList[i]
			id2 := nodeList[j]
			lm1, ok1 := nodeLogs[id1]
			lm2, ok2 := nodeLogs[id2]
			if !ok1 || !ok2 {
				continue
			}

			for idx, e1 := range lm1 {
				e2, exists := lm2[idx]
				if exists && e1.Term == e2.Term {
					for prevIdx := uint64(1); prevIdx < idx; prevIdx++ {
						p1, p1Exists := lm1[prevIdx]
						p2, p2Exists := lm2[prevIdx]
						if p1Exists && p2Exists {
							if p1.Term != p2.Term || string(p1.Data) != string(p2.Data) {
								return fmt.Errorf("[INVARIANT VIOLATION] Log Matching violated between %s and %s at index %d prior to match index %d",
									id1, id2, prevIdx, idx)
							}
						}
					}
				}
			}
		}
	}
	return nil
}

// checkLeaderCompleteness verifies that if an entry is committed in term T,
// it appears in the log of all leaders of terms > T (§5.4).
func (ic *InvariantChecker) checkLeaderCompleteness() error {
	// 1. Collect all known committed entries from event bus history (captures exact commit moment)
	for _, ev := range ic.cluster.EventBus().History() {
		if ev.Type == events.EntryCommitted {
			if entry, ok := ev.Data.(raft.LogEntry); ok {
				ic.committed[entry.Index] = entry
			}
		}
	}

	// Also poll active node states
	for _, id := range ic.cluster.NodeIDs() {
		n, ok := ic.cluster.GetNode(id)
		if !ok || n.IsStopped() {
			continue
		}
		st := n.GetNodeState()
		if st.CommitIndex > 0 {
			entries := n.GetLogEntries()
			for _, e := range entries {
				if e.Index <= st.CommitIndex {
					ic.committed[e.Index] = e
				}
			}
		}
	}

	// 2. Check each active leader to ensure all committed entries from earlier terms exist
	for _, id := range ic.cluster.NodeIDs() {
		n, ok := ic.cluster.GetNode(id)
		if !ok || n.IsStopped() {
			continue
		}
		term, isLeader, _ := n.GetState()
		if !isLeader {
			continue
		}

		st := n.GetNodeState()
		leaderEntries := n.GetLogEntries()
		leaderMap := make(map[uint64]raft.LogEntry)
		for _, e := range leaderEntries {
			leaderMap[e.Index] = e
		}

		for idx, committedEntry := range ic.committed {
			if committedEntry.Term < term {
				// If index was compacted into snapshot, it was safely committed & compacted
				if idx <= st.LastIncludedIndex {
					continue
				}
				entryOnLeader, found := leaderMap[idx]
				if !found || entryOnLeader.Term != committedEntry.Term {
					return fmt.Errorf("[INVARIANT VIOLATION] Leader Completeness violated! Leader %s in term %d missing committed entry at index %d (term %d)",
						id, term, idx, committedEntry.Term)
				}
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
