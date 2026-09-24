package raft

import (
	"context"
	"fmt"
	"time"

	"raft-kv/internal/events"
)

// broadcastAppendEntries sends AppendEntries (heartbeats or replication) to all peers.
func (n *Node) broadcastAppendEntries() {
	if n.role != Leader {
		return
	}

	for _, peer := range n.cfg.Peers {
		prevIndex := n.nextIndex[peer] - 1
		if prevIndex < n.log.LastIncludedIndex() {
			// Follower is behind snapshot boundary; send InstallSnapshot
			n.sendInstallSnapshot(peer)
			continue
		}

		prevTerm, err := n.log.Term(prevIndex)
		if err != nil {
			prevTerm = 0
		}

		entries, _ := n.log.EntriesFrom(n.nextIndex[peer])

		req := &AppendEntriesRequest{
			Term:         n.currentTerm,
			LeaderID:     n.id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: n.commitIndex,
		}

		term := n.currentTerm
		entriesCount := len(entries)

		go func(p string, r *AppendEntriesRequest, eCount int) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.HeartbeatInterval*2)
			defer cancel()

			resp, err := n.transport.SendAppendEntries(ctx, p, r)
			if err != nil {
				return
			}

			select {
			case n.appRespCh <- appendResponseMsg{
				peer:        p,
				req:         r,
				resp:        resp,
				term:        term,
				entriesSent: eCount,
			}:
			case <-n.stopCh:
			}
		}(peer, req, entriesCount)
	}
}

// handleAppendResponse processes AppendEntries response from a peer.
func (n *Node) handleAppendResponse(a appendResponseMsg) {
	if n.role != Leader || a.term != n.currentTerm {
		return
	}

	if a.resp.Term > n.currentTerm {
		n.becomeFollower(a.resp.Term, "")
		return
	}

	if a.resp.Success {
		newMatch := a.req.PrevLogIndex + uint64(a.entriesSent)
		if newMatch > n.matchIndex[a.peer] {
			n.matchIndex[a.peer] = newMatch
		}
		n.nextIndex[a.peer] = n.matchIndex[a.peer] + 1

		// Raft Figure 8 Commit Rule:
		// If there exists an N > commitIndex, a majority of matchIndex[i] >= N,
		// and log[N].Term == currentTerm, set commitIndex = N.
		n.checkAndAdvanceCommitIndex()
	} else {
		if a.resp.ConflictTerm == 0 {
			n.nextIndex[a.peer] = a.resp.ConflictIndex
		} else {
			leaderHasTerm := false
			var lastIndexWithTerm uint64
			for i := n.log.LastIndex(); i > n.log.LastIncludedIndex(); i-- {
				if t, err := n.log.Term(i); err == nil && t == a.resp.ConflictTerm {
					leaderHasTerm = true
					lastIndexWithTerm = i
					break
				}
			}

			if leaderHasTerm {
				n.nextIndex[a.peer] = lastIndexWithTerm + 1
			} else {
				n.nextIndex[a.peer] = a.resp.ConflictIndex
			}
		}

		if n.nextIndex[a.peer] < 1 {
			n.nextIndex[a.peer] = 1
		}

		n.sendAppendEntriesToPeer(a.peer)
	}
}

// sendAppendEntriesToPeer sends an AppendEntries RPC to a single peer.
func (n *Node) sendAppendEntriesToPeer(peer string) {
	if n.role != Leader {
		return
	}

	prevIndex := n.nextIndex[peer] - 1
	if prevIndex < n.log.LastIncludedIndex() {
		n.sendInstallSnapshot(peer)
		return
	}

	prevTerm, _ := n.log.Term(prevIndex)
	entries, _ := n.log.EntriesFrom(n.nextIndex[peer])

	req := &AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}

	term := n.currentTerm
	entriesCount := len(entries)

	go func(p string, r *AppendEntriesRequest, eCount int) {
		ctx, cancel := context.WithTimeout(context.Background(), n.cfg.HeartbeatInterval*2)
		defer cancel()

		resp, err := n.transport.SendAppendEntries(ctx, p, r)
		if err != nil {
			return
		}

		select {
		case n.appRespCh <- appendResponseMsg{
			peer:        p,
			req:         r,
			resp:        resp,
			term:        term,
			entriesSent: eCount,
		}:
		case <-n.stopCh:
		}
	}(peer, req, entriesCount)
}

// checkAndAdvanceCommitIndex verifies Figure 8 commit rule.
func (n *Node) checkAndAdvanceCommitIndex() {
	lastIndex := n.log.LastIndex()
	totalNodes := len(n.cfg.Peers) + 1
	majority := (totalNodes / 2) + 1

	for idx := lastIndex; idx > n.commitIndex; idx-- {
		term, err := n.log.Term(idx)
		if err != nil {
			continue
		}
		// Figure 8: Leader only commits entries from its current term by counting replicas
		if term != n.currentTerm {
			continue
		}

		count := 1 // self
		for _, peer := range n.cfg.Peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}

		if count >= majority {
			n.commitIndex = idx
			n.events.Emit(n.id, events.EntryCommitted, n.role.String(), n.currentTerm,
				fmt.Sprintf("Committed entry index=%d (replicas=%d/%d)", idx, count, totalNodes), idx)
			n.applyEntries()
			break
		}
	}
}

// applyEntries streams committed entries to the apply channel via sequential queue.
func (n *Node) applyEntries() {
	var msgs []ApplyMsg
	for n.commitIndex > n.lastApplied {
		n.lastApplied++
		entry, err := n.log.Entry(n.lastApplied)
		if err != nil {
			continue
		}

		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Data,
			CommandIndex: entry.Index,
			CommandTerm:  entry.Term,
		}
		msgs = append(msgs, msg)

		n.events.Emit(n.id, events.EntryApplied, n.role.String(), n.currentTerm,
			fmt.Sprintf("Applied log entry index=%d term=%d", entry.Index, entry.Term), entry)
	}
	if len(msgs) > 0 {
		n.enqueueApply(msgs...)
	}
}

// HandleAppendEntries external handler (implements RPCHandler).
func (n *Node) HandleAppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	if n.IsStopped() {
		return nil, fmt.Errorf("node %s is stopped", n.id)
	}

	replyCh := make(chan any, 1)
	errCh := make(chan error, 1)

	select {
	case n.rpcCh <- rpcCall{req: req, reply: replyCh, err: errCh}:
	case <-n.stopCh:
		return nil, fmt.Errorf("node stopped")
	case <-time.After(1 * time.Second):
		return nil, fmt.Errorf("RPC timeout")
	}

	select {
	case res := <-replyCh:
		return res.(*AppendEntriesResponse), nil
	case err := <-errCh:
		return nil, err
	case <-n.stopCh:
		return nil, fmt.Errorf("node stopped")
	}
}

// processAppendEntries executes inside the event loop.
func (n *Node) processAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	// Rule 1: Reply false if term < currentTerm (§5.1)
	if req.Term < n.currentTerm {
		return &AppendEntriesResponse{
			Term:    n.currentTerm,
			Success: false,
		}
	}

	// If term >= currentTerm: recognize leader and reset election timer
	if req.Term > n.currentTerm || n.role != Follower {
		n.becomeFollower(req.Term, req.LeaderID)
	} else {
		n.leaderID = req.LeaderID
		n.resetElectionTimer()
	}

	// Rule 2: Reply false if log doesn't contain entry at prevLogIndex matching prevLogTerm (§5.3)
	lastLogIndex := n.log.LastIndex()

	// If our log is shorter than prevLogIndex:
	if req.PrevLogIndex > lastLogIndex {
		return &AppendEntriesResponse{
			Term:          n.currentTerm,
			Success:       false,
			ConflictIndex: lastLogIndex + 1,
			ConflictTerm:  0,
		}
	}

	// If prevLogIndex is behind snapshot boundary, cannot verify; request snapshot
	if req.PrevLogIndex < n.log.LastIncludedIndex() {
		return &AppendEntriesResponse{
			Term:          n.currentTerm,
			Success:       false,
			ConflictIndex: n.log.LastIncludedIndex() + 1,
			ConflictTerm:  0,
		}
	}

	// If prevLogIndex is at or after snapshot boundary, check term (§5.3)
	if req.PrevLogIndex >= n.log.LastIncludedIndex() {
		term, err := n.log.Term(req.PrevLogIndex)
		if err != nil || term != req.PrevLogTerm {
			// Find first index of conflicting term for fast backoff
			conflictTerm := term
			conflictIndex := req.PrevLogIndex
			for i := req.PrevLogIndex - 1; i > n.log.LastIncludedIndex(); i-- {
				if t, err := n.log.Term(i); err == nil && t == conflictTerm {
					conflictIndex = i
				} else {
					break
				}
			}
			return &AppendEntriesResponse{
				Term:          n.currentTerm,
				Success:       false,
				ConflictIndex: conflictIndex,
				ConflictTerm:  conflictTerm,
			}
		}
	}

	// Rule 3 & 4: Append new entries, resolving conflicts (§5.3)
	for i, entry := range req.Entries {
		if entry.Index <= n.log.LastIncludedIndex() {
			continue
		}
		if entry.Index <= n.log.LastIndex() {
			existingTerm, err := n.log.Term(entry.Index)
			if err == nil && existingTerm != entry.Term {
				// Delete existing entry and all following
				_ = n.log.Truncate(entry.Index)
				n.log.Append(req.Entries[i:]...)
				break
			}
		} else {
			n.log.Append(req.Entries[i:]...)
			break
		}
	}

	if len(req.Entries) > 0 {
		n.persist()
	}

	// Rule 5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) (§5.3)
	if req.LeaderCommit > n.commitIndex {
		var newCommit uint64
		if len(req.Entries) > 0 {
			lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
			if req.LeaderCommit < lastNewIndex {
				newCommit = req.LeaderCommit
			} else {
				newCommit = lastNewIndex
			}
		} else {
			lastLogIndex := n.log.LastIndex()
			if req.LeaderCommit < lastLogIndex {
				newCommit = req.LeaderCommit
			} else {
				newCommit = lastLogIndex
			}
		}
		if newCommit > n.commitIndex {
			n.commitIndex = newCommit
			n.applyEntries()
		}
	}

	return &AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: true,
	}
}
