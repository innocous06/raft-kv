package raft

import (
	"context"
	"fmt"
	"time"

	"raft-kv/internal/events"
)

type snapshotMsg struct {
	index    uint64
	data     []byte
	replyErr chan error
}

// Snapshot is called by the state machine to truncate the log up to index.
func (n *Node) Snapshot(index uint64, snapshotData []byte) error {
	if n.IsStopped() {
		return fmt.Errorf("node %s is stopped", n.id)
	}

	replyErr := make(chan error, 1)
	call := rpcCall{
		req: snapshotMsg{
			index:    index,
			data:     snapshotData,
			replyErr: replyErr,
		},
		reply: make(chan any, 1),
		err:   make(chan error, 1),
	}

	select {
	case n.rpcCh <- call:
	case <-n.stopCh:
		return fmt.Errorf("node stopped")
	}

	select {
	case err := <-replyErr:
		return err
	case <-n.stopCh:
		return fmt.Errorf("node stopped")
	}
}

// processSnapshot processes snapshot request inside the event loop.
func (n *Node) processSnapshot(msg snapshotMsg) error {
	if msg.index <= n.log.LastIncludedIndex() {
		return nil // already compacted
	}
	if msg.index > n.lastApplied {
		return fmt.Errorf("cannot snapshot index %d > lastApplied %d", msg.index, n.lastApplied)
	}

	term, err := n.log.Term(msg.index)
	if err != nil {
		return fmt.Errorf("failed to get term for snapshot index %d: %w", msg.index, err)
	}

	if err := n.log.Compact(msg.index, term); err != nil {
		return fmt.Errorf("failed to compact log: %w", err)
	}

	if n.storage != nil {
		if err := n.storage.SaveSnapshot(msg.data, msg.index, term); err != nil {
			return fmt.Errorf("failed to save snapshot: %w", err)
		}
	}

	n.persist()
	n.events.Emit(n.id, events.SnapshotSaved, n.role.String(), n.currentTerm,
		fmt.Sprintf("Snapshot created at index=%d term=%d", msg.index, term), nil)
	return nil
}

// sendInstallSnapshot sends an InstallSnapshot RPC to a peer.
func (n *Node) sendInstallSnapshot(peer string) {
	if n.role != Leader || n.storage == nil {
		return
	}

	data, lastIdx, lastTerm, err := n.storage.LoadSnapshot()
	if err != nil || len(data) == 0 {
		return
	}

	req := &InstallSnapshotRequest{
		Term:              n.currentTerm,
		LeaderID:          n.id,
		LastIncludedIndex: lastIdx,
		LastIncludedTerm:  lastTerm,
		Data:              data,
	}

	term := n.currentTerm
	go func(p string, r *InstallSnapshotRequest) {
		ctx, cancel := context.WithTimeout(context.Background(), n.cfg.HeartbeatInterval*5)
		defer cancel()

		resp, err := n.transport.SendInstallSnapshot(ctx, p, r)
		if err != nil {
			return
		}

		select {
		case n.snapRespCh <- installSnapshotResponseMsg{
			peer: p,
			req:  r,
			resp: resp,
			term: term,
		}:
		case <-n.stopCh:
		}
	}(peer, req)
}

// handleInstallSnapshotResponse handles peer response to InstallSnapshot.
func (n *Node) handleInstallSnapshotResponse(s installSnapshotResponseMsg) {
	if n.role != Leader || s.term != n.currentTerm {
		return
	}

	if s.resp.Term > n.currentTerm {
		n.becomeFollower(s.resp.Term, "")
		return
	}

	if s.req.LastIncludedIndex > n.matchIndex[s.peer] {
		n.matchIndex[s.peer] = s.req.LastIncludedIndex
	}
	n.nextIndex[s.peer] = n.matchIndex[s.peer] + 1
}

// HandleInstallSnapshot external RPC handler.
func (n *Node) HandleInstallSnapshot(req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
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
		return res.(*InstallSnapshotResponse), nil
	case err := <-errCh:
		return nil, err
	case <-n.stopCh:
		return nil, fmt.Errorf("node stopped")
	}
}

// processInstallSnapshot executes inside the event loop.
func (n *Node) processInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	// Rule 1: Reply immediately if term < currentTerm
	if req.Term < n.currentTerm {
		return &InstallSnapshotResponse{Term: n.currentTerm}
	}

	if req.Term > n.currentTerm || n.role != Follower {
		n.becomeFollower(req.Term, req.LeaderID)
	} else {
		n.leaderID = req.LeaderID
		n.resetElectionTimer()
	}

	// If snapshot is older than what we already compacted, ignore
	if req.LastIncludedIndex <= n.log.LastIncludedIndex() {
		return &InstallSnapshotResponse{Term: n.currentTerm}
	}

	// Compact local log
	_ = n.log.Compact(req.LastIncludedIndex, req.LastIncludedTerm)

	if n.storage != nil {
		_ = n.storage.SaveSnapshot(req.Data, req.LastIncludedIndex, req.LastIncludedTerm)
	}
	n.persist()

	if req.LastIncludedIndex > n.commitIndex {
		n.commitIndex = req.LastIncludedIndex
	}
	if req.LastIncludedIndex > n.lastApplied {
		n.lastApplied = req.LastIncludedIndex
	}

	// Forward snapshot to state machine
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      req.Data,
		SnapshotIndex: req.LastIncludedIndex,
		SnapshotTerm:  req.LastIncludedTerm,
	}
	select {
	case n.applyCh <- msg:
	default:
		go func(m ApplyMsg) {
			n.applyCh <- m
		}(msg)
	}

	n.events.Emit(n.id, events.SnapshotInstalled, n.role.String(), n.currentTerm,
		fmt.Sprintf("Installed snapshot index=%d term=%d", req.LastIncludedIndex, req.LastIncludedTerm), nil)

	return &InstallSnapshotResponse{Term: n.currentTerm}
}

// handleRPC routes inbound calls inside the event loop.
func (n *Node) handleRPC(call rpcCall) {
	switch req := call.req.(type) {
	case *RequestVoteRequest:
		call.reply <- n.processRequestVote(req)
	case *AppendEntriesRequest:
		call.reply <- n.processAppendEntries(req)
	case *InstallSnapshotRequest:
		call.reply <- n.processInstallSnapshot(req)
	case snapshotMsg:
		err := n.processSnapshot(req)
		req.replyErr <- err
	case internalCall:
		call.reply <- req()
	default:
		call.err <- fmt.Errorf("unknown RPC call type: %T", req)
	}
}
