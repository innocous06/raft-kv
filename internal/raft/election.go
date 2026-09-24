package raft

import (
	"context"
	"fmt"
	"time"

	"raft-kv/internal/events"
)

// startElection converts node to candidate, votes for self, and requests votes from peers.
func (n *Node) startElection() {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.votesGranted = make(map[string]bool)
	n.votesGranted[n.id] = true
	n.leaderID = ""
	n.persist()
	n.resetElectionTimer()

	term := n.currentTerm
	lastLogIndex := n.log.LastIndex()
	lastLogTerm := n.log.LastTerm()

	n.events.Emit(n.id, events.TermChanged, n.role.String(), term,
		fmt.Sprintf("Started election for term %d", term), nil)

	totalNodes := len(n.cfg.Peers) + 1
	majority := (totalNodes / 2) + 1

	// If single node cluster, immediately win election
	if len(n.votesGranted) >= majority {
		n.becomeLeader()
		return
	}

	// Request votes concurrently from peers
	for _, peer := range n.cfg.Peers {
		go func(p string) {
			req := &RequestVoteRequest{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.MinElectionTimeout)
			defer cancel()

			resp, err := n.transport.SendRequestVote(ctx, p, req)
			if err != nil {
				return
			}

			select {
			case n.voteRespCh <- voteResponseMsg{peer: p, resp: resp, term: term}:
			case <-n.stopCh:
			}
		}(peer)
	}
}

// handleVoteResponse processes a vote response on the event loop.
func (n *Node) handleVoteResponse(v voteResponseMsg) {
	if n.role != Candidate || v.term != n.currentTerm {
		return
	}

	if v.resp.Term > n.currentTerm {
		n.becomeFollower(v.resp.Term, "")
		return
	}

	if v.resp.VoteGranted {
		n.votesGranted[v.peer] = true
		totalVotes := len(n.votesGranted)
		n.events.Emit(n.id, events.VoteGranted, n.role.String(), n.currentTerm,
			fmt.Sprintf("Received vote from %s (total: %d)", v.peer, totalVotes), nil)

		totalNodes := len(n.cfg.Peers) + 1
		majority := (totalNodes / 2) + 1

		if totalVotes >= majority {
			n.becomeLeader()
		}
	}
}

// becomeLeader converts the node to leader and initializes volatile leader state.
func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.id

	// Initialize leader state
	lastIndex := n.log.LastIndex()
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = lastIndex + 1
		n.matchIndex[p] = 0
	}

	n.events.Emit(n.id, events.ElectionWon, n.role.String(), n.currentTerm,
		fmt.Sprintf("Elected leader for term %d with %d votes", n.currentTerm, len(n.votesGranted)), nil)
	n.events.Emit(n.id, events.RoleChanged, n.role.String(), n.currentTerm, "Leader", nil)

	// Send immediate heartbeats to assert leadership
	n.broadcastAppendEntries()
}

// HandleRequestVote handles external RPC request (implements RPCHandler).
func (n *Node) HandleRequestVote(req *RequestVoteRequest) (*RequestVoteResponse, error) {
	if n.IsStopped() {
		return nil, fmt.Errorf("node %s is stopped", n.id)
	}

	replyCh := make(chan any, 1)
	errCh := make(chan error, 1)

	select {
	case n.rpcCh <- rpcCall{req: req, reply: replyCh, err: errCh}:
	case <-n.stopCh:
		return nil, fmt.Errorf("node stopped")
	case <-time.After(500 * time.Millisecond):
		return nil, fmt.Errorf("RPC timeout")
	}

	select {
	case res := <-replyCh:
		return res.(*RequestVoteResponse), nil
	case err := <-errCh:
		return nil, err
	case <-n.stopCh:
		return nil, fmt.Errorf("node stopped")
	}
}

// processRequestVote runs inside the event loop.
func (n *Node) processRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	// Rule 1: Term < currentTerm -> reject
	if req.Term < n.currentTerm {
		return &RequestVoteResponse{
			Term:        n.currentTerm,
			VoteGranted: false,
		}
	}

	// Rule: If RPC term > currentTerm -> step down
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term, "")
	}

	// Rule 2: Vote logic
	// Up-to-date check (Paper §5.4):
	// If candidate's last term != our last term: larger term is more up-to-date
	// If same term: longer log is more up-to-date
	ourLastTerm := n.log.LastTerm()
	ourLastIndex := n.log.LastIndex()

	upToDate := false
	if req.LastLogTerm > ourLastTerm {
		upToDate = true
	} else if req.LastLogTerm == ourLastTerm && req.LastLogIndex >= ourLastIndex {
		upToDate = true
	}

	canVote := (n.votedFor == "" || n.votedFor == req.CandidateID) && upToDate

	if canVote {
		n.votedFor = req.CandidateID
		n.persist()
		n.resetElectionTimer()
		n.events.Emit(n.id, events.VoteGranted, n.role.String(), n.currentTerm,
			fmt.Sprintf("Granted vote to %s for term %d", req.CandidateID, req.Term), nil)

		return &RequestVoteResponse{
			Term:        n.currentTerm,
			VoteGranted: true,
		}
	}

	n.events.Emit(n.id, events.VoteRejected, n.role.String(), n.currentTerm,
		fmt.Sprintf("Rejected vote for %s (upToDate=%v, votedFor=%s)", req.CandidateID, upToDate, n.votedFor), nil)

	return &RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}
}
