package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"raft-kv/internal/raft"
)

// HTTPTransport provides real network RPCs over HTTP/JSON for multi-process clusters.
type HTTPTransport struct {
	mu        sync.RWMutex
	peerAddrs map[string]string // nodeID -> base URL (e.g. "http://127.0.0.1:8001")
	client    *http.Client
	handler   raft.RPCHandler
	localID   string
}

// NewHTTPTransport creates an HTTP transport with peer address mappings.
func NewHTTPTransport(localID string, peerAddrs map[string]string) *HTTPTransport {
	return &HTTPTransport{
		localID:   localID,
		peerAddrs: peerAddrs,
		client: &http.Client{
			Timeout: 500 * time.Millisecond,
		},
	}
}

func (h *HTTPTransport) Register(nodeID string, handler raft.RPCHandler) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handler = handler
	h.localID = nodeID
}

func (h *HTTPTransport) Unregister(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.localID == nodeID {
		h.handler = nil
	}
}

// RegisterRoutes binds Raft RPC endpoints to an HTTP ServeMux.
func (h *HTTPTransport) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/raft/request_vote", h.handleRequestVoteHTTP)
	mux.HandleFunc("/raft/append_entries", h.handleAppendEntriesHTTP)
	mux.HandleFunc("/raft/install_snapshot", h.handleInstallSnapshotHTTP)
}

func (h *HTTPTransport) handleRequestVoteHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req raft.RequestVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	h.mu.RLock()
	handler := h.handler
	h.mu.RUnlock()

	if handler == nil {
		http.Error(w, "node not ready", http.StatusServiceUnavailable)
		return
	}

	resp, err := handler.HandleRequestVote(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *HTTPTransport) handleAppendEntriesHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req raft.AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	h.mu.RLock()
	handler := h.handler
	h.mu.RUnlock()

	if handler == nil {
		http.Error(w, "node not ready", http.StatusServiceUnavailable)
		return
	}

	resp, err := handler.HandleAppendEntries(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *HTTPTransport) handleInstallSnapshotHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req raft.InstallSnapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	h.mu.RLock()
	handler := h.handler
	h.mu.RUnlock()

	if handler == nil {
		http.Error(w, "node not ready", http.StatusServiceUnavailable)
		return
	}

	resp, err := handler.HandleInstallSnapshot(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// SendRequestVote calls remote peer's /raft/request_vote endpoint.
func (h *HTTPTransport) SendRequestVote(ctx context.Context, to string, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	h.mu.RLock()
	addr, ok := h.peerAddrs[to]
	h.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no address for peer %s", to)
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/raft/request_vote", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := h.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("rpc failed with status %d", httpResp.StatusCode)
	}

	var resp raft.RequestVoteResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SendAppendEntries calls remote peer's /raft/append_entries endpoint.
func (h *HTTPTransport) SendAppendEntries(ctx context.Context, to string, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	h.mu.RLock()
	addr, ok := h.peerAddrs[to]
	h.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no address for peer %s", to)
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/raft/append_entries", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := h.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("rpc failed with status %d", httpResp.StatusCode)
	}

	var resp raft.AppendEntriesResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SendInstallSnapshot calls remote peer's /raft/install_snapshot endpoint.
func (h *HTTPTransport) SendInstallSnapshot(ctx context.Context, to string, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	h.mu.RLock()
	addr, ok := h.peerAddrs[to]
	h.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no address for peer %s", to)
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/raft/install_snapshot", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := h.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("rpc failed with status %d", httpResp.StatusCode)
	}

	var resp raft.InstallSnapshotResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
