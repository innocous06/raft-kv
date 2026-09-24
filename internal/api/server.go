package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"raft-kv/internal/events"
	"raft-kv/internal/kv"
	"raft-kv/internal/raft"
)

type Server struct {
	nodeID   string
	raftNode *raft.Node
	sm       *kv.StateMachine
	eventBus *events.Bus
	mux      *http.ServeMux
	allNodes map[string]string // nodeID -> base url
}

func NewServer(nodeID string, raftNode *raft.Node, sm *kv.StateMachine, bus *events.Bus, allNodes map[string]string) *Server {
	if bus == nil {
		bus = events.DefaultBus
	}

	s := &Server{
		nodeID:   nodeID,
		raftNode: raftNode,
		sm:       sm,
		eventBus: bus,
		mux:      http.NewServeMux(),
		allNodes: allNodes,
	}

	s.registerRoutes()
	return s
}

func (s *Server) Mux() *http.ServeMux {
	return s.mux
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/api/v1/kv/put", s.handlePut)
	s.mux.HandleFunc("/api/v1/kv/get", s.handleGet)
	s.mux.HandleFunc("/api/v1/kv/delete", s.handleDelete)
	s.mux.HandleFunc("/api/v1/kv/all", s.handleGetAll)
	s.mux.HandleFunc("/api/v1/cluster/status", s.handleStatus)
	s.mux.HandleFunc("/api/v1/events/stream", s.handleEventsStream)
}

type PutRequest struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	ClientID string `json:"clientId"`
	SeqNum   uint64 `json:"seqNum"`
}

type DeleteRequest struct {
	Key      string `json:"key"`
	ClientID string `json:"clientId"`
	SeqNum   uint64 `json:"seqNum"`
}

type APIResponse struct {
	Success bool   `json:"success"`
	Value   string `json:"value,omitempty"`
	Error   string `json:"error,omitempty"`
	Leader  string `json:"leader,omitempty"`
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(APIResponse{Success: false, Error: "method not allowed"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 4*1024*1024)
	var req PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(APIResponse{Success: false, Error: "malformed JSON body: " + err.Error()})
		return
	}

	if strings.TrimSpace(req.Key) == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(APIResponse{Success: false, Error: "key cannot be empty"})
		return
	}

	op := kv.Op{
		Type:     kv.OpPut,
		Key:      req.Key,
		Value:    req.Value,
		ClientID: req.ClientID,
		SeqNum:   req.SeqNum,
	}

	res, err := s.sm.Execute(op, 3*time.Second)
	if err != nil {
		_, _, leaderID := s.raftNode.GetState()
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   err.Error(),
			Leader:  leaderID,
		})
		return
	}

	_ = json.NewEncoder(w).Encode(APIResponse{
		Success: true,
		Value:   res.Value,
	})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(APIResponse{Success: false, Error: "method not allowed"})
		return
	}

	key := r.URL.Query().Get("key")
	if strings.TrimSpace(key) == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(APIResponse{Success: false, Error: "missing key parameter"})
		return
	}

	op := kv.Op{
		Type: kv.OpGet,
		Key:  key,
	}

	res, err := s.sm.Execute(op, 3*time.Second)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) || err.Error() == kv.ErrKeyNotFound.Error() {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(APIResponse{
				Success: false,
				Error:   "key not found",
			})
			return
		}
		_, _, leaderID := s.raftNode.GetState()
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   err.Error(),
			Leader:  leaderID,
		})
		return
	}

	if !res.Found {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   "key not found",
		})
		return
	}

	_ = json.NewEncoder(w).Encode(APIResponse{
		Success: true,
		Value:   res.Value,
	})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(APIResponse{Success: false, Error: "method not allowed"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 4*1024*1024)
	var req DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(APIResponse{Success: false, Error: "malformed JSON body: " + err.Error()})
		return
	}

	if strings.TrimSpace(req.Key) == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(APIResponse{Success: false, Error: "key cannot be empty"})
		return
	}

	op := kv.Op{
		Type:     kv.OpDelete,
		Key:      req.Key,
		ClientID: req.ClientID,
		SeqNum:   req.SeqNum,
	}

	res, err := s.sm.Execute(op, 3*time.Second)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		_, _, leaderID := s.raftNode.GetState()
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   err.Error(),
			Leader:  leaderID,
		})
		return
	}

	_ = json.NewEncoder(w).Encode(APIResponse{
		Success: true,
		Value:   res.Value,
	})
}

func (s *Server) handleGetAll(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(APIResponse{Success: false, Error: "method not allowed"})
		return
	}
	all := s.sm.GetAll()
	_ = json.NewEncoder(w).Encode(all)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(APIResponse{Success: false, Error: "method not allowed"})
		return
	}
	state := s.raftNode.GetNodeState()
	_ = json.NewEncoder(w).Encode(state)
}

// handleEventsStream streams events to client via Server-Sent Events (SSE).
func (s *Server) handleEventsStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ch := s.eventBus.Subscribe(100)
	defer s.eventBus.Unsubscribe(ch)

	// Send current history first
	for _, evt := range s.eventBus.History() {
		data, _ := json.Marshal(evt)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
	}
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			data, err := json.Marshal(evt)
			if err == nil {
				_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			}
		}
	}
}
