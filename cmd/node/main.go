package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"raft-kv/harness"
	"raft-kv/internal/api"
	"raft-kv/internal/events"
	"raft-kv/internal/kv"
	"raft-kv/internal/raft"
	"raft-kv/internal/storage"
	"raft-kv/internal/transport"
	"raft-kv/web"
)

func main() {
	var (
		nodeID       = flag.String("id", "node-1", "Unique node ID")
		port         = flag.Int("port", 8001, "HTTP API & Web Dashboard port")
		peersFlag    = flag.String("peers", "", "Comma-separated peer mappings: id=url,id=url (e.g. node-2=http://127.0.0.1:8002)")
		dataDir      = flag.String("data", "", "Storage directory for WAL & snapshots (empty for memory)")
		clusterSize  = flag.Int("cluster", 0, "Convenience mode: spin up an in-process cluster of N nodes (e.g. -cluster=3)")
	)
	flag.Parse()

	if *clusterSize > 0 {
		runInProcessCluster(*clusterSize, *port)
		return
	}

	runSingleNode(*nodeID, *port, *peersFlag, *dataDir)
}

func runInProcessCluster(n int, port int) {
	fmt.Printf("\n[INFO] Initializing In-Process Raft KV Cluster (%d nodes)...\n", n)
	c, err := harness.NewCluster(n, false, "", time.Now().UnixNano())
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize cluster: %v", err)
	}

	c.Start()
	defer c.Stop()

	leader, err := c.WaitLeader(3 * time.Second)
	if err != nil {
		log.Printf("[WARN] Waiting for leader: %v", err)
	} else {
		fmt.Printf("[LEADER] Initial leader elected: %s\n", leader)
	}

	mux := http.NewServeMux()

	// Cluster Status for all nodes
	mux.HandleFunc("/api/v1/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		var states []raft.NodeState
		for _, id := range c.NodeIDs() {
			if n, ok := c.GetNode(id); ok {
				states = append(states, n.GetNodeState())
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(states)
	})

	// Unified KV Put routing to current cluster leader
	mux.HandleFunc("/api/v1/kv/put", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(api.APIResponse{Success: false, Error: "method not allowed"})
			return
		}
		var req api.PutRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(api.APIResponse{Success: false, Error: "malformed JSON body: " + err.Error()})
			return
		}
		if req.Key == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(api.APIResponse{Success: false, Error: "key cannot be empty"})
			return
		}
		res, err := c.Submit(kv.Op{
			Type:     kv.OpPut,
			Key:      req.Key,
			Value:    req.Value,
			ClientID: req.ClientID,
			SeqNum:   req.SeqNum,
		}, 3*time.Second)

		if err != nil {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(api.APIResponse{Success: false, Error: err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(api.APIResponse{Success: true, Value: res.Value})
	})

	// Unified KV Get routing
	mux.HandleFunc("/api/v1/kv/get", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(api.APIResponse{Success: false, Error: "method not allowed"})
			return
		}
		key := r.URL.Query().Get("key")
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(api.APIResponse{Success: false, Error: "missing key parameter"})
			return
		}
		res, err := c.Submit(kv.Op{Type: kv.OpGet, Key: key}, 3*time.Second)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(api.APIResponse{Success: false, Error: err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(api.APIResponse{Success: true, Value: res.Value})
	})

	// Unified KV Delete routing
	mux.HandleFunc("/api/v1/kv/delete", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost && r.Method != http.MethodDelete {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(api.APIResponse{Success: false, Error: "method not allowed"})
			return
		}
		var req api.DeleteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(api.APIResponse{Success: false, Error: "malformed JSON body: " + err.Error()})
			return
		}
		if req.Key == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(api.APIResponse{Success: false, Error: "key cannot be empty"})
			return
		}
		res, err := c.Submit(kv.Op{
			Type:     kv.OpDelete,
			Key:      req.Key,
			ClientID: req.ClientID,
			SeqNum:   req.SeqNum,
		}, 3*time.Second)

		if err != nil {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(api.APIResponse{Success: false, Error: err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(api.APIResponse{Success: true, Value: res.Value})
	})

	// Replicated KV All
	mux.HandleFunc("/api/v1/kv/all", func(w http.ResponseWriter, r *http.Request) {
		leader, err := c.WaitLeader(500 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			_ = json.NewEncoder(w).Encode(map[string]string{})
			return
		}
		if sm, ok := c.GetStateMachine(leader); ok {
			_ = json.NewEncoder(w).Encode(sm.GetAll())
		} else {
			_ = json.NewEncoder(w).Encode(map[string]string{})
		}
	})

	// Live SSE Telemetry Stream
	nodeIDs := c.NodeIDs()
	firstNode, _ := c.GetNode(nodeIDs[0])
	firstSM, _ := c.GetStateMachine(nodeIDs[0])
	defaultAPI := api.NewServer(nodeIDs[0], firstNode, firstSM, c.EventBus(), nil)
	mux.HandleFunc("/api/v1/events/stream", defaultAPI.Mux().ServeHTTP)

	// Live Chaos Control Endpoints
	mux.HandleFunc("/api/v1/chaos/isolate_leader", func(w http.ResponseWriter, r *http.Request) {
		isolated, err := harness.IsolateLeader(c)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "isolated": isolated})
	})

	mux.HandleFunc("/api/v1/chaos/partition", func(w http.ResponseWriter, r *http.Request) {
		maj, min := harness.PartitionMajorityMinority(c)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "majority": maj, "minority": min})
	})

	mux.HandleFunc("/api/v1/chaos/heal", func(w http.ResponseWriter, r *http.Request) {
		harness.HealNetwork(c)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
	})

	mux.HandleFunc("/api/v1/chaos/kill", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		nodeID := r.URL.Query().Get("node")
		if nodeID == "" {
			var body struct {
				Node string `json:"node"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			nodeID = body.Node
		}
		if nodeID == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "error": "missing node parameter"})
			return
		}
		err := c.CrashNode(nodeID)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "killed": nodeID})
	})

	mux.HandleFunc("/api/v1/chaos/restart", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		nodeID := r.URL.Query().Get("node")
		if nodeID == "" {
			var body struct {
				Node string `json:"node"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			nodeID = body.Node
		}
		if nodeID == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "error": "missing node parameter"})
			return
		}
		err := c.RestartNode(nodeID)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "restarted": nodeID})
	})

	// Mount Dashboard UI
	staticFS := http.StripPrefix("/static/", web.Handler())
	mux.Handle("/static/", staticFS)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		web.Handler().ServeHTTP(w, r)
	})

	addr := fmt.Sprintf(":%d", port)
	server := &http.Server{Addr: addr, Handler: mux}

	go func() {
		fmt.Printf("[HTTP] Live Dashboard and REST API listening at: http://127.0.0.1:%d\n", port)
		fmt.Printf("       Press Ctrl+C to terminate.\n\n")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] HTTP server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	fmt.Println("\n[INFO] Terminating cluster...")
}

func runSingleNode(nodeID string, port int, peersFlag, dataDir string) {
	fmt.Printf("\n[INFO] Starting Raft KV Node [%s] on port %d...\n", nodeID, port)

	peerAddrs := make(map[string]string)
	var peerIDs []string
	if peersFlag != "" {
		for _, part := range strings.Split(peersFlag, ",") {
			kvPair := strings.Split(part, "=")
			if len(kvPair) == 2 {
				pID := strings.TrimSpace(kvPair[0])
				pURL := strings.TrimSpace(kvPair[1])
				peerAddrs[pID] = pURL
				peerIDs = append(peerIDs, pID)
			}
		}
	}

	var store raft.Storage
	if dataDir != "" {
		nodeData := filepath.Join(dataDir, nodeID)
		var err error
		store, err = storage.NewDiskStorage(nodeData)
		if err != nil {
			log.Fatalf("Failed to initialize disk storage: %v", err)
		}
	} else {
		store = storage.NewMemoryStorage()
	}

	httpTrans := transport.NewHTTPTransport(nodeID, peerAddrs)
	cfg := raft.DefaultConfig(nodeID, peerIDs)
	cfg.Storage = store
	cfg.Transport = httpTrans
	cfg.EventBus = events.DefaultBus

	node, err := raft.NewNode(cfg)
	if err != nil {
		log.Fatalf("Failed to create Raft node: %v", err)
	}

	sm := kv.NewStateMachine(nodeID, node, 100, events.DefaultBus)

	mux := http.NewServeMux()
	httpTrans.RegisterRoutes(mux)

	apiServer := api.NewServer(nodeID, node, sm, events.DefaultBus, peerAddrs)
	mux.Handle("/api/v1/", apiServer.Mux())

	staticFS := http.StripPrefix("/static/", web.Handler())
	mux.Handle("/static/", staticFS)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		web.Handler().ServeHTTP(w, r)
	})

	node.Start()
	defer node.Stop()
	defer sm.Close()

	addr := fmt.Sprintf(":%d", port)
	server := &http.Server{Addr: addr, Handler: mux}

	go func() {
		fmt.Printf("[HTTP] Node API and Dashboard listening at: http://127.0.0.1:%d\n\n", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] HTTP server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	fmt.Println("\n[INFO] Stopping node...")
}
