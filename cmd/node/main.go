package main

import (
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
	fmt.Printf("\n🚀 Launching In-Process Raft KV Cluster (%d nodes)...\n", n)
	c, err := harness.NewCluster(n, false, "", time.Now().UnixNano())
	if err != nil {
		log.Fatalf("Failed to initialize cluster: %v", err)
	}

	c.Start()
	defer c.Stop()

	leader, err := c.WaitLeader(3 * time.Second)
	if err != nil {
		log.Printf("Waiting for leader: %v", err)
	} else {
		fmt.Printf("✓ Initial Leader Elected: %s\n", leader)
	}

	// Choose first node to bind dashboard API server
	nodeIDs := c.NodeIDs()
	firstID := nodeIDs[0]
	firstNode, _ := c.GetNode(firstID)
	firstSM, _ := c.GetStateMachine(firstID)

	mux := http.NewServeMux()
	apiServer := api.NewServer(firstID, firstNode, firstSM, c.EventBus(), nil)

	// Mount API
	mux.Handle("/api/v1/", apiServer.Mux())

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
		fmt.Printf("🌐 Live Dashboard & API running at: http://127.0.0.1:%d\n", port)
		fmt.Printf("   Press Ctrl+C to terminate.\n\n")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	fmt.Println("\nShutting down cluster...")
}

func runSingleNode(nodeID string, port int, peersFlag, dataDir string) {
	fmt.Printf("\n🚀 Starting Raft KV Node [%s] on port %d...\n", nodeID, port)

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
		fmt.Printf("🌐 Node API & Dashboard running at: http://127.0.0.1:%d\n\n", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	fmt.Println("\nStopping node...")
}
