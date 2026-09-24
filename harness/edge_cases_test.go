package harness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"raft-kv/internal/api"
	"raft-kv/internal/kv"
	"raft-kv/internal/raft"
)

// TestEdgeCase_EmptyKeyHandling verifies that empty keys are rejected cleanly.
func TestEdgeCase_EmptyKeyHandling(t *testing.T) {
	c, err := NewCluster(3, false, "", 201)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer c.Stop()
	c.Start()

	_, err = c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("Failed to elect leader: %v", err)
	}

	// 1. Submit Put with empty key
	_, err = c.Submit(kv.Op{Type: kv.OpPut, Key: "", Value: "val"}, 2*time.Second)
	if err == nil {
		t.Fatalf("Expected error when submitting empty key to Put, got nil")
	}

	// 2. Submit Get with empty key
	_, err = c.Submit(kv.Op{Type: kv.OpGet, Key: ""}, 2*time.Second)
	if err == nil {
		t.Fatalf("Expected error when submitting empty key to Get, got nil")
	}

	// 3. Submit Delete with empty key
	_, err = c.Submit(kv.Op{Type: kv.OpDelete, Key: ""}, 2*time.Second)
	if err == nil {
		t.Fatalf("Expected error when submitting empty key to Delete, got nil")
	}
}

// TestEdgeCase_DeduplicationAndIdempotency verifies that repeated requests with same (ClientID, SeqNum)
// return the cached result and do not execute again.
func TestEdgeCase_DeduplicationAndIdempotency(t *testing.T) {
	c, err := NewCluster(3, false, "", 202)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer c.Stop()
	c.Start()

	leader, err := c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("Failed to elect leader: %v", err)
	}

	// First execution: Put key=counter, value=1
	res1, err := c.Submit(kv.Op{
		Type:     kv.OpPut,
		Key:      "counter",
		Value:    "1",
		ClientID: "client-dedup",
		SeqNum:   1,
	}, 2*time.Second)
	if err != nil {
		t.Fatalf("First Put failed: %v", err)
	}
	if res1.Value != "1" {
		t.Fatalf("Expected value '1', got %q", res1.Value)
	}

	// Duplicate execution: same ClientID and SeqNum, but different Value="2"
	res2, err := c.Submit(kv.Op{
		Type:     kv.OpPut,
		Key:      "counter",
		Value:    "2",
		ClientID: "client-dedup",
		SeqNum:   1,
	}, 2*time.Second)
	if err != nil {
		t.Fatalf("Duplicate Put failed: %v", err)
	}
	// Deduplication table should return original result "1", not "2"
	if res2.Value != "1" {
		t.Fatalf("Expected cached value '1' from deduplication, got %q", res2.Value)
	}

	// Check actual value in state machine
	sm, ok := c.GetStateMachine(leader)
	if !ok {
		t.Fatalf("Failed to get state machine for leader %s", leader)
	}
	all := sm.GetAll()
	if all["counter"] != "1" {
		t.Fatalf("State machine mutated by duplicate request! Expected '1', got %q", all["counter"])
	}
}

// TestEdgeCase_MalformedCommandHandling verifies that malformed commands applied to Raft
// do not panic or stall the state machine apply loop.
func TestEdgeCase_MalformedCommandHandling(t *testing.T) {
	c, err := NewCluster(3, false, "", 203)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer c.Stop()
	c.Start()

	leaderID, err := c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("Failed to elect leader: %v", err)
	}

	leaderNode, ok := c.GetNode(leaderID)
	if !ok {
		t.Fatalf("Failed to get leader node %s", leaderID)
	}

	// Propose raw invalid JSON directly into Raft log
	invalidJSON := []byte("this is not json { [}")
	idx, _, isLeader := leaderNode.Propose(invalidJSON)
	if !isLeader {
		t.Fatalf("Leader lost leadership during propose")
	}

	// Wait for entry to be applied
	time.Sleep(300 * time.Millisecond)

	// Now propose a valid operation and verify the state machine processes it cleanly
	res, err := c.Submit(kv.Op{
		Type:  kv.OpPut,
		Key:   "healthy-key",
		Value: "healthy-val",
	}, 2*time.Second)
	if err != nil {
		t.Fatalf("State machine stalled after malformed command: %v", err)
	}
	if res.Value != "healthy-val" {
		t.Fatalf("Expected 'healthy-val', got %q", res.Value)
	}
	t.Logf("Malformed command at index %d was gracefully handled without stalling", idx)
}

// TestEdgeCase_NonExistentNodeChaos verifies that stopping/restarting unknown node IDs returns an error.
func TestEdgeCase_NonExistentNodeChaos(t *testing.T) {
	c, err := NewCluster(3, false, "", 204)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer c.Stop()
	c.Start()

	err = c.CrashNode("node-unknown-999")
	if err == nil {
		t.Fatalf("Expected error when crashing unknown node, got nil")
	}

	err = c.RestartNode("node-unknown-999")
	if err == nil {
		t.Fatalf("Expected error when restarting unknown node, got nil")
	}
}

// TestEdgeCase_AsymmetricPartition verifies that asymmetric partitions (node can receive but cannot send)
// are properly resolved by Raft consensus without violating invariants.
func TestEdgeCase_AsymmetricPartition(t *testing.T) {
	c, err := NewCluster(5, false, "", 205)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer c.Stop()
	checker := NewInvariantChecker(c)
	c.Start()

	leader, err := c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("Failed to elect leader: %v", err)
	}

	// Create asymmetric partition: block leader from sending to 3 followers,
	// but followers can still send to leader (one-way transmission block)
	peers := c.NodeIDs()
	var targets []string
	for _, p := range peers {
		if p != leader {
			targets = append(targets, p)
		}
	}

	for i := 0; i < 3 && i < len(targets); i++ {
		c.SimNet().Disconnect(leader, targets[i])
	}

	// A new leader should emerge among the surviving quorum
	time.Sleep(600 * time.Millisecond)

	// Heal partition
	c.SimNet().Heal()
	time.Sleep(400 * time.Millisecond)

	// Verify all safety invariants
	if err := checker.CheckAll(); err != nil {
		t.Fatalf("Invariant violation under asymmetric partition: %v", err)
	}

	// Verify KV write succeeds
	_, err = c.Submit(kv.Op{Type: kv.OpPut, Key: "asym-test", Value: "ok"}, 3*time.Second)
	if err != nil {
		t.Fatalf("Write failed after healing asymmetric partition: %v", err)
	}
}

// TestEdgeCase_HTTPAPIRoutes verifies all HTTP API endpoints against malformed requests,
// incorrect methods, empty keys, and unknown nodes.
func TestEdgeCase_HTTPAPIRoutes(t *testing.T) {
	c, err := NewCluster(3, false, "", 206)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer c.Stop()
	c.Start()

	leaderID, err := c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("Failed to elect leader: %v", err)
	}

	firstNode, _ := c.GetNode(leaderID)
	firstSM, _ := c.GetStateMachine(leaderID)
	srv := api.NewServer(leaderID, firstNode, firstSM, c.EventBus(), nil)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	client := ts.Client()

	// 1. Wrong HTTP method on Put (GET instead of POST)
	resp, err := client.Get(ts.URL + "/api/v1/kv/put")
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("Expected 405 Method Not Allowed, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 2. Malformed JSON on Put
	resp, err = client.Post(ts.URL+"/api/v1/kv/put", "application/json", bytes.NewReader([]byte("{invalid-json")))
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("Expected 400 Bad Request on malformed JSON, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 3. Empty key on Put
	emptyKeyBody, _ := json.Marshal(api.PutRequest{Key: "", Value: "abc"})
	resp, err = client.Post(ts.URL+"/api/v1/kv/put", "application/json", bytes.NewReader(emptyKeyBody))
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("Expected 400 Bad Request on empty key Put, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 4. Missing key query param on Get
	resp, err = client.Get(ts.URL + "/api/v1/kv/get")
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("Expected 400 Bad Request on missing key param, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 5. Non-existent key on Get
	resp, err = client.Get(ts.URL + "/api/v1/kv/get?key=nonexistent-key-999")
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404 Not Found on missing key, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 6. Valid Put followed by valid Get
	putBody, _ := json.Marshal(api.PutRequest{Key: "hello", Value: "world", ClientID: "c1", SeqNum: 1})
	resp, err = client.Post(ts.URL+"/api/v1/kv/put", "application/json", bytes.NewReader(putBody))
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 OK on valid Put, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = client.Get(ts.URL + "/api/v1/kv/get?key=hello")
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 OK on valid Get, got %d", resp.StatusCode)
	}
	var getResp api.APIResponse
	_ = json.NewDecoder(resp.Body).Decode(&getResp)
	resp.Body.Close()
	if getResp.Value != "world" {
		t.Fatalf("Expected 'world', got %q", getResp.Value)
	}

	// 7. Cluster status endpoint
	resp, err = client.Get(ts.URL + "/api/v1/cluster/status")
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 OK on status, got %d", resp.StatusCode)
	}
	var status raft.NodeState
	_ = json.NewDecoder(resp.Body).Decode(&status)
	resp.Body.Close()
	if status.ID != leaderID {
		t.Fatalf("Expected node status ID %q, got %q", leaderID, status.ID)
	}
}

// TestEdgeCase_HeavyConcurrentWritesAndReads verifies high-concurrency correctness and thread safety.
func TestEdgeCase_HeavyConcurrentWritesAndReads(t *testing.T) {
	c, err := NewCluster(5, false, "", 207)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer c.Stop()
	c.Start()

	_, err = c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("Failed to elect leader: %v", err)
	}

	checker := NewInvariantChecker(c)

	numWorkers := 8
	opsPerWorker := 25
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			clientID := fmt.Sprintf("worker-%d", workerID)
			for i := 1; i <= opsPerWorker; i++ {
				key := fmt.Sprintf("k-%d", i%10)
				val := fmt.Sprintf("val-%d-%d", workerID, i)

				_, err := c.Submit(kv.Op{
					Type:     kv.OpPut,
					Key:      key,
					Value:    val,
					ClientID: clientID,
					SeqNum:   uint64(i),
				}, 3*time.Second)
				if err != nil {
					// In heavy write load, transient failover is acceptable
					continue
				}

				// Immediate read back
				_, _ = c.Submit(kv.Op{
					Type: kv.OpGet,
					Key:  key,
				}, 3*time.Second)
			}
		}(w)
	}

	wg.Wait()

	// Verify all safety invariants hold after intense concurrent traffic
	if err := checker.CheckAll(); err != nil {
		t.Fatalf("Invariant violation after concurrent traffic: %v", err)
	}
}
