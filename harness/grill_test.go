package harness

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"raft-kv/internal/api"
	"raft-kv/internal/kv"
	"raft-kv/internal/raft"
	"raft-kv/internal/storage"
)

func TestGrill_MassivePayloadStress(t *testing.T) {
	c, err := NewCluster(3, false, "", 301)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer c.Stop()
	c.Start()

	leader, err := c.WaitLeader(3 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	checker := NewInvariantChecker(c)

	largePayload := strings.Repeat("A-Z0-9-BLOCK-BOUNDARY-VERIFICATION-", 8192) // ~286 KB payload
	res, err := c.Submit(kv.Op{
		Type:     kv.OpPut,
		Key:      "large-blob",
		Value:    largePayload,
		ClientID: "stress-client-1",
		SeqNum:   1,
	}, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to submit large payload: %v", err)
	}
	if res.Value != largePayload {
		t.Fatalf("payload mismatch on submit return: got length %d, want %d", len(res.Value), len(largePayload))
	}

	getRes, err := c.Submit(kv.Op{
		Type: kv.OpGet,
		Key:  "large-blob",
	}, 3*time.Second)
	if err != nil {
		t.Fatalf("failed to read back large payload: %v", err)
	}
	if getRes.Value != largePayload {
		t.Fatalf("payload mismatch on get: got length %d, want %d", len(getRes.Value), len(largePayload))
	}

	sm, ok := c.GetStateMachine(leader)
	if !ok {
		t.Fatalf("failed to get leader state machine")
	}

	all := sm.GetAll()
	if all["large-blob"] != largePayload {
		t.Fatalf("leader state machine value mismatch")
	}

	// Verify snapshot serialization with large payload
	snap := kv.SnapshotData{
		Data: all,
	}
	snapBytes, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("failed to marshal snapshot of large payload: %v", err)
	}
	if len(snapBytes) < len(largePayload) {
		t.Fatalf("snapshot size smaller than payload: got %d bytes", len(snapBytes))
	}

	var restored kv.SnapshotData
	if err := json.Unmarshal(snapBytes, &restored); err != nil {
		t.Fatalf("failed to unmarshal snapshot: %v", err)
	}
	if restored.Data["large-blob"] != largePayload {
		t.Fatalf("restored snapshot value mismatch")
	}

	if err := checker.CheckAll(); err != nil {
		t.Fatalf("invariant check failed: %v", err)
	}
}

func TestGrill_CorruptWALRecovery(t *testing.T) {
	dir := t.TempDir()
	ds, err := storage.NewDiskStorage(dir)
	if err != nil {
		t.Fatalf("failed to initialize disk storage: %v", err)
	}

	validEntries := []raft.LogEntry{
		{Term: 1, Index: 1, Data: []byte("cmd-1")},
		{Term: 1, Index: 2, Data: []byte("cmd-2")},
		{Term: 2, Index: 3, Data: []byte("cmd-3")},
	}
	if err := ds.SaveState(2, "node-1", validEntries); err != nil {
		t.Fatalf("failed to save initial state: %v", err)
	}

	walPath := filepath.Join(dir, "wal.log")
	walInfoBefore, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("wal file missing: %v", err)
	}
	validSize := walInfoBefore.Size()

	// 1. Partial torn header (<8 bytes)
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open wal file: %v", err)
	}
	if _, err := f.Write([]byte{0x00, 0x00, 0x01}); err != nil {
		t.Fatalf("failed to write torn header: %v", err)
	}
	_ = f.Close()

	term, votedFor, loaded, err := ds.LoadState()
	if err != nil {
		t.Fatalf("LoadState failed on torn header: %v", err)
	}
	if term != 2 || votedFor != "node-1" {
		t.Fatalf("metadata mismatch: got term %d votedFor %s", term, votedFor)
	}
	if len(loaded) != 3 {
		t.Fatalf("expected 3 valid entries loaded, got %d", len(loaded))
	}

	walInfoAfter, _ := os.Stat(walPath)
	if walInfoAfter.Size() != validSize {
		t.Fatalf("wal was not truncated back to valid offset: got %d want %d", walInfoAfter.Size(), validSize)
	}

	// 2. Corrupt checksum in entry
	f, err = os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open wal file: %v", err)
	}
	badPayload := []byte(`{"index":4,"term":2,"data":"Y29ycnVwdA=="}`)
	var badHeader [8]byte
	binary.BigEndian.PutUint32(badHeader[0:4], uint32(len(badPayload)))
	binary.BigEndian.PutUint32(badHeader[4:8], 0xDEADBEEF) // Bad CRC
	_, _ = f.Write(badHeader[:])
	_, _ = f.Write(badPayload)
	_ = f.Close()

	term, votedFor, loaded, err = ds.LoadState()
	if err != nil {
		t.Fatalf("LoadState failed on bad crc: %v", err)
	}
	if len(loaded) != 3 {
		t.Fatalf("expected 3 valid entries loaded after corrupt crc, got %d", len(loaded))
	}

	walInfoAfter, _ = os.Stat(walPath)
	if walInfoAfter.Size() != validSize {
		t.Fatalf("wal was not truncated back after corrupt crc: got %d want %d", walInfoAfter.Size(), validSize)
	}

	// 3. Corrupt length exceeding 32MB safety bound
	f, err = os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open wal file: %v", err)
	}
	var hugeHeader [8]byte
	binary.BigEndian.PutUint32(hugeHeader[0:4], 0xFFFFFFFF) // 4GB claimed length
	binary.BigEndian.PutUint32(hugeHeader[4:8], 0x12345678)
	_, _ = f.Write(hugeHeader[:])
	_ = f.Close()

	term, votedFor, loaded, err = ds.LoadState()
	if err != nil {
		t.Fatalf("LoadState failed on huge length: %v", err)
	}
	if len(loaded) != 3 {
		t.Fatalf("expected 3 valid entries loaded after huge length header, got %d", len(loaded))
	}

	walInfoAfter, _ = os.Stat(walPath)
	if walInfoAfter.Size() != validSize {
		t.Fatalf("wal was not truncated back after huge length: got %d want %d", walInfoAfter.Size(), validSize)
	}

	// 4. Verify appending new valid entries still succeeds after truncation
	newEntries := append(validEntries, raft.LogEntry{
		Term: 2, Index: 4, Data: []byte("cmd-4-after-repair"),
	})
	if err := ds.SaveState(2, "node-1", newEntries); err != nil {
		t.Fatalf("failed to save state after repair: %v", err)
	}
	_, _, loaded, err = ds.LoadState()
	if err != nil || len(loaded) != 4 {
		t.Fatalf("expected 4 entries loaded after repair and append, got %d (err: %v)", len(loaded), err)
	}
}

func TestGrill_HTTPRouteBoundaryAndOversizedRejection(t *testing.T) {
	c, err := NewCluster(3, false, "", 302)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer c.Stop()
	c.Start()

	leaderID, err := c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	leaderNode, _ := c.GetNode(leaderID)
	leaderSM, _ := c.GetStateMachine(leaderID)
	srv := api.NewServer(leaderID, leaderNode, leaderSM, c.EventBus(), nil)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	client := ts.Client()

	// 1. Oversized body rejection (>4MB)
	hugeString := strings.Repeat("X", 5*1024*1024)
	hugeBody, _ := json.Marshal(api.PutRequest{
		Key:   "huge-key",
		Value: hugeString,
	})
	resp, err := client.Post(ts.URL+"/api/v1/kv/put", "application/json", bytes.NewReader(hugeBody))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 400 or 413 on oversized payload, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 2. Whitespace-only key rejection on PUT
	whitespacePut, _ := json.Marshal(api.PutRequest{Key: "   \t\n  ", Value: "val"})
	resp, err = client.Post(ts.URL+"/api/v1/kv/put", "application/json", bytes.NewReader(whitespacePut))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 on whitespace key in PUT, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 3. Whitespace-only key rejection on GET
	resp, err = client.Get(ts.URL + "/api/v1/kv/get?key=%20%20%20")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 on whitespace key in GET, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 4. Whitespace-only key rejection on DELETE
	whitespaceDel, _ := json.Marshal(api.DeleteRequest{Key: "   \n  "})
	resp, err = client.Post(ts.URL+"/api/v1/kv/delete", "application/json", bytes.NewReader(whitespaceDel))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 on whitespace key in DELETE, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 5. Method enforcement: POST on GET endpoint /api/v1/kv/get
	resp, err = client.Post(ts.URL+"/api/v1/kv/get?key=test", "application/json", nil)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on POST to /api/v1/kv/get, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 6. Method enforcement: DELETE on /api/v1/kv/all
	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/api/v1/kv/all", nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on DELETE to /api/v1/kv/all, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 7. Method enforcement: POST on /api/v1/cluster/status
	resp, err = client.Post(ts.URL+"/api/v1/cluster/status", "application/json", nil)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on POST to /api/v1/cluster/status, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestGrill_ConcurrentDeduplicationRace(t *testing.T) {
	c, err := NewCluster(3, false, "", 303)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer c.Stop()
	c.Start()

	leader, err := c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	checker := NewInvariantChecker(c)

	const concurrency = 25
	var wg sync.WaitGroup
	var successfulResponses int64
	var returnedValues sync.Map

	startGate := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()
			<-startGate

			res, err := c.Submit(kv.Op{
				Type:     kv.OpPut,
				Key:      "race-key",
				Value:    fmt.Sprintf("val-%d", workerIndex),
				ClientID: "racing-client-alpha",
				SeqNum:   42,
			}, 3*time.Second)

			if err == nil {
				atomic.AddInt64(&successfulResponses, 1)
				returnedValues.Store(res.Value, true)
			}
		}(i)
	}

	close(startGate)
	wg.Wait()

	if successfulResponses == 0 {
		t.Fatalf("all concurrent deduplication requests failed")
	}

	distinctValues := 0
	var firstVal string
	returnedValues.Range(func(key, value any) bool {
		distinctValues++
		firstVal = key.(string)
		return true
	})

	if distinctValues != 1 {
		t.Fatalf("deduplication race allowed multiple executions: saw %d distinct return values", distinctValues)
	}

	sm, ok := c.GetStateMachine(leader)
	if !ok {
		t.Fatalf("failed to get leader state machine")
	}
	all := sm.GetAll()
	if all["race-key"] != firstVal {
		t.Fatalf("state machine value %q does not match cached return value %q", all["race-key"], firstVal)
	}

	if err := checker.CheckAll(); err != nil {
		t.Fatalf("invariant check failed: %v", err)
	}
}

func TestGrill_NetworkFlappingUnderWriteStorm(t *testing.T) {
	c, err := NewCluster(5, false, "", 304)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer c.Stop()
	c.Start()

	_, err = c.WaitLeader(3 * time.Second)
	if err != nil {
		t.Fatalf("initial election failed: %v", err)
	}

	checker := NewInvariantChecker(c)

	var stopSignal int32
	var writeSuccesses int64
	var writeFailures int64
	var wg sync.WaitGroup

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			seq := uint64(1)
			clientID := fmt.Sprintf("storm-client-%d", writerID)

			for atomic.LoadInt32(&stopSignal) == 0 {
				key := fmt.Sprintf("storm-k-%d", seq%10)
				val := fmt.Sprintf("storm-v-%d-%d", writerID, seq)
				_, err := c.Submit(kv.Op{
					Type:     kv.OpPut,
					Key:      key,
					Value:    val,
					ClientID: clientID,
					SeqNum:   seq,
				}, 600*time.Millisecond)

				if err == nil {
					atomic.AddInt64(&writeSuccesses, 1)
				} else {
					atomic.AddInt64(&writeFailures, 1)
				}
				seq++
				time.Sleep(10 * time.Millisecond)
			}
		}(w)
	}

	for round := 0; round < 8; round++ {
		harnessPartitionMajorityMinority(c)
		time.Sleep(45 * time.Millisecond)
		HealNetwork(c)
		time.Sleep(60 * time.Millisecond)
	}

	atomic.StoreInt32(&stopSignal, 1)
	wg.Wait()

	HealNetwork(c)
	time.Sleep(500 * time.Millisecond)

	var postRecoveryLeader string
	for attempt := 0; attempt < 10; attempt++ {
		l, err := c.WaitLeader(1 * time.Second)
		if err == nil {
			postRecoveryLeader = l
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if postRecoveryLeader == "" {
		t.Fatalf("failed to elect leader after network flapping ceased")
	}

	_, err = c.Submit(kv.Op{
		Type:     kv.OpPut,
		Key:      "recovery-key",
		Value:    "recovery-val",
		ClientID: "verifier",
		SeqNum:   9999,
	}, 3*time.Second)
	if err != nil {
		t.Fatalf("post-flapping write failed: %v", err)
	}

	if err := checker.CheckAll(); err != nil {
		t.Fatalf("safety invariant violated during network flapping: %v", err)
	}

	t.Logf("Write storm summary: %d successful, %d failed/timed out under flapping",
		atomic.LoadInt64(&writeSuccesses), atomic.LoadInt64(&writeFailures))
}

func TestGrill_CascadingNodeCrashAndRecovery(t *testing.T) {
	c, err := NewCluster(5, false, "", 305)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer c.Stop()
	c.Start()

	leader1, err := c.WaitLeader(3 * time.Second)
	if err != nil {
		t.Fatalf("initial election failed: %v", err)
	}

	checker := NewInvariantChecker(c)

	for i := 1; i <= 3; i++ {
		_, err := c.Submit(kv.Op{
			Type:     kv.OpPut,
			Key:      fmt.Sprintf("init-%d", i),
			Value:    fmt.Sprintf("init-val-%d", i),
			ClientID: "init-client",
			SeqNum:   uint64(i),
		}, 2*time.Second)
		if err != nil {
			t.Fatalf("initial write %d failed: %v", i, err)
		}
	}

	allNodes := c.NodeIDs()
	var followers []string
	for _, id := range allNodes {
		if id != leader1 {
			followers = append(followers, id)
		}
	}

	deadNodes := []string{leader1, followers[0], followers[1]}
	for _, id := range deadNodes {
		if err := c.CrashNode(id); err != nil {
			t.Fatalf("failed to crash node %s: %v", id, err)
		}
	}

	_, err = c.Submit(kv.Op{
		Type:  kv.OpPut,
		Key:   "minority-write",
		Value: "should-fail",
	}, 600*time.Millisecond)
	if err == nil {
		t.Fatalf("expected write to fail in minority partition, but it succeeded")
	}

	restartedNodes := []string{deadNodes[0], deadNodes[1]}
	for _, id := range restartedNodes {
		if err := c.RestartNode(id); err != nil {
			t.Fatalf("failed to restart node %s: %v", id, err)
		}
	}

	leader2, err := c.WaitLeader(4 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect new leader after regaining quorum: %v", err)
	}
	t.Logf("Regained quorum with new leader: %s", leader2)

	res, err := c.Submit(kv.Op{
		Type:     kv.OpPut,
		Key:      "post-recovery",
		Value:    "success",
		ClientID: "recovery-client",
		SeqNum:   1,
	}, 3*time.Second)
	if err != nil {
		t.Fatalf("write after recovery failed: %v", err)
	}
	if res.Value != "success" {
		t.Fatalf("unexpected return value: got %q want %q", res.Value, "success")
	}

	for i := 1; i <= 3; i++ {
		getRes, err := c.Submit(kv.Op{
			Type: kv.OpGet,
			Key:  fmt.Sprintf("init-%d", i),
		}, 2*time.Second)
		if err != nil {
			t.Fatalf("failed to read initial key %d after recovery: %v", i, err)
		}
		expected := fmt.Sprintf("init-val-%d", i)
		if getRes.Value != expected {
			t.Fatalf("data corruption: got %q want %q", getRes.Value, expected)
		}
	}

	if err := checker.CheckAll(); err != nil {
		t.Fatalf("invariants failed after cascading crash and recovery: %v", err)
	}
}

func harnessPartitionMajorityMinority(c *Cluster) ([]string, []string) {
	return PartitionMajorityMinority(c)
}
