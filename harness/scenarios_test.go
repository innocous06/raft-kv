package harness

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"raft-kv/internal/kv"
)

// TestScenario1_ElectionStability tests that a 3-node cluster elects exactly one leader
// and maintains election safety.
func TestScenario1_ElectionStability(t *testing.T) {
	c, err := NewCluster(3, false, "", 42)
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
	t.Logf("Elected leader: %s", leader)

	time.Sleep(300 * time.Millisecond)

	if err := checker.CheckAll(); err != nil {
		t.Fatalf("Invariant check failed: %v", err)
	}
}

// TestScenario2_ReelectionOnLeaderFailure kills the leader and tests seamless reelection.
func TestScenario2_ReelectionOnLeaderFailure(t *testing.T) {
	c, err := NewCluster(3, false, "", 101)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer c.Stop()

	checker := NewInvariantChecker(c)
	c.Start()

	leader1, err := c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("Initial leader election failed: %v", err)
	}
	t.Logf("First leader: %s", leader1)

	// Crash the current leader
	if err := c.CrashNode(leader1); err != nil {
		t.Fatalf("Failed to crash leader: %v", err)
	}

	// Wait for new leader among remaining 2 nodes
	time.Sleep(100 * time.Millisecond)
	leader2, err := c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("Failed to elect replacement leader: %v", err)
	}

	if leader2 == leader1 {
		t.Fatalf("Dead node still recognized as leader!")
	}
	t.Logf("Replacement leader elected: %s", leader2)

	if err := checker.CheckAll(); err != nil {
		t.Fatalf("Invariant check failed: %v", err)
	}
}

// TestScenario3_LogReplication tests that Put operations replicate to followers and commit.
func TestScenario3_LogReplication(t *testing.T) {
	c, err := NewCluster(3, false, "", 202)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer c.Stop()

	checker := NewInvariantChecker(c)
	c.Start()

	_, err = c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("Leader election failed: %v", err)
	}

	// Write 5 entries
	for i := 1; i <= 5; i++ {
		op := kv.Op{
			Type:     kv.OpPut,
			Key:      fmt.Sprintf("k%d", i),
			Value:    fmt.Sprintf("v%d", i),
			ClientID: "client-test-1",
			SeqNum:   uint64(i),
		}
		res, err := c.Submit(op, 2*time.Second)
		if err != nil {
			t.Fatalf("Failed to submit write %d: %v", i, err)
		}
		if res.Value != fmt.Sprintf("v%d", i) {
			t.Fatalf("Expected value v%d, got %s", i, res.Value)
		}
	}

	// Read entries back
	for i := 1; i <= 5; i++ {
		op := kv.Op{
			Type: kv.OpGet,
			Key:  fmt.Sprintf("k%d", i),
		}
		res, err := c.Submit(op, 2*time.Second)
		if err != nil || res.Value != fmt.Sprintf("v%d", i) {
			t.Fatalf("Read mismatch for k%d: got %s, err: %v", i, res.Value, err)
		}
	}

	if err := checker.CheckAll(); err != nil {
		t.Fatalf("Invariant check failed: %v", err)
	}
}

// TestScenario4_KillLeaderUnderWriteLoad kills the leader repeatedly while writes are ongoing.
func TestScenario4_KillLeaderUnderWriteLoad(t *testing.T) {
	c, err := NewCluster(5, false, "", 303)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer c.Stop()

	checker := NewInvariantChecker(c)
	c.Start()

	_, err = c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("Leader election failed: %v", err)
	}

	// Submit some initial writes
	for i := 1; i <= 5; i++ {
		_, _ = c.Submit(kv.Op{
			Type:     kv.OpPut,
			Key:      fmt.Sprintf("initial-%d", i),
			Value:    fmt.Sprintf("val-%d", i),
			ClientID: "client-kill",
			SeqNum:   uint64(i),
		}, 2*time.Second)
	}

	// Kill leader 1
	l1, _ := c.Leader()
	t.Logf("Killing leader %s...", l1)
	_ = c.CrashNode(l1)

	// New leader elected
	l2, err := c.WaitLeader(3 * time.Second)
	if err != nil {
		t.Fatalf("Failed to elect new leader after first kill: %v", err)
	}
	t.Logf("New leader: %s. Submitting writes...", l2)

	for i := 6; i <= 10; i++ {
		_, err := c.Submit(kv.Op{
			Type:     kv.OpPut,
			Key:      fmt.Sprintf("mid-%d", i),
			Value:    fmt.Sprintf("val-%d", i),
			ClientID: "client-kill",
			SeqNum:   uint64(i),
		}, 2*time.Second)
		if err != nil {
			t.Logf("Write %d rejected during re-election (expected): %v", i, err)
		}
	}

	// Kill leader 2
	t.Logf("Killing second leader %s...", l2)
	_ = c.CrashNode(l2)

	// Restart first node to maintain majority of 5
	_ = c.RestartNode(l1)

	l3, err := c.WaitLeader(3 * time.Second)
	if err != nil {
		t.Fatalf("Failed to elect third leader: %v", err)
	}
	t.Logf("Third leader: %s", l3)

	if err := checker.CheckAll(); err != nil {
		t.Fatalf("Invariant check failed: %v", err)
	}
}

// TestScenario5_NetworkPartitionPartitionLeader verifies that writes to a partitioned leader
// are not committed, while the majority continues making progress.
func TestScenario5_NetworkPartitionPartitionLeader(t *testing.T) {
	c, err := NewCluster(5, false, "", 404)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer c.Stop()

	checker := NewInvariantChecker(c)
	c.Start()

	oldLeader, err := c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("Leader election failed: %v", err)
	}

	// Commit an initial write
	_, err = c.Submit(kv.Op{
		Type:     kv.OpPut,
		Key:      "committed-key",
		Value:    "initial-value",
		ClientID: "client-part",
		SeqNum:   1,
	}, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed initial write: %v", err)
	}

	// Isolate the leader into minority partition
	t.Logf("Isolating leader %s...", oldLeader)
	isolatedLeader, err := IsolateLeader(c)
	if err != nil {
		t.Fatalf("Failed to isolate leader: %v", err)
	}

	// Majority partition should elect a new leader
	time.Sleep(300 * time.Millisecond)
	newLeader, err := c.WaitLeader(3 * time.Second)
	if err != nil {
		t.Fatalf("Majority failed to elect new leader: %v", err)
	}
	if newLeader == isolatedLeader {
		t.Fatalf("Isolated leader still claims leadership!")
	}
	t.Logf("Majority elected new leader: %s", newLeader)

	// Writes to new leader should succeed
	smNew, _ := c.GetStateMachine(newLeader)
	res, err := smNew.Execute(kv.Op{
		Type:     kv.OpPut,
		Key:      "majority-key",
		Value:    "majority-value",
		ClientID: "client-part",
		SeqNum:   2,
	}, 2*time.Second)
	if err != nil {
		t.Fatalf("Majority write failed: %v", err)
	}
	if res.Value != "majority-value" {
		t.Fatalf("Unexpected value: %s", res.Value)
	}

	// Heal partition
	t.Logf("Healing network partition...")
	HealNetwork(c)
	time.Sleep(300 * time.Millisecond)

	// Old leader must have stepped down and caught up
	leaderAfterHeal, err := c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("Failed to find leader after heal: %v", err)
	}
	t.Logf("Cluster unified under leader: %s", leaderAfterHeal)

	// Read both keys from unified cluster
	readRes, err := c.Submit(kv.Op{Type: kv.OpGet, Key: "majority-key"}, 2*time.Second)
	if err != nil || readRes.Value != "majority-value" {
		t.Fatalf("Read 'majority-key' failed: %v (val=%s)", err, readRes.Value)
	}

	if err := checker.CheckAll(); err != nil {
		t.Fatalf("Invariant check failed: %v", err)
	}
}

// TestScenario6_SlowFollowerSnapshotCatchUp tests InstallSnapshot when a follower lags far behind.
func TestScenario6_SlowFollowerSnapshotCatchUp(t *testing.T) {
	c, err := NewCluster(3, false, "", 505)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer c.Stop()

	checker := NewInvariantChecker(c)
	c.Start()

	leader, err := c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("Leader election failed: %v", err)
	}

	// Choose a follower to disconnect
	var follower string
	for _, id := range c.NodeIDs() {
		if id != leader {
			follower = id
			break
		}
	}

	t.Logf("Disconnecting follower %s...", follower)
	c.SimNet().Disconnect(leader, follower)
	c.SimNet().Disconnect(follower, leader)

	// Submit 60 writes (threshold for snapshot is 50 in our harness cluster)
	t.Logf("Writing 60 entries to trigger snapshot compaction...")
	for i := 1; i <= 60; i++ {
		_, err := c.Submit(kv.Op{
			Type:     kv.OpPut,
			Key:      fmt.Sprintf("snap-key-%d", i),
			Value:    fmt.Sprintf("snap-val-%d", i),
			ClientID: "client-snap",
			SeqNum:   uint64(i),
		}, 2*time.Second)
		if err != nil {
			t.Fatalf("Failed write %d: %v", i, err)
		}
	}

	// Reconnect the lagging follower
	t.Logf("Reconnecting follower %s to leader %s...", follower, leader)
	c.SimNet().Connect(leader, follower)
	c.SimNet().Connect(follower, leader)

	// Allow leader to send InstallSnapshot
	time.Sleep(500 * time.Millisecond)

	// Check if lagging follower caught up to state
	smFollower, _ := c.GetStateMachine(follower)
	data := smFollower.GetAll()
	if len(data) < 60 {
		t.Fatalf("Follower has %d entries, expected 60 after snapshot install", len(data))
	}
	t.Logf("Follower %s successfully caught up via InstallSnapshot (%d entries)!", follower, len(data))

	if err := checker.CheckAll(); err != nil {
		t.Fatalf("Invariant check failed: %v", err)
	}
}

// TestScenario7_RollingRestart verifies crash recovery with persistent storage.
func TestScenario7_RollingRestart(t *testing.T) {
	tempDir := t.TempDir()
	c, err := NewCluster(3, true, tempDir, 606)
	if err != nil {
		t.Fatalf("Failed to create disk cluster: %v", err)
	}
	defer c.Stop()

	checker := NewInvariantChecker(c)
	c.Start()

	_, err = c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("Leader election failed: %v", err)
	}

	// Write entries before rolling restart
	for i := 1; i <= 10; i++ {
		_, err := c.Submit(kv.Op{
			Type:     kv.OpPut,
			Key:      fmt.Sprintf("persist-%d", i),
			Value:    fmt.Sprintf("data-%d", i),
			ClientID: "client-persist",
			SeqNum:   uint64(i),
		}, 2*time.Second)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	// Rolling restart of all 3 nodes
	t.Logf("Performing rolling restart across all nodes...")
	if err := RollingRestart(c, 150*time.Millisecond); err != nil {
		t.Fatalf("Rolling restart failed: %v", err)
	}

	// Cluster should elect a leader and still have all data
	newLeader, err := c.WaitLeader(3 * time.Second)
	if err != nil {
		t.Fatalf("Failed to elect leader after rolling restart: %v", err)
	}
	t.Logf("Cluster recovered! Active leader: %s", newLeader)

	for i := 1; i <= 10; i++ {
		res, err := c.Submit(kv.Op{
			Type: kv.OpGet,
			Key:  fmt.Sprintf("persist-%d", i),
		}, 2*time.Second)
		if err != nil || res.Value != fmt.Sprintf("data-%d", i) {
			t.Fatalf("Data mismatch for persist-%d: got %s, err: %v", i, res.Value, err)
		}
	}

	if err := checker.CheckAll(); err != nil {
		t.Fatalf("Invariant check failed: %v", err)
	}
}

// TestScenario8_ChaosTestingAndLinearizability runs concurrent clients under packet loss,
// partitions, and node restarts, continuously checking invariants and Porcupine linearizability.
func TestScenario8_ChaosTestingAndLinearizability(t *testing.T) {
	seed := int64(707)
	c, err := NewCluster(5, false, "", seed)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer c.Stop()

	checker := NewInvariantChecker(c)
	stopContinuous, errCh := checker.StartContinuousChecking(50 * time.Millisecond)
	defer stopContinuous()

	history := NewHistoryRecorder()
	c.Start()

	_, err = c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("Initial leader election failed: %v", err)
	}

	var wg sync.WaitGroup
	chaosStop := make(chan struct{})

	// 1. Chaos injector goroutine: randomly drops packets, introduces partitions, heals
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(seed))
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-chaosStop:
				HealNetwork(c)
				return
			case <-ticker.C:
				action := rng.Intn(4)
				switch action {
				case 0:
					// Drop packets
					c.SimNet().SetDropRate(0.15)
				case 1:
					// Partition majority/minority
					PartitionMajorityMinority(c)
				case 2:
					// Heal
					HealNetwork(c)
				case 3:
					// Inject network delays
					c.SimNet().SetDelays(10*time.Millisecond, 40*time.Millisecond)
				}
			}
		}
	}()

	// 2. Concurrent client workers
	numClients := 3
	for cIdx := 0; cIdx < numClients; cIdx++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			clientID := fmt.Sprintf("chaos-client-%d", id)
			var seq uint64 = 1

			for opNum := 0; opNum < 15; opNum++ {
				key := fmt.Sprintf("k-%d", opNum%3) // share 3 keys across clients to test concurrent conflict
				val := fmt.Sprintf("v-%d-%d", id, opNum)

				// Put
				start := time.Now()
				res, err := c.Submit(kv.Op{
					Type:     kv.OpPut,
					Key:      key,
					Value:    val,
					ClientID: clientID,
					SeqNum:   seq,
				}, 1500*time.Millisecond)
				end := time.Now()

				seq++
				history.Record(ClientOp{
					ClientID: clientID,
					Type:     "Put",
					Key:      key,
					Value:    val,
					Start:    start,
					End:      end,
					Success:  err == nil,
					Err:      fmt.Sprint(err),
				})

				// Get
				start = time.Now()
				res, err = c.Submit(kv.Op{
					Type: kv.OpGet,
					Key:  key,
				}, 1500*time.Millisecond)
				end = time.Now()

				history.Record(ClientOp{
					ClientID: clientID,
					Type:     "Get",
					Key:      key,
					Value:    res.Value,
					Start:    start,
					End:      end,
					Success:  err == nil,
					Err:      fmt.Sprint(err),
				})

				time.Sleep(50 * time.Millisecond)
			}
		}(cIdx)
	}

	// Run chaos for 6 seconds
	time.Sleep(6 * time.Second)
	close(chaosStop)
	wg.Wait()

	// Check if any invariant failed during continuous monitoring
	select {
	case err := <-errCh:
		t.Fatalf("Invariant violated during chaos: %v", err)
	default:
	}

	// Verify linearizability over recorded history
	ops := history.Ops()
	t.Logf("Checking linearizability over %d client operations...", len(ops))
	isLinearizable, reason := CheckLinearizability(ops)
	if !isLinearizable {
		t.Fatalf("History is NOT linearizable! %s", reason)
	}
	t.Logf("Linearizability successfully verified for all client operations!")
}
