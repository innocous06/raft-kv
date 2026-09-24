package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"raft-kv/harness"
	"raft-kv/internal/kv"
)

func main() {
	var (
		nodes    = flag.Int("nodes", 5, "Number of nodes in cluster")
		duration = flag.Duration("duration", 8*time.Second, "Chaos duration")
		seed     = flag.Int64("seed", 42, "PRNG seed for reproducible chaos")
	)
	flag.Parse()

	fmt.Println("=================================================================")
	fmt.Printf("🔥 Starting Raft Chaos Injection Engine\n")
	fmt.Printf("   Nodes: %d | Duration: %v | Seed: %d\n", *nodes, *duration, *seed)
	fmt.Println("=================================================================")

	c, err := harness.NewCluster(*nodes, false, "", *seed)
	if err != nil {
		fmt.Printf("❌ Failed to create cluster: %v\n", err)
		os.Exit(1)
	}
	defer c.Stop()

	checker := harness.NewInvariantChecker(c)
	stopChecker, errCh := checker.StartContinuousChecking(40 * time.Millisecond)
	defer stopChecker()

	history := harness.NewHistoryRecorder()
	c.Start()

	fmt.Print("⏳ Waiting for initial leader election... ")
	leader, err := c.WaitLeader(3 * time.Second)
	if err != nil {
		fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("✓ Elected leader: %s\n\n", leader)

	var wg sync.WaitGroup
	stopChaos := make(chan struct{})

	// Chaos injector
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(*seed))
		ticker := time.NewTicker(400 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stopChaos:
				harness.HealNetwork(c)
				return
			case <-ticker.C:
				action := rng.Intn(4)
				switch action {
				case 0:
					fmt.Println("💥 [CHAOS] Injecting 20% packet drop rate...")
					c.SimNet().SetDropRate(0.20)
				case 1:
					maj, min := harness.PartitionMajorityMinority(c)
					fmt.Printf("⚡ [CHAOS] Network Partition: Majority=%v vs Minority=%v\n", maj, min)
				case 2:
					fmt.Println("💚 [CHAOS] Healing network partition...")
					harness.HealNetwork(c)
				case 3:
					fmt.Println("⏱️  [CHAOS] Injecting 30ms latency jitter...")
					c.SimNet().SetDelays(15*time.Millisecond, 40*time.Millisecond)
				}
			}
		}
	}()

	// Concurrent workload generators
	numClients := 3
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(cID int) {
			defer wg.Done()
			clientName := fmt.Sprintf("chaos-worker-%d", cID)
			var seq uint64 = 1

			for {
				select {
				case <-stopChaos:
					return
				default:
				}

				key := fmt.Sprintf("key-%d", seq%3)
				val := fmt.Sprintf("val-%d-%d", cID, seq)

				start := time.Now()
				res, err := c.Submit(kv.Op{
					Type:     kv.OpPut,
					Key:      key,
					Value:    val,
					ClientID: clientName,
					SeqNum:   seq,
				}, 1500*time.Millisecond)
				end := time.Now()

				seq++
				history.Record(harness.ClientOp{
					ClientID: clientName,
					Type:     "Put",
					Key:      key,
					Value:    val,
					Start:    start,
					End:      end,
					Success:  err == nil,
					Err:      fmt.Sprint(err),
				})

				// Read operation
				start = time.Now()
				res, err = c.Submit(kv.Op{
					Type: kv.OpGet,
					Key:  key,
				}, 1500*time.Millisecond)
				end = time.Now()

				history.Record(harness.ClientOp{
					ClientID: clientName,
					Type:     "Get",
					Key:      key,
					Value:    res.Value,
					Start:    start,
					End:      end,
					Success:  err == nil,
					Err:      fmt.Sprint(err),
				})

				time.Sleep(60 * time.Millisecond)
			}
		}(i)
	}

	time.Sleep(*duration)
	close(stopChaos)
	wg.Wait()

	fmt.Println("\n=================================================================")
	fmt.Println("📊 Chaos Verification Report")
	fmt.Println("=================================================================")

	// Verify invariants
	select {
	case err := <-errCh:
		fmt.Printf("❌ Invariant Failure: %v\n", err)
		os.Exit(1)
	default:
		fmt.Println("✓ All 5 Raft Safety Invariants Maintained:")
		fmt.Println("   1. Election Safety: PASS (at most 1 leader per term)")
		fmt.Println("   2. Leader Append-Only: PASS (no leader truncation)")
		fmt.Println("   3. Log Matching: PASS (prefix consistency)")
		fmt.Println("   4. Leader Completeness: PASS (committed entry persistence)")
		fmt.Println("   5. State Machine Safety: PASS (no diverging applications)")
	}

	// Verify linearizability
	ops := history.Ops()
	fmt.Printf("✓ Checking Linearizability across %d recorded client operations...\n", len(ops))
	linearizable, errDesc := harness.CheckLinearizability(ops)
	if !linearizable {
		fmt.Printf("❌ Linearizability Check FAILED: %s\n", errDesc)
		os.Exit(1)
	}
	fmt.Println("✓ History is Strictly Linearizable (Sequential KV Consistency Verified)")
	fmt.Println("=================================================================")
	fmt.Println("🎉 Chaos Scenario Completed Successfully with 0 Invariant Violations!")
}
