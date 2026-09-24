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

	fmt.Println("-----------------------------------------------------------------")
	fmt.Printf("[CHAOS] Raft Consensus Fault-Injection & Invariant Suite\n")
	fmt.Printf("        Nodes: %d | Duration: %v | Seed: %d\n", *nodes, *duration, *seed)
	fmt.Println("-----------------------------------------------------------------")

	c, err := harness.NewCluster(*nodes, false, "", *seed)
	if err != nil {
		fmt.Printf("[ERROR] Failed to create cluster: %v\n", err)
		os.Exit(1)
	}
	defer c.Stop()

	checker := harness.NewInvariantChecker(c)
	stopChecker, errCh := checker.StartContinuousChecking(40 * time.Millisecond)
	defer stopChecker()

	history := harness.NewHistoryRecorder()
	c.Start()

	fmt.Print("[INIT] Awaiting initial leader election... ")
	leader, err := c.WaitLeader(3 * time.Second)
	if err != nil {
		fmt.Printf("Failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Elected leader: %s\n\n", leader)

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
					fmt.Println("[CHAOS] Injected 20% packet drop rate")
					c.SimNet().SetDropRate(0.20)
				case 1:
					maj, min := harness.PartitionMajorityMinority(c)
					fmt.Printf("[CHAOS] Network Split: Majority=%v vs Minority=%v\n", maj, min)
				case 2:
					fmt.Println("[CHAOS] Healing network partitions")
					harness.HealNetwork(c)
				case 3:
					fmt.Println("[CHAOS] Injected 30ms latency jitter")
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

	fmt.Println("\n-----------------------------------------------------------------")
	fmt.Println("[REPORT] Raft Consensus Invariant & Consistency Summary")
	fmt.Println("-----------------------------------------------------------------")

	// Verify invariants
	select {
	case err := <-errCh:
		fmt.Printf("[FAILURE] Safety Invariant Violated: %v\n", err)
		os.Exit(1)
	default:
		fmt.Println("[OK] Core Safety Invariants Maintained:")
		fmt.Println("     - Invariant 1 (Election Safety):       PASS [at most 1 leader per term]")
		fmt.Println("     - Invariant 2 (Leader Append-Only):    PASS [no leader truncation]")
		fmt.Println("     - Invariant 3 (Log Matching):          PASS [prefix consistency]")
		fmt.Println("     - Invariant 4 (Leader Completeness):   PASS [committed entries persist]")
		fmt.Println("     - Invariant 5 (State Machine Safety):  PASS [no diverging executions]")
	}

	// Verify linearizability
	ops := history.Ops()
	fmt.Printf("\n[VERIFY] Checking Linearizability over %d recorded operations...\n", len(ops))
	linearizable, errDesc := harness.CheckLinearizability(ops)
	if !linearizable {
		fmt.Printf("[FAILURE] Linearizability Check Failed: %s\n", errDesc)
		os.Exit(1)
	}
	fmt.Println("[OK] Execution History Verified: Strictly Linearizable (Sequential KV)")
	fmt.Println("-----------------------------------------------------------------")
	fmt.Println("[DONE] Chaos Scenario Concluded with 0 Invariant Violations.")
}
