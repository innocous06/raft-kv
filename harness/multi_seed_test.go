package harness

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"raft-kv/internal/kv"
)

func TestChaos_MultiSeedFuzzing(t *testing.T) {
	seeds := []int64{
		11, 23, 37, 42, 59, 73, 89, 101, 137, 199,
		257, 331, 409, 521, 607, 709, 811, 919, 1009, 1201,
		1423, 1607, 1801, 2003, 2207, 2411, 2609, 2801, 3001, 3333,
	}

	t.Logf("Running multi-seed chaos fuzzing across %d distinct deterministic seeds...", len(seeds))

	for _, seed := range seeds {
		func(currentSeed int64) {
			rng := rand.New(rand.NewSource(currentSeed))
			c, err := NewCluster(3, false, "", currentSeed)
			if err != nil {
				t.Fatalf("seed %d: failed to create cluster: %v", currentSeed, err)
			}
			defer c.Stop()

			checker := NewInvariantChecker(c)
			c.Start()

			leader, err := c.WaitLeader(2 * time.Second)
			if err != nil {
				t.Fatalf("seed %d: initial leader election failed: %v", currentSeed, err)
			}

			nodes := c.NodeIDs()
			for opIdx := 1; opIdx <= 10; opIdx++ {
				if rng.Float64() < 0.20 {
					targetFollower := nodes[rng.Intn(len(nodes))]
					if targetFollower != leader {
						c.SimNet().SetSlowNode(targetFollower, 20*time.Millisecond)
					}
				}

				key := fmt.Sprintf("seed-k-%d", rng.Intn(5))
				val := fmt.Sprintf("val-s%d-op%d", currentSeed, opIdx)

				_, err := c.Submit(kv.Op{
					Type:     kv.OpPut,
					Key:      key,
					Value:    val,
					ClientID: fmt.Sprintf("client-%d", currentSeed),
					SeqNum:   uint64(opIdx),
				}, 1*time.Second)

				if err != nil {
					continue
				}
			}

			c.SimNet().Heal()
			time.Sleep(50 * time.Millisecond)

			if err := checker.CheckAll(); err != nil {
				t.Fatalf("seed %d: invariant violation: %v", currentSeed, err)
			}
		}(seed)
	}

	t.Logf("Multi-seed chaos fuzzing passed: 0 invariant violations across all %d seeds", len(seeds))
}

func TestChaos_1000SeededRuns(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1,000-seed chaos run in short mode")
	}

	const totalRuns = 1000
	workers := runtime.NumCPU() * 2
	if workers < 8 {
		workers = 8
	}

	seedChan := make(chan int64, totalRuns)
	for i := int64(1); i <= totalRuns; i++ {
		seedChan <- i
	}
	close(seedChan)

	var completedRuns int64
	var failureCount int64
	var wg sync.WaitGroup

	start := time.Now()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for seed := range seedChan {
				rng := rand.New(rand.NewSource(seed * 7919))
				c, err := NewCluster(3, false, "", seed*7919)
				if err != nil {
					atomic.AddInt64(&failureCount, 1)
					continue
				}

				checker := NewInvariantChecker(c)
				c.Start()

				leader, err := c.WaitLeader(2 * time.Second)
				if err == nil {
					// 3 operations per run
					for op := 1; op <= 3; op++ {
						k := fmt.Sprintf("k-%d", rng.Intn(3))
						v := fmt.Sprintf("v-%d-%d", seed, op)
						_, _ = c.Submit(kv.Op{
							Type:     kv.OpPut,
							Key:      k,
							Value:    v,
							ClientID: fmt.Sprintf("c-%d", seed),
							SeqNum:   uint64(op),
						}, 300*time.Millisecond)
					}

					// Occasional network disruption
					if rng.Float64() < 0.3 {
						nodes := c.NodeIDs()
						victim := nodes[rng.Intn(len(nodes))]
						if victim != leader {
							c.SimNet().SetSlowNode(victim, 15*time.Millisecond)
						}
					}

					c.SimNet().Heal()
					time.Sleep(20 * time.Millisecond)

					if err := checker.CheckAll(); err != nil {
						atomic.AddInt64(&failureCount, 1)
					}
				} else {
					atomic.AddInt64(&failureCount, 1)
				}

				c.Stop()
				atomic.AddInt64(&completedRuns, 1)
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	completed := atomic.LoadInt64(&completedRuns)
	failures := atomic.LoadInt64(&failureCount)

	t.Logf("Completed %d seeded chaos runs in %v (Failures/Invariant Violations: %d)", completed, elapsed, failures)
	if failures > 0 {
		t.Fatalf("%d out of %d seeded chaos runs reported invariant violations", failures, completed)
	}
}
