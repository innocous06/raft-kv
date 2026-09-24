package harness

import (
	"fmt"
	"math/rand"
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

			// Perform 10 randomized operations under occasional message delays
			nodes := c.NodeIDs()
			for opIdx := 1; opIdx <= 10; opIdx++ {
				// Inject 20% transient delay
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
					// Transient timeout is acceptable under chaos
					continue
				}
			}

			// Restore network and verify all safety invariants
			c.SimNet().Heal()
			time.Sleep(50 * time.Millisecond)

			if err := checker.CheckAll(); err != nil {
				t.Fatalf("seed %d: invariant violation: %v", currentSeed, err)
			}
		}(seed)
	}

	t.Logf("Multi-seed chaos fuzzing passed: 0 invariant violations across all %d seeds", len(seeds))
}
