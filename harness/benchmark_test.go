package harness

import (
	"fmt"
	"testing"
	"time"

	"raft-kv/internal/kv"
)

func BenchmarkWriteThroughput_3Nodes(b *testing.B) {
	c, err := NewCluster(3, false, "", 401)
	if err != nil {
		b.Fatalf("failed to create cluster: %v", err)
	}
	defer c.Stop()
	c.Start()

	_, err = c.WaitLeader(3 * time.Second)
	if err != nil {
		b.Fatalf("failed to elect leader: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c.Submit(kv.Op{
			Type:     kv.OpPut,
			Key:      fmt.Sprintf("bench-k-%d", i%100),
			Value:    "benchmark-payload-value",
			ClientID: "benchmarker",
			SeqNum:   uint64(i + 1),
		}, 3*time.Second)
		if err != nil {
			b.Fatalf("write failed at iteration %d: %v", i, err)
		}
	}
	b.StopTimer()
}

func BenchmarkWriteThroughput_5Nodes(b *testing.B) {
	c, err := NewCluster(5, false, "", 402)
	if err != nil {
		b.Fatalf("failed to create cluster: %v", err)
	}
	defer c.Stop()
	c.Start()

	_, err = c.WaitLeader(3 * time.Second)
	if err != nil {
		b.Fatalf("failed to elect leader: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c.Submit(kv.Op{
			Type:     kv.OpPut,
			Key:      fmt.Sprintf("bench5-k-%d", i%100),
			Value:    "benchmark-payload-value",
			ClientID: "benchmarker-5",
			SeqNum:   uint64(i + 1),
		}, 3*time.Second)
		if err != nil {
			b.Fatalf("write failed at iteration %d: %v", i, err)
		}
	}
	b.StopTimer()
}

func BenchmarkReadThroughput(b *testing.B) {
	c, err := NewCluster(3, false, "", 403)
	if err != nil {
		b.Fatalf("failed to create cluster: %v", err)
	}
	defer c.Stop()
	c.Start()

	_, err = c.WaitLeader(3 * time.Second)
	if err != nil {
		b.Fatalf("failed to elect leader: %v", err)
	}

	// Seed key
	_, _ = c.Submit(kv.Op{
		Type:  kv.OpPut,
		Key:   "read-key",
		Value: "read-val",
	}, 3*time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c.Submit(kv.Op{
			Type: kv.OpGet,
			Key:  "read-key",
		}, 3*time.Second)
		if err != nil {
			b.Fatalf("read failed at iteration %d: %v", i, err)
		}
	}
	b.StopTimer()
}

func TestBenchmark_ElectionConvergenceLatency(t *testing.T) {
	clusterSizes := []int{3, 5}
	rounds := 5

	for _, n := range clusterSizes {
		var totalDuration time.Duration

		for r := 0; r < rounds; r++ {
			c, err := NewCluster(n, false, "", int64(500+r*10+n))
			if err != nil {
				t.Fatalf("failed to create cluster: %v", err)
			}

			start := time.Now()
			c.Start()
			_, err = c.WaitLeader(3 * time.Second)
			if err != nil {
				c.Stop()
				t.Fatalf("failed to elect leader in size %d: %v", n, err)
			}
			elapsed := time.Since(start)
			totalDuration += elapsed
			c.Stop()
		}

		avgMs := float64(totalDuration.Milliseconds()) / float64(rounds)
		t.Logf("Cluster Size %d Nodes -> Average Election Convergence: %.1f ms", n, avgMs)
	}
}
