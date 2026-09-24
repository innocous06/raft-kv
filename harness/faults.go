package harness

import (
	"fmt"
	"time"
)

// IsolateLeader partitions the current leader into a 1-node island.
func IsolateLeader(c *Cluster) (string, error) {
	leaderID, err := c.WaitLeader(2 * time.Second)
	if err != nil {
		return "", err
	}

	var others []string
	for _, id := range c.NodeIDs() {
		if id != leaderID {
			others = append(others, id)
		}
	}

	c.SimNet().Partition([]string{leaderID}, others)
	return leaderID, nil
}

// PartitionMajorityMinority splits the cluster into majority and minority sets.
func PartitionMajorityMinority(c *Cluster) (majority []string, minority []string) {
	nodes := c.NodeIDs()
	total := len(nodes)
	majCount := (total / 2) + 1

	majority = make([]string, majCount)
	copy(majority, nodes[:majCount])

	minority = make([]string, total-majCount)
	copy(minority, nodes[majCount:])

	c.SimNet().Partition(majority, minority)
	return majority, minority
}

// Partition3Way splits the cluster into 3 isolated groups.
func Partition3Way(c *Cluster) [][]string {
	nodes := c.NodeIDs()
	var g1, g2, g3 []string
	for i, id := range nodes {
		switch i % 3 {
		case 0:
			g1 = append(g1, id)
		case 1:
			g2 = append(g2, id)
		case 2:
			g3 = append(g3, id)
		}
	}
	c.SimNet().Partition(g1, g2, g3)
	return [][]string{g1, g2, g3}
}

// HealNetwork restores network connectivity and removes loss/latency.
func HealNetwork(c *Cluster) {
	c.SimNet().Heal()
	c.SimNet().SetDropRate(0)
	c.SimNet().SetDelays(0, 0)
}

// RollingRestart restarts each node sequentially with delay between each.
func RollingRestart(c *Cluster, delay time.Duration) error {
	for _, id := range c.NodeIDs() {
		if err := c.CrashNode(id); err != nil {
			return fmt.Errorf("failed to crash node %s: %w", id, err)
		}
		time.Sleep(delay)
		if err := c.RestartNode(id); err != nil {
			return fmt.Errorf("failed to restart node %s: %w", id, err)
		}
		time.Sleep(delay)
	}
	return nil
}
