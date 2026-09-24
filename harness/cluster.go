package harness

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"raft-kv/internal/events"
	"raft-kv/internal/kv"
	"raft-kv/internal/raft"
	"raft-kv/internal/storage"
	"raft-kv/internal/transport"
)

// Cluster manages an in-process Raft cluster connected via SimNet.
type Cluster struct {
	mu            sync.RWMutex
	nodeIDs       []string
	nodes         map[string]*raft.Node
	stateMachines map[string]*kv.StateMachine
	storages      map[string]raft.Storage
	simNet        *transport.SimNet
	eventBus      *events.Bus
	useDisk       bool
	baseDir       string
}

// NewCluster constructs a cluster of N nodes with simulated network.
func NewCluster(n int, useDisk bool, baseDir string, seed int64) (*Cluster, error) {
	if n <= 0 {
		n = 3
	}

	bus := events.NewBus(5000)
	simNet := transport.NewSimNet(seed)
	simNet.SetEventBus(bus)

	nodeIDs := make([]string, n)
	for i := 0; i < n; i++ {
		nodeIDs[i] = fmt.Sprintf("node-%d", i+1)
	}

	c := &Cluster{
		nodeIDs:       nodeIDs,
		nodes:         make(map[string]*raft.Node),
		stateMachines: make(map[string]*kv.StateMachine),
		storages:      make(map[string]raft.Storage),
		simNet:        simNet,
		eventBus:      bus,
		useDisk:       useDisk,
		baseDir:       baseDir,
	}

	for _, id := range nodeIDs {
		var store raft.Storage
		var err error
		if useDisk {
			dir := filepath.Join(baseDir, id)
			store, err = storage.NewDiskStorage(dir)
			if err != nil {
				return nil, err
			}
		} else {
			store = storage.NewMemoryStorage()
		}
		c.storages[id] = store

		if err := c.initNode(id); err != nil {
			return nil, err
		}
	}

	return c, nil
}

func (c *Cluster) initNode(id string) error {
	var peers []string
	for _, p := range c.nodeIDs {
		if p != id {
			peers = append(peers, p)
		}
	}

	cfg := raft.DefaultConfig(id, peers)
	cfg.Storage = c.storages[id]
	cfg.Transport = c.simNet
	cfg.EventBus = c.eventBus
	cfg.HeartbeatInterval = 30 * time.Millisecond
	cfg.MinElectionTimeout = 100 * time.Millisecond
	cfg.MaxElectionTimeout = 200 * time.Millisecond

	node, err := raft.NewNode(cfg)
	if err != nil {
		return err
	}

	sm := kv.NewStateMachine(id, node, 50, c.eventBus)
	c.nodes[id] = node
	c.stateMachines[id] = sm
	return nil
}

// Start boots all nodes in the cluster.
func (c *Cluster) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.nodes {
		n.Start()
	}
}

// Stop shuts down all nodes and state machines.
func (c *Cluster) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, sm := range c.stateMachines {
		sm.Close()
	}
	for _, n := range c.nodes {
		n.Stop()
	}
	if c.useDisk && c.baseDir != "" {
		_ = os.RemoveAll(c.baseDir)
	}
}

// EventBus returns the cluster event bus.
func (c *Cluster) EventBus() *events.Bus {
	return c.eventBus
}

// SimNet returns the cluster network simulator.
func (c *Cluster) SimNet() *transport.SimNet {
	return c.simNet
}

// NodeIDs returns all node identifiers.
func (c *Cluster) NodeIDs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	copied := make([]string, len(c.nodeIDs))
	copy(copied, c.nodeIDs)
	return copied
}

// GetNode returns a Raft node by ID.
func (c *Cluster) GetNode(id string) (*raft.Node, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n, ok := c.nodes[id]
	return n, ok
}

// GetStateMachine returns the KV state machine for a node.
func (c *Cluster) GetStateMachine(id string) (*kv.StateMachine, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	sm, ok := c.stateMachines[id]
	return sm, ok
}

// Leader inspects all alive nodes and returns the current leader if a single one exists.
func (c *Cluster) Leader() (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var leaders []string
	var maxTerm uint64

	for id, n := range c.nodes {
		if n.IsStopped() {
			continue
		}
		term, isLeader, _ := n.GetState()
		if isLeader {
			if term > maxTerm {
				maxTerm = term
				leaders = []string{id}
			} else if term == maxTerm {
				leaders = append(leaders, id)
			}
		}
	}

	if len(leaders) == 0 {
		return "", fmt.Errorf("no leader elected")
	}
	if len(leaders) > 1 {
		return "", fmt.Errorf("multiple leaders detected in term %d: %v", maxTerm, leaders)
	}
	return leaders[0], nil
}

// WaitLeader polls until exactly one alive node is recognized as leader.
func (c *Cluster) WaitLeader(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader, err := c.Leader()
		if err == nil && leader != "" {
			return leader, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return "", fmt.Errorf("timed out waiting for leader after %v", timeout)
}

// CrashNode kills a node immediately without wiping its storage.
func (c *Cluster) CrashNode(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.nodes[id]
	if !ok {
		return fmt.Errorf("node %s not found", id)
	}

	if sm, exists := c.stateMachines[id]; exists {
		sm.Close()
		delete(c.stateMachines, id)
	}

	node.Stop()
	return nil
}

// RestartNode restarts a previously crashed node, reloading its persisted state.
func (c *Cluster) RestartNode(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.storages[id]; !ok {
		return fmt.Errorf("node %s not found in cluster configuration", id)
	}

	if node, exists := c.nodes[id]; exists && !node.IsStopped() {
		return fmt.Errorf("node %s is already running", id)
	}

	if err := c.initNode(id); err != nil {
		return err
	}

	c.nodes[id].Start()
	return nil
}

// Submit sends a KV operation to the current leader.
func (c *Cluster) Submit(op kv.Op, timeout time.Duration) (kv.OpResult, error) {
	leaderID, err := c.WaitLeader(timeout)
	if err != nil {
		return kv.OpResult{}, err
	}

	c.mu.RLock()
	sm, ok := c.stateMachines[leaderID]
	c.mu.RUnlock()

	if !ok {
		return kv.OpResult{}, fmt.Errorf("state machine for leader %s unavailable", leaderID)
	}

	return sm.Execute(op, timeout)
}
