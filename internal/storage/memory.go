package storage

import (
	"sync"

	"raft-kv/internal/raft"
)

// MemoryStorage is a thread-safe in-memory implementation of raft.Storage for tests.
type MemoryStorage struct {
	mu                sync.RWMutex
	term              uint64
	votedFor          string
	entries           []raft.LogEntry
	snapshot          []byte
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
}

// NewMemoryStorage creates a new in-memory storage engine.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		entries: make([]raft.LogEntry, 0),
	}
}

func (m *MemoryStorage) SaveState(term uint64, votedFor string, entries []raft.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.term = term
	m.votedFor = votedFor
	m.entries = make([]raft.LogEntry, len(entries))
	copy(m.entries, entries)
	return nil
}

func (m *MemoryStorage) LoadState() (term uint64, votedFor string, entries []raft.LogEntry, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	copied := make([]raft.LogEntry, len(m.entries))
	copy(copied, m.entries)
	return m.term, m.votedFor, copied, nil
}

func (m *MemoryStorage) SaveSnapshot(snapshot []byte, lastIncludedIndex uint64, lastIncludedTerm uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.snapshot = make([]byte, len(snapshot))
	copy(m.snapshot, snapshot)
	m.lastIncludedIndex = lastIncludedIndex
	m.lastIncludedTerm = lastIncludedTerm
	return nil
}

func (m *MemoryStorage) LoadSnapshot() (snapshot []byte, lastIncludedIndex uint64, lastIncludedTerm uint64, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.snapshot) == 0 {
		return nil, 0, 0, nil
	}
	copied := make([]byte, len(m.snapshot))
	copy(copied, m.snapshot)
	return copied, m.lastIncludedIndex, m.lastIncludedTerm, nil
}

func (m *MemoryStorage) Close() error {
	return nil
}
