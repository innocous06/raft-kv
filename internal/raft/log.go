package raft

import (
	"errors"
	"fmt"
)

var (
	ErrIndexCompacted = errors.New("log index has been compacted into snapshot")
	ErrIndexOutOfRange = errors.New("log index out of range")
)

// RaftLog manages log entries, indexing, truncation, and snapshot offset translation.
// Log indices are 1-based (Raft convention).
type RaftLog struct {
	entries           []LogEntry
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
}

// NewRaftLog initializes a log with given entries and compacted snapshot markers.
func NewRaftLog(entries []LogEntry, lastIncludedIndex, lastIncludedTerm uint64) *RaftLog {
	rl := &RaftLog{
		entries:           make([]LogEntry, len(entries)),
		lastIncludedIndex: lastIncludedIndex,
		lastIncludedTerm:  lastIncludedTerm,
	}
	copy(rl.entries, entries)
	return rl
}

// LastIncludedIndex returns the index of the latest snapshot entry.
func (l *RaftLog) LastIncludedIndex() uint64 {
	return l.lastIncludedIndex
}

// LastIncludedTerm returns the term of the latest snapshot entry.
func (l *RaftLog) LastIncludedTerm() uint64 {
	return l.lastIncludedTerm
}

// LastIndex returns the 1-based index of the last entry in the log.
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) == 0 {
		return l.lastIncludedIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the term of the last entry in the log.
func (l *RaftLog) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return l.lastIncludedTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// toSliceIndex converts a global 1-based Raft log index to internal slice index.
func (l *RaftLog) toSliceIndex(index uint64) (int, error) {
	if index < l.lastIncludedIndex {
		return -1, ErrIndexCompacted
	}
	if index == l.lastIncludedIndex {
		return -1, nil // Refers to snapshot boundary
	}
	idx := int(index - l.lastIncludedIndex - 1)
	if idx >= len(l.entries) {
		return -1, ErrIndexOutOfRange
	}
	return idx, nil
}

// Term returns the term of the entry at the given index.
func (l *RaftLog) Term(index uint64) (uint64, error) {
	if index == 0 {
		return 0, nil
	}
	if index == l.lastIncludedIndex {
		return l.lastIncludedTerm, nil
	}
	idx, err := l.toSliceIndex(index)
	if err != nil {
		return 0, err
	}
	return l.entries[idx].Term, nil
}

// Entry returns a copy of the entry at index.
func (l *RaftLog) Entry(index uint64) (LogEntry, error) {
	idx, err := l.toSliceIndex(index)
	if err != nil {
		return LogEntry{}, err
	}
	if idx < 0 {
		return LogEntry{Index: l.lastIncludedIndex, Term: l.lastIncludedTerm}, nil
	}
	return l.entries[idx], nil
}

// EntriesFrom returns a slice of entries starting at fromIndex up to the end.
func (l *RaftLog) EntriesFrom(fromIndex uint64) ([]LogEntry, error) {
	if fromIndex > l.LastIndex()+1 {
		return nil, ErrIndexOutOfRange
	}
	if fromIndex <= l.lastIncludedIndex {
		return nil, ErrIndexCompacted
	}
	idx := int(fromIndex - l.lastIncludedIndex - 1)
	if idx < 0 {
		idx = 0
	}
	result := make([]LogEntry, len(l.entries[idx:]))
	copy(result, l.entries[idx:])
	return result, nil
}

// Append adds entries to the log.
func (l *RaftLog) Append(entries ...LogEntry) {
	for _, e := range entries {
		l.entries = append(l.entries, e)
	}
}

// Truncate discards all entries starting from fromIndex.
// If fromIndex > LastIndex(), no-op.
func (l *RaftLog) Truncate(fromIndex uint64) error {
	if fromIndex <= l.lastIncludedIndex {
		return fmt.Errorf("cannot truncate compacted log at index %d (snapshot at %d)", fromIndex, l.lastIncludedIndex)
	}
	if fromIndex > l.LastIndex() {
		return nil
	}
	idx := int(fromIndex - l.lastIncludedIndex - 1)
	l.entries = l.entries[:idx]
	return nil
}

// Compact discards entries up to lastIncludedIndex and saves the snapshot boundary.
func (l *RaftLog) Compact(lastIncludedIndex, lastIncludedTerm uint64) error {
	if lastIncludedIndex <= l.lastIncludedIndex {
		return nil // already compacted
	}
	if lastIncludedIndex > l.LastIndex() {
		// Log completely superseded by snapshot
		l.entries = nil
		l.lastIncludedIndex = lastIncludedIndex
		l.lastIncludedTerm = lastIncludedTerm
		return nil
	}

	idx := int(lastIncludedIndex - l.lastIncludedIndex - 1)
	retained := make([]LogEntry, len(l.entries[idx+1:]))
	copy(retained, l.entries[idx+1:])
	l.entries = retained
	l.lastIncludedIndex = lastIncludedIndex
	l.lastIncludedTerm = lastIncludedTerm
	return nil
}

// AllEntries returns a copy of all current uncompacted entries in the log (for WAL).
func (l *RaftLog) AllEntries() []LogEntry {
	res := make([]LogEntry, len(l.entries))
	copy(res, l.entries)
	return res
}

// TotalCount returns total entries count including compacted ones.
func (l *RaftLog) TotalCount() int {
	return int(l.lastIncludedIndex) + len(l.entries)
}
