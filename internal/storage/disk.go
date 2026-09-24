package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"raft-kv/internal/raft"
)

type metaState struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"votedFor"`
}

type snapHeader struct {
	LastIncludedIndex uint64 `json:"lastIncludedIndex"`
	LastIncludedTerm  uint64 `json:"lastIncludedTerm"`
	DataLength        uint32 `json:"dataLength"`
}

// DiskStorage implements raft.Storage with WAL, checksums, atomic metadata, and snapshot files.
type DiskStorage struct {
	mu       sync.Mutex
	dir      string
	metaFile string
	walFile  string
	snapFile string
}

// NewDiskStorage initializes or opens an existing storage directory.
func NewDiskStorage(dir string) (*DiskStorage, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage dir: %w", err)
	}

	return &DiskStorage{
		dir:      dir,
		metaFile: filepath.Join(dir, "metadata.json"),
		walFile:  filepath.Join(dir, "wal.log"),
		snapFile: filepath.Join(dir, "snapshot.bin"),
	}, nil
}

// SaveState atomically saves currentTerm, votedFor, and rewritten log entries with CRC32 checksums.
func (d *DiskStorage) SaveState(term uint64, votedFor string, entries []raft.LogEntry) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 1. Write metadata atomically
	metaBytes, err := json.Marshal(metaState{
		Term:     term,
		VotedFor: votedFor,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	tmpMeta := d.metaFile + ".tmp"
	if err := os.WriteFile(tmpMeta, metaBytes, 0644); err != nil {
		return fmt.Errorf("failed to write temp metadata: %w", err)
	}
	fMeta, err := os.Open(tmpMeta)
	if err == nil {
		_ = fMeta.Sync()
		_ = fMeta.Close()
	}
	if err := os.Rename(tmpMeta, d.metaFile); err != nil {
		return fmt.Errorf("failed to rename metadata: %w", err)
	}

	// 2. Write WAL with CRC32 checksums per record
	tmpWAL := d.walFile + ".tmp"
	f, err := os.OpenFile(tmpWAL, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open temp wal: %w", err)
	}

	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			_ = f.Close()
			return fmt.Errorf("failed to marshal log entry: %w", err)
		}

		length := uint32(len(data))
		checksum := crc32.ChecksumIEEE(data)

		var header [8]byte
		binary.BigEndian.PutUint32(header[0:4], length)
		binary.BigEndian.PutUint32(header[4:8], checksum)

		if _, err := f.Write(header[:]); err != nil {
			_ = f.Close()
			return err
		}
		if _, err := f.Write(data); err != nil {
			_ = f.Close()
			return err
		}
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to fsync wal: %w", err)
	}
	_ = f.Close()

	if err := os.Rename(tmpWAL, d.walFile); err != nil {
		return fmt.Errorf("failed to rename wal: %w", err)
	}

	return nil
}

// LoadState reads metadata and WAL entries, repairing any torn writes at the tail.
func (d *DiskStorage) LoadState() (uint64, string, []raft.LogEntry, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var term uint64
	var votedFor string
	var entries []raft.LogEntry

	// Read metadata
	if metaBytes, err := os.ReadFile(d.metaFile); err == nil {
		var meta metaState
		if err := json.Unmarshal(metaBytes, &meta); err == nil {
			term = meta.Term
			votedFor = meta.VotedFor
		}
	}

	// Read WAL
	f, err := os.Open(d.walFile)
	if err != nil {
		if os.IsNotExist(err) {
			return term, votedFor, entries, nil
		}
		return 0, "", nil, err
	}
	defer f.Close()

	var validOffset int64 = 0
	var header [8]byte

	for {
		startOffset := validOffset
		n, err := io.ReadFull(f, header[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil || n < 8 {
			break
		}

		length := binary.BigEndian.Uint32(header[0:4])
		expectedChecksum := binary.BigEndian.Uint32(header[4:8])

		data := make([]byte, length)
		n, err = io.ReadFull(f, data)
		if err != nil || uint32(n) < length {
			// Torn write at tail detected! Truncate to startOffset
			_ = f.Close()
			_ = os.Truncate(d.walFile, startOffset)
			break
		}

		actualChecksum := crc32.ChecksumIEEE(data)
		if actualChecksum != expectedChecksum {
			// Checksum corruption detected! Truncate to startOffset
			_ = f.Close()
			_ = os.Truncate(d.walFile, startOffset)
			break
		}

		var entry raft.LogEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			_ = f.Close()
			_ = os.Truncate(d.walFile, startOffset)
			break
		}

		entries = append(entries, entry)
		validOffset += int64(8 + length)
	}

	return term, votedFor, entries, nil
}

// SaveSnapshot saves the snapshot with atomic file replacement.
func (d *DiskStorage) SaveSnapshot(snapshot []byte, lastIncludedIndex uint64, lastIncludedTerm uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	header := snapHeader{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		DataLength:        uint32(len(snapshot)),
	}

	hdrBytes, err := json.Marshal(header)
	if err != nil {
		return err
	}

	tmpSnap := d.snapFile + ".tmp"
	f, err := os.OpenFile(tmpSnap, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	var hdrLen [4]byte
	binary.BigEndian.PutUint32(hdrLen[:], uint32(len(hdrBytes)))

	if _, err := f.Write(hdrLen[:]); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(hdrBytes); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(snapshot); err != nil {
		_ = f.Close()
		return err
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	_ = f.Close()

	return os.Rename(tmpSnap, d.snapFile)
}

// LoadSnapshot restores the latest snapshot if present.
func (d *DiskStorage) LoadSnapshot() ([]byte, uint64, uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	f, err := os.Open(d.snapFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, 0, nil
		}
		return nil, 0, 0, err
	}
	defer f.Close()

	var hdrLen [4]byte
	if _, err := io.ReadFull(f, hdrLen[:]); err != nil {
		return nil, 0, 0, nil
	}

	length := binary.BigEndian.Uint32(hdrLen[:])
	hdrBytes := make([]byte, length)
	if _, err := io.ReadFull(f, hdrBytes); err != nil {
		return nil, 0, 0, nil
	}

	var header snapHeader
	if err := json.Unmarshal(hdrBytes, &header); err != nil {
		return nil, 0, 0, err
	}

	snapData := make([]byte, header.DataLength)
	if _, err := io.ReadFull(f, snapData); err != nil {
		return nil, 0, 0, err
	}

	return snapData, header.LastIncludedIndex, header.LastIncludedTerm, nil
}

// Close flushes and releases resources.
func (d *DiskStorage) Close() error {
	return nil
}
