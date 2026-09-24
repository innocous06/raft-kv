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

// syncDir attempts to flush directory metadata changes to disk.
// Note on OS differences:
// On POSIX file systems (ext4, xfs, etc.), fsync on a directory file descriptor
// guarantees directory entry durability after rename.
// On Windows, directory handles cannot be flushed via standard user-mode sync
// (FlushFileBuffers fails on directory handles), so directory fsync is best-effort
// and a no-op on Windows.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return nil
	}
	defer d.Close()
	_ = d.Sync()
	return nil
}

// SaveState atomically saves currentTerm, votedFor, and rewritten log entries with CRC32 checksums.
// Note on durability and ordering:
// Metadata and WAL files are each fsynced and replaced atomically via rename.
// The sequence updates metadata first (currentTerm/votedFor), fsyncs the parent directory,
// then the WAL, and fsyncs the parent directory again.
// The two files are replaced sequentially; the pair is not jointly atomic across power loss.
func (d *DiskStorage) SaveState(term uint64, votedFor string, entries []raft.LogEntry) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	metaBytes, err := json.Marshal(metaState{
		Term:     term,
		VotedFor: votedFor,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	tmpMeta := d.metaFile + ".tmp"
	fMeta, err := os.OpenFile(tmpMeta, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open temp metadata: %w", err)
	}
	if _, err := fMeta.Write(metaBytes); err != nil {
		_ = fMeta.Close()
		return fmt.Errorf("failed to write temp metadata: %w", err)
	}
	if err := fMeta.Sync(); err != nil {
		_ = fMeta.Close()
		return fmt.Errorf("failed to sync temp metadata: %w", err)
	}
	if err := fMeta.Close(); err != nil {
		return fmt.Errorf("failed to close temp metadata: %w", err)
	}
	if err := os.Rename(tmpMeta, d.metaFile); err != nil {
		return fmt.Errorf("failed to rename metadata: %w", err)
	}
	_ = syncDir(d.dir)

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
	_ = syncDir(d.dir)

	return nil
}

// LoadState reads metadata and WAL entries, repairing any torn writes at the tail.
func (d *DiskStorage) LoadState() (uint64, string, []raft.LogEntry, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var term uint64
	var votedFor string
	var entries []raft.LogEntry

	if metaBytes, err := os.ReadFile(d.metaFile); err == nil {
		var meta metaState
		if err := json.Unmarshal(metaBytes, &meta); err == nil {
			term = meta.Term
			votedFor = meta.VotedFor
		}
	}

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
		if err == io.EOF {
			break
		}
		if err != nil || n < 8 {
			_ = f.Close()
			_ = os.Truncate(d.walFile, startOffset)
			break
		}

		length := binary.BigEndian.Uint32(header[0:4])
		expectedChecksum := binary.BigEndian.Uint32(header[4:8])
		if length > 32*1024*1024 {
			_ = f.Close()
			_ = os.Truncate(d.walFile, startOffset)
			break
		}

		data := make([]byte, length)
		n, err = io.ReadFull(f, data)
		if err != nil || uint32(n) < length {
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

	if err := os.Rename(tmpSnap, d.snapFile); err != nil {
		return err
	}
	_ = syncDir(d.dir)
	return nil
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
	if length > 16*1024*1024 {
		return nil, 0, 0, fmt.Errorf("corrupt snapshot header length: %d", length)
	}
	hdrBytes := make([]byte, length)
	if _, err := io.ReadFull(f, hdrBytes); err != nil {
		return nil, 0, 0, nil
	}

	var header snapHeader
	if err := json.Unmarshal(hdrBytes, &header); err != nil {
		return nil, 0, 0, err
	}

	if header.DataLength > 256*1024*1024 {
		return nil, 0, 0, fmt.Errorf("corrupt snapshot data length: %d", header.DataLength)
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
