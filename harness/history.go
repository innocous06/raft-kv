package harness

import (
	"sync"
	"time"
)

// ClientOp represents a recorded client operation with timing and return values.
type ClientOp struct {
	ID       int       `json:"id"`
	ClientID string    `json:"clientId"`
	Type     string    `json:"type"` // "Put", "Get", "Delete"
	Key      string    `json:"key"`
	Value    string    `json:"value"`  // For Put: value written. For Get: value returned.
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
	Success  bool      `json:"success"`
	Err      string    `json:"err,omitempty"`
}

// HistoryRecorder records client operations concurrently.
type HistoryRecorder struct {
	mu  sync.Mutex
	ops []ClientOp
}

// NewHistoryRecorder creates an empty history recorder.
func NewHistoryRecorder() *HistoryRecorder {
	return &HistoryRecorder{
		ops: make([]ClientOp, 0),
	}
}

// Record appends a completed client operation.
func (h *HistoryRecorder) Record(op ClientOp) {
	h.mu.Lock()
	defer h.mu.Unlock()
	op.ID = len(h.ops) + 1
	h.ops = append(h.ops, op)
}

// Ops returns a copy of all recorded operations.
func (h *HistoryRecorder) Ops() []ClientOp {
	h.mu.Lock()
	defer h.mu.Unlock()
	copied := make([]ClientOp, len(h.ops))
	copy(copied, h.ops)
	return copied
}

// Clear resets recorded history.
func (h *HistoryRecorder) Clear() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ops = h.ops[:0]
}
