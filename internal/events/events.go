package events

import (
	"sync"
	"time"
)

type EventType string

const (
	RoleChanged       EventType = "RoleChanged"
	TermChanged       EventType = "TermChanged"
	VoteRequested     EventType = "VoteRequested"
	VoteGranted       EventType = "VoteGranted"
	VoteRejected      EventType = "VoteRejected"
	ElectionWon       EventType = "ElectionWon"
	EntryAppended     EventType = "EntryAppended"
	EntryCommitted    EventType = "EntryCommitted"
	EntryApplied      EventType = "EntryApplied"
	HeartbeatSent     EventType = "HeartbeatSent"
	HeartbeatReceived EventType = "HeartbeatReceived"
	SnapshotSaved     EventType = "SnapshotSaved"
	SnapshotInstalled EventType = "SnapshotInstalled"
	MessageSent       EventType = "MessageSent"
	MessageReceived   EventType = "MessageReceived"
	MessageDropped    EventType = "MessageDropped"
	NodeCrashed       EventType = "NodeCrashed"
	NodeRestarted     EventType = "NodeRestarted"
	PartitionCreated  EventType = "PartitionCreated"
	PartitionHealed   EventType = "PartitionHealed"
	ClientRequest     EventType = "ClientRequest"
	ClientResponse    EventType = "ClientResponse"
	StorageFatal      EventType = "StorageFatal"
	SnapshotError     EventType = "SnapshotError"
)

// Event represents a structured event emitted by any node or cluster component.
type Event struct {
	ID        int64     `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	NodeID    string    `json:"nodeId"`
	Type      EventType `json:"type"`
	Role      string    `json:"role,omitempty"`
	Term      uint64    `json:"term,omitempty"`
	Details   string    `json:"details"`
	Data      any       `json:"data,omitempty"`
}

// Bus is a thread-safe event bus that broadcasts events to listeners.
type Bus struct {
	mu          sync.RWMutex
	subscribers map[chan Event]struct{}
	history     []Event
	maxHistory  int
	counter     int64
}

// NewBus creates an event bus retaining up to maxHistory recent events.
func NewBus(maxHistory int) *Bus {
	if maxHistory <= 0 {
		maxHistory = 1000
	}
	return &Bus{
		subscribers: make(map[chan Event]struct{}),
		history:     make([]Event, 0, maxHistory),
		maxHistory:  maxHistory,
	}
}

// Global default event bus
var DefaultBus = NewBus(2000)

// Publish emits an event to all subscribers and appends it to history.
func (b *Bus) Publish(e Event) {
	b.mu.Lock()
	b.counter++
	e.ID = b.counter
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now()
	}

	if len(b.history) >= b.maxHistory {
		b.history = b.history[1:]
	}
	b.history = append(b.history, e)

	// Send to subscribers non-blockingly
	for ch := range b.subscribers {
		select {
		case ch <- e:
		default:
			// Subscriber buffer full; drop to prevent stalling
		}
	}
	b.mu.Unlock()
}

// Subscribe returns a channel receiving newly published events.
func (b *Bus) Subscribe(bufSize int) chan Event {
	if bufSize <= 0 {
		bufSize = 100
	}
	ch := make(chan Event, bufSize)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subscribers[ch] = struct{}{}
	return ch
}

// Unsubscribe removes a channel from subscribers.
func (b *Bus) Unsubscribe(ch chan Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.subscribers[ch]; ok {
		delete(b.subscribers, ch)
		close(ch)
	}
}

// History returns a copy of recently recorded events.
func (b *Bus) History() []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()
	copied := make([]Event, len(b.history))
	copy(copied, b.history)
	return copied
}

// Emit is a convenience helper for node event reporting.
func (b *Bus) Emit(nodeID string, eventType EventType, role string, term uint64, details string, data any) {
	b.Publish(Event{
		NodeID:  nodeID,
		Type:    eventType,
		Role:    role,
		Term:    term,
		Details: details,
		Data:    data,
	})
}
