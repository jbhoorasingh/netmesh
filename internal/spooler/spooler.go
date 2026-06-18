// Package spooler implements a bounded, in-memory ring buffer for telemetry
// metrics. When the agent's control-plane connection drops, freshly produced
// metrics accumulate here (oldest discarded once full) and are flushed to the
// Controller on the next successful reconnect.
package spooler

import (
	"sync"

	"netmesh/internal/protocol"
)

// DefaultCapacity is the number of metrics retained while disconnected.
const DefaultCapacity = 1000

// Spooler is a fixed-capacity FIFO ring of metrics, safe for concurrent use.
// Once full, Push overwrites the oldest entry and increments OverflowDropped so
// the loss is observable rather than silent.
type Spooler struct {
	mu       sync.Mutex
	buf      []protocol.Metric
	head     int // index of oldest element
	size     int // number of valid elements
	overflow uint64
}

// New returns a spooler with the given capacity (DefaultCapacity if <= 0).
func New(capacity int) *Spooler {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	return &Spooler{buf: make([]protocol.Metric, capacity)}
}

// Push appends a metric. If the buffer is full the oldest metric is discarded
// and OverflowDropped is incremented.
func (s *Spooler) Push(m protocol.Metric) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tail := (s.head + s.size) % len(s.buf)
	if s.size == len(s.buf) {
		// Full: overwrite oldest and advance head.
		s.buf[tail] = m
		s.head = (s.head + 1) % len(s.buf)
		s.overflow++
		return
	}
	s.buf[tail] = m
	s.size++
}

// Drain returns all buffered metrics in FIFO order and empties the buffer.
// Returns nil when empty.
func (s *Spooler) Drain() []protocol.Metric {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.size == 0 {
		return nil
	}
	out := make([]protocol.Metric, s.size)
	for i := 0; i < s.size; i++ {
		out[i] = s.buf[(s.head+i)%len(s.buf)]
	}
	s.head = 0
	s.size = 0
	return out
}

// Len reports the number of currently buffered metrics.
func (s *Spooler) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// Cap reports the configured capacity.
func (s *Spooler) Cap() int { return len(s.buf) }

// OverflowDropped reports how many metrics were discarded due to overflow over
// the spooler's lifetime.
func (s *Spooler) OverflowDropped() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.overflow
}
