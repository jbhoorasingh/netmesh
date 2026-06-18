// Package logging provides structured JSON event logging for NetMesh together
// with an in-process event bus. Every significant state transition (agents
// joining/dropping, tests starting, sequence gaps, websocket reconnects) is
// emitted both to the process log (slog JSON) and to any subscribers — the
// Controller subscribes the UI websocket fan-out to this bus.
package logging

import (
	"log/slog"
	"os"
	"sync"
	"time"
)

// EventType is a stable identifier for a structured system event.
type EventType string

const (
	ControllerStarted    EventType = "CONTROLLER_STARTED"
	AgentStarted         EventType = "AGENT_STARTED"
	AgentJoined          EventType = "AGENT_JOINED"
	AgentRegistered      EventType = "AGENT_REGISTERED"
	AgentDropped         EventType = "AGENT_DROPPED"
	AgentConfigUpdated   EventType = "AGENT_CONFIG_UPDATED"
	AgentEvicted         EventType = "AGENT_EVICTED"
	WSConnected          EventType = "WS_CONNECTED"
	WSDisconnected       EventType = "WS_DISCONNECTED"
	WSReconnecting       EventType = "WS_RECONNECTING"
	HeartbeatTimeout     EventType = "HEARTBEAT_TIMEOUT"
	TestStarted          EventType = "TEST_STARTED"
	TestStopped          EventType = "TEST_STOPPED"
	RoutingUpdated       EventType = "ROUTING_UPDATED"
	MeshSummary          EventType = "MESH_SUMMARY"
	FlowDegraded         EventType = "FLOW_DEGRADED"
	FlowCritical         EventType = "FLOW_CRITICAL"
	FlowRecovered        EventType = "FLOW_RECOVERED"
	PacketSequenceMissed EventType = "PACKET_SEQUENCE_MISSED"
	TelemetrySpooled     EventType = "TELEMETRY_SPOOLED"
	TelemetryFlushed     EventType = "TELEMETRY_FLUSHED"
	DiagRequested        EventType = "DIAG_REQUESTED"
	DiagRejected         EventType = "DIAG_REJECTED"
	DiagCompleted        EventType = "DIAG_COMPLETED"
	AuthRejected         EventType = "AUTH_REJECTED"
)

// Event is a structured, JSON-serialisable system event.
type Event struct {
	Type    EventType      `json:"event"`
	Time    time.Time      `json:"time"`
	AgentID string         `json:"agentId,omitempty"`
	PeerID  string         `json:"peerId,omitempty"`
	Seq     uint64         `json:"seq,omitempty"`
	Detail  string         `json:"detail,omitempty"`
	Fields  map[string]any `json:"fields,omitempty"`
}

// Logger emits Events to the process log and to subscribers of its bus.
type Logger struct {
	slog *slog.Logger
	bus  *EventBus
}

// New returns a Logger writing newline-delimited JSON to stderr. The component
// label is attached to every record.
func New(component string) *Logger {
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	return &Logger{
		slog: slog.New(h).With("component", component),
		bus:  NewEventBus(),
	}
}

// Bus exposes the event bus so callers (the Controller UI fan-out) can
// subscribe to the live event stream.
func (l *Logger) Bus() *EventBus { return l.bus }

// Emit timestamps the event, logs it as structured JSON, and publishes it to
// the bus. Time is filled in if the caller left it zero.
func (l *Logger) Emit(e Event) {
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	attrs := []any{"event", string(e.Type)}
	if e.AgentID != "" {
		attrs = append(attrs, "agentId", e.AgentID)
	}
	if e.PeerID != "" {
		attrs = append(attrs, "peerId", e.PeerID)
	}
	if e.Seq != 0 {
		attrs = append(attrs, "seq", e.Seq)
	}
	if e.Detail != "" {
		attrs = append(attrs, "detail", e.Detail)
	}
	for k, v := range e.Fields {
		attrs = append(attrs, k, v)
	}
	l.slog.Info("event", attrs...)
	l.bus.Publish(e)
}

// Infof / Errorf provide non-event structured logging for incidental messages.
func (l *Logger) Infof(msg string, args ...any)  { l.slog.Info(msg, args...) }
func (l *Logger) Errorf(msg string, args ...any) { l.slog.Error(msg, args...) }
func (l *Logger) Warnf(msg string, args ...any)  { l.slog.Warn(msg, args...) }

// EventBus is a simple non-blocking fan-out of Events to subscriber channels.
// A slow subscriber drops events rather than stalling the publisher — the
// control plane must never block on a sluggish UI client.
type EventBus struct {
	mu   sync.RWMutex
	next int
	subs map[int]chan Event
}

// NewEventBus constructs an empty bus.
func NewEventBus() *EventBus {
	return &EventBus{subs: make(map[int]chan Event)}
}

// Subscribe returns a buffered channel of events and an unsubscribe function.
func (b *EventBus) Subscribe(buffer int) (<-chan Event, func()) {
	if buffer <= 0 {
		buffer = 64
	}
	ch := make(chan Event, buffer)
	b.mu.Lock()
	id := b.next
	b.next++
	b.subs[id] = ch
	b.mu.Unlock()

	return ch, func() {
		b.mu.Lock()
		if c, ok := b.subs[id]; ok {
			delete(b.subs, id)
			close(c)
		}
		b.mu.Unlock()
	}
}

// Publish fans out e to every subscriber without blocking.
func (b *EventBus) Publish(e Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.subs {
		select {
		case ch <- e:
		default: // subscriber lagging; drop rather than stall the publisher
		}
	}
}
