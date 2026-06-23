// Package uistream provides the shared browser-facing UI primitives used by
// both the Controller and the Agent: the message envelope pushed over /ws/ui,
// a non-blocking fan-out Hub, and a small latest-metric Store for the grid.
package uistream

import (
	"sync"

	"netmesh/internal/logging"
	"netmesh/internal/protocol"
)

// Message is the JSON envelope pushed to browser clients over /ws/ui.
type Message struct {
	Kind    string              `json:"kind"` // "event" | "telemetry" | "diag"
	Event   *logging.Event      `json:"event,omitempty"`
	Metrics []protocol.Metric   `json:"metrics,omitempty"`
	Replay  bool                `json:"replay,omitempty"`
	Diag    *protocol.DiagChunk `json:"diag,omitempty"`
}

// Client is a single connected browser session.
type Client struct {
	Ch         chan Message
	Privileged bool // eligible to receive privileged (diagnostic) output
}

// Hub fans messages out to connected browser clients without blocking on slow
// consumers.
type Hub struct {
	mu      sync.RWMutex
	clients map[*Client]struct{}
}

// NewHub returns an empty Hub.
func NewHub() *Hub { return &Hub{clients: make(map[*Client]struct{})} }

// Add registers a new client session.
func (h *Hub) Add(privileged bool) *Client {
	cl := &Client{Ch: make(chan Message, 256), Privileged: privileged}
	h.mu.Lock()
	h.clients[cl] = struct{}{}
	h.mu.Unlock()
	return cl
}

// Remove deregisters a client and closes its channel (idempotent).
func (h *Hub) Remove(cl *Client) {
	h.mu.Lock()
	if _, ok := h.clients[cl]; ok {
		delete(h.clients, cl)
		close(cl.Ch)
	}
	h.mu.Unlock()
}

// Broadcast fans msg out to all clients. When onlyPrivileged is set,
// non-privileged sessions are skipped (used for diagnostic output on a secured
// controller).
func (h *Hub) Broadcast(msg Message, onlyPrivileged bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for cl := range h.clients {
		if onlyPrivileged && !cl.Privileged {
			continue
		}
		select {
		case cl.Ch <- msg:
		default: // slow UI client: drop rather than stall the control plane
		}
	}
}

// Store keeps the latest metric for each flow so the UI grid can render current
// mesh state without replaying history. The key includes FlowID so two flows
// between the same (agent, peer, profile) — e.g. UDP to different dst ports —
// occupy distinct grid rows instead of collapsing into one.
type Store struct {
	mu     sync.RWMutex
	latest map[string]protocol.Metric
}

// NewStore returns an empty Store.
func NewStore() *Store { return &Store{latest: make(map[string]protocol.Metric)} }

func key(m protocol.Metric) string {
	return m.AgentID + "|" + m.PeerID + "|" + string(m.Profile) + "|" + m.FlowID
}

// Ingest records each metric as the latest for its tuple (newest Seq wins; a
// zero Seq always overwrites, since locally-produced metrics may be unsequenced).
func (s *Store) Ingest(metrics []protocol.Metric) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range metrics {
		k := key(m)
		if prev, ok := s.latest[k]; ok && m.Seq != 0 && prev.Seq > m.Seq {
			continue
		}
		s.latest[k] = m
	}
}

// Clear drops all latest metrics. A new test run starts from a clean view so
// stale metrics from deleted or protocol-changed flows do not affect summaries.
func (s *Store) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latest = make(map[string]protocol.Metric)
}

// Snapshot returns the current latest-metric set.
func (s *Store) Snapshot() []protocol.Metric {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]protocol.Metric, 0, len(s.latest))
	for _, m := range s.latest {
		out = append(out, m)
	}
	return out
}
