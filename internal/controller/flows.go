package controller

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"sync"

	"netmesh/internal/protocol"
)

// Flow is an operator-defined traffic item: srcAgent:srcPort --proto--> dstAgent:dstPort.
// srcPort 0 means a dynamic (ephemeral) source port; ports are ignored for ICMP.
type Flow struct {
	ID       string           `json:"id"`
	Name     string           `json:"name"`
	SrcAgent string           `json:"srcAgent"`
	SrcPort  int              `json:"srcPort"`
	Protocol protocol.Profile `json:"protocol"`
	DstAgent string           `json:"dstAgent"`
	DstPort  int              `json:"dstPort"`
	Enabled  bool             `json:"enabled"`
}

// flowStore holds the operator's traffic flows, persisted best-effort to JSON so
// they survive a controller restart.
type flowStore struct {
	mu    sync.Mutex
	flows []Flow
	path  string
}

func newFlowStore(path string) *flowStore {
	s := &flowStore{path: path}
	s.load()
	return s
}

func flowID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "flow"
	}
	return hex.EncodeToString(b[:])
}

func (s *flowStore) load() {
	if s.path == "" {
		return
	}
	b, err := os.ReadFile(s.path)
	if err != nil {
		return
	}
	var f []Flow
	if json.Unmarshal(b, &f) == nil {
		s.flows = f
	}
}

// saveLocked persists the current flows. The caller must hold s.mu so the
// marshal+write happens inside the same critical section as the mutation —
// otherwise two concurrent mutators could write their files in an order that
// disagrees with the final in-memory state.
func (s *flowStore) saveLocked() {
	if s.path == "" {
		return
	}
	b, _ := json.MarshalIndent(s.flows, "", "  ")
	_ = os.WriteFile(s.path, b, 0o644)
}

// all returns a copy of the current flows.
func (s *flowStore) all() []Flow {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Flow, len(s.flows))
	copy(out, s.flows)
	return out
}

// upsert adds a new flow (assigning an ID) or replaces an existing one by ID.
func (s *flowStore) upsert(f Flow) Flow {
	s.mu.Lock()
	if f.ID == "" {
		f.ID = flowID()
		s.flows = append(s.flows, f)
	} else {
		replaced := false
		for i := range s.flows {
			if s.flows[i].ID == f.ID {
				s.flows[i] = f
				replaced = true
				break
			}
		}
		if !replaced {
			s.flows = append(s.flows, f)
		}
	}
	s.saveLocked()
	s.mu.Unlock()
	return f
}

// remove deletes a flow by ID.
func (s *flowStore) remove(id string) {
	s.mu.Lock()
	out := s.flows[:0]
	for _, f := range s.flows {
		if f.ID != id {
			out = append(out, f)
		}
	}
	s.flows = out
	s.saveLocked()
	s.mu.Unlock()
}

// add appends a flow (always new), used by the mesh generator.
func (s *flowStore) add(f Flow) {
	s.mu.Lock()
	f.ID = flowID()
	s.flows = append(s.flows, f)
	s.saveLocked()
	s.mu.Unlock()
}

// clear removes all flows.
func (s *flowStore) clear() {
	s.mu.Lock()
	s.flows = nil
	s.saveLocked()
	s.mu.Unlock()
}
