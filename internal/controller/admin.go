package controller

import (
	"encoding/json"
	"os"
	"sync"
)

// AgentConfig is operator-managed metadata for an agent: a friendly label, a
// group, and whether it participates in tests. Default: enabled.
type AgentConfig struct {
	Label   string `json:"label"`
	Group   string `json:"group"`
	Enabled bool   `json:"enabled"`
}

func defaultConfig() AgentConfig {
	return AgentConfig{Enabled: true}
}

// AgentUpdate is a partial update to an agent's config; nil fields are left
// unchanged.
type AgentUpdate struct {
	AgentID string  `json:"agentId"`
	Label   *string `json:"label"`
	Group   *string `json:"group"`
	Enabled *bool   `json:"enabled"`
}

// adminStore holds per-agent operator config, persisted best-effort to a JSON
// file so renames and test config survive a controller restart.
type adminStore struct {
	mu   sync.RWMutex
	cfg  map[string]AgentConfig
	path string
}

func newAdminStore(path string) *adminStore {
	s := &adminStore{cfg: make(map[string]AgentConfig), path: path}
	s.load()
	return s
}

func (s *adminStore) load() {
	if s.path == "" {
		return
	}
	b, err := os.ReadFile(s.path)
	if err != nil {
		return
	}
	var m map[string]AgentConfig
	if json.Unmarshal(b, &m) == nil && m != nil {
		s.cfg = m
	}
}

func (s *adminStore) save() {
	if s.path == "" {
		return
	}
	s.mu.RLock()
	b, _ := json.MarshalIndent(s.cfg, "", "  ")
	s.mu.RUnlock()
	_ = os.WriteFile(s.path, b, 0o644)
}

// get returns the config for an agent, falling back to defaults.
func (s *adminStore) get(id string) AgentConfig {
	s.mu.RLock()
	c, ok := s.cfg[id]
	s.mu.RUnlock()
	if !ok {
		return defaultConfig()
	}
	return c
}

// update applies a partial change and persists it.
func (s *adminStore) update(u AgentUpdate) AgentConfig {
	s.mu.Lock()
	c, ok := s.cfg[u.AgentID]
	if !ok {
		c = defaultConfig()
	}
	if u.Label != nil {
		c.Label = *u.Label
	}
	if u.Group != nil {
		c.Group = *u.Group
	}
	if u.Enabled != nil {
		c.Enabled = *u.Enabled
	}
	s.cfg[u.AgentID] = c
	s.mu.Unlock()
	s.save()
	return c
}
