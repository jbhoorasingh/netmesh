package controller

import (
	"path/filepath"
	"testing"

	"netmesh/internal/protocol"
)

func ptr[T any](v T) *T { return &v }

func TestAdminDefaults(t *testing.T) {
	s := newAdminStore("")
	c := s.get("unknown")
	if !c.Enabled || len(c.Profiles) != len(protocol.AllProfiles) {
		t.Errorf("default = %+v, want enabled + all profiles", c)
	}
}

func TestAdminPartialUpdate(t *testing.T) {
	s := newAdminStore("")
	s.update(AgentUpdate{AgentID: "a", Label: ptr("New York")})
	if got := s.get("a"); got.Label != "New York" || !got.Enabled {
		t.Errorf("after label update: %+v", got)
	}
	// Disabling must not wipe the label.
	s.update(AgentUpdate{AgentID: "a", Enabled: ptr(false)})
	if got := s.get("a"); got.Label != "New York" || got.Enabled {
		t.Errorf("after disable: %+v", got)
	}
	// Profile update drops invalid entries.
	s.update(AgentUpdate{AgentID: "a", Profiles: ptr([]protocol.Profile{protocol.TCP, "bogus", protocol.ICMP})})
	if got := s.get("a"); len(got.Profiles) != 2 {
		t.Errorf("profiles = %v, want [tcp icmp]", got.Profiles)
	}
}

func TestAdminPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admin.json")
	s1 := newAdminStore(path)
	s1.update(AgentUpdate{AgentID: "edge-1", Label: ptr("Frankfurt"), Group: ptr("eu"), Enabled: ptr(false)})

	s2 := newAdminStore(path) // reload from disk
	got := s2.get("edge-1")
	if got.Label != "Frankfurt" || got.Group != "eu" || got.Enabled {
		t.Errorf("reloaded = %+v, want Frankfurt/eu/disabled", got)
	}
}
