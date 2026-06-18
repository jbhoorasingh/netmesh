package protocol

import (
	"encoding/json"
	"testing"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	in := RoutingTable{Epoch: 7, Peers: []Peer{{AgentID: "b", Address: "10.0.0.2:5999", Profiles: []Profile{TCP, UDPSymmetric}}}}
	env, err := NewEnvelope(TypeRouting, "ctrl", 42, 1700000000000, in)
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	if env.Seq != 42 || env.Type != TypeRouting {
		t.Errorf("envelope header wrong: %+v", env)
	}

	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Envelope
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var out RoutingTable
	if err := got.DecodePayload(&out); err != nil {
		t.Fatalf("DecodePayload: %v", err)
	}
	if out.Epoch != 7 || len(out.Peers) != 1 || out.Peers[0].AgentID != "b" {
		t.Errorf("roundtrip mismatch: %+v", out)
	}
}

func TestNilPayload(t *testing.T) {
	env, err := NewEnvelope(TypePing, "a", 1, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(env.Payload) != 0 {
		t.Errorf("ping payload = %q, want empty", env.Payload)
	}
	if err := env.DecodePayload(&struct{}{}); err == nil {
		t.Error("DecodePayload on empty should error")
	}
}

func TestProfileValid(t *testing.T) {
	for _, p := range AllProfiles {
		if !p.Valid() {
			t.Errorf("%s should be valid", p)
		}
	}
	if Profile("bogus").Valid() {
		t.Error("bogus profile should be invalid")
	}
}
