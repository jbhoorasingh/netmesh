// Package protocol defines the wire contract shared by the NetMesh Controller
// and Agents. Every control-plane message is a JSON Envelope carrying a
// monotonically increasing per-sender sequence number so the receiver can
// detect dropped, duplicated, or out-of-order messages across the mesh.
//
// The protocol is deliberately transport-agnostic: it is currently carried
// over a gorilla/websocket connection (see internal/transport) but nothing in
// this package imports the transport.
package protocol

import (
	"encoding/json"
	"fmt"
)

// MessageType identifies the kind of payload an Envelope carries.
type MessageType string

const (
	// Control plane: agent -> controller.
	TypeRegister   MessageType = "REGISTER"  // agent announces itself on (re)connect
	TypeTelemetry  MessageType = "TELEMETRY" // batch of probe metrics
	TypeDiagChunk  MessageType = "DIAG_CHUNK"
	TypeEvent      MessageType = "EVENT"       // agent-sourced structured event
	TypePortStatus MessageType = "PORT_STATUS" // agent reports test-port bind availability

	// Control plane: controller -> agent.
	TypeRegisterAck MessageType = "REGISTER_ACK"
	TypeFlowPlan    MessageType = "FLOW_PLAN" // listen ports + flows this agent generates
	TypeTestStart   MessageType = "TEST_START"
	TypeTestStop    MessageType = "TEST_STOP"
	TypeDiagRequest MessageType = "DIAG_REQUEST"

	// Heartbeat: both directions. These are application-layer (JSON) frames,
	// distinct from the websocket control-frame ping/pong, so that a silently
	// half-open TCP session is detected by the app even when the kernel still
	// believes the socket is alive.
	TypePing MessageType = "PING"
	TypePong MessageType = "PONG"
)

// Envelope is the single framing structure for all control-plane traffic.
type Envelope struct {
	Type    MessageType     `json:"type"`
	Seq     uint64          `json:"seq"`     // per-sender monotonic sequence
	AgentID string          `json:"agentId"` // origin agent ("" for controller)
	SentMS  int64           `json:"sentMs"`  // unix milliseconds at send time
	Payload json.RawMessage `json:"payload,omitempty"`
}

// DecodePayload unmarshals the envelope payload into v.
func (e Envelope) DecodePayload(v any) error {
	if len(e.Payload) == 0 {
		return fmt.Errorf("protocol: empty payload for %s", e.Type)
	}
	return json.Unmarshal(e.Payload, v)
}

// NewEnvelope builds an Envelope with its payload marshalled from v. A nil v
// produces an envelope with no payload (used for PING/PONG and ack frames).
func NewEnvelope(t MessageType, agentID string, seq uint64, sentMS int64, v any) (Envelope, error) {
	env := Envelope{Type: t, Seq: seq, AgentID: agentID, SentMS: sentMS}
	if v != nil {
		raw, err := json.Marshal(v)
		if err != nil {
			return Envelope{}, fmt.Errorf("protocol: marshal %s payload: %w", t, err)
		}
		env.Payload = raw
	}
	return env, nil
}

// --- Control payloads -------------------------------------------------------

// Register is sent by an agent immediately after each successful connect.
type Register struct {
	AgentID  string `json:"agentId"`
	Hostname string `json:"hostname"`
	Version  string `json:"version"`
	// DataPort is the agent's data-plane echo port; the controller advertises it
	// to peers in the routing table so probes target the right port.
	DataPort int `json:"dataPort"`
	// ResumeSeq is the last telemetry sequence the agent had emitted before the
	// connection dropped. The controller uses it to reconcile its own view and
	// to know whether spooled metrics are about to be replayed.
	ResumeSeq uint64 `json:"resumeSeq"`
}

// RegisterAck confirms registration and hands the agent its authoritative ID.
type RegisterAck struct {
	AgentID  string `json:"agentId"`
	Accepted bool   `json:"accepted"`
	Reason   string `json:"reason,omitempty"`
}

// ListenPort is a data-plane port an agent's responder must bind (it is the
// destination of one or more flows). The master computes these per agent.
type ListenPort struct {
	Port int  `json:"port"`
	UDP  bool `json:"udp"`
	TCP  bool `json:"tcp"`
}

// AgentFlow is a single traffic flow an agent originates, with the destination
// already resolved to an address by the master.
type AgentFlow struct {
	ID       string  `json:"id"`
	SrcPort  int     `json:"srcPort"` // 0 = dynamic / ephemeral source port
	Protocol Profile `json:"protocol"`
	DstAgent string  `json:"dstAgent"`
	DstAddr  string  `json:"dstAddr"` // resolved ip:port (ip only for icmp)
	DstPort  int     `json:"dstPort"`
}

// FlowPlan is the controller's per-agent instruction: which ports to bind for
// the responder, and which flows to generate.
type FlowPlan struct {
	Epoch       uint64       `json:"epoch"`
	ListenPorts []ListenPort `json:"listenPorts"`
	Flows       []AgentFlow  `json:"flows"`
}

// TestSpec parameterises an active test run (cadence/payload).
type TestSpec struct {
	RunID       string `json:"runId"`
	IntervalMS  int64  `json:"intervalMs"`  // probe cadence per flow
	PayloadSize int    `json:"payloadSize"` // bytes
	Count       int    `json:"count"`       // 0 == run until TestStop
}

// PortStatus reports whether one data-plane port bound on an agent.
type PortStatus struct {
	Port int    `json:"port"`
	UDP  bool   `json:"udp"`
	TCP  bool   `json:"tcp"`
	Err  string `json:"err,omitempty"`
}

// PortReport is an agent's batch report of its responder port bindings — the
// master's port-availability validation.
type PortReport struct {
	AgentID string       `json:"agentId"`
	Ports   []PortStatus `json:"ports"`
}

// DiagRequest is a Controller-initiated diagnostic command. Only whitelisted
// commands are honoured by the agent (see internal/diag).
type DiagRequest struct {
	RequestID string   `json:"requestId"`
	Command   string   `json:"command"` // e.g. "ping", "traceroute"
	Args      []string `json:"args"`
}

// DiagChunk streams a slice of diagnostic output back to the controller.
type DiagChunk struct {
	RequestID string `json:"requestId"`
	Stream    string `json:"stream"` // "stdout" | "stderr" | "meta"
	Data      string `json:"data"`
	EOF       bool   `json:"eof"`
	ExitCode  int    `json:"exitCode,omitempty"`
	Err       string `json:"err,omitempty"`
}
