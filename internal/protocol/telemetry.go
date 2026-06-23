package protocol

// Profile is the L4 protocol a flow uses. (The source/destination ports are
// carried by the flow itself; "symmetric" is simply a UDP flow whose source
// port equals its destination port.) The string values are stable wire
// identifiers — do not rename.
type Profile string

const (
	// UDP performs a UDP datagram echo round trip.
	UDP Profile = "udp"
	// TCP performs a stateful connect + payload round trip.
	TCP Profile = "tcp"
	// ICMP performs an echo request (port-less).
	ICMP Profile = "icmp"
)

// AllProfiles is the canonical protocol ordering used by the UI.
var AllProfiles = []Profile{UDP, TCP, ICMP}

// Valid reports whether p is a recognised protocol.
func (p Profile) Valid() bool {
	switch p {
	case UDP, TCP, ICMP:
		return true
	default:
		return false
	}
}

// HasPorts reports whether the protocol uses ports (UDP/TCP do, ICMP does not).
func (p Profile) HasPorts() bool { return p == UDP || p == TCP }

// Metric is a single probe result. Metrics are produced by the data-plane
// engine, carry a per-agent monotonic Seq, and are batched into TELEMETRY
// envelopes. The controller tracks Seq continuity per agent to surface
// PACKET_SEQUENCE_MISSED events.
type Metric struct {
	Seq     uint64  `json:"seq"`              // per-agent telemetry sequence (gap == loss)
	AgentID string  `json:"agentId"`          // origin agent
	PeerID  string  `json:"peerId"`           // target agent
	FlowID  string  `json:"flowId,omitempty"` // the traffic flow this probe belongs to
	Profile Profile `json:"profile"`          // L4 protocol (udp/tcp/icmp)
	Target  string  `json:"target"`           // resolved host:port actually probed
	TS      int64   `json:"ts"`               // unix milliseconds when the probe completed

	Success    bool    `json:"success"`
	RTTMicros  int64   `json:"rttUs"`   // round-trip time in microseconds (0 on failure)
	PacketLoss float64 `json:"lossPct"` // 0..100 for multi-packet probes

	// IP-header / socket detail observed for the probe.
	TTL        int    `json:"ttl,omitempty"`        // received hop limit (ICMP/UDP echo reply)
	LocalAddr  string `json:"localAddr,omitempty"`  // observed source ip:port
	RemoteAddr string `json:"remoteAddr,omitempty"` // observed destination ip:port

	Err string `json:"err,omitempty"`
}

// TelemetryBatch is the payload of a TELEMETRY envelope: an ordered run of
// metrics, oldest first. Batches may be produced live or replayed from the
// spooler after a reconnect; Replay flags the latter so the controller can
// reconcile rather than double-count.
type TelemetryBatch struct {
	Metrics []Metric `json:"metrics"`
	Replay  bool     `json:"replay"`
}
