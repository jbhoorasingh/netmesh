package protocol

// Profile enumerates the distinct traffic profiles the data-plane engine can
// generate. The string values are stable wire identifiers — do not rename.
type Profile string

const (
	// UDPSymmetric binds the local source port equal to the destination port
	// (e.g. 5060 -> 5060) to exercise strict/symmetric firewall rules such as
	// SIP/RTP pinholes.
	UDPSymmetric Profile = "udp_symmetric"
	// UDPDynamic uses an ephemeral OS-assigned source port.
	UDPDynamic Profile = "udp_dynamic"
	// TCP performs a stateful connect + payload round trip.
	TCP Profile = "tcp"
	// ICMP performs an echo request (native ping / raw socket).
	ICMP Profile = "icmp"
)

// AllProfiles is the canonical ordering used by the UI grid.
var AllProfiles = []Profile{UDPSymmetric, UDPDynamic, TCP, ICMP}

// Valid reports whether p is a recognised profile.
func (p Profile) Valid() bool {
	switch p {
	case UDPSymmetric, UDPDynamic, TCP, ICMP:
		return true
	default:
		return false
	}
}

// Metric is a single probe result. Metrics are produced by the data-plane
// engine, carry a per-agent monotonic Seq, and are batched into TELEMETRY
// envelopes. The controller tracks Seq continuity per agent to surface
// PACKET_SEQUENCE_MISSED events.
type Metric struct {
	Seq     uint64  `json:"seq"`     // per-agent telemetry sequence (gap == loss)
	AgentID string  `json:"agentId"` // origin agent
	PeerID  string  `json:"peerId"`  // target agent
	Profile Profile `json:"profile"`
	Target  string  `json:"target"` // resolved host:port actually probed
	TS      int64   `json:"ts"`     // unix milliseconds when the probe completed

	Success    bool    `json:"success"`
	RTTMicros  int64   `json:"rttUs"`   // round-trip time in microseconds (0 on failure)
	PacketLoss float64 `json:"lossPct"` // 0..100 for multi-packet probes
	TTL        int     `json:"ttl,omitempty"`
	Err        string  `json:"err,omitempty"`
}

// TelemetryBatch is the payload of a TELEMETRY envelope: an ordered run of
// metrics, oldest first. Batches may be produced live or replayed from the
// spooler after a reconnect; Replay flags the latter so the controller can
// reconcile rather than double-count.
type TelemetryBatch struct {
	Metrics []Metric `json:"metrics"`
	Replay  bool     `json:"replay"`
}
