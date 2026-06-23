package transport

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"netmesh/internal/logging"
	"netmesh/internal/protocol"
)

// portDetail renders a human-readable summary of a port-availability report.
func portDetail(ps protocol.PortStatus) string {
	d := "port " + strconv.Itoa(ps.Port) + " ("
	if ps.UDP {
		d += "udp ok, "
	} else {
		d += "udp -, "
	}
	if ps.TCP {
		d += "tcp ok)"
	} else {
		d += "tcp -)"
	}
	if ps.Err != "" {
		d += " — " + ps.Err
	}
	return d
}

// ErrAgentNotFound is returned when addressing an agent that is not connected.
var ErrAgentNotFound = errors.New("transport: agent not connected")

// agentUpgrader upgrades the /ws/agent endpoint. Agents are not browsers, so
// origin checks are not meaningful here; the endpoint may be additionally
// gated by a join token in a hardened deployment.
var agentUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(*http.Request) bool { return true },
}

// AgentInfo is a read-only snapshot of a connected agent for the UI/API.
type AgentInfo struct {
	ID         string    `json:"id"`
	Hostname   string    `json:"hostname"`
	RemoteAddr string    `json:"remoteAddr"`
	DataPort   int       `json:"dataPort"`
	JoinedAt   time.Time `json:"joinedAt"`
	LastSeq    uint64    `json:"lastSeq"`
	// Control-plane link health, measured from the app-layer heartbeat.
	WSRttMicros int64  `json:"wsRttUs"`
	FramesRx    uint64 `json:"framesRx"`
	FramesTx    uint64 `json:"framesTx"`
	// Last reported data-plane listen-port bind status (one per assigned port).
	Ports []protocol.PortStatus `json:"ports,omitempty"`
}

// Hub is the Controller-side registry of connected agents. It performs
// per-agent telemetry sequence tracking and emits lifecycle/sequence events.
type Hub struct {
	log       *logging.Logger
	joinToken string // when set, /ws/agent requires a matching ?token=

	mu      sync.RWMutex
	agents  map[string]*AgentConn
	lastSeq map[string]uint64

	// Sinks set by the Controller for downstream fan-out (UI store, etc.).
	onTelemetry func(agentID string, metrics []protocol.Metric, replay bool)
	onDiag      func(protocol.DiagChunk)
}

// NewHub constructs a Hub. joinToken, when non-empty, is a shared secret every
// agent must present on /ws/agent. The sinks may be nil.
func NewHub(log *logging.Logger, joinToken string,
	onTelemetry func(agentID string, metrics []protocol.Metric, replay bool),
	onDiag func(protocol.DiagChunk)) *Hub {
	return &Hub{
		log:         log,
		joinToken:   joinToken,
		agents:      make(map[string]*AgentConn),
		lastSeq:     make(map[string]uint64),
		onTelemetry: onTelemetry,
		onDiag:      onDiag,
	}
}

// ServeAgent is the http.Handler for /ws/agent. It upgrades the connection,
// drives its pumps, and unregisters the agent on disconnect.
func (h *Hub) ServeAgent(w http.ResponseWriter, r *http.Request) {
	// Optional shared-secret gate on the control plane. Rejected before the
	// websocket upgrade so an unauthenticated peer never reaches the pumps.
	if h.joinToken != "" {
		got := r.URL.Query().Get("token")
		if subtle.ConstantTimeCompare([]byte(got), []byte(h.joinToken)) != 1 {
			h.log.Emit(logging.Event{Type: logging.AuthRejected,
				Detail: "agent join: bad token", Fields: map[string]any{"remote": r.RemoteAddr}})
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
	}
	conn, err := agentUpgrader.Upgrade(w, r, nil)
	if err != nil {
		h.log.Warnf("transport: agent upgrade failed", "err", err, "remote", r.RemoteAddr)
		return
	}
	ac := &AgentConn{hub: h, remoteAddr: r.RemoteAddr, joinedAt: time.Now()}
	peer := NewPeer(conn, "", "controller", h.log, ac.onMessage)
	ac.peer = peer

	_ = peer.Run() // blocks until the connection closes

	if id := ac.agentID(); id != "" {
		h.unregister(ac)
	}
}

// register adds (or replaces) a connected agent and emits join events.
func (h *Hub) register(ac *AgentConn, reg protocol.Register) {
	id := reg.AgentID
	h.mu.Lock()
	if existing, ok := h.agents[id]; ok && existing != ac {
		// A reconnect beat our detection of the stale session: evict the old one.
		existing.peer.Close()
	}
	h.agents[id] = ac
	h.mu.Unlock()

	h.log.Emit(logging.Event{Type: logging.AgentJoined, AgentID: id, Detail: ac.remoteAddr})
	h.log.Emit(logging.Event{
		Type:    logging.AgentRegistered,
		AgentID: id,
		Fields:  map[string]any{"hostname": reg.Hostname, "version": reg.Version, "resumeSeq": reg.ResumeSeq},
	})
}

// unregister removes an agent if it is still the registered connection.
func (h *Hub) unregister(ac *AgentConn) {
	id := ac.agentID()
	h.mu.Lock()
	if cur, ok := h.agents[id]; ok && cur == ac {
		delete(h.agents, id)
	}
	h.mu.Unlock()
	h.log.Emit(logging.Event{Type: logging.AgentDropped, AgentID: id, Detail: causeString(ac.peer.Cause())})
}

// handleTelemetry advances the per-agent sequence watermark, emitting
// PACKET_SEQUENCE_MISSED for live gaps, and forwards metrics to the sink.
// Replayed (spool-flushed) batches advance the watermark but do not raise
// missed-sequence events, since they are filling a known outage.
func (h *Hub) handleTelemetry(agentID string, b protocol.TelemetryBatch) {
	if agentID == "" || len(b.Metrics) == 0 {
		return
	}
	if h.onTelemetry != nil {
		h.onTelemetry(agentID, b.Metrics, b.Replay)
	}

	type gap struct {
		afterSeq uint64
		seq      uint64
		missed   uint64
	}
	var gaps []gap

	// Detect gaps on a sorted, de-duplicated copy of this batch's sequence
	// numbers so that in-batch reordering and duplicate/retransmitted metrics
	// are not mistaken for loss. (Cross-batch reordering is already prevented on
	// the sender by flushing the spool ahead of live traffic.)
	seqs := make([]uint64, 0, len(b.Metrics))
	for _, m := range b.Metrics {
		if m.Seq != 0 {
			seqs = append(seqs, m.Seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })

	h.mu.Lock()
	last := h.lastSeq[agentID]
	for _, s := range seqs {
		if s <= last {
			continue // duplicate or late frame already accounted for
		}
		if !b.Replay && last != 0 && s > last+1 {
			gaps = append(gaps, gap{afterSeq: last, seq: s, missed: s - last - 1})
		}
		last = s
	}
	h.lastSeq[agentID] = last
	h.mu.Unlock()

	for _, g := range gaps {
		h.log.Emit(logging.Event{
			Type:    logging.PacketSequenceMissed,
			AgentID: agentID,
			Seq:     g.seq,
			Detail:  "telemetry sequence gap",
			Fields:  map[string]any{"afterSeq": g.afterSeq, "missed": g.missed},
		})
	}
}

// SendTo sends a control message to a single agent.
func (h *Hub) SendTo(agentID string, t protocol.MessageType, payload any) error {
	h.mu.RLock()
	ac := h.agents[agentID]
	h.mu.RUnlock()
	if ac == nil {
		return ErrAgentNotFound
	}
	return ac.peer.Send(t, payload)
}

// Broadcast sends a control message to every connected agent.
func (h *Hub) Broadcast(t protocol.MessageType, payload any) {
	h.mu.RLock()
	conns := make([]*AgentConn, 0, len(h.agents))
	for _, ac := range h.agents {
		conns = append(conns, ac)
	}
	h.mu.RUnlock()
	for _, ac := range conns {
		if err := ac.peer.Send(t, payload); err != nil {
			h.log.Warnf("transport: broadcast send failed", "agent", ac.agentID(), "err", err)
		}
	}
}

// Evict force-closes an agent's control-plane connection. The agent's reconnect
// loop will attempt to rejoin unless it has been disabled upstream.
func (h *Hub) Evict(agentID string) error {
	h.mu.RLock()
	ac := h.agents[agentID]
	h.mu.RUnlock()
	if ac == nil {
		return ErrAgentNotFound
	}
	ac.peer.Close()
	return nil
}

// Agents returns a snapshot of all connected agents for the UI/API.
func (h *Hub) Agents() []AgentInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]AgentInfo, 0, len(h.agents))
	for id, ac := range h.agents {
		ps := ac.portStatus()
		out = append(out, AgentInfo{
			ID:          id,
			Hostname:    ac.hostname(),
			RemoteAddr:  ac.remoteAddr,
			DataPort:    ac.dataPort(),
			JoinedAt:    ac.joinedAt,
			LastSeq:     h.lastSeq[id],
			WSRttMicros: ac.peer.RTTMicros(),
			FramesRx:    ac.peer.FramesRx(),
			FramesTx:    ac.peer.FramesTx(),
			Ports:       ps,
		})
	}
	return out
}

// Count returns the number of connected agents.
func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.agents)
}

// AgentConn is the Controller's handle on one connected agent.
type AgentConn struct {
	hub        *Hub
	peer       *Peer
	remoteAddr string
	joinedAt   time.Time

	mu    sync.RWMutex
	id    string
	host  string
	dport int
	ports []protocol.PortStatus
}

func (ac *AgentConn) agentID() string {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	return ac.id
}

func (ac *AgentConn) hostname() string {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	return ac.host
}

func (ac *AgentConn) dataPort() int {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	return ac.dport
}

func (ac *AgentConn) portStatus() []protocol.PortStatus {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	return ac.ports
}

// onMessage dispatches frames received from this agent.
func (ac *AgentConn) onMessage(env protocol.Envelope) {
	switch env.Type {
	case protocol.TypeRegister:
		var reg protocol.Register
		if err := env.DecodePayload(&reg); err != nil {
			ac.hub.log.Warnf("transport: bad REGISTER", "err", err)
			return
		}
		if reg.AgentID == "" {
			reg.AgentID = env.AgentID
		}
		ac.mu.Lock()
		ac.id = reg.AgentID
		ac.host = reg.Hostname
		ac.dport = reg.DataPort
		ac.mu.Unlock()

		ac.hub.register(ac, reg)
		_ = ac.peer.Send(protocol.TypeRegisterAck, protocol.RegisterAck{AgentID: reg.AgentID, Accepted: true})

	case protocol.TypeTelemetry:
		var b protocol.TelemetryBatch
		if err := env.DecodePayload(&b); err != nil {
			ac.hub.log.Warnf("transport: bad TELEMETRY", "err", err)
			return
		}
		ac.hub.handleTelemetry(ac.agentID(), b)

	case protocol.TypeDiagChunk:
		var ch protocol.DiagChunk
		if err := env.DecodePayload(&ch); err != nil {
			ac.hub.log.Warnf("transport: bad DIAG_CHUNK", "err", err)
			return
		}
		if ac.hub.onDiag != nil {
			ac.hub.onDiag(ch)
		}

	case protocol.TypePortStatus:
		var rep protocol.PortReport
		if err := env.DecodePayload(&rep); err != nil {
			ac.hub.log.Warnf("transport: bad PORT_STATUS", "err", err)
			return
		}
		ac.mu.Lock()
		ac.ports = rep.Ports
		ac.mu.Unlock()
		id := ac.agentID()
		for _, ps := range rep.Ports {
			if ps.UDP || ps.TCP {
				ac.hub.log.Emit(logging.Event{Type: logging.PortBound, AgentID: id,
					Detail: portDetail(ps), Fields: map[string]any{"port": ps.Port}})
			} else {
				ac.hub.log.Emit(logging.Event{Type: logging.PortUnavailable, AgentID: id,
					Detail: portDetail(ps), Fields: map[string]any{"port": ps.Port}})
			}
		}

	case protocol.TypeEvent:
		// Agent-sourced structured event: surface it on the controller bus.
		var ev logging.Event
		if err := env.DecodePayload(&ev); err == nil {
			ac.hub.log.Emit(ev)
		}

	default:
		ac.hub.log.Warnf("transport: unexpected frame from agent", "type", env.Type, "agent", ac.agentID())
	}
}
