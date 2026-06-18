package transport

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"netmesh/internal/logging"
	"netmesh/internal/protocol"
	"netmesh/internal/spooler"
)

// Reconnect backoff parameters. The loop starts at backoffInitial, multiplies
// by backoffFactor on each failed/short attempt, and never exceeds backoffMax.
const (
	backoffInitial  = 1 * time.Second
	backoffMax      = 30 * time.Second
	backoffFactor   = 2.0
	backoffJitter   = 0.2              // ±20%
	stableThreshold = 30 * time.Second // uptime after which backoff resets to initial

	handshakeTimeout = 10 * time.Second
	telemetryFlushMS = 250 // micro-batch interval for live telemetry
	maxBatch         = 200 // metrics per TELEMETRY envelope
	intakeBuffer     = 2048
)

// Sender is the minimal interface a control handler uses to reply to the
// Controller. *Peer satisfies it.
type Sender interface {
	Send(protocol.MessageType, any) error
}

// ControlHandler processes a Controller->Agent control envelope (routing, test
// start/stop, diagnostics) and may reply via out. It runs on the read pump
// goroutine, so long-running work (e.g. spawning a diagnostic) must be
// dispatched to its own goroutine by the handler.
type ControlHandler func(env protocol.Envelope, out Sender)

// Client is the Agent-side resilient control-plane client. It maintains a
// single logical connection to the Controller across TCP failures, buffering
// telemetry in a spooler while disconnected and flushing on reconnect.
type Client struct {
	agentID  string
	hostname string
	version  string
	log      *logging.Logger
	spool    *spooler.Spooler
	handler  ControlHandler

	masterMu   sync.RWMutex
	masterAddr string // host:port

	peerMu sync.RWMutex
	peer   *Peer

	telemetrySeq atomic.Uint64 // per-agent telemetry sequence (survives reconnects)
	telemetryIn  chan protocol.Metric

	joinToken string // presented to the controller on /ws/agent when set
	dataPort  int    // advertised so peers target the right echo port
}

// NewClient builds an Agent client. masterAddr may be empty for holding mode;
// set it later with SetMaster before the connection loop can make progress.
// joinToken, when non-empty, is presented to the controller to authenticate the
// agent on the control plane. dataPort is advertised to peers for probing.
func NewClient(agentID, hostname, version, masterAddr, joinToken string, dataPort int, log *logging.Logger, spool *spooler.Spooler, handler ControlHandler) *Client {
	return &Client{
		agentID:     agentID,
		hostname:    hostname,
		version:     version,
		log:         log,
		spool:       spool,
		handler:     handler,
		masterAddr:  masterAddr,
		joinToken:   joinToken,
		dataPort:    dataPort,
		telemetryIn: make(chan protocol.Metric, intakeBuffer),
	}
}

// SetMaster updates the controller address (used by holding mode once the
// operator supplies the Master IP via the local UI). It forces the current
// connection (if any) to drop so the reconnect loop targets the new address.
func (c *Client) SetMaster(addr string) {
	c.masterMu.Lock()
	c.masterAddr = addr
	c.masterMu.Unlock()
	if p := c.currentPeer(); p != nil {
		p.Close()
	}
}

func (c *Client) master() string {
	c.masterMu.RLock()
	defer c.masterMu.RUnlock()
	return c.masterAddr
}

// SubmitMetric stamps a metric with the agent's identity and next telemetry
// sequence number, then queues it for transmission/spooling. It never blocks:
// if the intake queue is saturated the metric falls straight into the spooler.
func (c *Client) SubmitMetric(m protocol.Metric) {
	m.AgentID = c.agentID
	m.Seq = c.telemetrySeq.Add(1)
	if m.TS == 0 {
		m.TS = time.Now().UnixMilli()
	}
	select {
	case c.telemetryIn <- m:
	default:
		c.spool.Push(m)
	}
}

func (c *Client) currentPeer() *Peer {
	c.peerMu.RLock()
	defer c.peerMu.RUnlock()
	return c.peer
}

// Status is a snapshot of the agent client's control-plane link health, used by
// the local UI.
type Status struct {
	Connected    bool   `json:"connected"`
	Master       string `json:"master"`
	RTTMicros    int64  `json:"rttUs"`
	FramesRx     uint64 `json:"framesRx"`
	FramesTx     uint64 `json:"framesTx"`
	TelemetrySeq uint64 `json:"telemetrySeq"`
	SpoolLen     int    `json:"spoolLen"`
}

// Status returns the current link health snapshot.
func (c *Client) Status() Status {
	st := Status{Master: c.master(), TelemetrySeq: c.telemetrySeq.Load(), SpoolLen: c.spool.Len()}
	if p := c.currentPeer(); p != nil {
		st.Connected = true
		st.RTTMicros = p.RTTMicros()
		st.FramesRx = p.FramesRx()
		st.FramesTx = p.FramesTx()
	}
	return st
}

func (c *Client) setPeer(p *Peer) {
	c.peerMu.Lock()
	c.peer = p
	c.peerMu.Unlock()
}

// Run drives the reconnect loop and the telemetry batcher until ctx is
// cancelled.
func (c *Client) Run(ctx context.Context) {
	go c.telemetryLoop(ctx)

	backoff := backoffInitial
	for {
		if ctx.Err() != nil {
			return
		}
		if c.master() == "" {
			// Holding state: nothing to dial yet. Wait for SetMaster.
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				continue
			}
		}

		start := time.Now()
		c.connectOnce(ctx)
		uptime := time.Since(start)

		if ctx.Err() != nil {
			return
		}
		if uptime >= stableThreshold {
			backoff = backoffInitial // the link was healthy; reset
		}

		wait := jittered(backoff)
		c.log.Emit(logging.Event{
			Type:    logging.WSReconnecting,
			AgentID: c.agentID,
			Detail:  fmt.Sprintf("reconnecting in %s", wait.Round(time.Millisecond)),
		})
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		backoff = nextBackoff(backoff)
	}
}

// connectOnce performs a single dial+register+serve cycle, blocking until the
// connection ends.
func (c *Client) connectOnce(ctx context.Context) {
	addr := c.master()
	u := url.URL{Scheme: "ws", Host: addr, Path: "/ws/agent"}
	q := u.Query()
	q.Set("id", c.agentID)
	if c.joinToken != "" {
		q.Set("token", c.joinToken)
	}
	u.RawQuery = q.Encode()

	dialer := websocket.Dialer{HandshakeTimeout: handshakeTimeout}
	conn, resp, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		status := ""
		if resp != nil {
			status = resp.Status
			resp.Body.Close() // honour the http contract on the failure path
		}
		c.log.Emit(logging.Event{
			Type:    logging.WSDisconnected,
			AgentID: c.agentID,
			Detail:  fmt.Sprintf("dial %s failed: %v %s", u.String(), err, status),
		})
		return
	}

	// If the master was changed mid-dial (SetMaster), drop this stale connection
	// so the reconnect loop targets the new address immediately.
	if c.master() != addr {
		conn.Close()
		return
	}

	peer := NewPeer(conn, c.agentID, "agent:"+c.agentID, c.log, nil)
	peer.onMessage = func(env protocol.Envelope) { c.onControl(env, peer) }

	// Enqueue REGISTER, then replay the spool, BEFORE exposing the peer to the
	// live telemetry path. Because writePump (not yet started) drains the send
	// queue strictly FIFO, this guarantees the controller receives REGISTER and
	// all replayed (lower-seq) metrics ahead of any live (higher-seq) batch —
	// the spec's in-order flush, with no spurious sequence-gap events.
	if err := peer.Send(protocol.TypeRegister, protocol.Register{
		AgentID:   c.agentID,
		Hostname:  c.hostname,
		Version:   c.version,
		DataPort:  c.dataPort,
		ResumeSeq: c.telemetrySeq.Load(),
	}); err != nil {
		peer.Close()
		return
	}
	c.flushSpool(peer)

	c.setPeer(peer)
	defer c.setPeer(nil)
	c.log.Emit(logging.Event{Type: logging.WSConnected, AgentID: c.agentID, Detail: addr})

	_ = peer.Run() // blocks until the connection closes
	c.log.Emit(logging.Event{
		Type:    logging.WSDisconnected,
		AgentID: c.agentID,
		Detail:  causeString(peer.Cause()),
	})
}

// onControl handles Controller->Agent frames. REGISTER_ACK is consumed here;
// everything else is delegated to the configured ControlHandler.
func (c *Client) onControl(env protocol.Envelope, out Sender) {
	switch env.Type {
	case protocol.TypeRegisterAck:
		var ack protocol.RegisterAck
		if err := env.DecodePayload(&ack); err == nil && !ack.Accepted {
			c.log.Warnf("transport: registration rejected", "reason", ack.Reason)
		}
	default:
		if c.handler != nil {
			c.handler(env, out)
		}
	}
}

// telemetryLoop micro-batches live metrics and routes them to the active peer,
// or to the spooler when disconnected.
func (c *Client) telemetryLoop(ctx context.Context) {
	ticker := time.NewTicker(telemetryFlushMS * time.Millisecond)
	defer ticker.Stop()

	batch := make([]protocol.Metric, 0, maxBatch)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		peer := c.currentPeer()
		if peer == nil {
			for _, m := range batch {
				c.spool.Push(m)
			}
			c.log.Emit(logging.Event{
				Type:    logging.TelemetrySpooled,
				AgentID: c.agentID,
				Fields:  map[string]any{"count": len(batch), "spoolLen": c.spool.Len()},
			})
			batch = batch[:0]
			return
		}
		if err := peer.Send(protocol.TypeTelemetry, protocol.TelemetryBatch{Metrics: batch}); err != nil {
			for _, m := range batch { // send raced a disconnect; preserve in spool
				c.spool.Push(m)
			}
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			return
		case m := <-c.telemetryIn:
			batch = append(batch, m)
			if len(batch) >= maxBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// flushSpool replays buffered metrics to a freshly connected peer in
// FIFO-ordered chunks, marked as replay so the Controller reconciles rather
// than double-counts.
func (c *Client) flushSpool(peer *Peer) {
	metrics := c.spool.Drain()
	if len(metrics) == 0 {
		return
	}
	sent := 0
	for start := 0; start < len(metrics); start += maxBatch {
		end := start + maxBatch
		if end > len(metrics) {
			end = len(metrics)
		}
		chunk := metrics[start:end]
		if err := peer.Send(protocol.TypeTelemetry, protocol.TelemetryBatch{Metrics: chunk, Replay: true}); err != nil {
			// Connection died mid-flush: re-spool the remainder for next time.
			for _, m := range metrics[start:] {
				c.spool.Push(m)
			}
			return
		}
		sent += len(chunk)
	}
	c.log.Emit(logging.Event{
		Type:    logging.TelemetryFlushed,
		AgentID: c.agentID,
		Fields:  map[string]any{"count": sent},
	})
}

// --- backoff helpers --------------------------------------------------------

func nextBackoff(d time.Duration) time.Duration {
	n := time.Duration(float64(d) * backoffFactor)
	if n > backoffMax {
		n = backoffMax
	}
	return n
}

// jittered applies ±backoffJitter to spread reconnects across agents (avoiding a
// thundering herd), then clamps so the effective wait never exceeds the 30s cap.
// math/rand/v2 is per-call, lock-free and auto-seeded on Go 1.22+.
func jittered(d time.Duration) time.Duration {
	delta := (rand.Float64()*2 - 1) * backoffJitter
	wait := time.Duration(float64(d) * (1 + delta))
	if wait > backoffMax {
		wait = backoffMax
	}
	return wait
}

func causeString(err error) string {
	if err == nil {
		return "clean close"
	}
	return err.Error()
}
