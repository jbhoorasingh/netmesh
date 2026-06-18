// Package transport implements the resilient WebSocket control plane that
// connects Agents to the Controller. It provides:
//
//   - Peer: the shared Read/Write Pump used by both ends of a connection,
//     including the application-layer PING/PONG heartbeat.
//   - Client: the Agent-side resilient client (exponential-backoff reconnect,
//     registration, and telemetry spool flush).
//   - Hub + ServeAgent: the Controller-side connection registry and upgrade
//     handler.
package transport

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"netmesh/internal/logging"
	"netmesh/internal/protocol"
)

// Heartbeat / write tuning. These satisfy the spec's "10s interval, 15s
// timeout" application-layer heartbeat: each side emits a PING every
// pingPeriod, and tears the connection down if no frame of any kind arrives
// within pongWait — which detects a silently dropped (half-open) TCP session.
const (
	writeWait       = 10 * time.Second // max time to flush a single frame
	pingPeriod      = 10 * time.Second // app-layer PING cadence
	pongWait        = 15 * time.Second // read deadline; refreshed on every frame
	sendBuffer      = 256              // outbound frame queue depth
	maxMessageBytes = 1 << 20          // 1 MiB read limit (bounds diag/telemetry batches)
)

var (
	errPeerClosed = errors.New("transport: peer closed")
)

// Peer wraps a single websocket connection with the Read/Write Pump pattern.
//
// Concurrency contract (required by gorilla/websocket): exactly one goroutine
// writes to the connection (writePump) and exactly one reads (readPump). All
// other goroutines enqueue outbound frames via Send, which hands them to
// writePump through a channel. PONG replies are likewise enqueued, never
// written directly.
type Peer struct {
	conn    *websocket.Conn
	localID string // AgentID stamped on outgoing envelopes ("" for controller)
	label   string // human label for logs, e.g. "agent:host-3" / "controller"
	log     *logging.Logger

	send chan []byte
	done chan struct{}
	seq  atomic.Uint64 // per-connection outbound frame counter

	// Live link health, measured from the application-layer heartbeat.
	lastPingNS atomic.Int64  // unix-nanos when our most recent PING was sent
	rttMicros  atomic.Int64  // last measured PING->PONG round-trip (microseconds)
	framesRx   atomic.Uint64 // frames read from the peer
	framesTx   atomic.Uint64 // frames written to the peer

	closeOnce sync.Once
	causeMu   sync.Mutex
	cause     error

	// onMessage handles non-heartbeat envelopes. It runs on the readPump
	// goroutine and must not block for long.
	onMessage func(protocol.Envelope)
}

// NewPeer wraps an established connection. onMessage may be nil.
func NewPeer(conn *websocket.Conn, localID, label string, log *logging.Logger, onMessage func(protocol.Envelope)) *Peer {
	return &Peer{
		conn:      conn,
		localID:   localID,
		label:     label,
		log:       log,
		send:      make(chan []byte, sendBuffer),
		done:      make(chan struct{}),
		onMessage: onMessage,
	}
}

// Run starts both pumps and blocks until the connection ends, returning the
// cause (nil for a clean local close).
func (p *Peer) Run() error {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.writePump()
	}()

	p.readPump() // blocks until a read error or external Close
	p.shutdown(nil)
	wg.Wait()
	return p.Cause()
}

// Send builds an envelope for the given payload, stamps it with this peer's
// next sequence number and the current time, and enqueues it for the writePump.
// It blocks until the frame is queued or the peer closes (bounded by writeWait
// because a stalled write trips its deadline and closes the peer).
func (p *Peer) Send(t protocol.MessageType, payload any) error {
	env, err := protocol.NewEnvelope(t, p.localID, p.seq.Add(1), time.Now().UnixMilli(), payload)
	if err != nil {
		return err
	}
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	select {
	case <-p.done:
		return errPeerClosed
	case p.send <- b:
		return nil
	}
}

// Close initiates a graceful shutdown of the peer.
func (p *Peer) Close() { p.shutdown(nil) }

// Done returns a channel closed when the peer is shutting down.
func (p *Peer) Done() <-chan struct{} { return p.done }

// Cause returns the error that ended the connection (nil for a clean close).
func (p *Peer) Cause() error {
	p.causeMu.Lock()
	defer p.causeMu.Unlock()
	return p.cause
}

func (p *Peer) shutdown(cause error) {
	p.closeOnce.Do(func() {
		if cause != nil {
			p.causeMu.Lock()
			p.cause = cause
			p.causeMu.Unlock()
		}
		close(p.done)
	})
}

// readPump is the sole reader. The read deadline is reset after every frame;
// because the remote emits a PING every pingPeriod (< pongWait), a healthy
// link always refreshes the deadline, while a dead link trips it.
func (p *Peer) readPump() {
	p.conn.SetReadLimit(maxMessageBytes)
	_ = p.conn.SetReadDeadline(time.Now().Add(pongWait))

	for {
		_, data, err := p.conn.ReadMessage()
		if err != nil {
			p.shutdown(err)
			return
		}
		_ = p.conn.SetReadDeadline(time.Now().Add(pongWait))
		p.framesRx.Add(1)

		var env protocol.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			p.log.Warnf("transport: dropping malformed frame", "label", p.label, "err", err)
			continue
		}

		switch env.Type {
		case protocol.TypePing:
			// Reply via the writePump (never write from the read goroutine).
			if err := p.Send(protocol.TypePong, nil); err != nil {
				return // peer is closing
			}
		case protocol.TypePong:
			// A PONG answers our most recent PING: record the round-trip.
			if sent := p.lastPingNS.Load(); sent != 0 {
				p.rttMicros.Store((time.Now().UnixNano() - sent) / 1000)
			}
		default:
			if p.onMessage != nil {
				p.onMessage(env)
			}
		}
	}
}

// writePump is the sole writer. It drains the send queue and emits an
// application-layer PING every pingPeriod. It owns closing the connection so
// that readPump unblocks on shutdown.
func (p *Peer) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	defer p.conn.Close() // unblocks readPump

	// Fire one heartbeat immediately so link RTT is measured within a round-trip
	// of connecting, rather than after a full ping period.
	if err := p.sendPing(); err != nil {
		p.shutdown(err)
		return
	}

	for {
		select {
		case <-p.done:
			// Best-effort graceful close frame.
			_ = p.conn.SetWriteDeadline(time.Now().Add(writeWait))
			_ = p.conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			return

		case b := <-p.send:
			if err := p.writeFrame(b); err != nil {
				p.shutdown(err)
				return
			}

		case <-ticker.C:
			if err := p.sendPing(); err != nil {
				p.shutdown(err)
				return
			}
		}
	}
}

// sendPing stamps the send time (so the matching PONG yields an RTT) and writes
// an application-layer PING frame.
func (p *Peer) sendPing() error {
	ping, err := p.pingFrame()
	if err != nil {
		return err
	}
	p.lastPingNS.Store(time.Now().UnixNano())
	return p.writeFrame(ping)
}

func (p *Peer) writeFrame(b []byte) error {
	_ = p.conn.SetWriteDeadline(time.Now().Add(writeWait))
	if err := p.conn.WriteMessage(websocket.TextMessage, b); err != nil {
		return err
	}
	p.framesTx.Add(1)
	return nil
}

// RTTMicros returns the most recent heartbeat round-trip in microseconds (0
// until the first PONG arrives).
func (p *Peer) RTTMicros() int64 { return p.rttMicros.Load() }

// FramesRx / FramesTx return the lifetime frame counts on this connection.
func (p *Peer) FramesRx() uint64 { return p.framesRx.Load() }
func (p *Peer) FramesTx() uint64 { return p.framesTx.Load() }

func (p *Peer) pingFrame() ([]byte, error) {
	env, err := protocol.NewEnvelope(protocol.TypePing, p.localID, p.seq.Add(1), time.Now().UnixMilli(), nil)
	if err != nil {
		return nil, err
	}
	return json.Marshal(env)
}
