// Package dataplane runs the Agent's traffic-generation engine. Given a routing
// table of peers and a test spec, it launches one goroutine per (peer, profile)
// pair, probes on the configured cadence, and submits Metrics to a sink (the
// transport client).
//
// Four distinct profiles are supported (see protocol.Profile):
//
//   - UDP Symmetric: source port is bound equal to the destination port to
//     exercise strict/symmetric firewall pinholes (e.g. SIP 5060->5060).
//   - UDP Dynamic:   ephemeral OS-assigned source port.
//   - TCP:           stateful connect + payload round trip.
//   - ICMP:          echo request (raw socket / native ping).
//
// All four profiles measure real round-trip time and packet loss against the
// peer-side Responder (UDP/TCP echo); ICMP uses the OS echo-reply path via
// unprivileged datagram sockets, so it needs no responder.
package dataplane

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"

	"netmesh/internal/logging"
	"netmesh/internal/protocol"
)

// MetricSink receives completed probe results (implemented by transport.Client).
type MetricSink interface {
	SubmitMetric(protocol.Metric)
}

// Prober executes a single probe against a target and returns a Metric.
type Prober interface {
	Probe(ctx context.Context, agentID, peerID, target string, spec protocol.TestSpec) protocol.Metric
}

// Engine owns the set of active probe loops for the current routing table.
type Engine struct {
	agentID string
	sink    MetricSink
	log     *logging.Logger
	probers map[protocol.Profile]Prober

	mu     sync.Mutex
	cancel context.CancelFunc // cancels the current run, if any
	wg     sync.WaitGroup
}

// NewEngine builds an Engine with the default prober set.
func NewEngine(agentID string, sink MetricSink, log *logging.Logger) *Engine {
	return &Engine{
		agentID: agentID,
		sink:    sink,
		log:     log,
		probers: map[protocol.Profile]Prober{
			protocol.TCP:          tcpProber{},
			protocol.UDPDynamic:   udpProber{symmetric: false},
			protocol.UDPSymmetric: udpProber{symmetric: true},
			protocol.ICMP:         icmpProber{},
		},
	}
}

// Start launches probe loops for every (peer, profile) in the routing table
// using the given spec. Any previously running test is stopped first.
func (e *Engine) Start(parent context.Context, table protocol.RoutingTable, spec protocol.TestSpec) {
	e.Stop()

	ctx, cancel := context.WithCancel(parent)
	e.mu.Lock()
	e.cancel = cancel
	e.mu.Unlock()

	interval := time.Duration(spec.IntervalMS) * time.Millisecond
	if interval <= 0 {
		interval = time.Second
	}

	for _, peer := range table.Peers {
		if peer.AgentID == e.agentID {
			continue // don't probe ourselves
		}
		for _, profile := range peer.Profiles {
			prober, ok := e.probers[profile]
			if !ok {
				e.log.Warnf("dataplane: no prober for profile", "profile", profile)
				continue
			}
			e.wg.Add(1)
			go e.loop(ctx, prober, peer, profile, spec, interval)
		}
	}
	e.log.Emit(logging.Event{
		Type:    logging.TestStarted,
		AgentID: e.agentID,
		Detail:  spec.RunID,
		Fields:  map[string]any{"peers": len(table.Peers), "intervalMs": spec.IntervalMS},
	})
}

// Stop cancels all running probe loops and waits for them to exit.
func (e *Engine) Stop() {
	e.mu.Lock()
	cancel := e.cancel
	e.cancel = nil
	e.mu.Unlock()
	if cancel != nil {
		cancel()
		e.wg.Wait()
		e.log.Emit(logging.Event{Type: logging.TestStopped, AgentID: e.agentID})
	}
}

func (e *Engine) loop(ctx context.Context, p Prober, peer protocol.Peer, profile protocol.Profile, spec protocol.TestSpec, interval time.Duration) {
	defer e.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sent := 0
	probe := func() {
		m := p.Probe(ctx, e.agentID, peer.AgentID, peer.Address, spec)
		m.Profile = profile
		e.sink.SubmitMetric(m)
		sent++
	}

	probe() // fire immediately rather than waiting a full interval
	for {
		if spec.Count > 0 && sent >= spec.Count {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			probe()
		}
	}
}

// --- Probers ---------------------------------------------------------------

const (
	probeTimeout = 1 * time.Second // window to collect a burst's echoes
	probeBurst   = 4               // packets per probe (for loss measurement)
	probeMagic   = 0x4E4D5044      // "NMPD"
	headerSize   = 20              // magic(4) | seq(8) | sendNanos(8)
)

// encodeProbe builds a probe packet of the given size (padded), carrying the
// sequence number and the send timestamp so the echo yields an RTT.
func encodeProbe(seq uint64, size int) []byte {
	if size < headerSize {
		size = headerSize
	}
	b := make([]byte, size)
	binary.BigEndian.PutUint32(b[0:], probeMagic)
	binary.BigEndian.PutUint64(b[4:], seq)
	binary.BigEndian.PutUint64(b[12:], uint64(time.Now().UnixNano()))
	return b
}

// decodeProbe validates the magic and extracts (seq, sendNanos).
func decodeProbe(b []byte) (seq uint64, sendNanos int64, ok bool) {
	if len(b) < headerSize || binary.BigEndian.Uint32(b) != probeMagic {
		return 0, 0, false
	}
	return binary.BigEndian.Uint64(b[4:]), int64(binary.BigEndian.Uint64(b[12:])), true
}

// burstResult aggregates a probe burst into a Metric's measurable fields.
type burstResult struct {
	received int
	rttSumNs int64
}

func (br burstResult) apply(m *protocol.Metric, sent int) {
	if br.received > 0 {
		m.Success = true
		m.RTTMicros = (br.rttSumNs / int64(br.received)) / 1000
	} else {
		m.Err = "no echo (all " + itoa(sent) + " probes lost)"
	}
	if sent > 0 {
		m.PacketLoss = float64(sent-br.received) / float64(sent) * 100
	}
}

func payloadSize(spec protocol.TestSpec, dflt int) int {
	if spec.PayloadSize >= headerSize {
		return spec.PayloadSize
	}
	return dflt
}

// tcpProber establishes a stateful TCP connection and measures a payload
// round trip against the peer's echo responder.
type tcpProber struct{}

func (tcpProber) Probe(ctx context.Context, agentID, peerID, target string, spec protocol.TestSpec) protocol.Metric {
	m := protocol.Metric{PeerID: peerID, Target: target, TS: time.Now().UnixMilli()}
	d := net.Dialer{Timeout: probeTimeout}
	conn, err := d.DialContext(ctx, "tcp", target)
	if err != nil {
		m.Err = err.Error()
		m.PacketLoss = 100
		return m
	}
	defer conn.Close()
	m.LocalAddr = conn.LocalAddr().String()
	m.RemoteAddr = conn.RemoteAddr().String()

	pkt := encodeProbe(0, payloadSize(spec, 64))
	_ = conn.SetDeadline(time.Now().Add(probeTimeout))
	start := time.Now()
	if _, err := conn.Write(pkt); err != nil {
		m.Err = "write: " + err.Error()
		m.PacketLoss = 100
		return m
	}
	echo := make([]byte, len(pkt))
	if _, err := io.ReadFull(conn, echo); err != nil {
		m.Err = "read: " + err.Error()
		m.PacketLoss = 100
		return m
	}
	m.Success = true
	m.RTTMicros = time.Since(start).Microseconds()
	return m
}

// udpProber sends a burst of UDP datagrams and measures echo RTT / loss. When
// symmetric is set it binds the local source port equal to the destination port
// (the distinguishing behaviour for strict/symmetric firewall profiles), using
// SO_REUSEPORT so it can coexist with the local responder.
type udpProber struct{ symmetric bool }

func (u udpProber) Probe(ctx context.Context, agentID, peerID, target string, spec protocol.TestSpec) protocol.Metric {
	m := protocol.Metric{PeerID: peerID, Target: target, TS: time.Now().UnixMilli()}

	raddr, err := net.ResolveUDPAddr("udp4", target)
	if err != nil {
		m.Err = "resolve: " + err.Error()
		m.PacketLoss = 100
		return m
	}
	m.RemoteAddr = raddr.String()

	lc := net.ListenConfig{}
	laddr := "0.0.0.0:0"
	if u.symmetric {
		// Bind the local source port equal to the destination port (the
		// distinguishing behaviour of the symmetric profile), with SO_REUSEPORT
		// so it can coexist with the local responder.
		laddr = "0.0.0.0:" + itoa(raddr.Port)
		lc.Control = reusePortControl
	}
	pc, err := lc.ListenPacket(ctx, "udp4", laddr)
	if err != nil {
		m.Err = "listen: " + err.Error()
		m.PacketLoss = 100
		return m
	}
	defer pc.Close()
	m.LocalAddr = pc.LocalAddr().String()

	// Wrap in an IPv4 packet conn to read the received hop limit (TTL).
	p4 := ipv4.NewPacketConn(pc)
	_ = p4.SetControlMessage(ipv4.FlagTTL, true)

	size := payloadSize(spec, 64)
	for seq := 0; seq < probeBurst; seq++ {
		if _, err := p4.WriteTo(encodeProbe(uint64(seq), size), nil, raddr); err != nil {
			m.Err = "write: " + err.Error()
			m.PacketLoss = 100
			return m
		}
	}

	_ = p4.SetReadDeadline(time.Now().Add(probeTimeout))
	seen := make(map[uint64]bool)
	var br burstResult
	buf := make([]byte, size+64)
	for br.received < probeBurst {
		n, cm, _, err := p4.ReadFrom(buf)
		if err != nil {
			break
		}
		seq, sendNs, ok := decodeProbe(buf[:n])
		if !ok || seen[seq] {
			continue
		}
		seen[seq] = true
		br.received++
		br.rttSumNs += time.Now().UnixNano() - sendNs
		if cm != nil && m.TTL == 0 {
			m.TTL = cm.TTL
		}
	}
	br.apply(&m, probeBurst)
	return m
}

// icmpProber issues ICMP echo requests via an unprivileged datagram socket
// ("udp4"), which works without root on macOS and on Linux with a permissive
// ping_group_range. The OS answers echo requests, so no responder is needed.
type icmpProber struct{}

func (icmpProber) Probe(ctx context.Context, agentID, peerID, target string, spec protocol.TestSpec) protocol.Metric {
	m := protocol.Metric{PeerID: peerID, Target: target, TS: time.Now().UnixMilli()}
	host := target
	if h, _, err := net.SplitHostPort(target); err == nil {
		host = h
	}
	m.Target = host

	ipAddr, err := net.ResolveIPAddr("ip4", host)
	if err != nil {
		m.Err = "resolve: " + err.Error()
		m.PacketLoss = 100
		return m
	}
	m.RemoteAddr = ipAddr.String()
	conn, err := icmp.ListenPacket("udp4", "0.0.0.0")
	if err != nil {
		m.Err = "icmp socket unavailable (needs privilege): " + err.Error()
		m.PacketLoss = 100
		return m
	}
	defer conn.Close()
	m.LocalAddr = conn.LocalAddr().String()
	p4 := conn.IPv4PacketConn()
	_ = p4.SetControlMessage(ipv4.FlagTTL, true)

	id := os.Getpid() & 0xffff
	dst := &net.UDPAddr{IP: ipAddr.IP}
	size := payloadSize(spec, 56)
	for seq := 0; seq < probeBurst; seq++ {
		wm := icmp.Message{Type: ipv4.ICMPTypeEcho, Code: 0,
			Body: &icmp.Echo{ID: id, Seq: seq, Data: encodeProbe(uint64(seq), size)}}
		wb, err := wm.Marshal(nil)
		if err != nil {
			m.Err = err.Error()
			m.PacketLoss = 100
			return m
		}
		if _, err := conn.WriteTo(wb, dst); err != nil {
			m.Err = "write: " + err.Error()
			m.PacketLoss = 100
			return m
		}
	}

	_ = p4.SetReadDeadline(time.Now().Add(probeTimeout))
	seen := make(map[int]bool)
	var br burstResult
	rb := make([]byte, 1500)
	for br.received < probeBurst {
		n, cm, _, err := p4.ReadFrom(rb)
		if err != nil {
			break
		}
		rm, err := icmp.ParseMessage(1, rb[:n]) // 1 = IANA protocol number for ICMPv4
		if err != nil || rm.Type != ipv4.ICMPTypeEchoReply {
			continue
		}
		echo, ok := rm.Body.(*icmp.Echo)
		if !ok || seen[echo.Seq] {
			continue
		}
		_, sendNs, ok := decodeProbe(echo.Data)
		if !ok {
			continue
		}
		seen[echo.Seq] = true
		br.received++
		br.rttSumNs += time.Now().UnixNano() - sendNs
		if cm != nil && m.TTL == 0 {
			m.TTL = cm.TTL
		}
	}
	br.apply(&m, probeBurst)
	return m
}
