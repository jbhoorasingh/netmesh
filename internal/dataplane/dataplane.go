// Package dataplane runs the Agent's traffic-generation engine. Given a set of
// flows (an Ixia-style traffic plan: srcPort -proto-> dstAddr:dstPort) and a
// test spec, it launches one goroutine per flow, probes on the configured
// cadence, and submits Metrics to a sink (the transport client).
//
// Protocols (see protocol.Profile): UDP and TCP measure a real round trip
// against the peer-side Responder; ICMP uses the OS echo-reply path via
// unprivileged datagram sockets, so it needs no responder. A flow's source port
// may be a specific number (e.g. 5060 -> 5060 for symmetric/SIP) or 0 for an
// ephemeral source port.
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

// Prober executes a single probe for a flow and returns a Metric.
type Prober interface {
	Probe(ctx context.Context, agentID string, flow protocol.AgentFlow, spec protocol.TestSpec) protocol.Metric
}

// Engine owns the set of active per-flow probe loops.
type Engine struct {
	agentID string
	sink    MetricSink
	log     *logging.Logger
	probers map[protocol.Profile]Prober

	mu     sync.Mutex
	cancel context.CancelFunc // cancels the current run, if any
	wg     sync.WaitGroup
}

// NewEngine builds an Engine with the default prober set (one per protocol).
func NewEngine(agentID string, sink MetricSink, log *logging.Logger) *Engine {
	return &Engine{
		agentID: agentID,
		sink:    sink,
		log:     log,
		probers: map[protocol.Profile]Prober{
			protocol.UDP:  udpProber{},
			protocol.TCP:  tcpProber{},
			protocol.ICMP: icmpProber{},
		},
	}
}

// Start launches one probe loop per flow using the given spec. Any previously
// running test is stopped first.
func (e *Engine) Start(parent context.Context, flows []protocol.AgentFlow, spec protocol.TestSpec) {
	e.Stop()

	ctx, cancel := context.WithCancel(parent)
	e.mu.Lock()
	e.cancel = cancel
	e.mu.Unlock()

	interval := time.Duration(spec.IntervalMS) * time.Millisecond
	if interval <= 0 {
		interval = time.Second
	}

	for _, flow := range flows {
		prober, ok := e.probers[flow.Protocol]
		if !ok {
			e.log.Warnf("dataplane: no prober for protocol", "protocol", flow.Protocol)
			continue
		}
		e.wg.Add(1)
		go e.loop(ctx, prober, flow, spec, interval)
	}
	e.log.Emit(logging.Event{
		Type:    logging.TestStarted,
		AgentID: e.agentID,
		Detail:  spec.RunID,
		Fields:  map[string]any{"flows": len(flows), "intervalMs": spec.IntervalMS},
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

func (e *Engine) loop(ctx context.Context, p Prober, flow protocol.AgentFlow, spec protocol.TestSpec, interval time.Duration) {
	defer e.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sent := 0
	probe := func() {
		m := p.Probe(ctx, e.agentID, flow, spec)
		m.FlowID = flow.ID
		m.PeerID = flow.DstAgent
		m.Profile = flow.Protocol
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

// addrIP extracts the IP from a net.Addr returned by an ICMP/UDP read.
func addrIP(a net.Addr) net.IP {
	switch v := a.(type) {
	case *net.UDPAddr:
		return v.IP
	case *net.IPAddr:
		return v.IP
	}
	return nil
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

func (tcpProber) Probe(ctx context.Context, agentID string, flow protocol.AgentFlow, spec protocol.TestSpec) protocol.Metric {
	m := protocol.Metric{Target: flow.DstAddr, TS: time.Now().UnixMilli()}
	d := net.Dialer{Timeout: probeTimeout}
	if flow.SrcPort > 0 {
		d.LocalAddr = &net.TCPAddr{Port: flow.SrcPort}
		d.Control = reusePortControl
	}
	conn, err := d.DialContext(ctx, "tcp", flow.DstAddr)
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

// udpProber sends a burst of UDP datagrams and measures echo RTT / loss. A
// non-zero source port is bound (with SO_REUSEPORT so it can coexist with the
// local responder); a zero source port uses an ephemeral one.
type udpProber struct{}

func (udpProber) Probe(ctx context.Context, agentID string, flow protocol.AgentFlow, spec protocol.TestSpec) protocol.Metric {
	m := protocol.Metric{Target: flow.DstAddr, TS: time.Now().UnixMilli()}

	// Dial a CONNECTED socket so the kernel delivers only datagrams from this
	// flow's destination to it. Without connecting, several flows that share a
	// source port form one SO_REUSEPORT group on that port (together with the
	// local responder), and the kernel demuxes inbound datagrams by hashing the
	// packet tuple — so an echo from one peer can land on a different flow's
	// socket and be lost/misattributed. A connected 4-tuple sidesteps that.
	d := net.Dialer{Timeout: probeTimeout}
	if flow.SrcPort > 0 {
		d.LocalAddr = &net.UDPAddr{IP: net.IPv4zero, Port: flow.SrcPort}
		d.Control = reusePortControl
	}
	conn, err := d.DialContext(ctx, "udp4", flow.DstAddr)
	if err != nil {
		m.Err = "dial: " + err.Error()
		m.PacketLoss = 100
		return m
	}
	defer conn.Close()
	m.LocalAddr = conn.LocalAddr().String()
	m.RemoteAddr = conn.RemoteAddr().String()

	// Read the received hop limit (TTL) via an IPv4 packet conn over the same fd.
	var p4 *ipv4.PacketConn
	if pc, ok := conn.(net.PacketConn); ok {
		p4 = ipv4.NewPacketConn(pc)
		_ = p4.SetControlMessage(ipv4.FlagTTL, true)
	}

	size := payloadSize(spec, 64)
	for seq := 0; seq < probeBurst; seq++ {
		if _, err := conn.Write(encodeProbe(uint64(seq), size)); err != nil {
			m.Err = "write: " + err.Error()
			m.PacketLoss = 100
			return m
		}
	}

	_ = conn.SetReadDeadline(time.Now().Add(probeTimeout))
	seen := make(map[uint64]bool)
	var br burstResult
	buf := make([]byte, size+64)
	for br.received < probeBurst {
		var (
			n  int
			cm *ipv4.ControlMessage
		)
		if p4 != nil {
			n, cm, _, err = p4.ReadFrom(buf)
		} else {
			n, err = conn.Read(buf)
		}
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

func (icmpProber) Probe(ctx context.Context, agentID string, flow protocol.AgentFlow, spec protocol.TestSpec) protocol.Metric {
	m := protocol.Metric{Target: flow.DstAddr, TS: time.Now().UnixMilli()}
	host := flow.DstAddr
	if h, _, err := net.SplitHostPort(host); err == nil {
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
		n, cm, src, err := p4.ReadFrom(rb)
		if err != nil {
			break
		}
		// Only count replies from this flow's destination. The kernel demuxes
		// echo replies per socket, but a source check is cheap defence in depth.
		if ip := addrIP(src); ip != nil && !ip.Equal(ipAddr.IP) {
			continue
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
