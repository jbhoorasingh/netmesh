package dataplane

import (
	"context"
	"net"
	"testing"
	"time"

	"netmesh/internal/logging"
	"netmesh/internal/protocol"
)

func TestProbeEncodeDecode(t *testing.T) {
	before := time.Now().UnixNano()
	pkt := encodeProbe(42, 64)
	if len(pkt) != 64 {
		t.Fatalf("len = %d, want 64 (padded)", len(pkt))
	}
	seq, sendNs, ok := decodeProbe(pkt)
	if !ok || seq != 42 {
		t.Fatalf("decode = (%d, ok=%v), want seq 42", seq, ok)
	}
	if sendNs < before || sendNs > time.Now().UnixNano() {
		t.Errorf("sendNanos %d out of range", sendNs)
	}
	// Sub-header buffers and bad magic must be rejected.
	if _, _, ok := decodeProbe([]byte{1, 2, 3}); ok {
		t.Error("short buffer should not decode")
	}
	bad := make([]byte, headerSize)
	if _, _, ok := decodeProbe(bad); ok {
		t.Error("zero magic should not decode")
	}
}

func TestBurstResultApply(t *testing.T) {
	var m protocol.Metric
	burstResult{received: 4, rttSumNs: 4_000_000}.apply(&m, 4) // 1ms avg
	if !m.Success || m.RTTMicros != 1000 || m.PacketLoss != 0 {
		t.Errorf("full success: %+v", m)
	}
	var m2 protocol.Metric
	burstResult{received: 1, rttSumNs: 2_000_000}.apply(&m2, 4) // 75% loss
	if !m2.Success || m2.PacketLoss != 75 {
		t.Errorf("partial loss: loss=%.1f want 75", m2.PacketLoss)
	}
	var m3 protocol.Metric
	burstResult{}.apply(&m3, 4)
	if m3.Success || m3.PacketLoss != 100 || m3.Err == "" {
		t.Errorf("total loss: %+v", m3)
	}
}

// TestResponderUDPEcho verifies the responder echoes a UDP datagram and that the
// dynamic UDP prober measures a real round trip against it.
func TestResponderUDPEcho(t *testing.T) {
	port := freeUDPPort(t)
	log := logging.New("test")
	r := NewResponder(log)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := r.Serve(ctx, []protocol.ListenPort{{Port: port, UDP: true, TCP: true}})
	if len(st) != 1 || !st[0].UDP {
		t.Fatalf("responder failed to bind UDP: %+v", st)
	}
	time.Sleep(100 * time.Millisecond) // let the listeners settle

	target := net.JoinHostPort("127.0.0.1", itoa(port))
	flow := protocol.AgentFlow{ID: "f", Protocol: protocol.UDP, DstAgent: "peer", DstAddr: target, DstPort: port}
	m := udpProber{}.Probe(ctx, "self", flow, protocol.TestSpec{PayloadSize: 64})
	if !m.Success {
		t.Fatalf("expected echo success, got err=%q loss=%.0f", m.Err, m.PacketLoss)
	}
	if m.RTTMicros <= 0 {
		t.Errorf("RTT = %d us, want > 0", m.RTTMicros)
	}
	if m.PacketLoss != 0 {
		t.Errorf("loss = %.1f, want 0 over loopback", m.PacketLoss)
	}
	if m.RemoteAddr == "" || m.LocalAddr == "" {
		t.Errorf("expected addrs captured, got local=%q remote=%q", m.LocalAddr, m.RemoteAddr)
	}
}

// TestResponderTCPEcho verifies the TCP prober completes a payload round trip.
func TestResponderTCPEcho(t *testing.T) {
	port := freeUDPPort(t) // any free port; TCP listener will use it too
	log := logging.New("test")
	r := NewResponder(log)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := r.Serve(ctx, []protocol.ListenPort{{Port: port, UDP: true, TCP: true}})
	if len(st) != 1 || !st[0].TCP {
		t.Fatalf("responder failed to bind TCP: %+v", st)
	}
	time.Sleep(100 * time.Millisecond)

	target := net.JoinHostPort("127.0.0.1", itoa(port))
	flow := protocol.AgentFlow{ID: "f", Protocol: protocol.TCP, DstAgent: "peer", DstAddr: target, DstPort: port}
	m := tcpProber{}.Probe(ctx, "self", flow, protocol.TestSpec{PayloadSize: 128})
	if !m.Success || m.RTTMicros <= 0 {
		t.Fatalf("tcp echo failed: success=%v err=%q", m.Success, m.Err)
	}
	if m.RemoteAddr == "" {
		t.Errorf("expected RemoteAddr captured")
	}
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := pc.LocalAddr().(*net.UDPAddr).Port
	pc.Close()
	return port
}
