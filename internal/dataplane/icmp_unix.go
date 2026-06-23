//go:build !windows

package dataplane

import (
	"context"
	"net"
	"os"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"

	"netmesh/internal/protocol"
)

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
