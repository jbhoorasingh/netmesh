package dataplane

import (
	"context"
	"io"
	"net"
	"time"

	"netmesh/internal/logging"
	"netmesh/internal/protocol"
)

// Responder is the peer-side echo service. Every agent runs one so that peers'
// UDP and TCP probes can complete a round trip and yield real RTT / loss.
// (ICMP needs no responder — the OS kernel answers echo requests directly.)
//
// Both listeners set SO_REUSEADDR/SO_REUSEPORT so the UDP-symmetric prober,
// which binds the same port number as its source, can coexist on a host that
// shares the data-plane port across roles.
type Responder struct {
	log *logging.Logger
}

// NewResponder builds a responder.
func NewResponder(log *logging.Logger) *Responder {
	return &Responder{log: log}
}

// reusePortControl (defined per-platform in reuseport_unix.go /
// reuseport_windows.go) enables address/port reuse on the raw socket so the
// symmetric prober can coexist with the responder. It is shared by the
// responder listeners and the symmetric prober.

// Serve binds the UDP and TCP echo servers on the given port and serves until
// ctx is cancelled. It returns a PortStatus describing which protocols bound —
// the master's port-availability signal. Binding happens before Serve returns;
// serving continues in the background until ctx is done.
func (r *Responder) Serve(ctx context.Context, port int) protocol.PortStatus {
	addr := ":" + itoa(port)
	lc := net.ListenConfig{Control: reusePortControl}
	st := protocol.PortStatus{Port: port}

	udpPC, uerr := lc.ListenPacket(ctx, "udp", addr)
	if uerr == nil {
		st.UDP = true
		go r.serveUDP(udpPC)
		go func() { <-ctx.Done(); udpPC.Close() }()
	} else {
		st.Err = "udp: " + uerr.Error()
	}

	tcpLn, terr := lc.Listen(ctx, "tcp", addr)
	if terr == nil {
		st.TCP = true
		go r.serveTCP(tcpLn)
		go func() { <-ctx.Done(); tcpLn.Close() }()
	} else {
		if st.Err != "" {
			st.Err += "; "
		}
		st.Err += "tcp: " + terr.Error()
	}

	r.log.Emit(logging.Event{Type: logging.AgentStarted, Detail: "data-plane responder bound",
		Fields: map[string]any{"port": port, "udp": st.UDP, "tcp": st.TCP}})
	return st
}

// serveUDP echoes every received datagram back to its sender, verbatim.
func (r *Responder) serveUDP(pc net.PacketConn) {
	buf := make([]byte, 64<<10)
	for {
		n, src, err := pc.ReadFrom(buf)
		if err != nil {
			return // listener closed
		}
		_, _ = pc.WriteTo(buf[:n], src)
	}
}

// serveTCP accepts connections and echoes their payload until the peer closes
// or goes idle.
func (r *Responder) serveTCP(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return // listener closed
		}
		go func(c net.Conn) {
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(10 * time.Second))
			// io.Copy(c, c) echoes input back to the sender until EOF/idle.
			_, _ = io.Copy(c, c)
		}(conn)
	}
}

// itoa avoids importing strconv just for the port string.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [12]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
