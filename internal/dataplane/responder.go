package dataplane

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"netmesh/internal/logging"
	"netmesh/internal/protocol"
)

// Responder is the peer-side echo service. Every agent runs one so that peers'
// UDP and TCP probes can complete a round trip and yield real RTT / loss.
// (ICMP needs no responder — the OS kernel answers echo requests directly.)
//
// Both listeners set SO_REUSEADDR/SO_REUSEPORT so a prober that binds the same
// port number as its source can coexist on a host that shares the data-plane
// port across roles.
type Responder struct {
	log *logging.Logger

	mu  sync.Mutex
	cur *binding // the active binding, if any
}

// binding is one set of bound listeners with a coordinated shutdown: cancelling
// its context closes the listeners and any in-flight connections, and wg becomes
// ready once every listener/connection goroutine has exited.
type binding struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewResponder builds a responder.
func NewResponder(log *logging.Logger) *Responder {
	return &Responder{log: log}
}

// reusePortControl (defined per-platform in reuseport_unix.go /
// reuseport_windows.go) enables address/port reuse on the raw socket so a
// prober can coexist with the responder. It is shared by the responder
// listeners and the probers.

// Serve binds the requested set of data-plane ports (UDP and/or TCP echo per
// port) and serves them in the background. It first tears down any previous
// binding and WAITS for its listeners and connections to close, so a rebind
// never overlaps the old sockets or leaves echo goroutines from a prior plan
// alive. It returns a PortStatus per port — the master's port-availability
// signal. Serving continues until parent is cancelled or the next Serve call.
func (r *Responder) Serve(parent context.Context, ports []protocol.ListenPort) []protocol.PortStatus {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.cur != nil {
		r.cur.cancel()
		r.cur.wg.Wait()
		r.cur = nil
	}

	ctx, cancel := context.WithCancel(parent)
	b := &binding{cancel: cancel}
	lc := net.ListenConfig{Control: reusePortControl}
	out := make([]protocol.PortStatus, 0, len(ports))
	active := false

	for _, lp := range ports {
		addr := ":" + itoa(lp.Port)
		st := protocol.PortStatus{Port: lp.Port}

		if lp.UDP {
			if udpPC, err := lc.ListenPacket(ctx, "udp", addr); err == nil {
				st.UDP = true
				active = true
				b.wg.Add(2)
				go func(pc net.PacketConn) { defer b.wg.Done(); <-ctx.Done(); pc.Close() }(udpPC)
				go func(pc net.PacketConn) { defer b.wg.Done(); r.serveUDP(pc) }(udpPC)
			} else {
				st.Err = "udp: " + err.Error()
			}
		}
		if lp.TCP {
			if tcpLn, err := lc.Listen(ctx, "tcp", addr); err == nil {
				st.TCP = true
				active = true
				b.wg.Add(2)
				go func(ln net.Listener) { defer b.wg.Done(); <-ctx.Done(); ln.Close() }(tcpLn)
				go func(ln net.Listener) { defer b.wg.Done(); r.serveTCP(ctx, b, ln) }(tcpLn)
			} else {
				if st.Err != "" {
					st.Err += "; "
				}
				st.Err += "tcp: " + err.Error()
			}
		}
		out = append(out, st)
	}

	if active {
		r.cur = b
	} else {
		cancel() // nothing bound; don't leak the context
	}

	r.log.Emit(logging.Event{Type: logging.AgentStarted, Detail: "data-plane responder bound",
		Fields: map[string]any{"ports": len(ports)}})
	return out
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

// serveTCP accepts connections and echoes their payload until the peer closes,
// goes idle (10s), or the binding is torn down. Each connection goroutine is
// tracked on the binding's WaitGroup and closed on ctx cancellation so it never
// outlives its listen plan.
func (r *Responder) serveTCP(ctx context.Context, b *binding, ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return // listener closed
		}
		b.wg.Add(1)
		go func(c net.Conn) {
			defer b.wg.Done()
			defer c.Close()
			// Close the connection when the binding is cancelled so a rebind does
			// not leave it (and its goroutine) alive.
			done := make(chan struct{})
			defer close(done)
			go func() {
				select {
				case <-ctx.Done():
					c.Close()
				case <-done:
				}
			}()
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
