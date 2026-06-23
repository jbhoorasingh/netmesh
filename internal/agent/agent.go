// Package agent implements NetMesh Agent (Node) mode. It maintains the
// resilient control-plane client, runs the data-plane probe engine on command,
// executes whitelisted diagnostics, and serves a local UI — including the
// holding-state page that prompts the operator for a Master IP.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"netmesh/internal/config"
	"netmesh/internal/dataplane"
	"netmesh/internal/diag"
	"netmesh/internal/logging"
	"netmesh/internal/protocol"
	"netmesh/internal/spooler"
	"netmesh/internal/transport"
	"netmesh/internal/uistream"
	"netmesh/web"
)

// Version is the advertised build version.
const Version = "0.1.0-dev"

// Agent is the Node runtime.
type Agent struct {
	cfg       *config.Config
	log       *logging.Logger
	client    *transport.Client
	engine    *dataplane.Engine
	responder *dataplane.Responder
	store     *uistream.Store
	ui        *uistream.Hub

	mu      sync.RWMutex
	routing protocol.RoutingTable
	ctx     context.Context // base context for engine/diag work

	respMu     sync.Mutex
	respCancel context.CancelFunc // cancels the current responder binding
}

// New constructs an Agent.
func New(cfg *config.Config, log *logging.Logger) *Agent {
	a := &Agent{
		cfg:   cfg,
		log:   log,
		store: uistream.NewStore(),
		ui:    uistream.NewHub(),
	}
	spool := spooler.New(spooler.DefaultCapacity)
	a.client = transport.NewClient(cfg.AgentID, hostname(), Version, cfg.MasterAddr, cfg.Token, cfg.DataPort, log, spool, a.onControl)
	// The engine submits metrics to the agent, which tees them to the wire (via
	// the client) and to the local UI store/stream.
	a.engine = dataplane.NewEngine(cfg.AgentID, a, log)
	a.responder = dataplane.NewResponder(log)
	return a
}

// SubmitMetric satisfies dataplane.MetricSink. It forwards each metric to the
// control-plane client (authoritative: it stamps the sequence number and
// sends/spools) and mirrors a locally-stamped copy into the agent's own UI.
func (a *Agent) SubmitMetric(m protocol.Metric) {
	a.client.SubmitMetric(m)
	m.AgentID = a.cfg.AgentID
	if m.TS == 0 {
		m.TS = time.Now().UnixMilli()
	}
	a.store.Ingest([]protocol.Metric{m})
	a.ui.Broadcast(uistream.Message{Kind: "telemetry", Metrics: []protocol.Metric{m}}, false)
}

// Run starts the client, data-plane engine, and local UI server, blocking until
// ctx is cancelled.
func (a *Agent) Run(ctx context.Context) error {
	a.mu.Lock()
	a.ctx = ctx
	a.mu.Unlock()

	a.log.Emit(logging.Event{
		Type:    logging.AgentStarted,
		AgentID: a.cfg.AgentID,
		Detail:  a.cfg.Mode.String(),
		Fields:  map[string]any{"master": a.cfg.MasterAddr, "port": a.cfg.Port},
	})

	go a.client.Run(ctx)
	go a.pumpEventsToUI(ctx)
	// Bind the responder on the default data port as a baseline; a test may
	// rebind it to a master-chosen port.
	a.bindResponder(ctx, a.cfg.DataPort)

	srv := &http.Server{
		Addr:              a.cfg.ListenAddr(),
		Handler:           a.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case <-ctx.Done():
		a.engine.Stop()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func (a *Agent) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws/ui", a.serveUI)
	mux.HandleFunc("/api/info", a.handleInfo)
	mux.HandleFunc("/api/status", a.handleInfo) // alias
	mux.HandleFunc("/api/metrics", a.handleMetrics)
	mux.HandleFunc("/api/join", a.handleJoin)
	mux.Handle("/", http.FileServer(http.FS(web.FS())))
	return mux
}

// onControl handles Controller->Agent frames on the read-pump goroutine.
// Long-running work (diagnostics) is dispatched to its own goroutine.
func (a *Agent) onControl(env protocol.Envelope, out transport.Sender) {
	switch env.Type {
	case protocol.TypeRouting:
		var table protocol.RoutingTable
		if err := env.DecodePayload(&table); err != nil {
			a.log.Warnf("agent: bad routing table", "err", err)
			return
		}
		a.mu.Lock()
		a.routing = table
		a.mu.Unlock()
		a.log.Emit(logging.Event{Type: logging.RoutingUpdated, AgentID: a.cfg.AgentID,
			Fields: map[string]any{"peers": len(table.Peers), "epoch": table.Epoch}})

	case protocol.TypeTestStart:
		var spec protocol.TestSpec
		if err := env.DecodePayload(&spec); err != nil {
			a.log.Warnf("agent: bad test spec", "err", err)
			return
		}
		// Bind the responder on the master-chosen port and report availability.
		port := spec.Port
		if port <= 0 {
			port = a.cfg.DataPort
		}
		status := a.bindResponder(a.baseCtx(), port)
		status.AgentID = a.cfg.AgentID
		if err := out.Send(protocol.TypePortStatus, status); err != nil {
			a.log.Warnf("agent: port status send failed", "err", err)
		}
		a.engine.Start(a.baseCtx(), a.currentRouting(), spec)

	case protocol.TypeTestStop:
		a.engine.Stop()
		a.bindResponder(a.baseCtx(), a.cfg.DataPort) // return to the baseline port

	case protocol.TypeDiagRequest:
		var req protocol.DiagRequest
		if err := env.DecodePayload(&req); err != nil {
			a.log.Warnf("agent: bad diag request", "err", err)
			return
		}
		go a.runDiag(req, out)

	default:
		a.log.Warnf("agent: unexpected control frame", "type", env.Type)
	}
}

func (a *Agent) runDiag(req protocol.DiagRequest, out transport.Sender) {
	err := diag.Run(a.baseCtx(), req, func(chunk protocol.DiagChunk) {
		if serr := out.Send(protocol.TypeDiagChunk, chunk); serr != nil {
			a.log.Warnf("agent: diag chunk send failed", "err", serr)
		}
	})
	if err != nil {
		a.log.Emit(logging.Event{Type: logging.DiagRejected, AgentID: a.cfg.AgentID,
			Detail: req.Command, Fields: map[string]any{"err": err.Error()}})
		return
	}
	a.log.Emit(logging.Event{Type: logging.DiagCompleted, AgentID: a.cfg.AgentID, Detail: req.Command})
}

func (a *Agent) baseCtx() context.Context {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.ctx == nil {
		return context.Background()
	}
	return a.ctx
}

func (a *Agent) currentRouting() protocol.RoutingTable {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.routing
}

// bindResponder (re)binds the data-plane echo responder to port, cancelling any
// previous binding, and returns the resulting port-availability status. Serving
// continues in the background until the next rebind or until parent is done.
func (a *Agent) bindResponder(parent context.Context, port int) protocol.PortStatus {
	a.respMu.Lock()
	if a.respCancel != nil {
		a.respCancel()
	}
	ctx, cancel := context.WithCancel(parent)
	a.respCancel = cancel
	a.respMu.Unlock()
	return a.responder.Serve(ctx, port)
}

func (a *Agent) pumpEventsToUI(ctx context.Context) {
	events, unsub := a.log.Bus().Subscribe(256)
	defer unsub()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			e := ev
			a.ui.Broadcast(uistream.Message{Kind: "event", Event: &e}, false)
		}
	}
}

// --- Local UI / API ---------------------------------------------------------

func (a *Agent) handleInfo(w http.ResponseWriter, r *http.Request) {
	st := a.client.Status()
	host, _ := os.Hostname()
	a.mu.RLock()
	peers := len(a.routing.Peers)
	a.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"role":         "agent",
		"agentId":      a.cfg.AgentID,
		"host":         host,
		"hostAddr":     localIP(),
		"port":         a.cfg.Port,
		"dataPort":     a.cfg.DataPort,
		"mode":         a.cfg.Mode.String(),
		"master":       st.Master,
		"joined":       st.Master != "",
		"connected":    st.Connected,
		"wsRttUs":      st.RTTMicros,
		"framesRx":     st.FramesRx,
		"framesTx":     st.FramesTx,
		"spoolLen":     st.SpoolLen,
		"peers":        peers,
		"diagCommands": diag.Allowed(),
	})
}

func (a *Agent) handleMetrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.store.Snapshot())
}

var uiUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 8192,
	CheckOrigin:     sameOrigin,
}

func (a *Agent) serveUI(w http.ResponseWriter, r *http.Request) {
	conn, err := uiUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	client := a.ui.Add(true)
	defer a.ui.Remove(client)

	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				a.ui.Remove(client)
				return
			}
		}
	}()

	for msg := range client.Ch {
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := conn.WriteJSON(msg); err != nil {
			return
		}
	}
}

// handleJoin lets the holding-state UI supply a Master IP at runtime. It is
// restricted to loopback callers: repointing an agent's controller is a
// control-plane takeover, so it must originate on the node itself, not from the
// LAN.
func (a *Agent) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if !isLoopback(r.RemoteAddr) {
		a.log.Emit(logging.Event{Type: logging.AuthRejected, AgentID: a.cfg.AgentID,
			Detail: "join attempt from non-loopback", Fields: map[string]any{"remote": r.RemoteAddr}})
		http.Error(w, "join is permitted only from localhost", http.StatusForbidden)
		return
	}
	var body struct {
		Master string `json:"master"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	addr, err := normaliseMaster(body.Master, a.cfg.Port)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	a.cfg.MasterAddr = addr
	a.client.SetMaster(addr)
	a.log.Emit(logging.Event{Type: logging.AgentStarted, AgentID: a.cfg.AgentID,
		Detail: "master set via local UI", Fields: map[string]any{"master": addr}})
	writeJSON(w, http.StatusAccepted, map[string]any{"master": addr})
}

// --- helpers ----------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// sameOrigin allows non-browser clients and same-host browser clients.
func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return strings.EqualFold(u.Host, r.Host)
}

// isLoopback reports whether the request's remote address is a loopback IP.
func isLoopback(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// localIP returns this host's primary outbound IPv4, or 127.0.0.1.
func localIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	if a, ok := conn.LocalAddr().(*net.UDPAddr); ok {
		return a.IP.String()
	}
	return "127.0.0.1"
}

var errEmptyMaster = errors.New("master address is empty")

func hostname() string {
	hn, err := os.Hostname()
	if err != nil || hn == "" {
		return "agent"
	}
	return hn
}

// normaliseMaster mirrors config.normaliseMaster for the runtime join path.
func normaliseMaster(master string, defaultPort int) (string, error) {
	master = strings.TrimSpace(master)
	master = strings.TrimPrefix(master, "ws://")
	master = strings.TrimPrefix(master, "http://")
	if master == "" {
		return "", errEmptyMaster
	}
	if host, port, err := net.SplitHostPort(master); err == nil {
		return net.JoinHostPort(host, port), nil
	}
	return net.JoinHostPort(master, strconv.Itoa(defaultPort)), nil
}
