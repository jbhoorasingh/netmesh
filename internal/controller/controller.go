// Package controller implements NetMesh Controller (Master) mode: it hosts the
// agent control-plane websocket, the browser UI websocket, the REST API (with
// RBAC), and an in-memory store of the latest mesh telemetry.
package controller

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"netmesh/internal/auth"
	"netmesh/internal/config"
	"netmesh/internal/diag"
	"netmesh/internal/logging"
	"netmesh/internal/protocol"
	"netmesh/internal/transport"
	"netmesh/internal/uistream"
	"netmesh/web"
)

// maxBodyBytes bounds REST request bodies (mirrors the websocket read limit).
const maxBodyBytes = 1 << 20

// Controller wires together the hub, auth, telemetry store, and HTTP surface.
type Controller struct {
	cfg     *config.Config
	log     *logging.Logger
	auth    *auth.Authenticator
	hub     *transport.Hub
	store   *uistream.Store
	ui      *uistream.Hub
	uiToken string // disclosed only to authenticated callers; privileges a /ws/ui session

	admin *adminStore

	running     atomic.Bool  // whether a test is currently active
	curInterval atomic.Int64 // interval of the active test (for re-applying config)
	curPort     atomic.Int64 // data-plane port of the active test

	curMu        sync.Mutex
	curProtocols []protocol.Profile // protocols of the active test

	flowMu       sync.Mutex
	flowStatus   map[string]string // last emitted status per flow (for transition events)
	flowLastEmit map[string]int64  // unix-ms of last event per flow (throttle)
}

// New constructs a Controller.
func New(cfg *config.Config, log *logging.Logger) *Controller {
	c := &Controller{
		cfg:          cfg,
		log:          log,
		auth:         auth.New(cfg.AuthEnabled, cfg.AdminUser, cfg.AdminPass, log),
		store:        uistream.NewStore(),
		ui:           uistream.NewHub(),
		uiToken:      randomToken(),
		admin:        newAdminStore("netmesh-admin.json"),
		flowStatus:   make(map[string]string),
		flowLastEmit: make(map[string]int64),
	}
	c.hub = transport.NewHub(log, cfg.Token, c.onTelemetry, c.onDiag)
	return c
}

// Run starts the HTTP/WS server and blocks until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) error {
	go c.pumpEventsToUI(ctx)
	go c.meshSummaryLoop(ctx)

	c.log.Emit(logging.Event{
		Type:   logging.ControllerStarted,
		Detail: c.cfg.ListenAddr(),
		Fields: map[string]any{"authEnabled": c.cfg.AuthEnabled, "joinTokenSet": c.cfg.Token != ""},
	})

	srv := &http.Server{
		Addr:              c.cfg.ListenAddr(),
		Handler:           c.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
		// No WriteTimeout: /ws/agent and /ws/ui are long-lived; the websocket
		// pumps enforce their own per-frame write deadlines.
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case <-ctx.Done():
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

func (c *Controller) routes() http.Handler {
	mux := http.NewServeMux()

	// Control plane (agents). Optionally gated by a shared join token (-token).
	mux.HandleFunc("/ws/agent", c.hub.ServeAgent)

	// Read-only surfaces — available to anonymous users even when secured.
	mux.HandleFunc("/ws/ui", c.serveUI)
	mux.HandleFunc("/api/info", c.handleInfo)
	mux.HandleFunc("/api/auth", c.handleAuthInfo)
	mux.HandleFunc("/api/agents", c.handleAgents)
	mux.HandleFunc("/api/metrics", c.handleMetrics)

	// Privileged surfaces — require credentials when secured.
	mux.Handle("/api/tests/start", c.auth.RequireWriteFunc(c.handleTestStart))
	mux.Handle("/api/tests/stop", c.auth.RequireWriteFunc(c.handleTestStop))
	mux.Handle("/api/routing", c.auth.RequireWriteFunc(c.handleRouting))
	mux.Handle("/api/diag", c.auth.RequireWriteFunc(c.handleDiag))
	mux.Handle("/api/admin/agents", c.auth.RequireWriteFunc(c.handleAdminUpdate))
	mux.Handle("/api/admin/agents/evict", c.auth.RequireWriteFunc(c.handleAdminEvict))

	// Embedded UI assets.
	mux.Handle("/", http.FileServer(http.FS(web.FS())))

	return logRequests(c.log, mux)
}

// --- Telemetry store + UI fan-out ------------------------------------------

func (c *Controller) onTelemetry(agentID string, metrics []protocol.Metric, replay bool) {
	c.store.Ingest(metrics)
	c.ui.Broadcast(uistream.Message{Kind: "telemetry", Metrics: metrics, Replay: replay}, false)
	if !replay {
		c.emitFlowTransitions(metrics)
	}
}

func (c *Controller) onDiag(chunk protocol.DiagChunk) {
	// Diagnostic command OUTPUT is privileged: on a secured controller it is
	// delivered only to authenticated UI sessions, never to anonymous viewers.
	c.ui.Broadcast(uistream.Message{Kind: "diag", Diag: &chunk}, c.auth.Enabled())
}

func (c *Controller) pumpEventsToUI(ctx context.Context) {
	events, unsub := c.log.Bus().Subscribe(256)
	defer unsub()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			evCopy := ev
			c.ui.Broadcast(uistream.Message{Kind: "event", Event: &evCopy}, false)
		}
	}
}

// --- HTTP handlers ----------------------------------------------------------

func (c *Controller) handleInfo(w http.ResponseWriter, r *http.Request) {
	host, _ := os.Hostname()
	writeJSON(w, http.StatusOK, map[string]any{
		"role":          "controller",
		"host":          host,
		"hostAddr":      localIP(),
		"port":          c.cfg.Port,
		"agentId":       c.cfg.AgentID,
		"authEnabled":   c.cfg.AuthEnabled,
		"authenticated": c.auth.Authenticated(r),
		"diagCommands":  diag.Allowed(),
		"testRunning":   c.running.Load(),
	})
}

func (c *Controller) handleAuthInfo(w http.ResponseWriter, r *http.Request) {
	resp := map[string]any{
		"authEnabled":   c.cfg.AuthEnabled,
		"authenticated": c.auth.Authenticated(r),
	}
	// Hand authenticated callers the token that privileges their /ws/ui session
	// (so the diagnostics stream reaches the operator, not anonymous viewers).
	if c.auth.Authenticated(r) {
		resp["wsToken"] = c.uiToken
	}
	writeJSON(w, http.StatusOK, resp)
}

// agentView is a connected agent merged with its operator-managed config.
type agentView struct {
	transport.AgentInfo
	Label    string             `json:"label"`
	Group    string             `json:"group"`
	Enabled  bool               `json:"enabled"`
	Profiles []protocol.Profile `json:"profiles"`
}

func (c *Controller) handleAgents(w http.ResponseWriter, r *http.Request) {
	infos := c.hub.Agents()
	views := make([]agentView, 0, len(infos))
	for _, ai := range infos {
		cfg := c.admin.get(ai.ID)
		views = append(views, agentView{AgentInfo: ai, Label: cfg.Label, Group: cfg.Group, Enabled: cfg.Enabled, Profiles: cfg.Profiles})
	}
	writeJSON(w, http.StatusOK, views)
}

func (c *Controller) handleAdminUpdate(w http.ResponseWriter, r *http.Request) {
	var u AgentUpdate
	if !decodeJSON(w, r, &u) {
		return
	}
	if u.AgentID == "" {
		http.Error(w, "agentId required", http.StatusBadRequest)
		return
	}
	cfg := c.admin.update(u)
	c.log.Emit(logging.Event{Type: logging.AgentConfigUpdated, AgentID: u.AgentID,
		Detail: adminDetail(cfg), Fields: map[string]any{"enabled": cfg.Enabled, "label": cfg.Label, "group": cfg.Group}})
	// If a test is live, re-apply so enable/disable and profile changes take effect now.
	if c.running.Load() {
		c.startMesh(c.curInterval.Load(), int(c.curPort.Load()), c.currentProtocols())
	}
	writeJSON(w, http.StatusOK, cfg)
}

func (c *Controller) handleAdminEvict(w http.ResponseWriter, r *http.Request) {
	var body struct {
		AgentID string `json:"agentId"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	if err := c.hub.Evict(body.AgentID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	c.log.Emit(logging.Event{Type: logging.AgentEvicted, AgentID: body.AgentID, Detail: "evicted by operator"})
	writeJSON(w, http.StatusOK, map[string]any{"status": "evicted", "agentId": body.AgentID})
}

func adminDetail(cfg AgentConfig) string {
	state := "enabled"
	if !cfg.Enabled {
		state = "disabled"
	}
	profs := make([]string, 0, len(cfg.Profiles))
	for _, p := range cfg.Profiles {
		profs = append(profs, profDisplay(p))
	}
	d := "config updated · " + state
	if len(profs) > 0 {
		d += " · " + strings.Join(profs, ",")
	}
	if cfg.Label != "" {
		d += " · \"" + cfg.Label + "\""
	}
	return d
}

func (c *Controller) handleMetrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, c.store.Snapshot())
}

// startRequest is the (optional) body for POST /api/tests/start. With no body,
// the controller auto-builds a full mesh across the connected agents using each
// agent's default data port and configured profiles.
type startRequest struct {
	Routing    protocol.RoutingTable `json:"routing"`
	Spec       protocol.TestSpec     `json:"spec"`
	IntervalMS int64                 `json:"intervalMs"`
	// Master-driven test setup: the data-plane port to bind/probe (0 = each
	// agent's default), and which protocols to run (empty = per-agent config).
	Port      int                `json:"port"`
	Protocols []protocol.Profile `json:"protocols"`
}

func (c *Controller) handleTestStart(w http.ResponseWriter, r *http.Request) {
	var req startRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	interval := req.Spec.IntervalMS
	if interval <= 0 {
		interval = req.IntervalMS
	}
	if interval <= 0 {
		interval = 250
	}

	c.resetFlows()
	c.curInterval.Store(interval)
	c.running.Store(true)

	var runID string
	var agents int
	if len(req.Routing.Peers) > 0 {
		// Explicit routing table supplied: broadcast it verbatim (advanced use).
		if req.Spec.RunID == "" {
			req.Spec = protocol.TestSpec{RunID: "run-" + time.Now().Format("150405"), IntervalMS: interval, PayloadSize: 64}
		}
		c.hub.Broadcast(protocol.TypeRouting, req.Routing)
		c.hub.Broadcast(protocol.TypeTestStart, req.Spec)
		runID, agents = req.Spec.RunID, len(req.Routing.Peers)
	} else {
		runID, agents = c.startMesh(interval, req.Port, req.Protocols)
	}

	c.log.Emit(logging.Event{Type: logging.TestStarted, Detail: runID,
		Fields: map[string]any{"intervalMs": interval, "agents": agents, "port": req.Port}})
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "started", "runId": runID, "agents": agents})
}

// startMesh sends each enabled agent a routing table of the other enabled
// agents (probed with that agent's configured profiles) plus a TEST_START.
// Disabled agents are told to stop. Returns the run id and number of agents
// started. It is also called to re-apply config changes mid-run.
func (c *Controller) startMesh(intervalMs int64, port int, protocols []protocol.Profile) (string, int) {
	// Remember the test config so an admin change can re-apply it mid-run.
	c.curPort.Store(int64(port))
	c.setProtocols(protocols)

	runID := "run-" + time.Now().Format("150405")
	spec := protocol.TestSpec{RunID: runID, IntervalMS: intervalMs, PayloadSize: 64, Port: port}
	epoch := uint64(time.Now().Unix())

	// Resolve a dialable address per enabled agent: a master-chosen port applies
	// mesh-wide; otherwise each agent's own advertised data port is used.
	type addr struct{ id, address string }
	infos := c.hub.Agents()
	var enabled []addr
	for _, a := range infos {
		if !c.admin.get(a.ID).Enabled {
			continue
		}
		host, _, err := net.SplitHostPort(a.RemoteAddr)
		if err != nil {
			host = a.RemoteAddr
		}
		p := port
		if p <= 0 {
			if p = a.DataPort; p == 0 {
				p = c.cfg.Port + 1
			}
		}
		enabled = append(enabled, addr{a.ID, net.JoinHostPort(host, strconv.Itoa(p))})
	}

	started := 0
	for _, a := range infos {
		cfg := c.admin.get(a.ID)
		profiles := intersectProfiles(protocols, cfg.Profiles)
		if !cfg.Enabled || len(profiles) == 0 {
			_ = c.hub.SendTo(a.ID, protocol.TypeTestStop, nil)
			continue
		}
		peers := make([]protocol.Peer, 0, len(enabled))
		for _, pa := range enabled {
			if pa.id == a.ID {
				continue
			}
			peers = append(peers, protocol.Peer{AgentID: pa.id, Address: pa.address, Profiles: profiles})
		}
		_ = c.hub.SendTo(a.ID, protocol.TypeRouting, protocol.RoutingTable{Epoch: epoch, Peers: peers})
		_ = c.hub.SendTo(a.ID, protocol.TypeTestStart, spec)
		started++
	}
	return runID, started
}

// intersectProfiles restricts the test-selected protocols to those an agent is
// configured to run. An empty selection means "all the agent's profiles".
func intersectProfiles(selected, agentProfiles []protocol.Profile) []protocol.Profile {
	if len(selected) == 0 {
		return agentProfiles
	}
	allow := make(map[protocol.Profile]bool, len(agentProfiles))
	for _, p := range agentProfiles {
		allow[p] = true
	}
	out := make([]protocol.Profile, 0, len(selected))
	for _, p := range selected {
		if allow[p] {
			out = append(out, p)
		}
	}
	return out
}

func (c *Controller) setProtocols(p []protocol.Profile) {
	c.curMu.Lock()
	c.curProtocols = p
	c.curMu.Unlock()
}

func (c *Controller) currentProtocols() []protocol.Profile {
	c.curMu.Lock()
	defer c.curMu.Unlock()
	return c.curProtocols
}

func (c *Controller) handleTestStop(w http.ResponseWriter, r *http.Request) {
	c.running.Store(false)
	c.resetFlows()
	c.hub.Broadcast(protocol.TypeTestStop, nil)
	c.log.Emit(logging.Event{Type: logging.TestStopped})
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "stopped"})
}

func (c *Controller) handleRouting(w http.ResponseWriter, r *http.Request) {
	var table protocol.RoutingTable
	if !decodeJSON(w, r, &table) {
		return
	}
	c.hub.Broadcast(protocol.TypeRouting, table)
	c.log.Emit(logging.Event{Type: logging.RoutingUpdated, Fields: map[string]any{"peers": len(table.Peers), "epoch": table.Epoch}})
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "routing-updated"})
}

// diagRequest is the body for POST /api/diag.
type diagRequest struct {
	AgentID string   `json:"agentId"`
	Command string   `json:"command"`
	Args    []string `json:"args"`
}

func (c *Controller) handleDiag(w http.ResponseWriter, r *http.Request) {
	var req diagRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	reqID := req.AgentID + "-" + req.Command + "-" + time.Now().Format("150405.000")
	dr := protocol.DiagRequest{RequestID: reqID, Command: req.Command, Args: req.Args}
	if err := c.hub.SendTo(req.AgentID, protocol.TypeDiagRequest, dr); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	c.log.Emit(logging.Event{Type: logging.DiagRequested, AgentID: req.AgentID,
		Detail: req.Command, Fields: map[string]any{"requestId": reqID}})
	writeJSON(w, http.StatusAccepted, map[string]any{"requestId": reqID})
}

// --- Test-activity events ---------------------------------------------------

func (c *Controller) resetFlows() {
	c.flowMu.Lock()
	c.flowStatus = make(map[string]string)
	c.flowLastEmit = make(map[string]int64)
	c.flowMu.Unlock()
}

// metricStatus classifies a metric using the same thresholds as the UI.
func metricStatus(m protocol.Metric) string {
	if !m.Success || m.PacketLoss > 5 {
		return "critical"
	}
	lat := float64(m.RTTMicros) / 1000
	if lat > 50 || m.PacketLoss >= 1 {
		return "degraded"
	}
	return "healthy"
}

func profDisplay(p protocol.Profile) string {
	switch p {
	case protocol.UDPSymmetric:
		return "UDP-Sym"
	case protocol.UDPDynamic:
		return "UDP-Dyn"
	case protocol.TCP:
		return "TCP"
	case protocol.ICMP:
		return "ICMP"
	default:
		return string(p)
	}
}

// emitFlowTransitions emits an event when a flow's health changes, throttled to
// at most one event per flow per 10s so an oscillating link cannot flood the
// log. The benign first-sighting of a healthy flow is silent.
func (c *Controller) emitFlowTransitions(metrics []protocol.Metric) {
	now := time.Now().UnixMilli()
	type change struct {
		typ    logging.EventType
		src    string
		dst    string
		detail string
	}
	var changes []change

	c.flowMu.Lock()
	for _, m := range metrics {
		if m.PeerID == "" || m.PeerID == m.AgentID {
			continue
		}
		k := m.AgentID + ">" + m.PeerID + "/" + string(m.Profile)
		cur := metricStatus(m)
		prev := c.flowStatus[k]
		if cur == prev {
			continue
		}
		c.flowStatus[k] = cur
		if prev == "" && cur == "healthy" {
			continue // expected; don't announce
		}
		if now-c.flowLastEmit[k] < 10000 {
			continue // throttle this flow
		}
		c.flowLastEmit[k] = now

		flow := m.AgentID + "→" + m.PeerID + " " + profDisplay(m.Profile)
		lat := float64(m.RTTMicros) / 1000
		var typ logging.EventType
		var detail string
		switch cur {
		case "healthy":
			typ = logging.FlowRecovered
			detail = fmt.Sprintf("%s recovered · %.1fms", flow, lat)
		case "critical":
			typ = logging.FlowCritical
			if !m.Success {
				detail = fmt.Sprintf("%s unreachable · %.0f%% loss", flow, m.PacketLoss)
			} else {
				detail = fmt.Sprintf("%s critical · %.0f%% loss", flow, m.PacketLoss)
			}
		default:
			typ = logging.FlowDegraded
			detail = fmt.Sprintf("%s degraded · %.1fms · %.1f%% loss", flow, lat, m.PacketLoss)
		}
		changes = append(changes, change{typ: typ, src: m.AgentID, dst: m.PeerID, detail: detail})
	}
	c.flowMu.Unlock()

	for _, ch := range changes {
		c.log.Emit(logging.Event{Type: ch.typ, AgentID: ch.src, PeerID: ch.dst, Detail: ch.detail})
	}
}

// meshSummaryLoop emits a periodic, real summary of mesh health while a test is
// running, so the event log reflects ongoing test activity.
func (c *Controller) meshSummaryLoop(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !c.running.Load() {
				continue
			}
			var healthy, degraded, critical, latN int
			var latSum, lossSum float64
			for _, m := range c.store.Snapshot() {
				if m.PeerID == "" || m.PeerID == m.AgentID {
					continue
				}
				switch metricStatus(m) {
				case "healthy":
					healthy++
				case "degraded":
					degraded++
				default:
					critical++
				}
				if m.Success {
					latSum += float64(m.RTTMicros) / 1000
					latN++
				}
				lossSum += m.PacketLoss
			}
			total := healthy + degraded + critical
			if total == 0 {
				continue
			}
			avgLat := 0.0
			if latN > 0 {
				avgLat = latSum / float64(latN)
			}
			c.log.Emit(logging.Event{
				Type: logging.MeshSummary,
				Detail: fmt.Sprintf("probe cycle · %d flows · %d ok · %d degraded · %d critical · avg %.1fms · %.1f%% loss",
					total, healthy, degraded, critical, avgLat, lossSum/float64(total)),
			})
		}
	}
}

// --- UI websocket -----------------------------------------------------------

// uiUpgrader enforces a same-origin check to prevent cross-site WebSocket
// hijacking of the live telemetry/diagnostics stream.
var uiUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 8192,
	CheckOrigin:     sameOrigin,
}

func (c *Controller) serveUI(w http.ResponseWriter, r *http.Request) {
	conn, err := uiUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close() // ensures the reader goroutine below always unblocks

	// A session is privileged (eligible for diagnostic output) when the
	// controller is open, or when it presents the token handed to authenticated
	// callers by /api/auth.
	privileged := !c.cfg.AuthEnabled ||
		subtle.ConstantTimeCompare([]byte(r.URL.Query().Get("token")), []byte(c.uiToken)) == 1

	client := c.ui.Add(privileged)
	defer c.ui.Remove(client)

	// Reader: detect a client close so the writer loop can exit. Closing conn
	// (deferred above) guarantees this returns even on a half-open socket.
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				c.ui.Remove(client)
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

// --- helpers ----------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// decodeJSON reads a size-limited JSON body into v, writing a 400 on failure.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

// sameOrigin allows non-browser clients (no Origin header) and browser clients
// whose Origin host matches the request host.
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

// randomToken returns a 128-bit hex token.
func randomToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "netmesh-ui"
	}
	return hex.EncodeToString(b[:])
}

// localIP returns this host's primary outbound IPv4, or 127.0.0.1.
func localIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80") // no packets sent; just selects a route
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	if a, ok := conn.LocalAddr().(*net.UDPAddr); ok {
		return a.IP.String()
	}
	return "127.0.0.1"
}

func logRequests(log *logging.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		log.Infof("http", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
	})
}
