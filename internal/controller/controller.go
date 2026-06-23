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
	flows *flowStore

	running     atomic.Bool  // whether a test is currently active
	curInterval atomic.Int64 // interval of the active test (for re-applying config)

	applyMu sync.Mutex // serializes applyFlows so per-agent control frames stay ordered

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
		flows:        newFlowStore("netmesh-flows.json"),
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
	mux.HandleFunc("/api/flows", c.handleFlowsList) // GET — read-only flow list

	// Privileged surfaces — require credentials when secured.
	mux.Handle("/api/tests/start", c.auth.RequireWriteFunc(c.handleTestStart))
	mux.Handle("/api/tests/stop", c.auth.RequireWriteFunc(c.handleTestStop))
	mux.Handle("/api/flows/upsert", c.auth.RequireWriteFunc(c.handleFlowUpsert))
	mux.Handle("/api/flows/delete", c.auth.RequireWriteFunc(c.handleFlowDelete))
	mux.Handle("/api/flows/mesh", c.auth.RequireWriteFunc(c.handleFlowsMesh))
	mux.Handle("/api/flows/clear", c.auth.RequireWriteFunc(c.handleFlowsClear))
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
	Label   string `json:"label"`
	Group   string `json:"group"`
	Enabled bool   `json:"enabled"`
}

func (c *Controller) handleAgents(w http.ResponseWriter, r *http.Request) {
	infos := c.hub.Agents()
	views := make([]agentView, 0, len(infos))
	for _, ai := range infos {
		cfg := c.admin.get(ai.ID)
		views = append(views, agentView{AgentInfo: ai, Label: cfg.Label, Group: cfg.Group, Enabled: cfg.Enabled})
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
	// If a test is live, re-apply so enable/disable takes effect now.
	if c.running.Load() {
		c.applyFlows(c.curInterval.Load())
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
	d := "config updated · " + state
	if cfg.Label != "" {
		d += " · \"" + cfg.Label + "\""
	}
	return d
}

func (c *Controller) handleMetrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, c.store.Snapshot())
}

// --- Flows -----------------------------------------------------------------

func (c *Controller) handleFlowsList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, c.flows.all())
}

func (c *Controller) handleFlowUpsert(w http.ResponseWriter, r *http.Request) {
	var f Flow
	if !decodeJSON(w, r, &f) {
		return
	}
	if f.SrcAgent == "" || f.DstAgent == "" || !f.Protocol.Valid() {
		http.Error(w, "srcAgent, dstAgent and a valid protocol are required", http.StatusBadRequest)
		return
	}
	if f.SrcAgent == f.DstAgent {
		http.Error(w, "source and destination agent must differ", http.StatusBadRequest)
		return
	}
	if f.Protocol.HasPorts() && f.DstPort <= 0 {
		http.Error(w, "a destination port is required for UDP/TCP flows", http.StatusBadRequest)
		return
	}
	saved := c.flows.upsert(f)
	c.store.Clear()
	c.reapplyIfRunning()
	writeJSON(w, http.StatusOK, saved)
}

func (c *Controller) handleFlowDelete(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID string `json:"id"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	c.flows.remove(body.ID)
	c.store.Clear()
	c.reapplyIfRunning()
	writeJSON(w, http.StatusOK, map[string]any{"status": "deleted", "id": body.ID})
}

func (c *Controller) handleFlowsClear(w http.ResponseWriter, r *http.Request) {
	c.flows.clear()
	c.store.Clear()
	c.reapplyIfRunning()
	writeJSON(w, http.StatusOK, map[string]any{"status": "cleared"})
}

// handleFlowsMesh appends a full mesh of flows across the connected agents.
func (c *Controller) handleFlowsMesh(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Port      int                `json:"port"`
		Protocols []protocol.Profile `json:"protocols"`
		Symmetric bool               `json:"symmetric"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if len(req.Protocols) == 0 {
		req.Protocols = protocol.AllProfiles
	}
	agents := c.hub.Agents()
	added := 0
	for _, a := range agents {
		for _, b := range agents {
			if a.ID == b.ID {
				continue
			}
			for _, proto := range req.Protocols {
				if !proto.Valid() {
					continue
				}
				f := Flow{SrcAgent: a.ID, DstAgent: b.ID, Protocol: proto, Enabled: true}
				if proto.HasPorts() {
					f.DstPort = req.Port
					if req.Symmetric {
						f.SrcPort = req.Port
					}
				}
				c.flows.add(f)
				added++
			}
		}
	}
	c.store.Clear()
	c.reapplyIfRunning()
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "mesh-generated", "added": added})
}

func (c *Controller) reapplyIfRunning() {
	if c.running.Load() {
		c.applyFlows(c.curInterval.Load())
	}
}

// --- Test lifecycle --------------------------------------------------------

// startRequest is the (optional) body for POST /api/tests/start.
type startRequest struct {
	IntervalMS int64 `json:"intervalMs"`
}

func (c *Controller) handleTestStart(w http.ResponseWriter, r *http.Request) {
	var req startRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	interval := req.IntervalMS
	if interval <= 0 {
		interval = 250
	}

	c.resetFlows()
	c.store.Clear()
	c.running.Store(true)
	runID, flows := c.applyFlows(interval)

	c.log.Emit(logging.Event{Type: logging.TestStarted, Detail: runID,
		Fields: map[string]any{"intervalMs": interval, "flows": flows}})
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "started", "runId": runID, "flows": flows})
}

func (c *Controller) handleTestStop(w http.ResponseWriter, r *http.Request) {
	c.running.Store(false)
	c.resetFlows()
	c.store.Clear()
	c.hub.Broadcast(protocol.TypeTestStop, nil)
	c.log.Emit(logging.Event{Type: logging.TestStopped})
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "stopped"})
}

// applyFlows builds and sends a per-agent FlowPlan (listen ports + flows to
// generate) plus a TEST_START to every connected, enabled agent. Disabled
// agents are told to stop. Returns the run id and the total flow count.
//
// applyMu serializes the whole operation so two concurrent callers (e.g. an
// operator pressing Start while an admin toggle re-applies) cannot interleave
// each agent's FlowPlan→TestStart frames. Flow plans are delivered to every
// destination first, then TestStart is sent, so destinations have bound their
// listen ports before any source begins probing them.
func (c *Controller) applyFlows(intervalMs int64) (string, int) {
	c.applyMu.Lock()
	defer c.applyMu.Unlock()

	c.curInterval.Store(intervalMs)
	runID := "run-" + time.Now().Format("150405")
	spec := protocol.TestSpec{RunID: runID, IntervalMS: intervalMs, PayloadSize: 64}

	plans, total := c.buildFlowPlans()
	agents := c.hub.Agents()

	// Pass 1: deliver flow plans (and stop disabled agents) so every destination
	// binds its listen ports before any source is told to start.
	for _, a := range agents {
		if !c.admin.get(a.ID).Enabled {
			_ = c.hub.SendTo(a.ID, protocol.TypeTestStop, nil)
			continue
		}
		_ = c.hub.SendTo(a.ID, protocol.TypeFlowPlan, plans[a.ID]) // empty plan resets the agent
	}
	// Pass 2: start the tests.
	for _, a := range agents {
		if c.admin.get(a.ID).Enabled {
			_ = c.hub.SendTo(a.ID, protocol.TypeTestStart, spec)
		}
	}
	return runID, total
}

// buildFlowPlans resolves the operator's flows into a per-agent FlowPlan: the
// ports each agent must listen on (as a destination) and the flows it
// originates (as a source), with destinations resolved to addresses. Flows
// whose endpoints are not both connected+enabled are skipped. Returns the plans
// keyed by agent ID and the total resolved flow count.
func (c *Controller) buildFlowPlans() (map[string]protocol.FlowPlan, int) {
	epoch := uint64(time.Now().Unix())
	ipByID := make(map[string]string)
	enabled := make(map[string]bool)
	for _, a := range c.hub.Agents() {
		host, _, err := net.SplitHostPort(a.RemoteAddr)
		if err != nil {
			host = a.RemoteAddr
		}
		ipByID[a.ID] = host
		enabled[a.ID] = c.admin.get(a.ID).Enabled
	}

	type buildPlan struct {
		flows  []protocol.AgentFlow
		listen map[int]*protocol.ListenPort
	}
	plans := make(map[string]*buildPlan)
	get := func(id string) *buildPlan {
		if plans[id] == nil {
			plans[id] = &buildPlan{listen: make(map[int]*protocol.ListenPort)}
		}
		return plans[id]
	}

	total := 0
	for _, f := range c.flows.all() {
		if !f.Enabled || f.SrcAgent == f.DstAgent || !enabled[f.SrcAgent] || !enabled[f.DstAgent] {
			continue
		}
		dstIP, ok := ipByID[f.DstAgent]
		if !ok {
			continue
		}
		if _, ok := ipByID[f.SrcAgent]; !ok {
			continue
		}
		dstAddr := dstIP
		if f.Protocol.HasPorts() {
			dstAddr = net.JoinHostPort(dstIP, strconv.Itoa(f.DstPort))
		}
		get(f.SrcAgent).flows = append(get(f.SrcAgent).flows, protocol.AgentFlow{
			ID: f.ID, SrcPort: f.SrcPort, Protocol: f.Protocol,
			DstAgent: f.DstAgent, DstAddr: dstAddr, DstPort: f.DstPort,
		})
		if f.Protocol.HasPorts() {
			dst := get(f.DstAgent)
			lp := dst.listen[f.DstPort]
			if lp == nil {
				lp = &protocol.ListenPort{Port: f.DstPort}
				dst.listen[f.DstPort] = lp
			}
			if f.Protocol == protocol.UDP {
				lp.UDP = true
			} else if f.Protocol == protocol.TCP {
				lp.TCP = true
			}
		}
		total++
	}

	out := make(map[string]protocol.FlowPlan)
	for id, bp := range plans {
		fp := protocol.FlowPlan{Epoch: epoch, Flows: bp.flows}
		for _, lp := range bp.listen {
			fp.ListenPorts = append(fp.ListenPorts, *lp)
		}
		out[id] = fp
	}
	return out, total
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
	case protocol.UDP:
		return "UDP"
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
		k := m.AgentID + ">" + m.PeerID + "/" + string(m.Profile) + "/" + m.FlowID
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
