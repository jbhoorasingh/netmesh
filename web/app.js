/* NetMesh UI — vanilla port of the Claude Design prototype (NetMesh.dc.html),
 * driven by the real backend: /api/info, /api/agents, /api/metrics, /ws/ui,
 * /api/tests/*, /api/diag. One host runs in ONE fixed role (controller|agent),
 * reported by /api/info — there is no demo role toggle.
 */
(() => {
  'use strict';

  // ---------- palette / helpers ----------
  const P = { green: '#3fb950', yellow: '#d29922', red: '#f85149', accent: '#58a6ff' };
  const PROF = {
    'UDP-Sym': ['#7ee787', '#7ee78744'], 'UDP-Dyn': ['#a5a5ff', '#a5a5ff44'],
    'TCP': ['#58a6ff', '#58a6ff44'], 'ICMP': ['#f0883e', '#f0883e44'], 'WS': ['#58a6ff', '#58a6ff44'],
  };
  const PROF_DISPLAY = { udp_symmetric: 'UDP-Sym', udp_dynamic: 'UDP-Dyn', tcp: 'TCP', icmp: 'ICMP', '': 'WS' };
  const PORT_FOR = { 'UDP-Sym': 9000, 'UDP-Dyn': 0, 'TCP': 5201, 'ICMP': 0, 'WS': 5999 };

  const esc = (s) => String(s == null ? '' : s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const portFromAddr = (a) => { if (!a) return 0; const i = a.lastIndexOf(':'); return i < 0 ? 0 : (parseInt(a.slice(i + 1), 10) || 0); };
  const pad = (n) => (n < 10 ? '0' : '') + n;
  const timeStr = (ms) => { const d = new Date(ms); return pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes()) + ':' + pad(d.getUTCSeconds()); };
  const colorFor = (s) => s === 'green' ? P.green : s === 'yellow' ? P.yellow : P.red;
  const statusOf = (lat, loss) => (loss > 5 ? 'red' : (lat > 50 || loss >= 1) ? 'yellow' : 'green');
  const stName = { green: 'HEALTHY', yellow: 'DEGRADED', red: 'CRITICAL' };

  // ---------- state ----------
  const S = {
    info: { role: 'controller', authEnabled: false, authenticated: true, hostAddr: '—', port: 5999, diagCommands: ['ping', 'traceroute', 'nslookup', 'netstat'] },
    creds: null, wsToken: null,
    page: 'dashboard',
    agents: [], metrics: [],
    flows: [], flowById: {},
    events: [], seqBuf: {}, lastSeqForFlow: {}, lastRtt: {},
    hLat: [], hJit: [], hLoss: [],
    txInterval: 250,
    sortKey: 'latency', sortDir: 'desc', filter: 'ALL', query: '',
    selectedNode: null, detail: null,
    testLog: [], tstats: null, openFlowId: null,
    diagOpen: false, diagAgent: null, diagLines: [], diagBusy: false,
    clock: timeStr(Date.now()),
    wsUp: false, running: false,
    logMax: false, logPaused: false, logNew: 0,
    logLevels: new Set(['INFO', 'WARN', 'ERR', 'TEST', 'ADMIN']),
    logQuery: '',
    testSetup: false, testPort: 0,
    structKey: '',
  };
  for (let i = 0; i < 90; i++) { S.hLat.push(0); S.hJit.push(0); S.hLoss.push(0); }

  const readOnly = () => S.info.authEnabled && !S.info.authenticated;
  const isController = () => S.info.role === 'controller';

  // ---------- api ----------
  const authHeaders = () => S.creds ? { Authorization: 'Basic ' + btoa(S.creds.user + ':' + S.creds.pass) } : {};
  async function getJSON(url) { const r = await fetch(url, { headers: authHeaders() }); if (!r.ok) throw new Error(r.status); return r.json(); }
  async function postJSON(url, body) {
    const r = await fetch(url, { method: 'POST', headers: Object.assign({ 'Content-Type': 'application/json' }, authHeaders()), body: body == null ? '' : JSON.stringify(body) });
    return r;
  }

  // ---------- websocket (/ws/ui) ----------
  let ws = null;
  function connectWS() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    const q = S.wsToken ? ('?token=' + encodeURIComponent(S.wsToken)) : '';
    ws = new WebSocket(`${proto}://${location.host}/ws/ui${q}`);
    ws.onopen = () => { S.wsUp = true; };
    ws.onclose = () => { S.wsUp = false; setTimeout(connectWS, 1500); };
    ws.onmessage = (m) => { try { onWSMessage(JSON.parse(m.data)); } catch (e) { /* ignore */ } };
  }
  function onWSMessage(msg) {
    if (msg.kind === 'event' && msg.event) {
      pushEvent(msg.event);
    } else if (msg.kind === 'telemetry' && msg.metrics) {
      msg.metrics.forEach(ingestMetric);
    } else if (msg.kind === 'diag' && msg.diag) {
      ingestDiag(msg.diag);
    }
  }

  // ---------- event mapping ----------
  const EVENT_STYLE = {
    PACKET_SEQUENCE_MISSED: ['ERR', P.red], AGENT_DROPPED: ['ERR', P.red], DIAG_REJECTED: ['ERR', P.red], AUTH_REJECTED: ['ERR', P.red], FLOW_CRITICAL: ['ERR', P.red], PORT_UNAVAILABLE: ['ERR', P.red],
    HEARTBEAT_TIMEOUT: ['WARN', P.yellow], WS_RECONNECTING: ['WARN', P.yellow], WS_DISCONNECTED: ['WARN', P.yellow], TELEMETRY_SPOOLED: ['WARN', P.yellow], FLOW_DEGRADED: ['WARN', P.yellow],
    AGENT_JOINED: ['INFO', P.green], AGENT_REGISTERED: ['INFO', P.green], TELEMETRY_FLUSHED: ['INFO', P.green], WS_CONNECTED: ['INFO', P.green], DIAG_COMPLETED: ['INFO', P.green], FLOW_RECOVERED: ['INFO', P.green], TEST_STARTED: ['INFO', P.green], PORT_BOUND: ['INFO', P.green],
    MESH_SUMMARY: ['TEST', P.accent], TEST_STOPPED: ['INFO', P.yellow], AGENT_CONFIG_UPDATED: ['ADMIN', P.accent], AGENT_EVICTED: ['ADMIN', P.yellow],
  };
  // friendly label lookup (admin-assigned name falls back to agent id)
  function labelOf(id) { const a = S.agents.find((x) => x.id === id); return a && a.label ? a.label : id; }
  function pushEvent(ev) {
    const style = EVENT_STYLE[ev.event] || ['INFO', P.accent];
    const t = ev.time ? Date.parse(ev.time) : Date.now();
    let msg;
    if (ev.event === 'MESH_SUMMARY' || ev.event === 'FLOW_CRITICAL' || ev.event === 'FLOW_DEGRADED' || ev.event === 'FLOW_RECOVERED') {
      msg = ev.detail || ev.event.toLowerCase();
    } else {
      msg = ev.event.replace(/_/g, ' ').toLowerCase();
      if (ev.agentId) msg = ev.agentId + ' · ' + msg;
      if (ev.detail) msg += ' — ' + ev.detail;
      if (ev.seq) msg += ' (seq ' + ev.seq + ')';
    }
    const row = { id: 'e' + t + Math.random(), time: timeStr(t), tag: style[0], color: style[1], msg };
    S.events.push(row);
    if (S.events.length > 500) S.events = S.events.slice(-500);
    if (S.logPaused && S.logLevels.has(row.tag)) S.logNew++;
    updateEvents();
    if (ev.event === 'TEST_STARTED') setRunning(true);
    else if (ev.event === 'TEST_STOPPED') setRunning(false);
  }
  function setRunning(v) { if (S.running !== v) { S.running = v; render(); } }

  // ---------- telemetry ingest ----------
  function flowIdOf(m) { return m.agentId + '>' + m.peerId + '/' + (m.profile || 'ws'); }
  function ingestMetric(m) {
    const id = flowIdOf(m);
    const rtt = (m.rttUs || 0) / 1000;
    // rolling per-flow jitter
    const last = S.lastRtt[id]; S.lastRtt[id] = rtt;
    // sequence strip state
    let st = m.success ? 'OK' : 'MISS';
    if (m.success && m.seq && S.lastSeqForFlow[id] && m.seq < S.lastSeqForFlow[id]) st = 'REORD';
    if (m.seq) S.lastSeqForFlow[id] = Math.max(S.lastSeqForFlow[id] || 0, m.seq);
    const buf = S.seqBuf[id] || (S.seqBuf[id] = []);
    buf.push(st); if (buf.length > 58) buf.shift();
  }
  function jitterFor(id, rtt) { const l = S.lastRttSnap ? S.lastRttSnap[id] : undefined; return l == null ? 0 : Math.abs(rtt - l); }

  // ---------- derive flows from agents + metrics ----------
  function rebuildFlows() {
    const flows = [];
    // control-plane flows (real WS RTT to controller) — controller view
    if (isController()) {
      S.agents.forEach((a) => {
        const rtt = (a.wsRttUs || 0) / 1000;
        flows.push({ id: 'ctrl>' + a.id, src: a.id, dst: 'controller', profile: 'WS', port: 5999, latency: rtt, jitter: estJit('ctrl>' + a.id, rtt), loss: 0, ctrl: true, success: true, pkts: a.framesTx || 0 });
      });
    }
    // data-plane flows (from latest metrics)
    S.metrics.forEach((m) => {
      const prof = PROF_DISPLAY[m.profile] || m.profile;
      const rtt = (m.rttUs || 0) / 1000;
      const id = m.agentId + '>' + m.peerId + '/' + m.profile;
      const port = portFromAddr(m.remoteAddr) || PORT_FOR[prof] || 0;
      flows.push({ id, src: m.agentId, dst: m.peerId, profile: prof, port, latency: rtt, jitter: estJit(id, rtt), loss: m.success ? (m.lossPct || 0) : 100, success: m.success, err: m.err, ctrl: false, ttl: m.ttl || 0, localAddr: m.localAddr || '', remoteAddr: m.remoteAddr || '' });
    });
    S.flows = flows;
    S.flowById = {}; flows.forEach((f) => S.flowById[f.id] = f);
  }
  const _jitState = {};
  function estJit(id, rtt) { const l = _jitState[id]; _jitState[id] = rtt; return l == null ? 0 : +Math.abs(rtt - l).toFixed(2); }

  // ---------- data refresh ----------
  async function refresh() {
    try {
      if (isController()) {
        const [agents, metrics] = await Promise.all([getJSON('/api/agents'), getJSON('/api/metrics')]);
        S.agents = agents || []; S.metrics = metrics || [];
      } else {
        const [info, metrics] = await Promise.all([getJSON('/api/info'), getJSON('/api/metrics')]);
        S.info = Object.assign(S.info, info); S.metrics = metrics || [];
      }
      rebuildFlows();
      renderData();
    } catch (e) { /* transient */ }
  }

  function tick() {
    S.clock = timeStr(Date.now());
    // sample global history from current flows
    const dp = S.flows.filter((f) => !f.ctrl);
    const live = dp.length ? dp : S.flows;
    const n = live.length || 1;
    const avgL = live.reduce((s, f) => s + f.latency, 0) / n;
    const avgJ = live.reduce((s, f) => s + (f.jitter || 0), 0) / n;
    const avgLo = live.reduce((s, f) => s + (f.loss || 0), 0) / n;
    S.hLat.push(avgL); S.hLat.shift();
    S.hJit.push(avgJ); S.hJit.shift();
    S.hLoss.push(avgLo); S.hLoss.shift();
    updateClock();
  }

  // ================= rendering =================
  const app = document.getElementById('app');

  // structural render: rebuilt only when the layout (role/page/drawer/diag/auth) changes
  function structureKey() {
    return [S.info.role, S.page, S.detail ? S.detail.mode : '', !!S.diagOpen, !!S.logMax, !!S.testSetup, readOnly(), S.info.authEnabled && S.info.authenticated, !!S.selectedNode, S.info.role === 'agent' && !infoJoined(), S.running].join('|');
  }
  function infoJoined() { return S.info.joined || (S.info.master && S.info.master !== ''); }

  function maybeRenderStructure() {
    const k = structureKey();
    if (k === S.structKey) return false;
    S.structKey = k;
    app.innerHTML = isController() ? controllerHTML() : agentHTML();
    bindCanvases();
    bindLog();
    populateLog();
    return true;
  }

  function render() { maybeRenderStructure(); renderData(); }

  // ---------- header ----------
  function headerHTML() {
    const isC = isController();
    const modeLabel = isC ? 'CONTROLLER' : 'AGENT NODE';
    const modeColor = isC ? '#7cb7ff' : '#7ee787', modeBorder = isC ? '#1f6feb55' : '#3fb95055', modeBg = isC ? '#1f6feb14' : '#3fb9500f';
    const ro = readOnly();
    const running = S.running;
    const startDisabled = ro || running;
    const stopDisabled = ro || !running;
    const startStyle = `font:600 10px 'IBM Plex Mono';letter-spacing:.04em;padding:8px 13px;border-radius:6px;border:1px solid ${startDisabled ? '#1c2632' : '#2f6b3a'};background:${startDisabled ? '#0d121a' : '#13351f'};color:${startDisabled ? '#3a4757' : '#7ee787'};opacity:${startDisabled ? '.55' : '1'}`;
    const stopStyle = `font:600 10px 'IBM Plex Mono';letter-spacing:.04em;padding:8px 11px;border-radius:6px;border:1px solid ${stopDisabled ? '#1c2632' : '#5a2730'};background:${stopDisabled ? '#0d121a' : '#2a1418'};color:${stopDisabled ? '#3a4757' : '#f0a3a3'};opacity:${stopDisabled ? '.55' : '1'}`;
    const runPill = running
      ? `<div style="display:flex;align-items:center;gap:6px;font:600 9px 'IBM Plex Mono';letter-spacing:.1em;color:#7ee787;border:1px solid #2f6b3a;background:#13351f;padding:5px 9px;border-radius:5px"><span style="width:6px;height:6px;border-radius:50%;background:#3fb950;box-shadow:0 0 6px #3fb950;animation:nm-blink 1.2s infinite"></span>TEST RUNNING</div>`
      : `<div style="font:600 9px 'IBM Plex Mono';letter-spacing:.1em;color:#566273;border:1px solid #1b2430;padding:5px 9px;border-radius:5px">IDLE</div>`;
    const auth = S.info.authEnabled
      ? (S.info.authenticated
        ? `<div style="display:flex;align-items:center;gap:9px"><div style="width:26px;height:26px;border-radius:50%;background:linear-gradient(135deg,#2d3a4d,#1a2230);border:1px solid #2a3645;display:flex;align-items:center;justify-content:center;font:600 11px 'IBM Plex Mono';color:#9fb1c6">NS</div><div style="line-height:1.25"><div style="font-size:11px;color:#cfdae8;font-weight:600">noc-engineer</div><div style="font:500 9px 'IBM Plex Mono';color:#566273">cluster-admin</div></div><button data-act="logout" style="font:500 10px 'IBM Plex Mono';color:#6b7888;background:transparent;border:1px solid #1f2a37;padding:6px 9px;border-radius:5px">Logout</button></div>`
        : `<div style="display:flex;align-items:center;gap:10px"><span style="font:600 9px 'IBM Plex Mono';letter-spacing:.13em;color:#d29922;border:1px solid #d2992255;background:#d299220f;padding:5px 8px;border-radius:4px">READ-ONLY MODE</span><button data-act="login" style="font:600 11px 'IBM Plex Sans';color:#dbe6f2;background:#1f6feb;border:none;padding:8px 14px;border-radius:6px;box-shadow:0 2px 8px #1f6feb55">Login</button></div>`)
      : '';
    return `<header style="height:54px;flex:0 0 54px;display:flex;align-items:center;gap:18px;padding:0 18px;background:linear-gradient(180deg,#0e131b,#0b0f15);border-bottom:1px solid #1b2430;z-index:20">
      <div style="display:flex;align-items:center;gap:11px">
        <div style="width:26px;height:26px;border-radius:6px;background:linear-gradient(135deg,#1f6feb,#0b3a8a);display:flex;align-items:center;justify-content:center;box-shadow:0 0 0 1px #2f81f74d,0 4px 12px #1f6feb44"><div style="width:11px;height:11px;border:2px solid #cfe0ff;border-radius:50%;position:relative"><div style="position:absolute;inset:2px;background:#cfe0ff;border-radius:50%"></div></div></div>
        <div style="font-weight:700;font-size:15px;letter-spacing:.02em;color:#eef3fa">NetMesh</div>
        <div style="font:600 9.5px/1 'IBM Plex Mono';letter-spacing:.16em;color:${modeColor};border:1px solid ${modeBorder};background:${modeBg};padding:4px 7px;border-radius:4px">${modeLabel}</div>
      </div>
      <div style="display:flex;align-items:center;gap:7px;font:500 11px/1 'IBM Plex Mono';color:#7b8898;background:#0a0e14;border:1px solid #1b2430;border-radius:6px;padding:5px 10px">
        <span style="width:5px;height:5px;border-radius:50%;background:${modeColor};box-shadow:0 0 6px ${modeColor}"></span>
        <span style="font-weight:700;color:#cdd9e5">${esc(S.info.hostAddr)}</span><span style="color:#465061">:${S.info.port}</span>
      </div>
      <div style="flex:1"></div>
      <div style="display:flex;align-items:center;gap:7px;font:500 11px/1 'IBM Plex Mono';color:#5e6b7d">
        <span style="width:6px;height:6px;border-radius:50%;background:${S.wsUp ? '#3fb950' : '#f85149'};box-shadow:0 0 6px ${S.wsUp ? '#3fb950' : '#f85149'};animation:nm-blink 2s infinite"></span>
        <span id="nm-clock" style="color:#8b98a8">${S.clock} UTC</span>
      </div>
      <div style="width:1px;height:22px;background:#1b2430"></div>
      ${isC ? `${runPill}
      <button data-act="openTestSetup" ${startDisabled ? 'disabled' : ''} style="${startStyle}">▷ START TEST</button>
      <button data-act="stopTest" ${stopDisabled ? 'disabled' : ''} style="${stopStyle}">▢ STOP</button>
      <div style="width:1px;height:22px;background:#1b2430"></div>` : ''}
      ${auth}
    </header>`;
  }

  // ---------- controller layout ----------
  function controllerHTML() {
    return `${headerHTML()}
    <div style="flex:1;min-height:0;position:relative">
      <div style="position:absolute;inset:0;display:flex;flex-direction:column;gap:10px;padding:12px;overflow:hidden">
        <div style="flex:0 0 auto;display:flex;gap:5px">
          <button data-act="goDash" style="${subTab(S.page === 'dashboard')}">▦ Dashboard</button>
          <button data-act="goSeq" style="${subTab(S.page === 'sequences')}">⠿ Sequence Monitor</button>
          <button data-act="goAdmin" style="${subTab(S.page === 'admin')}">⚙ Admin</button>
        </div>
        ${S.page === 'dashboard' ? controllerDash() : S.page === 'sequences' ? seqMonitor(false) : adminPage()}
      </div>
      ${S.detail && S.page !== 'admin' ? drawerHTML() : ''}
      ${S.diagOpen ? diagHTML() : ''}
      ${S.logMax ? logMaxHTML() : ''}
      ${S.testSetup ? testSetupHTML() : ''}
    </div>`;
  }

  function testSetupHTML() {
    const ro = readOnly();
    const PROFS = [['udp_symmetric', 'UDP-Sym'], ['udp_dynamic', 'UDP-Dyn'], ['tcp', 'TCP'], ['icmp', 'ICMP']];
    return `<div style="position:absolute;inset:0;background:#05070ad9;backdrop-filter:blur(3px);display:flex;align-items:center;justify-content:center;z-index:55">
      <div id="nm-test-setup" style="width:460px;max-width:calc(100% - 36px);background:linear-gradient(180deg,#10161f,#0b0f15);border:1px solid #233044;border-radius:12px;box-shadow:0 30px 80px #000c;padding:24px;animation:nm-rise .2s ease">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px">
          <div style="width:28px;height:28px;border-radius:7px;background:linear-gradient(135deg,#1f6feb,#0b3a8a);display:flex;align-items:center;justify-content:center;color:#cfe0ff">▷</div>
          <div style="font:700 15px 'IBM Plex Sans';color:#eef3fa">Set up data-plane test</div>
          <div style="flex:1"></div>
          <button data-act="closeTestSetup" style="background:transparent;border:none;color:#6b7888;font-size:16px;cursor:pointer">✕</button>
        </div>

        <div style="font:600 9px 'IBM Plex Mono';letter-spacing:.13em;color:#5d6a7c;margin-bottom:6px">DATA-PLANE PORT</div>
        <input id="nm-test-port" type="number" min="0" max="65535" value="${S.testPort || ''}" placeholder="0 = each agent's default data port" style="width:100%;background:#0a0e14;border:1px solid #233044;border-radius:7px;color:#eef3fa;font:500 13px 'IBM Plex Mono';padding:10px 12px;outline:none;margin-bottom:4px"/>
        <div style="font:500 9px 'IBM Plex Mono';color:#465061;margin-bottom:16px">agents bind this port and probe each other on it; <b style="color:#7b8898">0</b> uses each agent's own data port</div>

        <div style="font:600 9px 'IBM Plex Mono';letter-spacing:.13em;color:#5d6a7c;margin-bottom:7px">TRAFFIC PROFILES</div>
        <div style="display:flex;gap:7px;flex-wrap:wrap;margin-bottom:16px">
          ${PROFS.map(([p, lbl]) => `<label style="display:inline-flex;align-items:center;gap:5px;font:500 11px 'IBM Plex Mono';color:#9fb1c6;background:#0a0e14;border:1px solid #1c2632;border-radius:6px;padding:7px 10px;cursor:pointer"><input type="checkbox" class="nm-test-proto" data-prof="${p}" ${(!S.testProtocols || S.testProtocols.has(p)) ? 'checked' : ''}/>${lbl}</label>`).join('')}
        </div>

        <div style="font:600 9px 'IBM Plex Mono';letter-spacing:.13em;color:#5d6a7c;margin-bottom:6px">TX INTERVAL (ms)</div>
        <input id="nm-test-interval" type="number" min="50" max="5000" value="${S.txInterval}" style="width:100%;background:#0a0e14;border:1px solid #233044;border-radius:7px;color:#eef3fa;font:500 13px 'IBM Plex Mono';padding:10px 12px;outline:none;margin-bottom:18px"/>

        <button data-act="runTest" ${ro ? 'disabled' : ''} style="width:100%;font:600 13px 'IBM Plex Sans';color:#fff;background:${ro ? '#1c2632' : '#1f6feb'};border:none;padding:12px;border-radius:8px;cursor:${ro ? 'not-allowed' : 'pointer'};box-shadow:0 4px 16px #1f6feb55">▷ Start test</button>
      </div>
    </div>`;
  }

  function controllerDash() {
    return `<div style="flex:1;min-height:0;display:flex;flex-direction:column;gap:10px">
      <div style="flex:0 0 auto;display:flex;gap:10px;align-items:stretch">
        <div id="nm-kpis" style="display:flex;gap:10px;flex:1"></div>
        <div style="flex:1.5;background:linear-gradient(180deg,#0f141d,#0b0f16);border:1px solid #1a2330;border-radius:8px;padding:11px 16px;display:flex;flex-direction:column;justify-content:center;gap:9px">
          <div style="display:flex;justify-content:space-between;align-items:center">
            <div style="font:600 9.5px/1 'IBM Plex Mono';letter-spacing:.13em;color:#5d6a7c">DATA-PLANE TX INTERVAL</div>
            <div style="font:600 14px 'IBM Plex Mono';color:#58a6ff"><span id="nm-tx">${S.txInterval}</span><span style="font-size:10px;color:#566273"> ms</span></div>
          </div>
          <input type="range" class="nm-slider" data-act="setTx" min="50" max="1000" step="10" value="${S.txInterval}" ${readOnly() ? 'disabled' : ''} style="width:100%"/>
          <div style="display:flex;justify-content:space-between;font:500 9px 'IBM Plex Mono';color:#465061"><span>50ms · aggressive</span><span id="nm-txrate">${Math.round(1000 / S.txInterval)} pkt/s/link</span><span>1000ms · idle</span></div>
        </div>
      </div>
      <div style="flex:1.15;min-height:0;display:flex;gap:10px">
        <div style="flex:1.7;min-width:0;background:radial-gradient(120% 120% at 50% 40%,#0d1320,#0a0e14);border:1px solid #1a2330;border-radius:8px;position:relative;overflow:hidden">
          <div style="position:absolute;top:11px;left:14px;z-index:3"><div style="font:600 10px/1 'IBM Plex Mono';letter-spacing:.14em;color:#8b98a8">DATA-PLANE TEST MESH</div><div style="font:500 9px 'IBM Plex Mono';color:#465061;margin-top:4px">node ↔ node tests · live packets · per-link health</div></div>
          <div id="nm-topolegend" style="position:absolute;top:10px;right:12px;z-index:3;display:flex;gap:12px;font:500 9.5px 'IBM Plex Mono';color:#7b8898"></div>
          <div style="position:absolute;bottom:10px;left:14px;z-index:3;font:500 9px 'IBM Plex Mono';color:#566273">Click an agent node to open remote diagnostics →</div>
          <canvas id="nm-topo" data-act="topoClick" style="position:absolute;inset:0;width:100%;height:100%;cursor:pointer"></canvas>
        </div>
        <div style="flex:1;min-width:0;display:flex;flex-direction:column;gap:10px">
          ${chartCard('LATENCY · JITTER', 'nm-lat', `<span style="color:#58a6ff">■ lat <span id="nm-curlat">0</span>ms</span><span style="color:#d29922">■ jit <span id="nm-curjit">0</span>ms</span>`)}
          ${chartCard('PACKET LOSS %', 'nm-loss', `<span id="nm-curloss" style="color:#3fb950">■ 0% global</span>`)}
        </div>
      </div>
      <div style="flex:1;min-height:0;display:flex;gap:10px">
        ${gridHTML()}
        ${eventLogHTML()}
      </div>
    </div>`;
  }

  function chartCard(title, canvasId, legend) {
    return `<div style="flex:1;min-height:0;background:linear-gradient(180deg,#0f141d,#0a0e14);border:1px solid #1a2330;border-radius:8px;position:relative;padding:11px 12px 6px;display:flex;flex-direction:column">
      <div style="display:flex;justify-content:space-between;align-items:center"><div style="font:600 10px/1 'IBM Plex Mono';letter-spacing:.13em;color:#6b7888">${title}</div><div style="display:flex;gap:11px;font:500 9.5px 'IBM Plex Mono'">${legend}</div></div>
      <canvas id="${canvasId}" style="flex:1;width:100%;min-height:0"></canvas>
    </div>`;
  }

  function gridHTML() {
    const cols = [['src', 'SOURCE'], ['dst', 'DEST'], ['profile', 'PROFILE'], ['port', 'PORT'], ['latency', 'LAT ms'], ['jitter', 'JIT ms'], ['loss', 'LOSS %'], ['status', 'STATUS']];
    const filters = ['ALL', 'UDP-Sym', 'UDP-Dyn', 'TCP', 'ICMP'];
    return `<div style="flex:1.5;min-width:0;background:#0c1118;border:1px solid #1a2330;border-radius:8px;display:flex;flex-direction:column;overflow:hidden">
      <div style="flex:0 0 auto;display:flex;align-items:center;gap:10px;padding:9px 12px;border-bottom:1px solid #161e29">
        <div style="font:600 10px/1 'IBM Plex Mono';letter-spacing:.13em;color:#6b7888">TELEMETRY · <span id="nm-rowcount">0</span> FLOWS</div>
        <span id="nm-nodechip"></span>
        <div style="flex:1"></div>
        <div style="display:flex;gap:3px">${filters.map((f) => `<button data-act="filter" data-arg="${f}" style="${filterBtn(f)}">${f}</button>`).join('')}</div>
        <input data-act="setQuery" value="${esc(S.query)}" placeholder="filter node…" style="background:#0a0e14;border:1px solid #1c2632;border-radius:5px;color:#c4cfdd;font:500 10px 'IBM Plex Mono';padding:5px 8px;width:108px;outline:none"/>
      </div>
      <div style="flex:1;min-height:0;overflow:auto">
        <table style="width:100%;border-collapse:collapse;font:500 11px 'IBM Plex Mono'">
          <thead style="position:sticky;top:0;background:#0d131c;z-index:2"><tr style="color:#5d6a7c;text-align:left">
            ${cols.map((c) => `<th data-act="sort" data-arg="${c[0]}" style="padding:7px 10px;font:600 9px 'IBM Plex Mono';letter-spacing:.08em;white-space:nowrap;border-bottom:1px solid #1a2330;user-select:none">${c[1]}${S.sortKey === c[0] ? (S.sortDir === 'asc' ? ' ↑' : ' ↓') : ''}</th>`).join('')}
          </tr></thead>
          <tbody id="nm-rows"></tbody>
        </table>
      </div>
    </div>`;
  }

  const LOG_TAGS = ['INFO', 'WARN', 'ERR', 'TEST', 'ADMIN'];
  const TAG_COLOR = { INFO: P.accent, WARN: P.yellow, ERR: P.red, TEST: P.accent, ADMIN: P.accent };

  function levelChips(compact) {
    return LOG_TAGS.map((t) => {
      const on = S.logLevels.has(t); const c = TAG_COLOR[t] || P.accent;
      return `<button data-act="toggleLevel" data-arg="${t}" title="${t}" style="font:600 8.5px 'IBM Plex Mono';letter-spacing:.03em;padding:${compact ? '3px 4px' : '4px 9px'};border-radius:4px;border:1px solid ${on ? c + '66' : '#1c2632'};background:${on ? c + '1f' : 'transparent'};color:${on ? c : '#566273'};cursor:pointer">${compact ? t[0] : t}</button>`;
    }).join('');
  }
  function pauseBtnStyle() {
    const on = S.logPaused;
    return `font:600 11px 'IBM Plex Mono';padding:4px 8px;border-radius:5px;border:1px solid ${on ? '#d2992266' : '#243043'};background:${on ? '#d299221f' : '#0d141d'};color:${on ? '#d29922' : '#9fb1c6'};cursor:pointer`;
  }

  function eventLogHTML() {
    return `<div style="flex:1;min-width:0;max-width:460px;background:#070a0e;border:1px solid #1a2330;border-radius:8px;display:flex;flex-direction:column;overflow:hidden">
      <div style="flex:0 0 auto;display:flex;align-items:center;gap:6px;padding:7px 10px;border-bottom:1px solid #161e29">
        <span style="width:7px;height:7px;border-radius:50%;background:#3fb950;box-shadow:0 0 6px #3fb950;animation:nm-blink 1.4s infinite;flex:0 0 auto"></span>
        <div style="font:600 9px/1 'IBM Plex Mono';letter-spacing:.08em;color:#6b7888;flex:0 0 auto">EVENTS</div>
        <div id="nm-loglevels" style="display:flex;gap:2px">${levelChips(true)}</div>
        <input data-act="logQuery" value="${esc(S.logQuery)}" placeholder="filter…" style="flex:1;min-width:36px;background:#0a0e14;border:1px solid #1c2632;border-radius:5px;color:#c4cfdd;font:500 10px 'IBM Plex Mono';padding:4px 7px;outline:none"/>
        <button id="nm-pause" data-act="toggleLogPause" title="Pause / resume tail" style="${pauseBtnStyle()}">${S.logPaused ? '▶' : '⏸'}</button>
        <button data-act="maximizeLog" title="Maximize" style="font:600 11px 'IBM Plex Mono';padding:4px 7px;border-radius:5px;border:1px solid #243043;background:#0d141d;color:#9fb1c6;cursor:pointer">⛶</button>
      </div>
      <div style="position:relative;flex:1;min-height:0">
        <div id="nm-events" style="position:absolute;inset:0;overflow:auto;padding:8px 11px;font:500 11px/1.55 'IBM Plex Mono'"></div>
        <div id="nm-jump" style="display:none;position:absolute;left:0;right:0;bottom:8px;text-align:center;pointer-events:none"></div>
      </div>
    </div>`;
  }

  function logMaxHTML() {
    return `<div style="position:absolute;inset:14px;background:#070a0e;border:1px solid #233044;border-radius:10px;box-shadow:0 30px 80px #000c;z-index:60;display:flex;flex-direction:column;overflow:hidden;animation:nm-rise .16s ease">
      <div style="flex:0 0 auto;display:flex;align-items:center;gap:10px;padding:11px 14px;border-bottom:1px solid #161e29">
        <span style="width:8px;height:8px;border-radius:50%;background:#3fb950;box-shadow:0 0 6px #3fb950;animation:nm-blink 1.4s infinite"></span>
        <div style="font:600 11px 'IBM Plex Mono';letter-spacing:.13em;color:#8b98a8">EVENT &amp; AUDIT LOG</div>
        <div id="nm-loglevels-max" style="display:flex;gap:4px">${levelChips(false)}</div>
        <input data-act="logQuery" value="${esc(S.logQuery)}" placeholder="filter messages…" style="width:240px;background:#0a0e14;border:1px solid #1c2632;border-radius:5px;color:#c4cfdd;font:500 11px 'IBM Plex Mono';padding:6px 9px;outline:none"/>
        <div style="flex:1"></div>
        <button id="nm-pause-max" data-act="toggleLogPause" style="${pauseBtnStyle()}">${S.logPaused ? '▶ Resume' : '⏸ Pause'}</button>
        <button data-act="clearLog" style="font:600 10px 'IBM Plex Mono';padding:6px 11px;border-radius:6px;border:1px solid #243043;background:#0d141d;color:#9fb1c6;cursor:pointer">Clear</button>
        <button data-act="closeLogMax" style="font:600 11px 'IBM Plex Mono';padding:6px 11px;border-radius:6px;border:1px solid #243043;background:#0d141d;color:#9fb1c6;cursor:pointer">✕ Close</button>
      </div>
      <div style="position:relative;flex:1;min-height:0">
        <div id="nm-events-max" style="position:absolute;inset:0;overflow:auto;padding:12px 16px;font:500 12px/1.7 'IBM Plex Mono'"></div>
        <div id="nm-jump-max" style="display:none;position:absolute;left:0;right:0;bottom:14px;text-align:center;pointer-events:none"></div>
      </div>
    </div>`;
  }

  // ---------- log rendering (filter + pause aware) ----------
  function filteredEvents() {
    const q = (S.logQuery || '').trim().toLowerCase();
    return S.events.filter((e) => S.logLevels.has(e.tag) && (!q || e.msg.toLowerCase().includes(q) || e.tag.toLowerCase().includes(q)));
  }
  function eventRowsHTML(list) {
    if (!list.length) return `<div style="color:#465061;text-align:center;padding:22px">no matching events</div>`;
    return list.map((e) => `<div style="display:flex;gap:9px;animation:nm-rise .2s ease"><span style="color:#3c4757;flex:0 0 auto">${e.time}</span><span style="color:${e.color};font-weight:600;flex:0 0 42px">${e.tag}</span><span style="color:#8b98a8;word-break:break-word">${esc(e.msg)}</span></div>`).join('');
  }
  function renderLogRows(id, limit, scrollBottom) {
    const el = document.getElementById(id); if (!el) return;
    const list = filteredEvents();
    S._logProg = true;
    el.innerHTML = eventRowsHTML(limit ? list.slice(-limit) : list);
    if (scrollBottom) el.scrollTop = el.scrollHeight;
    S._logProg = false;
  }
  function updateEvents() {
    if (!S.logPaused) {
      renderLogRows('nm-events', 150, true);
      if (S.logMax) renderLogRows('nm-events-max', 0, true);
    }
    updateLogChrome();
  }
  // populateLog fills the log on a fresh structure render (so a rebuild while
  // paused doesn't leave it empty); it shows the current snapshot.
  function populateLog() {
    renderLogRows('nm-events', 150, !S.logPaused);
    if (S.logMax) renderLogRows('nm-events-max', 0, !S.logPaused);
    updateLogChrome();
  }
  function forceLogRefresh() { // filter changed: rebuild even if paused
    renderLogRows('nm-events', 150, !S.logPaused);
    if (S.logMax) renderLogRows('nm-events-max', 0, !S.logPaused);
    S.logNew = 0;
    refreshLevelChips();
    updateLogChrome();
  }
  function resumeLog() {
    S.logPaused = false; S.logNew = 0;
    renderLogRows('nm-events', 150, true);
    if (S.logMax) renderLogRows('nm-events-max', 0, true);
    updateLogChrome();
  }
  function refreshLevelChips() {
    const a = document.getElementById('nm-loglevels'); if (a) a.innerHTML = levelChips(true);
    const b = document.getElementById('nm-loglevels-max'); if (b) b.innerHTML = levelChips(false);
  }
  function updateLogChrome() {
    const pb = document.getElementById('nm-pause'); if (pb) { pb.textContent = S.logPaused ? '▶' : '⏸'; pb.setAttribute('style', pauseBtnStyle()); }
    const pbm = document.getElementById('nm-pause-max'); if (pbm) { pbm.textContent = S.logPaused ? '▶ Resume' : '⏸ Pause'; pbm.setAttribute('style', pauseBtnStyle()); }
    ['nm-jump', 'nm-jump-max'].forEach((id) => {
      const el = document.getElementById(id); if (!el) return;
      if (S.logPaused) {
        el.innerHTML = `<button data-act="jumpLatest" style="font:600 10px 'IBM Plex Mono';padding:6px 14px;border-radius:20px;border:1px solid #2f81f7;background:#1f6febee;color:#fff;cursor:pointer;box-shadow:0 4px 14px #0009;pointer-events:auto">↓ ${S.logNew > 0 ? S.logNew + ' new' : 'jump to latest'}</button>`;
        el.style.display = 'block';
      } else { el.innerHTML = ''; el.style.display = 'none'; }
    });
  }
  function bindLog() {
    ['nm-events', 'nm-events-max'].forEach((id) => {
      const el = document.getElementById(id); if (!el) return;
      el.addEventListener('scroll', () => {
        if (S._logProg) return; // ignore our own programmatic scrolls
        const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 24;
        if (atBottom && S.logPaused) resumeLog();
        else if (!atBottom && !S.logPaused) { S.logPaused = true; updateLogChrome(); }
      });
    });
  }

  // ---------- sequence monitor ----------
  function seqMonitor(isAgentLocal) {
    const summary = isAgentLocal ? '' : `<div id="nm-seqsummary" style="flex:0 0 auto;display:flex;gap:10px"></div>`;
    const title = isAgentLocal ? `LOCAL PACKET SEQUENCES` : `PACKET SEQUENCE MONITOR`;
    return `<div style="flex:1;min-height:0;display:flex;flex-direction:column;gap:10px">
      ${summary}
      <div style="flex:1;min-height:0;background:#0c1118;border:1px solid #1a2330;border-radius:8px;display:flex;flex-direction:column;overflow:hidden">
        <div style="flex:0 0 auto;display:flex;align-items:center;gap:12px;padding:9px 12px;border-bottom:1px solid #161e29">
          <span style="width:7px;height:7px;border-radius:50%;background:#3fb950;box-shadow:0 0 6px #3fb950;animation:nm-blink 1.4s infinite"></span>
          <div style="font:600 10px 'IBM Plex Mono';letter-spacing:.13em;color:#6b7888">${title} · <span id="nm-seqcount">0</span> TESTS · live</div>
          <div style="flex:1"></div>
          <div style="display:flex;gap:13px;font:500 9.5px 'IBM Plex Mono';color:#7b8898">
            <span style="display:flex;align-items:center;gap:5px"><span style="width:9px;height:9px;border-radius:2px;background:${P.green}"></span>received</span>
            <span style="display:flex;align-items:center;gap:5px"><span style="width:9px;height:9px;border-radius:2px;background:${P.yellow}"></span>reordered</span>
            <span style="display:flex;align-items:center;gap:5px"><span style="width:9px;height:9px;border-radius:2px;background:${P.red}"></span>missed</span>
          </div>
        </div>
        <div id="nm-seqlist" style="flex:1;min-height:0;overflow:auto;padding:7px 9px"></div>
      </div>
    </div>`;
  }

  // ---------- admin page ----------
  function adminPage() {
    if (readOnly()) {
      return `<div style="flex:1;display:flex;align-items:center;justify-content:center">
        <div style="text-align:center;max-width:380px">
          <div style="font-size:26px;margin-bottom:10px">🔒</div>
          <div style="font:600 14px 'IBM Plex Sans';color:#cdd9e5;margin-bottom:8px">Agent administration is restricted</div>
          <div style="font:500 11px/1.5 'IBM Plex Mono';color:#566273;margin-bottom:18px">Log in to rename agents, manage groups, toggle traffic profiles, and evict nodes.</div>
          <button data-act="login" style="font:600 12px 'IBM Plex Sans';color:#fff;background:#1f6feb;border:none;padding:10px 20px;border-radius:7px;box-shadow:0 2px 8px #1f6feb55">Login</button>
        </div></div>`;
    }
    return `<div style="flex:1;min-height:0;display:flex;flex-direction:column;gap:10px">
      <div style="flex:0 0 auto;display:flex;align-items:center;gap:10px">
        <div style="font:600 11px 'IBM Plex Mono';letter-spacing:.13em;color:#8b98a8">AGENT ADMINISTRATION</div>
        <div style="font:500 10px 'IBM Plex Mono';color:#566273">rename · group · enable/disable · per-agent profiles · evict</div>
        <div style="flex:1"></div>
        <div style="font:500 10px 'IBM Plex Mono';color:#465061">${S.running ? 'edits re-apply to the running test' : 'edits apply on next START'}</div>
        <button data-act="refreshAdmin" style="font:600 10px 'IBM Plex Mono';padding:6px 11px;border-radius:6px;border:1px solid #243043;background:#0d141d;color:#9fb1c6">↻ Refresh</button>
      </div>
      <div style="flex:1;min-height:0;overflow:auto;background:#0c1118;border:1px solid #1a2330;border-radius:8px">
        <table style="width:100%;border-collapse:collapse;font:500 11px 'IBM Plex Mono'">
          <thead style="position:sticky;top:0;background:#0d131c;z-index:2"><tr style="color:#5d6a7c;text-align:left">
            ${['FRIENDLY NAME', 'AGENT ID', 'GROUP', 'DATA PORT', 'WS RTT', 'STATE', 'TRAFFIC PROFILES', 'ACTIONS'].map((h) => `<th style="padding:9px 10px;font:600 9px 'IBM Plex Mono';letter-spacing:.08em;border-bottom:1px solid #1a2330;white-space:nowrap">${h}</th>`).join('')}
          </tr></thead>
          <tbody id="nm-admin-rows">${adminRowsHTML()}</tbody>
        </table>
      </div>
    </div>`;
  }

  function adminRowsHTML() {
    if (!S.agents.length) return `<tr><td colspan="8" style="padding:22px;text-align:center;color:#465061">no agents connected</td></tr>`;
    const PROFS = [['udp_symmetric', 'UDP-Sym'], ['udp_dynamic', 'UDP-Dyn'], ['tcp', 'TCP'], ['icmp', 'ICMP']];
    return S.agents.map((a) => {
      const profs = a.profiles || ['udp_symmetric', 'udp_dynamic', 'tcp', 'icmp'];
      const enabled = a.enabled !== false;
      const s = statusOf((a.wsRttUs || 0) / 1000, 0);
      const profBoxes = PROFS.map(([p, lbl]) => `<label style="display:inline-flex;align-items:center;gap:3px;margin-right:9px;color:#9fb1c6;cursor:pointer"><input type="checkbox" class="nm-prof" data-prof="${p}" ${profs.includes(p) ? 'checked' : ''}/>${lbl}</label>`).join('');
      return `<tr data-agent="${esc(a.id)}" style="border-bottom:1px solid #11181f;${enabled ? '' : 'opacity:.55'}">
        <td style="padding:7px 10px"><input class="nm-label" value="${esc(a.label || '')}" placeholder="${esc(a.id)}" style="background:#0a0e14;border:1px solid #1c2632;border-radius:5px;color:#eef3fa;font:500 11px 'IBM Plex Mono';padding:6px 8px;width:140px;outline:none"/></td>
        <td style="padding:7px 10px;color:#8b98a8;white-space:nowrap">${esc(a.id)}</td>
        <td style="padding:7px 10px"><input class="nm-group" value="${esc(a.group || '')}" placeholder="—" style="background:#0a0e14;border:1px solid #1c2632;border-radius:5px;color:#cdd9e5;font:500 11px 'IBM Plex Mono';padding:6px 8px;width:84px;outline:none"/></td>
        <td style="padding:7px 10px;color:#7b8898">${a.dataPort || '—'}${a.testPort ? `<span title="active test port: UDP ${a.portUdp ? 'ok' : 'fail'} / TCP ${a.portTcp ? 'ok' : 'fail'}" style="margin-left:7px;font-size:10px;color:${a.portUdp && a.portTcp ? '#7ee787' : '#d29922'}">▸${a.testPort} ${a.portUdp && a.portTcp ? '✓' : '!'}</span>` : ''}</td>
        <td style="padding:7px 10px;color:#cdd9e5">${((a.wsRttUs || 0) / 1000).toFixed(1)}ms</td>
        <td style="padding:7px 10px"><label style="display:inline-flex;align-items:center;gap:6px;cursor:pointer;color:${enabled ? colorFor(s) : '#566273'}"><input type="checkbox" class="nm-enabled" ${enabled ? 'checked' : ''}/><span style="width:6px;height:6px;border-radius:50%;background:${enabled ? colorFor(s) : '#3a4757'};${enabled ? 'box-shadow:0 0 5px ' + colorFor(s) : ''}"></span>${enabled ? 'enabled' : 'disabled'}</label></td>
        <td style="padding:7px 10px;white-space:nowrap">${profBoxes}</td>
        <td style="padding:7px 10px;white-space:nowrap"><button data-act="saveAgent" data-arg="${esc(a.id)}" style="font:600 10px 'IBM Plex Mono';padding:6px 11px;border-radius:5px;border:1px solid #2f6b3a;background:#13351f;color:#7ee787;margin-right:6px;cursor:pointer">Save</button><button data-act="evictAgent" data-arg="${esc(a.id)}" style="font:600 10px 'IBM Plex Mono';padding:6px 11px;border-radius:5px;border:1px solid #5a2730;background:#2a1418;color:#f0a3a3;cursor:pointer">Evict</button></td>
      </tr>`;
    }).join('');
  }

  function updateAdminRows() {
    const el = document.getElementById('nm-admin-rows');
    if (el) el.innerHTML = adminRowsHTML();
  }

  function adminRow(id) {
    const sel = (window.CSS && CSS.escape) ? CSS.escape(id) : id.replace(/"/g, '\\"');
    return document.querySelector(`[data-agent="${sel}"]`);
  }

  // ---------- agent layout ----------
  function agentHTML() {
    return `${headerHTML()}
    <div style="flex:1;min-height:0;position:relative">
      <div style="position:absolute;inset:0;display:flex;flex-direction:column;gap:10px;padding:12px;overflow:auto">
        <div style="flex:0 0 auto;display:flex;gap:5px">
          <button data-act="goDash" style="${subTab(S.page === 'dashboard')}">▦ Node Dashboard</button>
          <button data-act="goSeq" style="${subTab(S.page === 'sequences')}">⠿ Sequence Monitor</button>
        </div>
        ${S.page === 'dashboard' ? agentDash() : seqMonitor(true)}
      </div>
      ${(!infoJoined()) ? joinModal() : ''}
    </div>`;
  }

  function agentDash() {
    return `<div style="flex:1;min-height:0;display:flex;flex-direction:column;gap:10px">
      <div style="flex:0 0 auto;display:flex;gap:10px">
        <div style="flex:1.3;background:linear-gradient(110deg,#0f141d,#0a0e14);border:1px solid #1a2330;border-radius:8px;padding:14px 16px;display:flex;align-items:center;gap:18px">
          <div style="display:flex;align-items:center;gap:12px">
            <div style="width:40px;height:40px;border-radius:9px;background:#0a0e14;border:1px solid #233044;display:flex;align-items:center;justify-content:center"><span style="width:13px;height:13px;border:2px solid #58a6ff;border-radius:50%;position:relative"><span style="position:absolute;inset:3px;background:#58a6ff;border-radius:50%"></span></span></div>
            <div><div style="font:600 14px 'IBM Plex Sans';color:#eef3fa">${esc(S.info.agentId || S.info.host)}</div><div style="font:500 10px 'IBM Plex Mono';color:#566273">${esc(S.info.hostAddr)} · agent</div></div>
          </div>
          <div style="width:1px;height:38px;background:#1b2430"></div>
          <div><div style="font:600 9px 'IBM Plex Mono';letter-spacing:.12em;color:#5d6a7c">MASTER CONTROLLER</div><div style="font:600 13px 'IBM Plex Mono';color:#cdd9e5;margin-top:4px">${esc(S.info.master || '—')}</div></div>
          <div style="flex:1"></div>
        </div>
        <div style="flex:1;background:linear-gradient(110deg,#0f141d,#0a0e14);border:1px solid #1a2330;border-radius:8px;padding:14px 16px;display:flex;align-items:center;gap:14px">
          <div style="position:relative;width:34px;height:34px;flex:0 0 auto">
            <span style="position:absolute;inset:0;border-radius:50%;background:${(S.info.connected ? '#3fb950' : '#f85149')}33;animation:nm-pulse 1.8s infinite"></span>
            <span style="position:absolute;inset:9px;border-radius:50%;background:${S.info.connected ? '#3fb950' : '#f85149'};box-shadow:0 0 10px ${S.info.connected ? '#3fb950' : '#f85149'}"></span>
          </div>
          <div>
            <div style="font:600 9px 'IBM Plex Mono';letter-spacing:.12em;color:#5d6a7c">WEBSOCKET LINK</div>
            <div id="nm-wslink" style="font:600 14px 'IBM Plex Sans';color:${S.info.connected ? '#3fb950' : '#f85149'};margin-top:3px">${S.info.connected ? 'Healthy · ' + ((S.info.wsRttUs || 0) / 1000).toFixed(1) + 'ms' : 'Disconnected'}</div>
            <div id="nm-wsframes" style="font:500 10px 'IBM Plex Mono';color:#566273;margin-top:2px">${(S.info.framesRx || 0) + (S.info.framesTx || 0)} frames</div>
          </div>
        </div>
      </div>
      <div id="nm-kpis" style="flex:0 0 auto;display:flex;gap:10px"></div>
      <div style="flex:0 0 190px;background:linear-gradient(180deg,#0f141d,#0a0e14);border:1px solid #1a2330;border-radius:8px;padding:11px 12px 6px;display:flex;flex-direction:column">
        <div style="display:flex;justify-content:space-between"><div style="font:600 10px 'IBM Plex Mono';letter-spacing:.13em;color:#6b7888">LOCAL LATENCY · JITTER (this node)</div><div style="display:flex;gap:11px;font:500 9.5px 'IBM Plex Mono'"><span style="color:#58a6ff">■ lat <span id="nm-curlat">0</span>ms</span><span style="color:#d29922">■ jit <span id="nm-curjit">0</span>ms</span></div></div>
        <canvas id="nm-lat" style="flex:1;width:100%;min-height:0"></canvas>
      </div>
      <div style="flex:1;min-height:200px;background:#0c1118;border:1px solid #1a2330;border-radius:8px;display:flex;flex-direction:column;overflow:hidden">
        <div style="flex:0 0 auto;padding:9px 12px;border-bottom:1px solid #161e29;font:600 10px 'IBM Plex Mono';letter-spacing:.13em;color:#6b7888">LOCAL FLOWS · originating from or targeting ${esc(S.info.agentId || 'node')}</div>
        <div style="flex:1;min-height:0;overflow:auto">
          <table style="width:100%;border-collapse:collapse;font:500 11px 'IBM Plex Mono'">
            <thead style="position:sticky;top:0;background:#0d131c"><tr style="color:#5d6a7c;text-align:left">
              ${['SOURCE', 'DEST', 'PROFILE', 'LAT', 'JIT', 'LOSS', 'STATUS'].map((h) => `<th style="padding:7px 10px;font:600 9px 'IBM Plex Mono';letter-spacing:.08em;border-bottom:1px solid #1a2330">${h}</th>`).join('')}
            </tr></thead>
            <tbody id="nm-rows"></tbody>
          </table>
        </div>
      </div>
    </div>`;
  }

  function joinModal() {
    return `<div style="position:absolute;inset:0;background:#05070ad9;backdrop-filter:blur(3px);display:flex;align-items:center;justify-content:center;z-index:50">
      <div style="width:430px;max-width:calc(100% - 36px);background:linear-gradient(180deg,#10161f,#0b0f15);border:1px solid #233044;border-radius:12px;box-shadow:0 30px 80px #000c;padding:26px;animation:nm-rise .2s ease">
        <div style="display:flex;align-items:center;gap:11px;margin-bottom:6px"><div style="width:30px;height:30px;border-radius:7px;background:linear-gradient(135deg,#1f6feb,#0b3a8a);display:flex;align-items:center;justify-content:center"><span style="width:11px;height:11px;border:2px solid #cfe0ff;border-radius:50%"></span></div><div style="font:700 16px 'IBM Plex Sans';color:#eef3fa">Join a Master Controller</div></div>
        <div style="font:500 12px/1.5 'IBM Plex Sans';color:#7b8898;margin-bottom:18px">This node (${esc(S.info.agentId || S.info.host)}) is not yet attached to a cluster. Enter the controller address to establish the data-plane and WebSocket control link.</div>
        <div style="font:600 9px 'IBM Plex Mono';letter-spacing:.13em;color:#5d6a7c;margin-bottom:7px">MASTER CONTROLLER IP / HOSTNAME</div>
        <input id="nm-joininput" placeholder="10.10.10.5" style="width:100%;background:#0a0e14;border:1px solid #233044;border-radius:7px;color:#eef3fa;font:500 13px 'IBM Plex Mono';padding:11px 13px;outline:none;margin-bottom:7px"/>
        <div style="font:500 10px 'IBM Plex Mono';color:#465061;margin-bottom:18px">default port :${S.info.port} · joins via local node only</div>
        <button data-act="joinMaster" style="width:100%;font:600 13px 'IBM Plex Sans';color:#fff;background:#1f6feb;border:none;padding:12px;border-radius:8px;box-shadow:0 4px 16px #1f6feb55">Connect to Controller →</button>
      </div>
    </div>`;
  }

  // ---------- drawer (node / test detail) ----------
  function drawerHTML() {
    return `<div style="position:absolute;top:0;right:0;bottom:0;width:474px;max-width:calc(100% - 24px);background:linear-gradient(180deg,#0c1119,#080b10);border-left:1px solid #1f2a37;box-shadow:-26px 0 60px #0009;z-index:45;display:flex;flex-direction:column;animation:nm-rise .18s ease">
      ${S.detail.mode === 'node' ? '<div id="nm-nodedetail" style="display:flex;flex-direction:column;min-height:0;flex:1"></div>' : '<div id="nm-testdetail" style="display:flex;flex-direction:column;min-height:0;flex:1"></div>'}
    </div>`;
  }

  // ---------- diagnostics terminal ----------
  function diagHTML() {
    const cmds = (S.info.diagCommands || []).map((c) => c.charAt(0).toUpperCase() + c.slice(1));
    const ro = readOnly();
    return `<div style="position:absolute;right:22px;bottom:22px;width:520px;max-width:calc(100% - 44px);background:#05080b;border:1px solid #233044;border-radius:9px;box-shadow:0 24px 60px #000a,0 0 0 1px #1f6feb22;z-index:40;overflow:hidden;animation:nm-rise .18s ease">
      <div style="display:flex;align-items:center;gap:9px;padding:9px 12px;background:linear-gradient(180deg,#0d141d,#0a0f16);border-bottom:1px solid #1c2735">
        <span style="display:flex;gap:5px"><span style="width:10px;height:10px;border-radius:50%;background:#f85149"></span><span style="width:10px;height:10px;border-radius:50%;background:#d29922"></span><span style="width:10px;height:10px;border-radius:50%;background:#3fb950"></span></span>
        <div style="font:600 11px 'IBM Plex Mono';color:#cdd9e5">remote@${esc(S.diagAgent)}</div>
        <span style="font:500 9px 'IBM Plex Mono';color:#465061;letter-spacing:.1em">SECURE CHANNEL · WHITELISTED</span>
        <div style="flex:1"></div>
        <button data-act="closeDiag" style="background:transparent;border:none;color:#6b7888;font-size:15px;line-height:1">✕</button>
      </div>
      <div id="nm-term" style="height:210px;overflow:auto;padding:10px 13px;font:500 11.5px/1.6 'IBM Plex Mono';color:#9fe6b0;background:#05080b"></div>
      <div style="display:flex;align-items:center;gap:6px;padding:9px 12px;background:#0a0f16;border-top:1px solid #1c2735">
        <span style="font:600 10px 'IBM Plex Mono';color:#566273">cmd:</span>
        ${cmds.map((c) => `<button data-act="runDiag" data-arg="${c.toLowerCase()}" ${ro ? 'disabled' : ''} style="font:600 10px 'IBM Plex Mono';padding:5px 10px;border-radius:5px;border:1px solid #233044;background:#0d141d;color:${ro ? '#3a4757' : '#9fb1c6'};opacity:${ro ? '.5' : '1'}">${c}</button>`).join('')}
        <div style="flex:1"></div>
        ${ro ? `<span style="font:600 9px 'IBM Plex Mono';color:#d29922;letter-spacing:.1em">🔒 READ-ONLY</span>` : ''}
      </div>
    </div>`;
  }

  // ---------- style helpers ----------
  function subTab(active) { return `display:flex;align-items:center;gap:7px;font:600 10px 'IBM Plex Mono';letter-spacing:.04em;padding:7px 14px;border-radius:6px;border:1px solid ${active ? '#243a57' : '#161e29'};background:${active ? '#101826' : 'transparent'};color:${active ? P.accent : '#6b7888'}`; }
  function filterBtn(f) { const a = S.filter === f; return `font:600 9px 'IBM Plex Mono';letter-spacing:.06em;padding:5px 8px;border-radius:5px;border:1px solid ${a ? '#2f81f7' : '#1c2632'};background:${a ? '#1f6feb22' : 'transparent'};color:${a ? '#7cb7ff' : '#6b7888'}`; }

  // ================= data updates (no structural change) =================
  function renderData() {
    rebuildFlows();
    updateKpis();
    updateGrid();
    updateEvents();
    updateTopoLegend();
    updateCharts();
    updateSeq();
    updateDrawer();
    updateClock();
    if (S.diagOpen) updateDiag();
  }

  function statusRow(f) { const s = statusOf(f.latency, f.loss); return [stName[s], colorFor(s), s]; }

  function decoratedRows() {
    let rows = S.flows.slice();
    if (isController()) {
      if (S.selectedNode) rows = rows.filter((r) => r.src === S.selectedNode || r.dst === S.selectedNode);
      if (S.filter !== 'ALL') rows = rows.filter((r) => r.profile === S.filter);
      if (S.query.trim()) { const q = S.query.toLowerCase(); rows = rows.filter((r) => (r.src || '').toLowerCase().includes(q) || (r.dst || '').toLowerCase().includes(q)); }
      const dir = S.sortDir === 'asc' ? 1 : -1;
      rows.sort((a, b) => { let av = a[S.sortKey], bv = b[S.sortKey]; if (typeof av === 'string') return (av || '').localeCompare(bv || '') * dir; return ((av || 0) - (bv || 0)) * dir; });
    } else {
      const me = S.info.agentId;
      rows = rows.filter((r) => r.src === me || r.dst === me || r.ctrl);
    }
    return rows;
  }

  function rowHTML(r, withPort) {
    const st = statusRow(r); const pm = PROF[r.profile] || ['#8b98a8', '#8b98a844'];
    const lossColor = r.loss > 5 ? P.red : r.loss >= 1 ? P.yellow : '#7b8898';
    const sel = S.detail && S.detail.mode === 'test' && S.detail.flowId === r.id;
    const bg = sel ? P.accent + '1c' : (st[2] === 'red' ? P.red + '10' : 'transparent');
    return `<tr data-act="openTest" data-arg="${esc(r.id)}" style="border-bottom:1px solid #11181f;background:${bg};cursor:pointer">
      <td style="padding:6px 10px;color:#cdd9e5;white-space:nowrap">${esc(r.src)}</td>
      <td style="padding:6px 10px;color:#8b98a8;white-space:nowrap">${esc(r.dst)}</td>
      <td style="padding:6px 10px"><span style="color:${pm[0]};border:1px solid ${pm[1]};padding:1px 6px;border-radius:4px;font-size:10px">${esc(r.profile)}</span></td>
      ${withPort ? `<td style="padding:6px 10px;color:#7b8898">${r.port || '—'}</td>` : ''}
      <td style="padding:6px 10px;color:#cdd9e5;text-align:right">${r.latency.toFixed(1)}</td>
      <td style="padding:6px 10px;color:#9fb1c6;text-align:right">${(r.jitter || 0).toFixed(2)}</td>
      <td style="padding:6px 10px;text-align:right;color:${lossColor}">${(r.loss || 0).toFixed(2)}</td>
      <td style="padding:6px 10px"><span style="display:inline-flex;align-items:center;gap:5px;color:${st[1]}"><span style="width:6px;height:6px;border-radius:50%;background:${st[1]};box-shadow:0 0 5px ${st[1]}"></span>${st[0]}</span></td>
    </tr>`;
  }

  function updateGrid() {
    const tb = document.getElementById('nm-rows'); if (!tb) return;
    const rows = decoratedRows();
    tb.innerHTML = rows.length ? rows.map((r) => rowHTML(r, isController())).join('')
      : `<tr><td colspan="8" style="padding:18px;text-align:center;color:#465061">no flows yet — ${isController() ? 'press START TEST or wait for agents' : 'awaiting test from controller'}</td></tr>`;
    const rc = document.getElementById('nm-rowcount'); if (rc) rc.textContent = rows.length;
    const chip = document.getElementById('nm-nodechip');
    if (chip) chip.innerHTML = S.selectedNode ? `<button data-act="clearNode" style="font:600 9px 'IBM Plex Mono';letter-spacing:.04em;padding:4px 8px;border-radius:5px;border:1px solid #2f81f7;background:#1f6feb22;color:#7cb7ff;display:flex;align-items:center;gap:6px">▣ ${esc(S.selectedNode)}<span style="color:#9fb1c6">✕</span></button>` : '';
  }

  function updateKpis() {
    const el = document.getElementById('nm-kpis'); if (!el) return;
    if (isController()) {
      const ag = S.agents;
      let g = 0, y = 0, rd = 0;
      ag.forEach((a) => { const st = statusOf((a.wsRttUs || 0) / 1000, 0); st === 'green' ? g++ : st === 'yellow' ? y++ : rd++; });
      const dp = S.flows.filter((f) => !f.ctrl);
      const avgLat = ag.length ? ag.reduce((s, a) => s + (a.wsRttUs || 0) / 1000, 0) / ag.length : 0;
      const avgLoss = dp.length ? dp.reduce((s, f) => s + (f.loss || 0), 0) / dp.length : 0;
      const cards = [
        { label: 'ACTIVE AGENTS', value: (ag.length - rd) + '/' + ag.length, delta: rd ? '-' + rd : '●', dc: rd ? P.red : P.green, sub: g + ' healthy · ' + y + ' degraded', accent: P.green },
        { label: 'NODE-TO-NODE TESTS', value: dp.length, delta: 'live', dc: P.accent, sub: S.flows.length + ' total flows', accent: P.accent },
        { label: 'AVG WS LATENCY', value: avgLat.toFixed(1), delta: 'ms', dc: '#566273', sub: 'control-plane rtt', accent: avgLat > 50 ? P.red : avgLat > 40 ? P.yellow : P.green },
        { label: 'GLOBAL LOSS', value: avgLoss.toFixed(2) + '%', delta: avgLoss > 1 ? '▲' : '▼', dc: avgLoss > 1 ? P.yellow : P.green, sub: 'data-plane mean', accent: avgLoss > 5 ? P.red : avgLoss > 1 ? P.yellow : P.green },
      ];
      el.innerHTML = cards.map(kpiCard).join('');
    } else {
      const mine = S.flows.filter((f) => f.src === S.info.agentId || f.dst === S.info.agentId);
      const avgLat = (S.info.wsRttUs || 0) / 1000;
      const avgLoss = mine.length ? mine.reduce((s, f) => s + (f.loss || 0), 0) / mine.length : 0;
      const avgJit = mine.length ? mine.reduce((s, f) => s + (f.jitter || 0), 0) / mine.length : 0;
      const cards = [
        { label: 'NODE LATENCY', value: avgLat.toFixed(1), unit: ' ms', sub: 'rtt to master', accent: avgLat > 50 ? P.red : avgLat > 40 ? P.yellow : P.green },
        { label: 'NODE JITTER', value: avgJit.toFixed(2), unit: ' ms', sub: 'inter-packet delay', accent: P.yellow },
        { label: 'NODE LOSS', value: avgLoss.toFixed(2), unit: ' %', sub: 'data-plane window', accent: avgLoss > 5 ? P.red : avgLoss > 1 ? P.yellow : P.green },
        { label: 'LOCAL FLOWS', value: mine.length, unit: '', sub: 'active to/from node', accent: P.accent },
      ];
      el.innerHTML = cards.map((k) => kpiCard({ label: k.label, value: k.value + `<span style="font:500 11px 'IBM Plex Mono';color:#566273">${k.unit}</span>`, sub: k.sub, accent: k.accent })).join('');
    }
  }
  function kpiCard(k) {
    return `<div style="flex:1;background:linear-gradient(180deg,#0f141d,#0b0f16);border:1px solid #1a2330;border-radius:8px;padding:11px 14px;position:relative;overflow:hidden">
      <div style="font:600 9.5px/1 'IBM Plex Mono';letter-spacing:.13em;color:#5d6a7c">${k.label}</div>
      <div style="display:flex;align-items:baseline;gap:6px;margin-top:8px"><div style="font:600 25px/1 'IBM Plex Sans';color:#eef3fa">${k.value}</div>${k.delta ? `<div style="font:500 11px 'IBM Plex Mono';color:${k.dc}">${k.delta}</div>` : ''}</div>
      <div style="font:500 10px 'IBM Plex Mono';color:#4f5b6c;margin-top:4px">${k.sub}</div>
      <div style="position:absolute;left:0;top:0;bottom:0;width:3px;background:${k.accent}"></div>
    </div>`;
  }

  function updateTopoLegend() {
    const el = document.getElementById('nm-topolegend'); if (!el) return;
    let g = 0, y = 0, rd = 0;
    S.agents.forEach((a) => { const s = statusOf((a.wsRttUs || 0) / 1000, 0); s === 'green' ? g++ : s === 'yellow' ? y++ : rd++; });
    el.innerHTML = `<span style="display:flex;align-items:center;gap:5px"><span style="width:8px;height:8px;border-radius:50%;background:#3fb950"></span>Healthy ${g}</span><span style="display:flex;align-items:center;gap:5px"><span style="width:8px;height:8px;border-radius:50%;background:#d29922"></span>Degraded ${y}</span><span style="display:flex;align-items:center;gap:5px"><span style="width:8px;height:8px;border-radius:50%;background:#f85149"></span>Critical ${rd}</span>`;
  }

  function updateClock() { const c = document.getElementById('nm-clock'); if (c) c.textContent = S.clock + ' UTC'; const tx = document.getElementById('nm-tx'); if (tx) tx.textContent = S.txInterval; const tr = document.getElementById('nm-txrate'); if (tr) tr.textContent = Math.round(1000 / S.txInterval) + ' pkt/s/link'; }
  function updateCharts() {
    const cl = document.getElementById('nm-curlat'); if (cl) cl.textContent = (S.hLat[S.hLat.length - 1] || 0).toFixed(1);
    const cj = document.getElementById('nm-curjit'); if (cj) cj.textContent = (S.hJit[S.hJit.length - 1] || 0).toFixed(1);
    const closs = document.getElementById('nm-curloss'); const v = S.hLoss[S.hLoss.length - 1] || 0; if (closs) { closs.textContent = '■ ' + v.toFixed(2) + '% global'; closs.style.color = v > 5 ? P.red : v > 1 ? P.yellow : P.green; }
  }

  // ---------- sequence monitor data ----------
  function seqTests() {
    let flows = S.flows.filter((f) => !f.ctrl);
    if (!isController()) flows = flows.filter((f) => f.src === S.info.agentId || f.dst === S.info.agentId);
    return flows.map((r) => {
      const buf = S.seqBuf[r.id] || [];
      const sent = buf.length, missed = buf.filter((x) => x === 'MISS').length, reord = buf.filter((x) => x === 'REORD').length;
      const lossPct = sent ? (missed / sent * 100) : (r.loss || 0);
      const s = statusOf(r.latency, r.loss);
      const pm = PROF[r.profile] || ['#8b98a8', '#8b98a844'];
      return { id: r.id, src: r.src, dst: r.dst, profile: r.profile, profColor: pm[0], profBorder: pm[1], latency: r.latency, lossPct, lossColor: lossPct > 5 ? P.red : lossPct >= 1 ? P.yellow : '#7b8898', missed, sent, statColor: colorFor(s), strip: buf.slice(-52) };
    });
  }
  function updateSeq() {
    const list = document.getElementById('nm-seqlist'); if (!list) return;
    const tests = seqTests();
    const sc = document.getElementById('nm-seqcount'); if (sc) sc.textContent = tests.length;
    list.innerHTML = tests.length ? tests.map((t) => `<button data-act="openTest" data-arg="${esc(t.id)}" style="width:100%;text-align:left;display:flex;align-items:center;gap:13px;background:#0d131c;border:1px solid #18212d;border-radius:7px;padding:9px 12px;margin-bottom:6px">
        <div style="flex:0 0 188px;display:flex;align-items:center;gap:7px;min-width:0"><span style="font:600 11px 'IBM Plex Mono';color:#cdd9e5;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(t.src)}</span><span style="color:#465061">→</span><span style="font:600 11px 'IBM Plex Mono';color:#8b98a8;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(t.dst)}</span></div>
        <span style="flex:0 0 auto;color:${t.profColor};border:1px solid ${t.profBorder};padding:1px 6px;border-radius:4px;font:500 9px 'IBM Plex Mono'">${esc(t.profile)}</span>
        <div style="flex:1;min-width:0;display:flex;gap:1px;align-items:stretch;height:18px">${t.strip.map((x) => `<span style="flex:1;border-radius:1px;background:${colorFor(x === 'MISS' ? 'red' : x === 'REORD' ? 'yellow' : 'green')}"></span>`).join('')}</div>
        <span style="flex:0 0 56px;text-align:right;font:500 10px 'IBM Plex Mono';color:#9fb1c6">${t.latency.toFixed(1)}ms</span>
        <span style="flex:0 0 52px;text-align:right;font:600 10px 'IBM Plex Mono';color:${t.lossColor}">${t.lossPct.toFixed(1)}%</span>
        <span style="flex:0 0 74px;text-align:right;font:500 9px 'IBM Plex Mono';color:#566273">${t.missed} missed</span>
        <span style="width:7px;height:7px;border-radius:50%;background:${t.statColor};box-shadow:0 0 5px ${t.statColor};flex:0 0 auto"></span>
      </button>`).join('') : `<div style="padding:18px;text-align:center;color:#465061">no node-to-node tests running</div>`;
    const summary = document.getElementById('nm-seqsummary');
    if (summary) {
      const totMissed = tests.reduce((a, t) => a + t.missed, 0), totSent = tests.reduce((a, t) => a + t.sent, 0);
      const lost = totSent ? (totMissed / totSent * 100) : 0;
      const cards = [
        { label: 'TESTS MONITORED', value: tests.length, sub: 'node ↔ node data-plane', accent: P.accent },
        { label: 'WINDOW LOSS', value: lost.toFixed(2) + '%', sub: 'across all sequences', accent: lost > 5 ? P.red : lost > 1 ? P.yellow : P.green },
        { label: 'MISSED (window)', value: totMissed, sub: 'last ~58 probes / test', accent: totMissed ? P.yellow : P.green },
        { label: 'REORDERED', value: tests.reduce((a, t) => a + (S.seqBuf[t.id] || []).filter((x) => x === 'REORD').length, 0), sub: 'out-of-order delivery', accent: P.yellow },
      ];
      summary.innerHTML = cards.map((k) => `<div style="flex:1;background:linear-gradient(180deg,#0f141d,#0b0f16);border:1px solid #1a2330;border-radius:8px;padding:11px 14px;position:relative;overflow:hidden"><div style="font:600 9.5px/1 'IBM Plex Mono';letter-spacing:.13em;color:#5d6a7c">${k.label}</div><div style="font:600 23px/1 'IBM Plex Sans';color:#eef3fa;margin-top:8px">${k.value}</div><div style="font:500 10px 'IBM Plex Mono';color:#4f5b6c;margin-top:5px">${k.sub}</div><div style="position:absolute;left:0;top:0;bottom:0;width:3px;background:${k.accent}"></div></div>`).join('');
    }
  }

  // ---------- drawer data ----------
  function updateDrawer() {
    if (!S.detail) return;
    if (S.detail.mode === 'node') renderNodeDetail();
    else renderTestDetail();
  }
  function agentById(id) { return S.agents.find((a) => a.id === id); }
  function renderNodeDetail() {
    const el = document.getElementById('nm-nodedetail'); if (!el) return;
    const id = S.detail.nodeId; const a = agentById(id);
    const flows = S.flows.filter((f) => f.src === id || f.dst === id);
    const dp = flows.filter((f) => !f.ctrl);
    const lat = a ? (a.wsRttUs || 0) / 1000 : (dp[0] ? dp[0].latency : 0);
    const loss = dp.length ? dp.reduce((s, f) => s + (f.loss || 0), 0) / dp.length : 0;
    const s = statusOf(lat, loss);
    const ip = a ? (a.remoteAddr || '').split(':')[0] : id;
    el.innerHTML = `
      <div style="flex:0 0 auto;padding:14px 16px;border-bottom:1px solid #161e29;display:flex;align-items:flex-start;gap:11px">
        <div style="width:38px;height:38px;border-radius:9px;background:#0a0e14;border:1px solid #233044;display:flex;align-items:center;justify-content:center;flex:0 0 auto"><span style="width:13px;height:13px;border:2px solid ${colorFor(s)};border-radius:50%"></span></div>
        <div style="flex:1;min-width:0"><div style="font:700 15px 'IBM Plex Sans';color:#eef3fa">${esc(labelOf(id))}</div><div style="font:500 10px 'IBM Plex Mono';color:#566273;margin-top:2px">${esc(id)} · ${esc(ip)}${a && a.group ? ' · ' + esc(a.group) : ''}</div></div>
        <span style="display:inline-flex;align-items:center;gap:6px;font:600 9px 'IBM Plex Mono';letter-spacing:.07em;color:${colorFor(s)};border:1px solid ${colorFor(s)}55;padding:4px 8px;border-radius:5px"><span style="width:6px;height:6px;border-radius:50%;background:${colorFor(s)}"></span>${stName[s]}</span>
        <button data-act="closeDetail" style="background:transparent;border:none;color:#6b7888;font-size:16px;line-height:1">✕</button>
      </div>
      <div style="flex:0 0 auto;display:grid;grid-template-columns:repeat(3,1fr);gap:8px;padding:12px 16px">
        ${miniStat('WS LATENCY', lat.toFixed(1), ' ms')}${miniStat('LOSS', loss.toFixed(2), ' %')}${miniStat('TESTS', dp.length, '')}
        ${miniStat('FRAMES RX', a ? a.framesRx || 0 : 0, '')}${miniStat('FRAMES TX', a ? a.framesTx || 0 : 0, '')}${miniStat('LAST SEQ', a ? a.lastSeq || 0 : 0, '')}
      </div>
      ${a && a.testPort ? `<div style="flex:0 0 auto;padding:0 16px 10px"><div style="display:flex;align-items:center;gap:8px;background:#0e141d;border:1px solid #1a2330;border-radius:7px;padding:9px 11px;font:500 10px 'IBM Plex Mono'"><span style="color:#5d6a7c;letter-spacing:.08em">DATA-PLANE PORT</span><b style="color:#cdd9e5">${a.testPort}</b><div style="flex:1"></div><span style="color:${a.portUdp ? '#7ee787' : '#f85149'}">UDP ${a.portUdp ? '✓' : '✕'}</span><span style="color:${a.portTcp ? '#7ee787' : '#f85149'}">TCP ${a.portTcp ? '✓' : '✕'}</span></div></div>` : ''}
      <div style="flex:0 0 auto;padding:4px 16px 8px;font:600 9px 'IBM Plex Mono';letter-spacing:.12em;color:#5d6a7c">TESTS &amp; FLOWS — click for probe detail</div>
      <div style="flex:1;min-height:0;overflow:auto;padding:0 12px 12px">
        ${flows.map((f) => { const st = statusOf(f.latency, f.loss); const pm = PROF[f.profile] || ['#8b98a8', '#8b98a844']; const peer = f.ctrl ? 'controller' : (f.src === id ? f.dst : f.src); const dir = f.ctrl ? 'WS·CTRL' : (f.src === id ? 'OUT →' : '← IN'); return `<button data-act="openTest" data-arg="${esc(f.id)}" style="width:100%;text-align:left;display:flex;align-items:center;gap:10px;background:#0d131c;border:1px solid #18212d;border-radius:7px;padding:9px 11px;margin-bottom:6px"><span style="font:600 8px 'IBM Plex Mono';color:${f.ctrl ? P.accent : '#7b8898'};width:42px;flex:0 0 auto">${dir}</span><span style="font:600 11px 'IBM Plex Mono';color:#cdd9e5;flex:1;min-width:0">${esc(peer)}</span><span style="color:${pm[0]};border:1px solid ${pm[1]};padding:1px 6px;border-radius:4px;font:500 9px 'IBM Plex Mono'">${esc(f.profile)}</span><span style="font:500 10px 'IBM Plex Mono';color:#8b98a8;width:50px;text-align:right">${f.latency.toFixed(1)}ms</span><span style="width:7px;height:7px;border-radius:50%;background:${colorFor(st)};box-shadow:0 0 5px ${colorFor(st)};flex:0 0 auto"></span><span style="color:#465061;font-size:13px">›</span></button>`; }).join('')}
      </div>
      ${isController() ? `<div style="flex:0 0 auto;padding:11px 16px;border-top:1px solid #161e29"><button data-act="openDiag" data-arg="${esc(id)}" ${readOnly() ? 'disabled' : ''} style="width:100%;font:600 11px 'IBM Plex Mono';letter-spacing:.04em;padding:10px;border-radius:7px;border:1px solid ${readOnly() ? '#1c2632' : '#233044'};background:${readOnly() ? '#0d121a' : '#101826'};color:${readOnly() ? '#3a4757' : P.accent};opacity:${readOnly() ? '.6' : '1'}">⌗ Open Remote Diagnostics Terminal</button></div>` : ''}`;
  }
  function miniStat(label, val, unit) { return `<div style="background:#0e141d;border:1px solid #1a2330;border-radius:7px;padding:9px 10px"><div style="font:600 8.5px 'IBM Plex Mono';letter-spacing:.08em;color:#5d6a7c">${label}</div><div style="font:600 17px 'IBM Plex Sans';color:#eef3fa;margin-top:4px">${val}<span style="font:500 9px 'IBM Plex Mono';color:#566273">${unit}</span></div></div>`; }

  function renderTestDetail() {
    const el = document.getElementById('nm-testdetail'); if (!el) return;
    const r = S.flowById[S.detail.flowId]; if (!r) { el.innerHTML = ''; return; }
    const st = statusOf(r.latency, r.loss); const pm = PROF[r.profile] || ['#8b98a8', '#8b98a844'];
    const buf = S.seqBuf[r.id] || [];
    const ts = S.tstats || { sent: buf.length, missed: buf.filter((x) => x === 'MISS').length, reord: buf.filter((x) => x === 'REORD').length };
    const lostPct = ts.sent ? (ts.missed / ts.sent * 100) : (r.loss || 0);
    el.innerHTML = `
      <div style="flex:0 0 auto;padding:12px 16px;border-bottom:1px solid #161e29">
        <div style="display:flex;align-items:center;gap:10px"><button data-act="backToNode" style="background:transparent;border:1px solid #1f2a37;color:#8b98a8;border-radius:6px;padding:5px 9px;font:500 10px 'IBM Plex Mono'">‹ back</button><div style="font:600 10px 'IBM Plex Mono';letter-spacing:.12em;color:#6b7888;flex:1">PROBE / TEST DETAIL</div><button data-act="closeDetail" style="background:transparent;border:none;color:#6b7888;font-size:16px;line-height:1">✕</button></div>
        <div style="display:flex;align-items:center;gap:8px;margin-top:12px;flex-wrap:wrap"><span style="font:600 13px 'IBM Plex Mono';color:#cdd9e5">${esc(r.src)}</span><span style="color:#465061">→</span><span style="font:600 13px 'IBM Plex Mono';color:#cdd9e5">${esc(r.dst)}</span><span style="color:${pm[0]};border:1px solid ${pm[1]};padding:2px 7px;border-radius:4px;font:500 10px 'IBM Plex Mono'">${esc(r.profile)}</span><span style="font:500 10px 'IBM Plex Mono';color:#566273">:${r.port || '—'}</span></div>
      </div>
      <div style="flex:0 0 auto;display:grid;grid-template-columns:repeat(3,1fr);gap:8px;padding:12px 16px">
        ${miniStat('LATENCY', r.latency.toFixed(1), ' ms')}${miniStat('JITTER', (r.jitter || 0).toFixed(2), ' ms')}${miniStat('LOST', lostPct.toFixed(1), ' %')}
        ${miniStat('SENT', ts.sent, '')}${miniStat('MISSED', ts.missed, '')}${miniStat('REORDERED', ts.reord, '')}
      </div>
      <div style="flex:0 0 auto;padding:0 16px 12px">
        <div style="font:600 9px 'IBM Plex Mono';letter-spacing:.1em;color:#5d6a7c;margin-bottom:6px">IP HEADER / SOCKET</div>
        <div style="display:flex;gap:6px;flex-wrap:wrap;font:500 10px 'IBM Plex Mono'">
          <span style="background:#0e141d;border:1px solid #1a2330;border-radius:5px;padding:5px 9px;color:#cdd9e5">ttl <b style="color:${r.ttl ? '#7ee787' : '#566273'}">${r.ttl || '—'}</b></span>
          <span style="background:#0e141d;border:1px solid #1a2330;border-radius:5px;padding:5px 9px;color:#8b98a8">src <b style="color:#cdd9e5">${esc(r.localAddr || '—')}</b></span>
          <span style="color:#465061;align-self:center">→</span>
          <span style="background:#0e141d;border:1px solid #1a2330;border-radius:5px;padding:5px 9px;color:#8b98a8">dst <b style="color:#cdd9e5">${esc(r.remoteAddr || '—')}</b></span>
        </div>
        ${r.profile === 'TCP' ? `<div style="font:500 9px 'IBM Plex Mono';color:#465061;margin-top:5px">ttl not exposed for TCP by the OS socket API</div>` : ''}
      </div>
      <div style="flex:0 0 auto;padding:0 16px 10px"><div style="font:600 9px 'IBM Plex Mono';letter-spacing:.1em;color:#5d6a7c;margin-bottom:6px">PACKET SEQUENCE · recent probes</div><div style="display:flex;gap:2px;align-items:flex-end;height:20px">${buf.slice(-46).map((x) => `<span style="flex:1;height:20px;border-radius:2px;background:${colorFor(x === 'MISS' ? 'red' : x === 'REORD' ? 'yellow' : 'green')}"></span>`).join('')}</div></div>
      <div style="flex:0 0 auto;padding:0 16px 6px;font:600 9px 'IBM Plex Mono';letter-spacing:.1em;color:#5d6a7c">PROBE MESSAGES · live</div>
      <div id="nm-testlog" style="flex:1;min-height:0;overflow:auto;margin:0 12px 12px;padding:8px 11px;background:#05080b;border:1px solid #161e29;border-radius:7px;font:500 11px/1.6 'IBM Plex Mono'">${(S.testLog).slice(-60).map((m) => `<div style="color:${m.color};white-space:pre-wrap">${esc(m.txt)}</div>`).join('')}</div>`;
    const tl = document.getElementById('nm-testlog'); if (tl) tl.scrollTop = tl.scrollHeight;
  }

  // ---------- diagnostics ----------
  function ingestDiag(ch) {
    if (S.diagAgent && ch.requestId && ch.requestId.indexOf(S.diagAgent) !== 0) { /* not ours */ }
    const color = ch.stream === 'stderr' ? '#f0883e' : ch.stream === 'meta' ? '#566273' : '#9fe6b0';
    if (ch.data) S.diagLines.push({ text: ch.data, color });
    if (ch.err) S.diagLines.push({ text: '✕ ' + ch.err, color: '#f85149' });
    if (ch.eof) { S.diagBusy = false; if (ch.exitCode !== undefined) S.diagLines.push({ text: '— exit ' + ch.exitCode + ' —', color: '#566273' }); }
    if (S.diagLines.length > 200) S.diagLines = S.diagLines.slice(-200);
    if (S.diagOpen) updateDiag();
  }
  function updateDiag() {
    const term = document.getElementById('nm-term'); if (!term) return;
    term.innerHTML = S.diagLines.map((l) => `<div style="color:${l.color};white-space:pre-wrap">${esc(l.text)}</div>`).join('') + (S.diagBusy ? `<span style="display:inline-block;width:8px;height:14px;background:#9fe6b0;animation:nm-blink .8s infinite;vertical-align:middle"></span>` : '');
    term.scrollTop = term.scrollHeight;
  }

  // ================= canvas =================
  let raf = null;
  function bindCanvases() { if (!raf) raf = requestAnimationFrame(drawLoop); }
  function drawLoop() { try { draw(); } catch (e) { } raf = requestAnimationFrame(drawLoop); }
  function draw() { if (isController()) drawTopo(); drawLat(); if (isController()) drawLoss(); }

  function prep(cv) { if (!cv) return null; const dpr = window.devicePixelRatio || 1; const w = cv.clientWidth, h = cv.clientHeight; if (!w || !h) return null; if (cv.width !== w * dpr || cv.height !== h * dpr) { cv.width = w * dpr; cv.height = h * dpr; } const ctx = cv.getContext('2d'); ctx.setTransform(dpr, 0, 0, dpr, 0, 0); ctx.clearRect(0, 0, w, h); return { ctx, w, h }; }

  function nodePoints(w, h) {
    const cx = w / 2, cy = h / 2, R = Math.min(w, h) * 0.33;
    const pts = [{ x: cx, y: cy, controller: true }];
    const ag = S.agents;
    ag.forEach((a, i) => { const ang = (i / Math.max(1, ag.length)) * Math.PI * 2 - Math.PI / 2; pts.push({ x: cx + Math.cos(ang) * R, y: cy + Math.sin(ang) * R, agent: a }); });
    return pts;
  }
  // expose for click hit-testing
  S._nodePoints = nodePoints;

  function drawTopo() {
    const cv = document.getElementById('nm-topo'); const p = prep(cv); if (!p) return;
    const { ctx, w, h } = p; const t = performance.now() / 1000; const acc = P.accent;
    const pts = nodePoints(w, h); const ctrl = pts[0];
    const posById = {}; pts.slice(1).forEach((pt) => posById[pt.agent.id] = pt);

    // control-plane dashed spokes
    ctx.save(); ctx.setLineDash([2, 6]);
    pts.slice(1).forEach((pt) => { const s = statusOf((pt.agent.wsRttUs || 0) / 1000, 0); ctx.beginPath(); ctx.strokeStyle = (s === 'red' ? P.red : acc) + '24'; ctx.lineWidth = 1; ctx.moveTo(ctrl.x, ctrl.y); ctx.lineTo(pt.x, pt.y); ctx.stroke(); });
    ctx.restore();

    // data-plane links
    const links = S.flows.filter((r) => !r.ctrl);
    links.forEach((r, li) => {
      const a = posById[r.src], b = posById[r.dst]; if (!a || !b) return;
      const s = statusOf(r.latency, r.loss); const col = colorFor(s);
      const mx = (a.x + b.x) / 2, my = (a.y + b.y) / 2; const dx = b.x - a.x, dy = b.y - a.y; const len = Math.hypot(dx, dy) || 1;
      let nx = -dy / len, ny = dx / len; const ox = mx - ctrl.x, oy = my - ctrl.y; if (nx * ox + ny * oy < 0) { nx = -nx; ny = -ny; }
      const cm = len * 0.17 + 14; const cp = { x: mx + nx * cm, y: my + ny * cm };
      ctx.beginPath(); ctx.moveTo(a.x, a.y); ctx.quadraticCurveTo(cp.x, cp.y, b.x, b.y); ctx.strokeStyle = col + (s === 'green' ? '5e' : 'd0'); ctx.lineWidth = s === 'green' ? 1.5 : 2.5; ctx.shadowColor = col; ctx.shadowBlur = s === 'green' ? 0 : 5; ctx.stroke(); ctx.shadowBlur = 0;
      const sp = Math.max(0.15, (S.txInterval / 600));
      const bez = (tt) => ({ x: (1 - tt) * (1 - tt) * a.x + 2 * (1 - tt) * tt * cp.x + tt * tt * b.x, y: (1 - tt) * (1 - tt) * a.y + 2 * (1 - tt) * tt * cp.y + tt * tt * b.y });
      for (let k = 0; k < 2; k++) { const ph = ((t / sp + li * 0.31 + k * 0.5) % 1); const pp = bez(ph); ctx.beginPath(); ctx.fillStyle = col; ctx.shadowColor = col; ctx.shadowBlur = 9; ctx.arc(pp.x, pp.y, 2.3, 0, 7); ctx.fill(); ctx.shadowBlur = 0; }
      const tp = bez(0.5); ctx.font = "600 8.5px 'IBM Plex Mono'"; ctx.textAlign = 'center'; const lbl = r.profile; const tw = ctx.measureText(lbl).width; ctx.fillStyle = 'rgba(8,11,16,0.82)'; ctx.fillRect(tp.x - tw / 2 - 3, tp.y - 6.5, tw + 6, 13); ctx.fillStyle = col; ctx.fillText(lbl, tp.x, tp.y + 3);
    });

    // agent nodes
    pts.slice(1).forEach((pt) => {
      const a = pt.agent; const s = statusOf((a.wsRttUs || 0) / 1000, 0); const col = colorFor(s);
      if (a.id === S.selectedNode) { ctx.beginPath(); ctx.strokeStyle = acc; ctx.lineWidth = 2; ctx.setLineDash([4, 3]); ctx.arc(pt.x, pt.y, 18, 0, 7); ctx.stroke(); ctx.setLineDash([]); }
      if (s !== 'green') { const pr = 6 + ((Math.sin(t * 3) + 1) / 2) * 9; ctx.beginPath(); ctx.strokeStyle = col + '66'; ctx.lineWidth = 1.5; ctx.arc(pt.x, pt.y, pr + 8, 0, 7); ctx.stroke(); }
      ctx.beginPath(); ctx.fillStyle = '#0b1018'; ctx.arc(pt.x, pt.y, 11, 0, 7); ctx.fill();
      ctx.beginPath(); ctx.strokeStyle = col; ctx.lineWidth = 2; ctx.arc(pt.x, pt.y, 11, 0, 7); ctx.stroke();
      ctx.beginPath(); ctx.fillStyle = col; ctx.shadowColor = col; ctx.shadowBlur = 11; ctx.arc(pt.x, pt.y, 4.5, 0, 7); ctx.fill(); ctx.shadowBlur = 0;
      ctx.font = "600 10px 'IBM Plex Mono'"; ctx.fillStyle = '#9fb1c6'; ctx.textAlign = 'center'; const ly = pt.y + (pt.y > ctrl.y ? 26 : -18); ctx.fillText(labelOf(a.id), pt.x, ly);
      ctx.font = "500 9px 'IBM Plex Mono'"; ctx.fillStyle = '#566273'; ctx.fillText(((a.wsRttUs || 0) / 1000).toFixed(0) + 'ms · ws', pt.x, ly + 12);
    });

    // controller hub
    const cr = 15; ctx.beginPath(); ctx.fillStyle = '#0c1424'; ctx.arc(ctrl.x, ctrl.y, cr, 0, 7); ctx.fill();
    const ringP = 6 + ((Math.sin(t * 2) + 1) / 2) * 6; ctx.save(); ctx.setLineDash([3, 4]); ctx.beginPath(); ctx.strokeStyle = acc + '44'; ctx.lineWidth = 1.5; ctx.arc(ctrl.x, ctrl.y, cr + ringP, 0, 7); ctx.stroke(); ctx.restore();
    ctx.beginPath(); ctx.strokeStyle = acc; ctx.lineWidth = 2; ctx.arc(ctrl.x, ctrl.y, cr, 0, 7); ctx.stroke();
    ctx.beginPath(); ctx.fillStyle = acc; ctx.shadowColor = acc; ctx.shadowBlur = 14; ctx.arc(ctrl.x, ctrl.y, 5, 0, 7); ctx.fill(); ctx.shadowBlur = 0;
    ctx.font = "700 9px 'IBM Plex Mono'"; ctx.fillStyle = acc; ctx.textAlign = 'center'; ctx.fillText('CONTROLLER · WS', ctrl.x, ctrl.y + cr + 14);
  }

  function drawLine(cv, series) {
    const p = prep(cv); if (!p) return; const { ctx, w, h } = p; const pad = { l: 4, r: 4, t: 8, b: 6 };
    const gw = w - pad.l - pad.r, gh = h - pad.t - pad.b;
    ctx.strokeStyle = '#15202c'; ctx.lineWidth = 1; for (let i = 0; i <= 3; i++) { const y = pad.t + gh * i / 3; ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(w - pad.r, y); ctx.stroke(); }
    series.forEach((se) => {
      const data = se.data; const mx = Math.max(se.max || Math.max.apply(null, data) * 1.2, 0.001); const mn = se.min || 0;
      const X = (i) => pad.l + gw * i / (data.length - 1); const Y = (v) => pad.t + gh * (1 - (v - mn) / (mx - mn));
      if (se.fill) { ctx.beginPath(); ctx.moveTo(X(0), h - pad.b); data.forEach((v, i) => ctx.lineTo(X(i), Y(v))); ctx.lineTo(X(data.length - 1), h - pad.b); ctx.closePath(); const g = ctx.createLinearGradient(0, pad.t, 0, h); g.addColorStop(0, se.color + '44'); g.addColorStop(1, se.color + '02'); ctx.fillStyle = g; ctx.fill(); }
      ctx.beginPath(); data.forEach((v, i) => { const x = X(i), y = Y(v); i ? ctx.lineTo(x, y) : ctx.moveTo(x, y); }); ctx.strokeStyle = se.color; ctx.lineWidth = 1.8; ctx.lineJoin = 'round'; ctx.stroke();
      const lx = X(data.length - 1), ly = Y(data[data.length - 1]); ctx.beginPath(); ctx.fillStyle = se.color; ctx.shadowColor = se.color; ctx.shadowBlur = 8; ctx.arc(lx, ly, 2.6, 0, 7); ctx.fill(); ctx.shadowBlur = 0;
    });
  }
  function drawLat() { const cv = document.getElementById('nm-lat'); const mx = Math.max.apply(null, S.hLat) * 1.25 || 1; drawLine(cv, [{ data: S.hLat.slice(-60), color: P.accent, fill: true, min: 0, max: mx }, { data: S.hJit.slice(-60), color: P.yellow, fill: false, min: 0, max: mx }]); }
  function drawLoss() { const cv = document.getElementById('nm-loss'); drawLine(cv, [{ data: S.hLoss.slice(-60), color: P.red, fill: true, min: 0, max: Math.max(0.5, Math.max.apply(null, S.hLoss) * 1.4) }]); }

  // ================= interactions =================
  const A = {
    goDash: () => { S.page = 'dashboard'; render(); },
    goSeq: () => { S.page = 'sequences'; render(); },
    goAdmin: () => { S.page = 'admin'; render(); },
    maximizeLog: () => { S.logMax = true; render(); },
    closeLogMax: () => { S.logMax = false; render(); },
    toggleLogPause: () => { if (S.logPaused) resumeLog(); else { S.logPaused = true; updateLogChrome(); } },
    jumpLatest: () => resumeLog(),
    toggleLevel: (arg) => { if (S.logLevels.has(arg)) S.logLevels.delete(arg); else S.logLevels.add(arg); forceLogRefresh(); },
    clearLog: () => { S.events = []; S.logNew = 0; forceLogRefresh(); },
    logQuery: (e) => { S.logQuery = e.target.value; forceLogRefresh(); },
    refreshAdmin: async () => { await refresh(); updateAdminRows(); },
    saveAgent: async (id) => {
      const row = adminRow(id); if (!row) return;
      const profiles = [...row.querySelectorAll('.nm-prof')].filter((c) => c.checked).map((c) => c.getAttribute('data-prof'));
      const body = {
        agentId: id,
        label: row.querySelector('.nm-label').value.trim(),
        group: row.querySelector('.nm-group').value.trim(),
        enabled: row.querySelector('.nm-enabled').checked,
        profiles,
      };
      const r = await postJSON('/api/admin/agents', body);
      if (r.ok) { await refresh(); updateAdminRows(); } else { alert('Save failed: ' + (await r.text())); }
    },
    evictAgent: async (id) => {
      if (!confirm('Evict ' + id + '? It will drop the control link (and may reconnect unless you also disable it).')) return;
      const r = await postJSON('/api/admin/agents/evict', { agentId: id });
      if (!r.ok) { alert('Evict failed: ' + (await r.text())); return; }
      setTimeout(async () => { await refresh(); updateAdminRows(); }, 700);
    },
    openTestSetup: () => { if (readOnly()) return loginPrompt(); if (S.running) return; S.testSetup = true; render(); },
    closeTestSetup: () => { S.testSetup = false; render(); },
    runTest: async () => {
      if (readOnly()) return loginPrompt();
      const port = parseInt((document.getElementById('nm-test-port') || {}).value || '0', 10) || 0;
      const interval = parseInt((document.getElementById('nm-test-interval') || {}).value || S.txInterval, 10) || S.txInterval;
      const protocols = [...document.querySelectorAll('.nm-test-proto')].filter((c) => c.checked).map((c) => c.getAttribute('data-prof'));
      S.txInterval = interval; S.testPort = port;
      const r = await postJSON('/api/tests/start', { port, protocols, intervalMs: interval });
      if (r.ok) { S.testSetup = false; S.running = true; render(); } else { alert('Start failed: ' + (await r.text())); }
    },
    stopTest: async () => { if (readOnly()) return loginPrompt(); if (!S.running) return; const r = await postJSON('/api/tests/stop', {}); if (r.ok) setRunning(false); },
    setTx: (e) => { S.txInterval = +e.target.value; updateClock(); },
    setQuery: (e) => { S.query = e.target.value; updateGrid(); },
    sort: (arg) => { if (S.sortKey === arg) S.sortDir = S.sortDir === 'asc' ? 'desc' : 'asc'; else { S.sortKey = arg; S.sortDir = 'desc'; } render(); },
    filter: (arg) => { S.filter = arg; render(); },
    clearNode: () => { S.selectedNode = null; render(); },
    openTest: (arg) => { S.openFlowId = arg; S.testLog = []; S.tstats = null; S.detail = { mode: 'test', flowId: arg, nodeId: (S.detail && S.detail.nodeId) || (S.flowById[arg] ? S.flowById[arg].src : null) }; render(); },
    backToNode: () => { S.detail = { mode: 'node', nodeId: (S.detail && S.detail.nodeId) || S.selectedNode }; render(); },
    closeDetail: () => { S.detail = null; render(); },
    openDiag: (arg) => { if (readOnly()) return loginPrompt(); S.diagOpen = true; S.diagAgent = arg; S.diagLines = [{ text: '$ secure channel to ' + arg + ' (whitelisted)', color: '#566273' }]; S.diagBusy = false; render(); },
    closeDiag: () => { S.diagOpen = false; render(); },
    runDiag: async (arg) => {
      if (readOnly()) return loginPrompt();
      const a = agentById(S.diagAgent); const ip = a ? (a.remoteAddr || '').split(':')[0] : '';
      const args = arg === 'netstat' ? [] : [ip || '127.0.0.1'];
      S.diagBusy = true; S.diagLines.push({ text: '$ ' + arg + ' ' + args.join(' '), color: '#cdd9e5' }); updateDiag();
      const r = await postJSON('/api/diag', { agentId: S.diagAgent, command: arg, args });
      if (!r.ok) { S.diagBusy = false; S.diagLines.push({ text: '✕ ' + (await r.text()), color: '#f85149' }); updateDiag(); }
    },
    joinMaster: async () => { const v = (document.getElementById('nm-joininput') || {}).value || ''; const r = await postJSON('/api/join', { master: v.trim() || '127.0.0.1' }); if (r.ok) { setTimeout(() => fetchInfo().then(render), 400); } else { alert(await r.text()); } },
    login: () => loginPrompt(),
    logout: async () => { S.creds = null; S.wsToken = null; S.info.authenticated = false; if (ws) ws.close(); connectWS(); render(); },
    topoClick: (e, el) => {
      const r = el.getBoundingClientRect(); const x = e.clientX - r.left, y = e.clientY - r.top;
      const pts = S._nodePoints(r.width, r.height); let hit = null, best = 1e9;
      pts.forEach((p) => { if (!p.agent) return; const d = Math.hypot(p.x - x, p.y - y); if (d < 26 && d < best) { best = d; hit = p; } });
      if (hit) { S.selectedNode = hit.agent.id; S.detail = { mode: 'node', nodeId: hit.agent.id }; render(); }
    },
  };

  async function loginPrompt() {
    const user = window.prompt('Username', 'admin'); if (user == null) return;
    const pass = window.prompt('Password'); if (pass == null) return;
    S.creds = { user, pass };
    try {
      const a = await getJSON('/api/auth');
      if (a.authenticated) { S.info.authenticated = true; S.wsToken = a.wsToken || null; if (ws) ws.close(); connectWS(); render(); }
      else { S.creds = null; alert('Invalid credentials'); }
    } catch (e) { S.creds = null; alert('Login failed'); }
  }

  // delegated events
  app.addEventListener('click', (e) => {
    const el = e.target.closest('[data-act]'); if (!el) return;
    const act = el.getAttribute('data-act'); const arg = el.getAttribute('data-arg');
    if (el.hasAttribute('disabled')) return;
    if (act === 'topoClick') { A.topoClick(e, el); return; }
    if (A[act]) { e.preventDefault(); A[act](arg); }
  });
  app.addEventListener('input', (e) => { const el = e.target.closest('[data-act]'); if (!el) return; const act = el.getAttribute('data-act'); if (act === 'setTx' || act === 'setQuery' || act === 'logQuery') A[act](e); });

  // ================= boot =================
  async function fetchInfo() {
    try { const info = await getJSON('/api/info'); S.info = Object.assign(S.info, info); if (typeof info.testRunning === 'boolean') S.running = info.testRunning; } catch (e) { }
  }
  async function boot() {
    await fetchInfo();
    connectWS();
    render();
    await refresh();
    setInterval(refresh, 2000);
    setInterval(tick, 1000);
  }
  boot();
})();
