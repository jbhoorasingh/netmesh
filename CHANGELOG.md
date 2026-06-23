# Changelog

All notable changes to NetMesh are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-06-23

This release replaces the fixed four-profile data plane with an Ixia-style
per-flow traffic model and folds in a round of adversarial-review hardening.

### Added

- **Ixia-style per-flow traffic model.** Define individual flows of the form
  `srcAgent:srcPort --proto--> dstAgent:dstPort`, where `proto` is `udp`/`tcp`
  (with ports) or `icmp` (port-less), and `srcPort 0` means a dynamic
  (ephemeral) source port.
- **Master-driven test setup.** The controller computes a per-agent `FlowPlan`
  (listen ports + flows to originate) and pushes it via a new `FLOW_PLAN`
  control frame when a test is set up. Agents bind the assigned ports **on
  demand** and report per-port availability back via a `PORT_STATUS` frame —
  including bind errors (e.g. `address already in use`).
- **Traffic Flows editor** in the web UI: agent dropdowns, src/dst ports,
  protocol selector, per-flow enable toggle, a full-mesh generator, and
  clear-all. Flows persist to `netmesh-flows.json` (gitignored).
- **IP-header / socket capture.** Probes record TTL and the resolved
  local/remote socket addresses, surfaced in the probe detail drawer.
- REST surface: `GET /api/flows` plus privileged
  `POST /api/flows/{upsert,delete,mesh,clear}`.

### Changed

- **Breaking:** the four fixed profiles (UDP-symmetric, UDP-dynamic, TCP, ICMP
  as agent roles) are replaced by `udp`/`tcp`/`icmp` with per-flow source and
  destination ports. Agents **no longer take a data port at startup**; the
  `-data-port` flag is legacy and unused for binding.
- The telemetry store and flow-transition keys now include the flow id, so
  multiple flows between the same agent pair and protocol render as distinct
  rows in the live grid instead of collapsing into one.
- Per-agent admin config dropped the now-meaningless "profiles" field; the
  agents view now reports each agent's bound listen ports.

### Fixed

- **UDP echo cross-delivery under `SO_REUSEPORT`.** Flows sharing a source port
  formed one reuseport group, so the kernel could deliver an echo to the wrong
  flow's socket — inflating measured loss/RTT. The UDP prober now dials a
  connected socket so only the intended peer's datagrams arrive.
- **Sequence Monitor detail drawer.** Clicking a flow row on the Sequence
  Monitor page no longer opened the probe detail drawer (the redesign had
  narrowed the drawer's page gate to the dashboard only). It is restored on the
  dashboard and sequence pages.
- `applyFlows` is serialized and now delivers every flow plan before starting
  any source, so destinations are listening before they are probed.
- Self-flows (`srcAgent == dstAgent`) and port-less UDP/TCP flows are rejected
  by the API and the editor.
- The responder tears down a prior binding synchronously on rebind and closes
  in-flight TCP connections, so they cannot outlive their listen plan.
- The flow store persists under its lock (no write-reordering); ICMP replies
  are source-checked; the editor preserves an offline agent's selection.

## [0.1.0] - 2026-06-18

Initial release: single Go binary running as Controller (`-master=self`) or
Agent (`-master=<IP>`), forming a connectivity-testing mesh.

### Added

- Resilient WebSocket control plane (read/write pumps, app-layer PING/PONG
  heartbeat, exponential-backoff reconnect with jitter).
- Telemetry spooler (1000-entry ring buffer) with per-agent monotonic sequence
  numbers and live gap detection (`PACKET_SEQUENCE_MISSED`).
- Data-plane probers measuring real RTT/loss against a peer-side echo responder.
- Structured JSON event logging and a master-driven, whitelist-only diagnostics
  console (no raw shell).
- Vanilla-JS single-page UI (embedded) with live dashboard, packet-sequence
  monitor, event log, and an admin tab; optional RBAC via `-admin=user:pass`.
- GitHub Actions CI and a cross-platform release workflow.

[0.2.0]: https://github.com/jbhoorasingh/netmesh/releases/tag/v0.2.0
[0.1.0]: https://github.com/jbhoorasingh/netmesh/releases/tag/v0.1.0
