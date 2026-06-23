#!/usr/bin/env sh
# Seed the running NetMesh stack with a full mesh of flows and start a test, so
# the dashboard shows live traffic without clicking through the UI.
#
#   ./seed.sh [controller_url] [port] [protocols_csv]
#
# Defaults: http://localhost:5999, port 5201, protocols "udp,tcp,icmp".
# (ICMP is port-less; the port only applies to UDP/TCP flows.)
set -eu

CTRL="${1:-http://localhost:5999}"
PORT="${2:-5201}"
PROTOS="${3:-udp,tcp,icmp}"
WANT_AGENTS="${WANT_AGENTS:-3}"

echo "waiting for controller at $CTRL ..."
until curl -fsS "$CTRL/api/info" >/dev/null 2>&1; do sleep 1; done

echo "waiting for $WANT_AGENTS agents to register ..."
while [ "$(curl -fsS "$CTRL/api/agents" | grep -o '"id"' | wc -l | tr -d ' ')" -lt "$WANT_AGENTS" ]; do
  sleep 1
done

# Turn "udp,tcp,icmp" into a JSON array: ["udp","tcp","icmp"]
protos_json=$(printf '%s' "$PROTOS" | awk -F, '{for(i=1;i<=NF;i++){printf "%s\"%s\"",(i>1?",":""),$i}}')

echo "generating full mesh on port $PORT for [$PROTOS] ..."
curl -fsS -X POST "$CTRL/api/flows/mesh" \
  -H 'Content-Type: application/json' \
  -d "{\"port\":$PORT,\"protocols\":[$protos_json],\"symmetric\":false}"
echo

echo "starting test (250ms interval) ..."
curl -fsS -X POST "$CTRL/api/tests/start" \
  -H 'Content-Type: application/json' \
  -d '{"intervalMs":250}'
echo

echo "done — open $CTRL and watch the Dashboard / Sequence Monitor."
