# NetMesh — Docker Compose test mesh

A self-contained mesh you can bring up in one command: one **Controller** and
three **Agents**, each running the released NetMesh binary in a minimal Alpine
container, all on a single bridge network so agent-to-agent data-plane probes
(UDP/TCP/ICMP) actually reach each other.

The image pulls the binary from a GitHub release — no Go toolchain needed.

## Requirements

- Docker Engine with Compose v2 (`docker compose`, not the legacy
  `docker-compose`). Docker Desktop on macOS/Windows works out of the box.

## Quick start

```sh
cd deploy
docker compose up --build -d
```

Then open the controller dashboard at **http://localhost:5999**. The three
agents register automatically within a few seconds (watch the topology / agent
list populate).

To generate traffic immediately, seed a full mesh and start a test:

```sh
./seed.sh
```

Or do it from the UI: go to **⇄ Traffic Flows**, add flows (or **Generate
mesh**), then press **▷ START TEST** in the header.

Tear down:

```sh
docker compose down
```

## What's running

| Service      | Role             | Container cmd                                  | Host URL                |
|--------------|------------------|------------------------------------------------|-------------------------|
| `controller` | Master           | `-master=self -port=5999 -id=controller`       | http://localhost:5999   |
| `agent1`     | Node             | `-master=controller:5999 -port=5999 -id=agent1`| http://localhost:6001   |
| `agent2`     | Node             | `-master=controller:5999 -port=5999 -id=agent2`| http://localhost:6002   |
| `agent3`     | Node             | `-master=controller:5999 -port=5999 -id=agent3`| http://localhost:6003   |

Every container listens on `5999` internally; the host ports above are just the
published mappings. Agents reach the controller by its service name
(`controller:5999`) over Docker's internal DNS, and reach each other by
container IP on the `mesh` network — which is the address the controller hands
out in each flow plan.

## Configuration

- **Release version** — defaults to `v0.2.0`. Override per command:

  ```sh
  NETMESH_VERSION=v0.2.0 docker compose up --build -d
  ```

  (You can also set it in a `.env` file next to `docker-compose.yml`.)

- **Secured controller (RBAC)** — add `-admin=user:pass` to the controller
  command, and a shared join secret with `-token=...` on every service.

- **ICMP** — agents set `net.ipv4.ping_group_range` so unprivileged ICMP echo
  sockets work without `--privileged` or `NET_RAW`. If your host blocks that
  namespaced sysctl, drop ICMP from your flows (UDP/TCP still work).

## Add more agents

Each agent needs a unique `-id` and host port, so copy an `agentN` block in
`docker-compose.yml` rather than using `--scale`:

```yaml
  agent4:
    <<: [*svc, *agent]
    command: ["-master=controller:5999", "-port=5999", "-id=agent4"]
    ports:
      - "6004:5999"
```

NetMesh is designed for a mesh of ~25 nodes, so adding a handful more is fine.

## Troubleshooting

- **Agents not registering** — check `docker compose logs -f agent1`. They retry
  with backoff, so a controller that starts a moment later is fine.
- **A flow shows `address already in use`** — something else on that container
  already holds the port; pick a different destination port for the flow. The
  controller surfaces the bind error per port in the agent's listen-port status.
- **Rebuild after a new release** — `docker compose build --pull` then
  `docker compose up -d`.
