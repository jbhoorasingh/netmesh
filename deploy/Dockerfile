# syntax=docker/dockerfile:1
#
# Packages a released NetMesh binary into a minimal Linux image. No source is
# built here — the binary is pulled from a GitHub release. Override the release
# with  --build-arg VERSION=vX.Y.Z  (or NETMESH_VERSION in docker-compose).
#
# TARGETARCH is provided automatically by BuildKit (amd64 / arm64) and matches
# the release asset names (netmesh_linux_amd64 / netmesh_linux_arm64).

ARG VERSION=v0.2.0

FROM alpine:3.20 AS fetch
ARG VERSION
ARG TARGETARCH
RUN apk add --no-cache curl ca-certificates
RUN set -eux; \
    arch="${TARGETARCH:-}"; \
    if [ -z "$arch" ]; then \
      case "$(uname -m)" in \
        x86_64) arch=amd64 ;; \
        aarch64|arm64) arch=arm64 ;; \
        *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;; \
      esac; \
    fi; \
    url="https://github.com/jbhoorasingh/netmesh/releases/download/${VERSION}/netmesh_linux_${arch}"; \
    echo "fetching ${url}"; \
    curl -fSL -o /usr/local/bin/netmesh "${url}"; \
    chmod +x /usr/local/bin/netmesh

FROM alpine:3.20
# The NetMesh binary is statically linked (CGO disabled), so the runtime image
# needs nothing but CA certs.
RUN apk add --no-cache ca-certificates
COPY --from=fetch /usr/local/bin/netmesh /usr/local/bin/netmesh

# Web UI / REST API / control-plane WebSocket (one per container).
EXPOSE 5999
ENTRYPOINT ["netmesh"]
