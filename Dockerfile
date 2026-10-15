# Runs the service only — no RSSHub, Redis or supervisor. The optional RSSHub
# group reuses this image for the collector plugin, so the two cannot drift.
FROM golang:1.27-bookworm AS build
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/newsscope ./cmd/newsscope

FROM debian:bookworm-slim AS runtime
# python3 for the collector plugin, curl for the healthcheck, ca-certificates for
# TLS egress, tzdata for TZ.
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates curl python3 tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 newsscope \
    && useradd --system --uid 10001 --gid 10001 --home-dir /data --shell /usr/sbin/nologin newsscope \
    && mkdir -p /data /opt/newsscope/collector/RSSHub \
    && chown -R newsscope:newsscope /data /opt/newsscope

COPY --from=build /out/newsscope /usr/local/bin/newsscope
COPY collector/RSSHub/rsshub_collector.py collector/RSSHub/index.html /opt/newsscope/collector/RSSHub/
COPY collector/RSSHub/assets /opt/newsscope/collector/RSSHub/assets
COPY docker/healthcheck.sh /usr/local/bin/newsscope-healthcheck
RUN chmod 0755 /opt/newsscope/collector/RSSHub/rsshub_collector.py /usr/local/bin/newsscope-healthcheck

USER newsscope
WORKDIR /data
EXPOSE 8886 8887 8888 8889
VOLUME ["/data"]

# Container defaults, all overridable by the compose stack or .env. Every
# listener is a host:port address. RSSHUB_PANEL_ADDR is deliberately absent:
# its presence is how the bundled healthcheck tells the plugin apart from the
# service, so compose sets it only on the plugin container.
ENV TZ=Asia/Shanghai \
    COLLECTOR_ADDR=0.0.0.0:8886 \
    NORMAL_ADDR=0.0.0.0:8887 \
    VIP_ADDR=0.0.0.0:8888 \
    ADMIN_ADDR=127.0.0.1:8889 \
    DATABASE_PATH=/data/newsscope/newsscope.db

HEALTHCHECK --interval=30s --timeout=8s --start-period=20s --retries=3 \
    CMD ["/usr/local/bin/newsscope-healthcheck"]
ENTRYPOINT ["/usr/local/bin/newsscope"]
