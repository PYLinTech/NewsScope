ARG RSSHUB_IMAGE=ghcr.io/diygod/rsshub:chromium-bundled-d55daa06c1340db6fa9cc5f865f627748eb5b346

FROM golang:1.27-bookworm AS newsscope-build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/newsscope ./cmd/newsscope

FROM ${RSSHUB_IMAGE} AS runtime
USER root

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates curl python3 redis-server socat supervisor \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/newsscope/collector/RSSHub \
        /data/newsscope /data/rsshub /data/redis /usr/share/licenses/RSSHub \
    && chown -R node:node /opt/newsscope /data \
    && chown -R redis:redis /data/redis

RUN git config --system --add safe.directory /app

COPY --from=newsscope-build /out/newsscope /usr/local/bin/newsscope
COPY collector/RSSHub/rsshub_collector.py collector/RSSHub/index.html /opt/newsscope/collector/RSSHub/
COPY collector/RSSHub/assets /opt/newsscope/collector/RSSHub/assets
COPY docker/RSSHUB-LICENSE /usr/share/licenses/RSSHub/AGPL-3.0.txt
COPY docker/supervisord.conf /etc/supervisor/supervisord.conf
COPY docker/healthcheck.sh /usr/local/bin/newsscope-healthcheck
RUN chmod 0755 /opt/newsscope/collector/RSSHub/rsshub_collector.py /usr/local/bin/newsscope-healthcheck

ENV NODE_ENV=production \
    TZ=Asia/Shanghai \
    PORT=1200 \
    LISTEN_INADDR_ANY=1 \
    CACHE_TYPE=redis \
    CACHE_EXPIRE=300 \
    CACHE_CONTENT_EXPIRE=3600 \
    REDIS_URL=redis://127.0.0.1:6379/ \
    RSSHUB_REDIS_MAXMEMORY=256mb \
    NORMAL_ADDR=0.0.0.0:8887 \
    VIP_ADDR=0.0.0.0:8888 \
    COLLECTOR_PORT=8886 \
    ADMIN_PORT=8889 \
    DATABASE_PATH=/data/newsscope/newsscope.db \
    XDG_CONFIG_HOME=/data/config \
    RSSHUB_CONFIG=/data/rsshub/config.json \
    RSSHUB_PANEL_HOST=0.0.0.0 \
    RSSHUB_PANEL_PORT=8885 \
    RSSHUB_PANEL_CONTAINER_MODE=1 \
    RSSHUB_POLL_INTERVAL=60 \
    RSSHUB_POLL_WORKERS=4

WORKDIR /app
EXPOSE 1200 8885 18886 8887 8888
VOLUME ["/data"]
HEALTHCHECK --interval=30s --timeout=8s --start-period=60s --retries=3 \
    CMD ["/usr/local/bin/newsscope-healthcheck"]
ENTRYPOINT ["/usr/bin/supervisord", "-n", "-c", "/etc/supervisor/supervisord.conf"]
