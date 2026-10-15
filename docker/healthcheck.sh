#!/bin/sh
# The same image runs the collector plugin, which serves the panel rather than
# the query listeners, so the check follows RSSHUB_PANEL_ADDR when set.
set -eu

# Wildcard binds are not connectable; probe them over the loopback address.
probe() {
    hostport=$1
    path=$2
    host=${hostport%:*}
    port=${hostport##*:}
    case $host in
        0.0.0.0 | :: | '') host=127.0.0.1 ;;
    esac
    curl --fail --silent --show-error --max-time 5 "http://${host}:${port}${path}" >/dev/null
}

if [ -n "${RSSHUB_PANEL_ADDR:-}" ]; then
    probe "$RSSHUB_PANEL_ADDR" /api/state
    exit 0
fi

probe "${NORMAL_ADDR:-0.0.0.0:8887}" /
probe "${VIP_ADDR:-0.0.0.0:8888}" /
