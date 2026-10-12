#!/bin/sh
set -eu

redis-cli -h 127.0.0.1 ping | grep -q '^PONG$'
curl --fail --silent --show-error --max-time 5 http://127.0.0.1:1200/healthz >/dev/null
curl --fail --silent --show-error --max-time 5 http://127.0.0.1:8885/api/state >/dev/null
curl --fail --silent --show-error --max-time 5 http://127.0.0.1:8887/ >/dev/null
curl --fail --silent --show-error --max-time 5 http://127.0.0.1:8888/ >/dev/null
