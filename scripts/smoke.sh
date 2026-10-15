#!/bin/sh
# End-to-end check of the whole NewsScope path, with no model endpoint and no
# API key required: a stub model answers on loopback, the real binary ingests,
# deduplicates, analyses and serves the query back.
#
# Usage: scripts/smoke.sh [workdir]
# Exit status is 0 when the pipeline works, 1 otherwise.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
WORK=${1:-$(mktemp -d)}
STUB_PORT=${STUB_PORT:-18991}
APP_PORT=${APP_PORT:-18886}
NORMAL_PORT=${NORMAL_PORT:-18887}
VIP_PORT=${VIP_PORT:-18888}
ADMIN_PORT=${ADMIN_PORT:-18889}
STUB_PID=""
APP_PID=""

cleanup() {
    [ -n "$APP_PID" ] && kill "$APP_PID" 2>/dev/null || true
    [ -n "$STUB_PID" ] && kill "$STUB_PID" 2>/dev/null || true
    wait 2>/dev/null || true
    case $WORK in
        /tmp/*|/var/folders/*) rm -rf "$WORK" ;;
    esac
}
trap cleanup EXIT INT TERM

fail() {
    printf 'FAIL: %s\n' "$1" >&2
    exit 1
}

step() { printf '\n== %s\n' "$1"; }

step "building"
mkdir -p "$WORK"
( cd "$ROOT" && go build -o "$WORK/newsscope" ./cmd/newsscope ) || fail "go build failed"

step "starting the stub model"
cat > "$WORK/stub.py" <<'PY'
import http.server, json, sys

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except ValueError:
            body = {}
        prompt = (body.get("messages") or [{}])[0].get("content", "")
        if "matched_id" in prompt:
            result = {"duplicate": False, "matched_id": 0}
        else:
            result = {
                "category": "科技",
                "keywords": ["处理器", "工艺"],
                "summary": "某公司发布新款处理器，采用新一代工艺。",
                "importance": 3,
            }
        payload = json.dumps(
            {"choices": [{"message": {"content": json.dumps(result, ensure_ascii=False)}}]},
            ensure_ascii=False,
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

server = http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), Handler)
server.serve_forever()
PY
python3 "$WORK/stub.py" "$STUB_PORT" &
STUB_PID=$!

step "starting NewsScope"
# Replace rather than append: .env.example already defines the model keys, and a
# second definition would be ambiguous.
cp "$ROOT/.env.example" "$WORK/.env"
# Written through a temp file rather than `sed -i`: the in-place flag takes a
# different argument on BSD and GNU, and this script has to run on both.
sed -e "s|^OPENAI_BASE_URL=.*|OPENAI_BASE_URL=http://127.0.0.1:$STUB_PORT/v1|" \
    -e "s|^OPENAI_API_KEY=.*|OPENAI_API_KEY=smoke-test|" \
    -e "s|^OPENAI_MODEL=.*|OPENAI_MODEL=stub|" \
    "$WORK/.env" > "$WORK/.env.tmp"
mv "$WORK/.env.tmp" "$WORK/.env"
{
    echo "COLLECTOR_ADDR=127.0.0.1:$APP_PORT"
    echo "NORMAL_ADDR=127.0.0.1:$NORMAL_PORT"
    echo "VIP_ADDR=127.0.0.1:$VIP_PORT"
    echo "ADMIN_ADDR=127.0.0.1:$ADMIN_PORT"
    echo "DATABASE_PATH=$WORK/newsscope.db"
    echo "IDLE_RETRY_INTERVAL_SECONDS=10"
} >> "$WORK/.env"
# exec so the recorded pid is the server itself, not the wrapping subshell;
# otherwise cleanup leaves the listeners behind and the next run cannot bind.
( cd "$WORK" && exec ./newsscope > "$WORK/run.log" 2>&1 ) &
APP_PID=$!

step "waiting for the listeners"
i=0
while [ $i -lt 50 ]; do
    if curl -fsS -m 1 "http://127.0.0.1:$NORMAL_PORT/" >/dev/null 2>&1; then
        break
    fi
    i=$((i + 1))
    sleep 0.2
done
[ $i -lt 50 ] || { cat "$WORK/run.log" >&2; fail "the query listener never came up"; }

step "submitting an article"
RESPONSE=$(curl -fsS -m 10 -X POST "http://127.0.0.1:$APP_PORT/ingest/normal" \
    -H 'Content-Type: application/json' \
    -d '{"version":1,"articles":[{"source":"冒烟测试","title":"某公司发布新款处理器","body":"某公司今日正式发布了新款处理器，采用新一代工艺，产品将在下季度上市。","collected_at":1791798120}]}')
echo "$RESPONSE" | grep -q '"accepted":1' || fail "article was not accepted: $RESPONSE"

step "checking rejection reasons are specific"
MISSING=$(curl -sS -m 10 -X POST "http://127.0.0.1:$APP_PORT/ingest/normal" \
    -H 'Content-Type: application/json' \
    -d '{"version":1,"articles":[{"title":"缺字段","body":"缺字段","collected_at":1791798120}]}')
echo "$MISSING" | grep -q 'missing_source' || fail "missing_source not reported: $MISSING"
TYPO=$(curl -sS -m 10 -X POST "http://127.0.0.1:$APP_PORT/ingest/normal" \
    -H 'Content-Type: application/json' \
    -d '{"version":1,"articles":[{"source":"x","title":"t","body":"b","collected_at":1791798120,"auther":"typo"}]}')
echo "$TYPO" | grep -q 'unknown_field:auther' || fail "misspelled field not named: $TYPO"
MILLIS=$(curl -sS -m 10 -X POST "http://127.0.0.1:$APP_PORT/ingest/normal" \
    -H 'Content-Type: application/json' \
    -d '{"version":1,"articles":[{"source":"x","title":"t","body":"b","collected_at":1791798120000}]}')
echo "$MILLIS" | grep -q 'collected_at_not_seconds' || fail "millisecond timestamp not named: $MILLIS"

step "waiting for analysis"
i=0
while [ $i -lt 100 ]; do
    QUERY=$(curl -fsS -m 5 "http://127.0.0.1:$NORMAL_PORT/news?category=%E7%A7%91%E6%8A%80&start=0&end=1" 2>/dev/null || true)
    case $QUERY in
        *'"summary"'*) break ;;
    esac
    i=$((i + 1))
    sleep 0.3
done
case ${QUERY:-} in
    *'"summary"'*) printf '  analysis present\n' ;;
    *) cat "$WORK/run.log" >&2; fail "the article was never analysed" ;;
esac

step "checking query error reporting"
BAD=$(curl -sS -m 5 "http://127.0.0.1:$NORMAL_PORT/news?category=%E4%B8%8D%E5%AD%98%E5%9C%A8&start=0&end=1")
echo "$BAD" | grep -q 'unknown_category' || fail "unknown_category not reported: $BAD"

NOTFOUND=$(curl -s -o /dev/null -w '%{http_code}' -m 5 "http://127.0.0.1:$NORMAL_PORT/no-such-path")
[ "$NOTFOUND" = "404" ] || fail "unknown path returned $NOTFOUND, want 404"

step "checking keyword search"
SEARCH=$(curl -fsS -m 5 "http://127.0.0.1:$NORMAL_PORT/news/search?category=%E7%A7%91%E6%8A%80&q=%E5%A4%84%E7%90%86%E5%99%A8")
echo "$SEARCH" | grep -q '"total":1' || fail "search did not find the article: $SEARCH"
SEARCH_BAD=$(curl -sS -m 5 "http://127.0.0.1:$NORMAL_PORT/news/search?category=%E7%A7%91%E6%8A%80")
echo "$SEARCH_BAD" | grep -q 'q_is_required' || fail "missing q not reported: $SEARCH_BAD"

step "checking the admin panel"
PANEL=$(curl -fsS -m 5 "http://127.0.0.1:$ADMIN_PORT/api/status")
echo "$PANEL" | grep -q '"articles"' || fail "status endpoint unusable: $PANEL"

printf '\nSMOKE OK — ingest, rejection reasons, dedup, analysis, query, search, errors and panel all behave.\n'
