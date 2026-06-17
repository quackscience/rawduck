#!/usr/bin/env bash
# HTTP API compatibility smoke test (start raw_serve first; sync is the default)
set -euo pipefail

HOST="${RAWDUCK_API_HOST:-127.0.0.1:9999}"
TOKEN="${RAWDUCK_API_TOKEN:-rt_secret}"
BASE="http://${HOST}"
AUTH=(-H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json")

json_get() {
	python3 -c "import json,sys; d=json.load(sys.stdin); print(d$1)"
}

echo "== health =="
curl -sf "${BASE}/health" | json_get "['status']" | grep -q ok

echo "== insert =="
INS=$(curl -sf -X POST "${BASE}/v1/tables/events" "${AUTH[@]}" \
	-d '[{"action":"click","user":"alice"}]')
echo "$INS" | json_get "['inserted']" | grep -q 1
test "$(echo "$INS" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')" -eq 1

echo "== query (object rows) =="
Q=$(curl -sf -X POST "${BASE}/v1/query" "${AUTH[@]}" \
	-d '{"sql":"SELECT action, count(*) AS c FROM events GROUP BY action"}')
echo "$Q" | json_get "['data'][0]['action']" | grep -q click
echo "$Q" | json_get "['statistics']['rows_read']" | grep -qE '^[0-9]+$'
echo "$Q" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["hints"]==[]'

echo "== list tables =="
L=$(curl -sf "${BASE}/v1/tables" -H "Authorization: Bearer ${TOKEN}")
echo "$L" | json_get "['tables'][0]['total_rows']" | grep -qE '^[0-9]+$'
echo "$L" | json_get "['project']['name']" | grep -q default

echo "== describe =="
D=$(curl -sf "${BASE}/v1/tables/events" -H "Authorization: Bearer ${TOKEN}")
echo "$D" | json_get "['table']['name']" | grep -q events
echo "$D" | json_get "['table']['columns'][0]['type']" | grep -qE 'String|VARCHAR'

echo "== otlp auto-transform =="
OTLP='{"resourceSpans":[{"resource":{},"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","name":"test"}]}]}]}'
curl -sf -X POST "${BASE}/v1/tables/traces" "${AUTH[@]}" -d "$OTLP" | json_get "['inserted']" | grep -q 1
DESC=$(curl -sf "${BASE}/v1/tables/traces" -H "Authorization: Bearer ${TOKEN}")
echo "$DESC" | python3 -c 'import json,sys; cols={c["name"] for c in json.load(sys.stdin)["table"]["columns"]}; assert "traceId" in cols or "trace_id" in cols or any("trace" in c.lower() for c in cols)'

echo "== transform query param =="
curl -sf -X POST "${BASE}/v1/tables/traces2?transform=otlp-traces" "${AUTH[@]}" -d "$OTLP" | json_get "['inserted']" | grep -q 1

echo "OK: API compatibility smoke test passed"
