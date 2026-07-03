#!/usr/bin/env bash
# Parallel OTLP/JSON trace posts — fan-in schema stability check.
# Prerequisite: raw_serve running (sync ingest).
set -euo pipefail

HOST="${RAWDUCK_API_HOST:-127.0.0.1:9999}"
TOKEN="${RAWDUCK_API_TOKEN:-rt_secret}"
BASE="http://${HOST}"
TABLE="${RAWDUCK_OTLP_TABLE:-dist_traces}"
WRITERS="${RAWDUCK_DIST_WRITERS:-4}"
SPANS_PER_WRITER="${RAWDUCK_DIST_SPANS:-50}"
AUTH=(-H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json")

json_get() {
	python3 -c "import json,sys; d=json.load(sys.stdin); print(d$1)"
}

otlp_envelope() {
	local wid=$1
	local sid=$2
	python3 - <<PY
import json
wid, sid = ${wid}, ${sid}
print(json.dumps({
    "resourceSpans": [{
        "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": f"svc-{wid}"}}]},
        "scopeSpans": [{
            "spans": [{
                "traceId": "00000000000000000000000000000001",
                "spanId": f"{wid:016x}"[:16].rjust(16, "0"),
                "name": f"span-{wid}-{sid}",
                "kind": 1,
            }]
        }]
    }]
}))
PY
}

echo "== health =="
curl -sf "${BASE}/health" | json_get "['status']" | grep -q ok

post_spans() {
	local wid=$1
	local s
	for ((s = 0; s < SPANS_PER_WRITER; s++)); do
		body=$(otlp_envelope "${wid}" "${s}")
		curl -sf -X POST "${BASE}/v1/tables/${TABLE}?transform=otlp-traces" "${AUTH[@]}" -d "${body}" >/dev/null
	done
}

pids=()
for ((w = 0; w < WRITERS; w++)); do
	post_spans "${w}" &
	pids+=($!)
done
for pid in "${pids[@]}"; do
	wait "${pid}"
done

expected=$((WRITERS * SPANS_PER_WRITER))
Q=$(curl -sf -X POST "${BASE}/v1/query" "${AUTH[@]}" \
	-d "{\"sql\":\"SELECT count(*) AS c FROM ${TABLE}\"}")
count=$(echo "$Q" | json_get "['data'][0]['c']")
if [[ "${count}" -ne "${expected}" ]]; then
	echo "ERROR: expected ${expected} spans, got ${count}" >&2
	exit 1
fi

echo "OK: ${expected} OTLP spans from ${WRITERS} concurrent writers"
