#!/usr/bin/env bash
# Parallel HTTP writers against one raw_serve instance.
# Prerequisite: server running with sync ingest (default async=false).
#
#   ./build/release/duckdb -unsigned -c "
#     LOAD './build/release/extension/rawduck/rawduck.duckdb_extension';
#     CALL raw_serve('127.0.0.1:9999', token := 'rt_secret');
#   "
set -euo pipefail

HOST="${RAWDUCK_API_HOST:-127.0.0.1:9999}"
TOKEN="${RAWDUCK_API_TOKEN:-rt_secret}"
BASE="http://${HOST}"
TABLE="${RAWDUCK_DIST_TABLE:-dist_events}"
WRITERS="${RAWDUCK_DIST_WRITERS:-8}"
ROWS_PER_WRITER="${RAWDUCK_DIST_ROWS:-100}"
AUTH=(-H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json")

json_get() {
	python3 -c "import json,sys; d=json.load(sys.stdin); print(d$1)"
}

echo "== health =="
curl -sf "${BASE}/health" | json_get "['status']" | grep -q ok

echo "== parallel writers: ${WRITERS} x ${ROWS_PER_WRITER} rows -> ${TABLE} =="

write_batch() {
	local wid=$1
	local payload="["
	local i
	for ((i = 0; i < ROWS_PER_WRITER; i++)); do
		[[ $i -gt 0 ]] && payload+=","
		payload+="{\"writer\":${wid},\"seq\":${i},\"action\":\"w${wid}\"}"
	done
	payload+="]"
	curl -sf -X POST "${BASE}/v1/tables/${TABLE}" "${AUTH[@]}" -d "${payload}" >/dev/null
}

pids=()
for ((w = 0; w < WRITERS; w++)); do
	write_batch "${w}" &
	pids+=($!)
done
fail=0
for pid in "${pids[@]}"; do
	wait "${pid}" || fail=1
done
if [[ "${fail}" -ne 0 ]]; then
	echo "ERROR: one or more writers failed" >&2
	exit 1
fi

expected=$((WRITERS * ROWS_PER_WRITER))
echo "== verify row count (expect ${expected}) =="
Q=$(curl -sf -X POST "${BASE}/v1/query" "${AUTH[@]}" \
	-d "{\"sql\":\"SELECT count(*) AS c FROM ${TABLE}\"}")
count=$(echo "$Q" | json_get "['data'][0]['c']")
if [[ "${count}" -ne "${expected}" ]]; then
	echo "ERROR: expected ${expected} rows, got ${count}" >&2
	exit 1
fi

writers=$(curl -sf -X POST "${BASE}/v1/query" "${AUTH[@]}" \
	-d "{\"sql\":\"SELECT count(DISTINCT writer) AS c FROM ${TABLE}\"}" | json_get "['data'][0]['c']")
if [[ "${writers}" -ne "${WRITERS}" ]]; then
	echo "ERROR: expected ${WRITERS} distinct writers, got ${writers}" >&2
	exit 1
fi

echo "OK: ${expected} rows from ${WRITERS} concurrent writers"
