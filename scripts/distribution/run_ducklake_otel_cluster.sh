#!/usr/bin/env zsh
# Multi-process OTEL writers → shared DuckLake (sqlite metadata) + direct reader.
#
# Each writer is a separate DuckDB process: attach lake, run raw_serve briefly,
# post OTLP, exit (releases DuckLake metadata lock). Sqlite-backed DuckLake does
# not allow concurrent persistent ATTACH from multiple processes — use one
# long-lived hub or postgres DuckLake metadata for always-on multi-host writers.
#
# Reader: separate process, ATTACH ducklake READ_ONLY after writers finish.
set -euo pipefail

typeset -a WRITER_PIDS

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DUCKDB="${DUCKDB:-${ROOT}/build/release/duckdb}"
EXT="${EXT:-${ROOT}/build/release/extension/rawduck/rawduck.duckdb_extension}"
WORKDIR="${WORKDIR:-${ROOT}/benchmark/work/ducklake_cluster}"
LAKE="${LAKE:-${WORKDIR}/shared.ducklake}"
DATA="${DATA:-${WORKDIR}/data}"
TOKEN="${RAWDUCK_API_TOKEN:-rt_secret}"
WRITERS="${RAWDUCK_CLUSTER_WRITERS:-2}"
SPANS_PER_WRITER="${RAWDUCK_CLUSTER_SPANS:-40}"

if [[ ! -x "${DUCKDB}" ]]; then
	echo "Build release first: GEN=ninja make release" >&2
	exit 1
fi

rm -rf "${WORKDIR}"
mkdir -p "${DATA}"

init_sql="
LOAD '${EXT}';
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:${LAKE}' AS lake (DATA_PATH '${DATA}');
"

otlp_post() {
	local port=$1 wid=$2 sid=$3 extra=$4
	python3 - <<PY | curl -sf -X POST "http://127.0.0.1:${port}/otlp/v1/traces?transform=otlp-traces" \
		-H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" -d @- >/dev/null
import json
wid, sid, extra = int(${wid}), int(${sid}), int(${extra})
span = {
    "traceId": "00000000000000000000000000000001",
    "spanId": f"{wid:08x}{sid:08x}"[:16],
    "name": f"span-{wid}-{sid}",
    "kind": 1,
    "attributes": [{"key": "writer.id", "value": {"intValue": wid}}],
}
if extra:
    span["attributes"].append({"key": "extra.attr", "value": {"stringValue": f"writer-{wid}"}})
print(json.dumps({"resourceSpans": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": f"writer-{wid}"}}]}, "scopeSpans": [{"spans": [span]}]}]}))
PY
}

run_writer_instance() {
	local wid=$1
	local port=$((9901 + wid))
	local fifo="${WORKDIR}/writer_${wid}.fifo"
	rm -f "${fifo}"
	mkfifo "${fifo}"

	"${DUCKDB}" -unsigned -batch < "${fifo}" &
	local pid=$!
	local fd
	exec {fd}>"${fifo}"
	cat <<SQL >&${fd}
${init_sql}
SELECT * FROM raw_serve(host := '127.0.0.1', port := ${port}, token := '${TOKEN}', ingest_prefix := 'lake.main');
SQL

	sleep 0.4
	if ! curl -sf "http://127.0.0.1:${port}/health" >/dev/null; then
		echo "ERROR: writer ${wid} failed to start on port ${port}" >&2
		kill "${pid}" 2>/dev/null || true
		exec {fd}>&-
		return 1
	fi

	local s extra
	for ((s = 0; s < SPANS_PER_WRITER; s++)); do
		extra=0
		[[ "${s}" -eq $((SPANS_PER_WRITER - 1)) ]] && extra=1
		otlp_post "${port}" "${wid}" "${s}" "${extra}"
	done

	curl -sf -X POST "http://127.0.0.1:${port}/v1/query" \
		-H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" \
		-d '{"sql":"CALL raw_serve_stop()"}' >/dev/null || true
	exec {fd}>&-
	wait "${pid}" 2>/dev/null || true
	echo "writer ${wid}: ${SPANS_PER_WRITER} spans"
}

echo "== ${WRITERS} writer process(es) → shared DuckLake (sequential, sqlite metadata) =="
for ((w = 0; w < WRITERS; w++)); do
	run_writer_instance "${w}"
done

expected=$((WRITERS * SPANS_PER_WRITER))
echo "== reader process: ATTACH ducklake READ_ONLY =="
read_out=$(
	${DUCKDB} -unsigned -batch -csv -noheader <<SQL
LOAD '${EXT}';
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:${LAKE}' AS lake (DATA_PATH '${DATA}', READ_ONLY);
SELECT count(*) FROM lake.main.otel_traces;
SELECT count(DISTINCT "resource.service.name") FROM lake.main.otel_traces;
SELECT count(*) FROM lake.main.otel_traces WHERE "extra.attr" IS NOT NULL;
SQL
)
typeset -a lines
lines=("${(f)read_out}")
count="${lines[1]:-0}"
services="${lines[2]:-0}"
extras="${lines[3]:-0}"

echo "rows=${count} (expect ${expected}), services=${services} (expect ${WRITERS}), extra.attr=${extras} (expect ${WRITERS})"
if [[ "${count}" -ne "${expected}" || "${services}" -ne "${WRITERS}" || "${extras}" -ne "${WRITERS}" ]]; then
	echo "FAIL" >&2
	exit 1
fi
echo "OK: multi-process writers → DuckLake → read-only reader"
