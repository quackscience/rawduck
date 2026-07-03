#!/usr/bin/env zsh
# Shared helpers for DuckLake distribution probes.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
export DUCKDB="${DUCKDB:-${ROOT}/build/release/duckdb}"
export EXT="${EXT:-${ROOT}/build/release/extension/rawduck/rawduck.duckdb_extension}"
export PROBE_WORK="${PROBE_WORK:-${ROOT}/benchmark/work/distribution_probe}"

duckdb_quiet() {
	if [[ "$*" == *"-csv"* ]]; then
		"${DUCKDB}" -unsigned -batch "$@"
	else
		"${DUCKDB}" -unsigned -batch "$@" >/dev/null
	fi
}

probe_require_build() {
	if [[ ! -x "${DUCKDB}" ]]; then
		echo "Build release first: GEN=ninja make release" >&2
		exit 1
	fi
}

probe_fresh_lake() {
	local tag=$1
	export LAKE="${PROBE_WORK}/${tag}/shared.ducklake"
	export DATA="${PROBE_WORK}/${tag}/data"
	rm -rf "${PROBE_WORK:?}/${tag}"
	mkdir -p "${DATA}"
}

probe_setup_sql() {
	cat <<SQL
LOAD '${EXT}';
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:${LAKE}' AS lake (DATA_PATH '${DATA}');
SQL
}

# record_result id category outcome detail [notes]
# outcome: pass | fail | expected_fail | skip
typeset -a PROBE_RESULTS

probe_record() {
	local id=$1 category=$2 outcome=$3 detail=$4 notes=${5:-}
	local esc_detail=${detail//\"/\\\"}
	local esc_notes=${notes//\"/\\\"}
	PROBE_RESULTS+=("{\"id\":\"${id}\",\"category\":\"${category}\",\"outcome\":\"${outcome}\",\"detail\":\"${esc_detail}\",\"notes\":\"${esc_notes}\"}")
}

probe_lake_count() {
	local sql=$1
	duckdb_quiet -csv -noheader <<SQL
$(probe_setup_sql)
${sql}
SQL
}

# Start raw_serve against an attached DuckLake; returns fifo fd via PROBE_HUB_FD and pid via PROBE_HUB_PID
probe_start_hub() {
	local port=$1
	local fifo="${PROBE_WORK}/hub_${port}.fifo"
	rm -f "${fifo}"
	mkfifo "${fifo}"
	"${DUCKDB}" -unsigned -batch < "${fifo}" >/dev/null 2>&1 &
	PROBE_HUB_PID=$!
	local fd
	exec {fd}>"${fifo}"
	cat <<SQL >&${fd}
$(probe_setup_sql)
SELECT * FROM raw_serve(host := '127.0.0.1', port := ${port}, token := 'probe', ingest_prefix := 'lake.main');
SQL
	PROBE_HUB_FD=${fd}
	local i=0
	while (( i < 20 )); do
		if curl -sf --max-time 1 "http://127.0.0.1:${port}/health" >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.2
		i=$((i + 1))
	done
	return 1
}

probe_stop_hub() {
	local port=$1
	curl -sf --max-time 2 -X POST "http://127.0.0.1:${port}/v1/query" \
		-H "Authorization: Bearer probe" -H "Content-Type: application/json" \
		-d '{"sql":"SELECT * FROM raw_serve_stop()"}' >/dev/null 2>&1 || true
	if [[ -n "${PROBE_HUB_FD:-}" ]]; then
		eval "exec ${PROBE_HUB_FD}>&-"
		unset PROBE_HUB_FD
	fi
	if [[ -n "${PROBE_HUB_PID:-}" ]]; then
		kill -TERM "${PROBE_HUB_PID}" 2>/dev/null || true
		sleep 0.2
		kill -KILL "${PROBE_HUB_PID}" 2>/dev/null || true
		wait "${PROBE_HUB_PID}" 2>/dev/null || true
		unset PROBE_HUB_PID
	fi
}

probe_write_report() {
	local report_json="${PROBE_WORK}/probe_report.json"
	local report_md="${PROBE_WORK}/probe_report.md"
	local raw="${PROBE_WORK}/probe_results.raw"
	mkdir -p "${PROBE_WORK}"
	printf '%s\n' "${PROBE_RESULTS[@]}" > "${raw}"

	python3 - <<PY
import json, datetime, pathlib
raw = pathlib.Path("${raw}").read_text().splitlines()
results = [json.loads(line) for line in raw if line.strip()]
out = {
    "generated_at": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    "duckdb": "${DUCKDB}",
    "scenarios": results,
}
pathlib.Path("${report_json}").write_text(json.dumps(out, indent=2) + "\n")
lines = [
    "# DuckLake distribution probe report",
    "",
    f"Generated: {out['generated_at']} UTC",
    "",
    "| Scenario | Category | Outcome | Detail | Notes |",
    "|---|---|---|---|---|",
]
for s in results:
    lines.append(f"| {s['id']} | {s['category']} | {s['outcome']} | {s['detail']} | {s.get('notes','')} |")
pathlib.Path("${report_md}").write_text("\n".join(lines) + "\n")
PY

	echo "Report: ${report_md}"
	echo "JSON:   ${report_json}"
}
