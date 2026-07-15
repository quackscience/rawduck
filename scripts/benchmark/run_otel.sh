#!/usr/bin/env zsh
# OTEL ingest benchmark: cold (schema discovery) then warm (same DuckDB session).
#
# Each run opens one DuckDB process, ingests a cold batch (schema discovery), then
# appends a warm batch with fresh timestamps and an already-absorbed shape.
#
# Examples:
#   ./scripts/benchmark/run_otel.sh --records 1000000 --runs 5
#   ./scripts/benchmark/run_otel.sh --quick          # 100k, 1 run, CI-friendly
set -euo pipefail

export PATH="/bin:/usr/bin:/usr/local/bin:${HOME}/.pyenv/shims:${PATH}"
PYTHON="${PYTHON:-$(command -v python3)}"
if [[ -z "${PYTHON}" || ! -x "${PYTHON}" ]]; then
	echo "python3 not found (set PYTHON=...)" >&2
	exit 1
fi

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck source=scripts/benchmark/lib.sh
source "${ROOT}/scripts/benchmark/lib.sh"

typeset RECORDS=1000000 RUNS=5 SIGNAL=all WARM_ONLY=0 COLD_ONLY=0
typeset OUTPUT=""

usage() {
	cat <<EOF
usage: $(basename "$0") [options]

  --records N       records per signal (default: 1000000)
  --runs N          session repetitions (default: 5; best wall time reported)
  --signal NAME     traces|logs|metrics|all (default: all)
  --quick           100k records, 1 run (CI smoke)
  --warm-only       report warm timing only (still runs cold first in-session)
  --cold-only       report cold timing only
  --output PATH     write JSON results (default: benchmark/results/otel_<ts>.json)
  -h, --help        this message
EOF
}

while (( $# > 0 )); do
	case "$1" in
		--records) RECORDS=$2; shift 2 ;;
		--runs) RUNS=$2; shift 2 ;;
		--signal) SIGNAL=$2; shift 2 ;;
		--quick) RECORDS=100000; RUNS=1; shift ;;
		--warm-only) WARM_ONLY=1; shift ;;
		--cold-only) COLD_ONLY=1; shift ;;
		--output) OUTPUT=$2; shift 2 ;;
		-h|--help) usage; exit 0 ;;
		*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
	esac
done

bench_require_build

typeset DUCKDB EXT DATA WORK RESULTS SESSION
DUCKDB="$(bench_duckdb)"
EXT="$(bench_extension)"
DATA="$(bench_data_dir)"
WORK="$(bench_work_dir)/otel_$$"
RESULTS="$(bench_results_dir)"
SESSION="${ROOT}/scripts/benchmark/run_otel_session.py"
/bin/mkdir -p "${DATA}" "${WORK}" "${RESULTS}"
/bin/chmod +x "${SESSION}" 2>/dev/null || true

typeset -a SIGNALS
if [[ "${SIGNAL}" == "all" ]]; then
	SIGNALS=(traces logs metrics)
else
	SIGNALS=("${SIGNAL}")
fi

transform_for() {
	case "$1" in
		traces) echo otlp-traces ;;
		logs) echo otlp-logs ;;
		metrics) echo otlp-metrics ;;
		*) echo "unknown signal: $1" >&2; return 1 ;;
	esac
}

# Nanosecond timestamp base for warm batches (~1 day after cold base).
WARM_TS_BASE=1700086400000000000

ensure_data() {
	local sig=$1
	local kind=$2
	local path="${DATA}/${sig}_$((RECORDS / 1000))k${kind}.ndjson"
	if [[ ! -f "${path}" ]]; then
		echo "Generating ${path} (${RECORDS} records)..." >&2
		if [[ "${kind}" == "_warm" ]]; then
			"${PYTHON}" "${ROOT}/scripts/benchmark/gen_otlp.py" "${sig}" "${RECORDS}" "${DATA}" \
				"${WARM_TS_BASE}" "_warm" >/dev/null
		else
			"${PYTHON}" "${ROOT}/scripts/benchmark/gen_otlp.py" "${sig}" "${RECORDS}" "${DATA}" >/dev/null
		fi
	fi
	echo "${path}"
}

py_cmp_lt() {
	"${PYTHON}" - "$1" "$2" <<'PY'
import sys
print(1 if float(sys.argv[1]) < float(sys.argv[2]) else 0)
PY
}

py_rec_s() {
	"${PYTHON}" - "$1" "$2" <<'PY'
import sys
rows, sec = float(sys.argv[1]), float(sys.argv[2])
print(int(rows / sec) if sec > 0 else 0)
PY
}

py_mb_s() {
	"${PYTHON}" - "$1" "$2" <<'PY'
import sys
b, sec = float(sys.argv[1]), float(sys.argv[2])
print(f"{(b / sec / 1e6):.1f}" if sec > 0 else "0")
PY
}

file_bytes() {
	"${PYTHON}" - "$1" <<'PY'
import os, sys
print(os.path.getsize(sys.argv[1]))
PY
}

typeset -a JSON_PARTS
JSON_PARTS=()

commit="$(git -C "${ROOT}" rev-parse HEAD)"
branch="$(git -C "${ROOT}" rev-parse --abbrev-ref HEAD)"

for sig in "${SIGNALS[@]}"; do
	cold_path="$(ensure_data "${sig}" "")"
	warm_path="$(ensure_data "${sig}" "_warm")"
	transform="$(transform_for "${sig}")"
	bytes="$(file_bytes "${cold_path}")"
	table="otel_${sig}"

	typeset cold_best="" warm_best=""
	typeset cold_rows=0 cold_added=0 cold_widened=0 cold_errors=0
	typeset warm_rows=0 warm_added=0 warm_widened=0 warm_errors=0

	for (( r = 1; r <= RUNS; r++ )); do
		db="${WORK}/${sig}_run_${r}.db"
		/bin/rm -f "${db}"
		lines="$("${PYTHON}" "${SESSION}" "${DUCKDB}" "${EXT}" "${db}" "${table}" \
			"${cold_path}" "${warm_path}" "${transform}")"
		cold_line="${lines%%$'\n'*}"
		warm_line="${lines#*$'\n'}"
		if (( ! WARM_ONLY )); then
			if [[ -z "${cold_best}" ]] || (( $(py_cmp_lt "${cold_line%%,*}" "${cold_best}") )); then
				cold_best="${cold_line%%,*}"
				IFS=',' read -r _ cold_rows cold_added cold_widened cold_errors <<<"${cold_line}"
			fi
		fi
		if (( ! COLD_ONLY )); then
			if [[ -z "${warm_best}" ]] || (( $(py_cmp_lt "${warm_line%%,*}" "${warm_best}") )); then
				warm_best="${warm_line%%,*}"
				IFS=',' read -r _ warm_rows warm_added warm_widened warm_errors <<<"${warm_line}"
			fi
		fi
	done

	if (( ! WARM_ONLY )); then
		rec_s=$(py_rec_s "${cold_rows}" "${cold_best}")
		mb_s=$(py_mb_s "${bytes}" "${cold_best}")
		echo "COLD ${sig}: ${cold_rows} rows in ${cold_best}s -> ${rec_s} rec/s, ${mb_s} MB/s (best of ${RUNS} sessions)" >&2
		JSON_PARTS+=("\"${sig}_cold\": {\"records\": ${cold_rows}, \"seconds\": ${cold_best}, \"records_per_sec\": ${rec_s}, \"mb_per_sec\": ${mb_s}, \"bytes\": ${bytes}, \"columns_added\": ${cold_added}, \"columns_widened\": ${cold_widened}, \"errors\": ${cold_errors}}")
	fi

	if (( ! COLD_ONLY )); then
		warm_bytes="$(file_bytes "${warm_path}")"
		warm_rec_s=$(py_rec_s "${warm_rows}" "${warm_best}")
		warm_mb_s=$(py_mb_s "${warm_bytes}" "${warm_best}")
		echo "WARM ${sig}: ${warm_rows} rows in ${warm_best}s -> ${warm_rec_s} rec/s, ${warm_mb_s} MB/s (best of ${RUNS} sessions, same process)" >&2
		JSON_PARTS+=("\"${sig}_warm\": {\"records\": ${warm_rows}, \"seconds\": ${warm_best}, \"records_per_sec\": ${warm_rec_s}, \"mb_per_sec\": ${warm_mb_s}, \"bytes\": ${warm_bytes}, \"columns_added\": ${warm_added}, \"columns_widened\": ${warm_widened}, \"errors\": ${warm_errors}}")
	fi
done

ts="$("${PYTHON}" - <<'PY'
from datetime import datetime, timezone
print(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
PY
)"
if [[ -z "${OUTPUT}" ]]; then
	OUTPUT="${RESULTS}/otel_${RECORDS}_${ts}.json"
fi

{
	echo "{"
	echo "  \"benchmark\": \"otel_ingest\","
	echo "  \"timestamp\": \"${ts}\","
	echo "  \"git_commit\": \"${commit}\","
	echo "  \"git_branch\": \"${branch}\","
	echo "  \"records_per_signal\": ${RECORDS},"
	echo "  \"runs\": ${RUNS},"
	echo "  \"session\": \"single_process_cold_then_warm\","
	echo "  \"results\": {"
	printf "    %s\n" "$(IFS=$',\n'; echo "${JSON_PARTS[*]}")" | /usr/bin/sed '$!s/$/,/'
	echo "  }"
	echo "}"
} > "${OUTPUT}"

echo "Wrote ${OUTPUT}" >&2
/bin/rm -rf "${WORK}"
