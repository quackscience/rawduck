#!/usr/bin/env zsh
# OTEL ingest benchmark: cold (schema discovery) and warm (stable re-ingest) timings.
#
# Examples:
#   ./scripts/benchmark/run_otel.sh --records 1000000 --runs 3
#   ./scripts/benchmark/run_otel.sh --quick          # 100k, 1 run, CI-friendly
#   ./scripts/benchmark/run_otel.sh --warm-only --records 1000000
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

typeset RECORDS=1000000 RUNS=3 SIGNAL=all WARM_ONLY=0 COLD_ONLY=0
typeset OUTPUT=""

usage() {
	cat <<EOF
usage: $(basename "$0") [options]

  --records N       records per signal (default: 1000000)
  --runs N          repetitions per mode (default: 3; best wall time reported)
  --signal NAME     traces|logs|metrics|all (default: all)
  --quick           100k records, 1 run (CI smoke)
  --warm-only       skip cold ingest (schema already in DB from prior run)
  --cold-only       skip warm re-ingest
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

typeset DUCKDB EXT DATA WORK RESULTS
DUCKDB="$(bench_duckdb)"
EXT="$(bench_extension)"
DATA="$(bench_data_dir)"
WORK="$(bench_work_dir)/otel_$$"
RESULTS="$(bench_results_dir)"
/bin/mkdir -p "${DATA}" "${WORK}" "${RESULTS}"

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

ensure_data() {
	local sig=$1
	local path="${DATA}/${sig}_$((RECORDS / 1000))k.ndjson"
	if [[ ! -f "${path}" ]]; then
		echo "Generating ${path} (${RECORDS} records)..." >&2
		"${PYTHON}" "${ROOT}/scripts/benchmark/gen_otlp.py" "${sig}" "${RECORDS}" "${DATA}" >/dev/null
	fi
	echo "${path}"
}

run_ingest() {
	local db=$1 table=$2 file=$3 transform=$4
	"${PYTHON}" - "${DUCKDB}" "${EXT}" "${db}" "${table}" "${file}" "${transform}" <<'PY'
import subprocess
import sys
import time

duckdb, ext, db, table, path, transform = sys.argv[1:7]
sql = f"""
LOAD '{ext}';
SELECT rows, columns_added, columns_widened, errors
FROM raw_ingest_file('{table}', '{path}', transform := '{transform}');
CHECKPOINT;
"""
start = time.perf_counter()
proc = subprocess.run([duckdb, db, "-unsigned", "-batch", "-csv", "-noheader"], input=sql.encode(), capture_output=True)
elapsed = time.perf_counter() - start
if proc.returncode != 0:
    sys.stderr.write(proc.stderr.decode())
    sys.stderr.write(proc.stdout.decode())
    raise SystemExit(proc.returncode)
lines = [ln.strip() for ln in proc.stdout.decode().splitlines() if ln.strip()]
parts = lines[-1].split(",") if lines else ["0", "0", "0", "0"]
rows = int(parts[0]) if parts else 0
print(f"{elapsed:.6f},{rows},{parts[1] if len(parts) > 1 else 0},{parts[2] if len(parts) > 2 else 0},{parts[3] if len(parts) > 3 else 0}")
PY
}

prepare_warm_table() {
	local db=$1 table=$2
	"${PYTHON}" - "${DUCKDB}" "${EXT}" "${db}" "${table}" <<'PY'
import subprocess
import sys

duckdb, ext, db, table = sys.argv[1:5]
sql = f"""
LOAD '{ext}';
DELETE FROM {table};
CHECKPOINT;
"""
proc = subprocess.run([duckdb, db, "-unsigned", "-batch"], input=sql.encode(), capture_output=True)
if proc.returncode != 0:
    sys.stderr.write(proc.stderr.decode())
    sys.stderr.write(proc.stdout.decode())
    raise SystemExit(proc.returncode)
PY
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
	path="$(ensure_data "${sig}")"
	transform="$(transform_for "${sig}")"
	bytes="$(file_bytes "${path}")"
	table="otel_${sig}"

	if (( ! WARM_ONLY )); then
		typeset cold_best="" cold_rows=0 cold_added=0 cold_widened=0 cold_errors=0
		for (( r = 1; r <= RUNS; r++ )); do
			db="${WORK}/${sig}_cold_${r}.db"
			/bin/rm -f "${db}"
			line="$(run_ingest "${db}" "${table}" "${path}" "${transform}")"
			if [[ -z "${cold_best}" ]] || (( $(py_cmp_lt "${line%%,*}" "${cold_best}") )); then
				cold_best="${line%%,*}"
				IFS=',' read -r _ cold_rows cold_added cold_widened cold_errors <<<"${line}"
			fi
		done
		rec_s=$(py_rec_s "${cold_rows}" "${cold_best}")
		mb_s=$(py_mb_s "${bytes}" "${cold_best}")
		echo "COLD ${sig}: ${cold_rows} rows in ${cold_best}s -> ${rec_s} rec/s, ${mb_s} MB/s (best of ${RUNS})" >&2
		JSON_PARTS+=("\"${sig}_cold\": {\"records\": ${cold_rows}, \"seconds\": ${cold_best}, \"records_per_sec\": ${rec_s}, \"mb_per_sec\": ${mb_s}, \"bytes\": ${bytes}, \"columns_added\": ${cold_added}, \"columns_widened\": ${cold_widened}, \"errors\": ${cold_errors}}")
	fi

	if (( ! COLD_ONLY )); then
		warm_db="${WORK}/${sig}_warm.db"
		if (( ! WARM_ONLY )); then
			/bin/cp "${WORK}/${sig}_cold_1.db" "${warm_db}" 2>/dev/null || true
		fi
		if [[ ! -f "${warm_db}" ]]; then
			/bin/rm -f "${warm_db}"
			run_ingest "${warm_db}" "${table}" "${path}" "${transform}" >/dev/null
		fi
		typeset warm_best="" warm_rows=0
		for (( r = 1; r <= RUNS; r++ )); do
			prepare_warm_table "${warm_db}" "${table}"
			line="$(run_ingest "${warm_db}" "${table}" "${path}" "${transform}")"
			if [[ -z "${warm_best}" ]] || (( $(py_cmp_lt "${line%%,*}" "${warm_best}") )); then
				warm_best="${line%%,*}"
				IFS=',' read -r _ warm_rows _ _ _ <<<"${line}"
			fi
		done
		warm_rec_s=$(py_rec_s "${warm_rows}" "${warm_best}")
		warm_mb_s=$(py_mb_s "${bytes}" "${warm_best}")
		echo "WARM ${sig}: ${warm_rows} rows in ${warm_best}s -> ${warm_rec_s} rec/s, ${warm_mb_s} MB/s (best of ${RUNS})" >&2
		JSON_PARTS+=("\"${sig}_warm\": {\"records\": ${warm_rows}, \"seconds\": ${warm_best}, \"records_per_sec\": ${warm_rec_s}, \"mb_per_sec\": ${warm_mb_s}, \"bytes\": ${bytes}}")
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
	echo "  \"results\": {"
	printf "    %s\n" "$(IFS=$',\n'; echo "${JSON_PARTS[*]}")" | /usr/bin/sed '$!s/$/,/'
	echo "  }"
	echo "}"
} > "${OUTPUT}"

echo "Wrote ${OUTPUT}" >&2
/bin/rm -rf "${WORK}"
