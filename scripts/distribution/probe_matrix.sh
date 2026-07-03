#!/usr/bin/env zsh
# Probe matrix: one DuckLake, multiple writers/readers — map what works and what doesn't.
#
# Usage:
#   GEN=ninja make release
#   ./scripts/distribution/probe_matrix.sh           # full matrix
#   ./scripts/distribution/probe_matrix.sh --quick   # skip slow / expected-fail probes
#
# Output: benchmark/work/distribution_probe/probe_report.{json,md}
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

QUICK=false
[[ "${1:-}" == "--quick" ]] && QUICK=true

probe_require_build

# Scenarios record their own outcome; do not abort the matrix on expected platform limits.
set +e

probe_run() {
	local label=$1
	shift
	printf "  [%s] " "${label}"
	if "$@"; then
		echo "done"
	else
		echo "FAILED (exit $?)"
	fi
}

run_timeout() {
	local secs=$1
	shift
	"${@}" &
	local pid=$!
	( sleep "${secs}" && kill "${pid}" 2>/dev/null ) &
	local killer=$!
	wait "${pid}" 2>/dev/null
	local ec=$?
	kill "${killer}" 2>/dev/null || true
	return "${ec}"
}

# --- in-process (sqllogictest via unittest) ---

probe_sql_tests() {
	local tests=(
		"test/sql/raw_distribution_ducklake.test"
		"test/sql/raw_distribution_ducklake_otel.test"
		"test/sql/raw_distribution_ducklake_inprocess.test"
	)
	local t out
	for t in "${tests[@]}"; do
		if out=$("${ROOT}/build/release/test/unittest" --test-dir "${ROOT}" "${t}" 2>&1); then
			probe_record "sql_${t##*/}" "in_process" "pass" "sqllogictest passed"
		else
			probe_record "sql_${t##*/}" "in_process" "fail" "${out##*$'\n'}"
		fi
	done
}

# --- multi-process: sequential writers (short-lived attach) ---

probe_mp_sequential_writers() {
	probe_fresh_lake "mp_sequential"
	local rows=10
	local expected=$((rows * 2))
	local setup
	setup=$(probe_setup_sql)

	duckdb_quiet <<SQL
${setup}
SELECT * FROM raw_ingest('lake.main.events', '[{"id":0,"writer":"A"}]');
SQL
	for ((i = 1; i < rows; i++)); do
		duckdb_quiet <<SQL
${setup}
SELECT * FROM raw_ingest('lake.main.events', '[{"id":${i},"writer":"A"}]');
SQL
	done
	for ((i = rows; i < expected; i++)); do
		duckdb_quiet <<SQL
${setup}
SELECT * FROM raw_ingest('lake.main.events', '[{"id":${i},"writer":"B"}]');
SQL
	done

	local count
	count=$(probe_lake_count "SELECT count(*) FROM lake.main.events;")
	if [[ "${count}" -eq "${expected}" ]]; then
		probe_record "mp_sequential_writers" "multi_process" "pass" "${expected} rows from 2 sequential processes"
	else
		probe_record "mp_sequential_writers" "multi_process" "fail" "expected ${expected} rows, got ${count}"
	fi
}

# --- multi-process: overlapping writers ---

probe_mp_overlapping_writers() {
	probe_fresh_lake "mp_overlap"
	local rows=15
	local setup
	setup=$(probe_setup_sql)

	write_proc() {
		local writer=$1 start=$2 end=$3
		local sql="${setup}"
		local i
		for ((i = start; i < end; i++)); do
			sql+="SELECT * FROM raw_ingest('lake.main.events', '[{\"id\":${i},\"writer\":\"${writer}\"}]');"
		done
	duckdb_quiet <<<"${sql}" 2>/dev/null
	}

	write_proc A 0 "${rows}" &
	local pid_a=$!
	write_proc B "${rows}" "$((rows * 2))" &
	local pid_b=$!
	wait "${pid_a}"
	local ec_a=$?
	wait "${pid_b}"
	local ec_b=$?

	if [[ "${ec_a}" -ne 0 || "${ec_b}" -ne 0 ]]; then
		probe_record "mp_overlapping_writers" "multi_process" "expected_fail" \
			"overlapping attach blocked (ecA=${ec_a} ecB=${ec_b})" \
			"sqlite metadata: only one ATTACH holder; use single hub for concurrent ingest"
		return
	fi

	local count writers
	count=$(probe_lake_count "SELECT count(*) FROM lake.main.events;")
	writers=$(probe_lake_count "SELECT count(DISTINCT writer) FROM lake.main.events;")
	if [[ "${count}" -eq $((rows * 2)) && "${writers}" -eq 2 ]]; then
		probe_record "mp_overlapping_writers" "multi_process" "pass" "${count} rows, 2 writers concurrent short-lived attach"
	else
		probe_record "mp_overlapping_writers" "multi_process" "fail" "rows=${count} writers=${writers}"
	fi
}

# --- multi-process: concurrent persistent ATTACH (expect failure on sqlite) ---

probe_mp_concurrent_persistent_attach() {
	probe_fresh_lake "mp_dual_attach"
	local fifo1="${PROBE_WORK}/attach1.fifo" fifo2="${PROBE_WORK}/attach2.fifo"
	rm -f "${fifo1}" "${fifo2}"
	mkfifo "${fifo1}" "${fifo2}"

	"${DUCKDB}" -unsigned -batch < "${fifo1}" &
	local pid1=$!
	"${DUCKDB}" -unsigned -batch < "${fifo2}" &
	local pid2=$!
	local fd1 fd2
	exec {fd1}>"${fifo1}"
	exec {fd2}>"${fifo2}"
	cat <<SQL >&${fd1}
LOAD '${EXT}';
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:${LAKE}' AS lake (DATA_PATH '${DATA}');
SELECT 'attached1';
SQL
	sleep 0.3
	cat <<SQL >&${fd2}
LOAD '${EXT}';
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:${LAKE}' AS lake (DATA_PATH '${DATA}');
SELECT 'attached2';
SQL
	sleep 0.5
	exec {fd1}>&- {fd2}>&-
	wait "${pid1}" 2>/dev/null || true
	local ec1=$?
	wait "${pid2}" 2>/dev/null || true
	local ec2=$?

	if [[ "${ec1}" -ne 0 || "${ec2}" -ne 0 ]]; then
		probe_record "mp_concurrent_persistent_attach" "multi_process" "expected_fail" \
			"sqlite metadata lock: one attach failed (ec1=${ec1} ec2=${ec2})" \
			"Use single hub or postgres metadata for always-on multi-host writers"
	else
		probe_record "mp_concurrent_persistent_attach" "multi_process" "pass" \
			"both processes attached — sqlite lock may not apply on this platform"
	fi
	kill "${pid1}" "${pid2}" 2>/dev/null || true
}

# --- multi-process: cross-process schema evolution ---

probe_mp_schema_evolution() {
	probe_fresh_lake "mp_schema"
	local setup
	setup=$(probe_setup_sql)

	duckdb_quiet <<SQL
${setup}
SELECT * FROM raw_ingest('lake.main.events', '[{"id":1,"writer":"A","action":"x"}]');
SQL
	duckdb_quiet <<SQL
${setup}
SELECT * FROM raw_ingest('lake.main.events', '[{"id":2,"writer":"B","meta":{"region":"eu"}}]');
SQL

	local count
	count=$(probe_lake_count "SELECT count(*) FROM lake.main.events WHERE \"meta.region\" = 'eu';")
	if [[ "${count}" -eq 1 ]]; then
		probe_record "mp_schema_evolution" "multi_process" "pass" "cross-process ADD COLUMN (meta.region) readable"
	else
		probe_record "mp_schema_evolution" "multi_process" "fail" "expected 1 row with meta.region, got ${count}"
	fi
}

# --- multi-process: reader after writers ---

probe_mp_reader_after_writers() {
	probe_fresh_lake "mp_reader"
	local setup
	setup=$(probe_setup_sql)

	for ((i = 0; i < 10; i++)); do
		duckdb_quiet <<SQL
${setup}
SELECT * FROM raw_ingest('lake.main.events', '[{"id":${i},"writer":"w"}]');
SQL
	done

	local count
	count=$(duckdb_quiet -csv -noheader <<SQL
LOAD '${EXT}';
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:${LAKE}' AS lake (DATA_PATH '${DATA}', READ_ONLY);
SELECT count(*) FROM lake.main.events;
SQL
)
	if [[ "${count}" -eq 10 ]]; then
		probe_record "mp_reader_readonly_after_writers" "multi_process" "pass" "READ_ONLY attach sees 10 rows"
	else
		probe_record "mp_reader_readonly_after_writers" "multi_process" "fail" "count=${count}"
	fi
}

# --- single hub on DuckLake: parallel HTTP writers ---

probe_hub_parallel_http() {
	probe_fresh_lake "hub_http"
	local port=9910
	if ! probe_start_hub "${port}" 2>/dev/null; then
		probe_record "hub_ducklake_parallel_http" "single_hub" "fail" "hub failed to start"
		return
	fi

	local writers=4 rows=10 auth=(-H "Authorization: Bearer probe" -H "Content-Type: application/json")
	local w i payload
	for ((w = 0; w < writers; w++)); do
		payload="["
		for ((i = 0; i < rows; i++)); do
			[[ $i -gt 0 ]] && payload+=","
			payload+="{\"writer\":${w},\"seq\":${i}}"
		done
		payload+="]"
		curl -sf -X POST "http://127.0.0.1:${port}/v1/tables/events" "${auth[@]}" -d "${payload}" >/dev/null 2>&1 &
	done
	wait

	local expected=$((writers * rows))
	local count
	count=$(curl -sf --max-time 10 -X POST "http://127.0.0.1:${port}/v1/query" "${auth[@]}" \
		-d '{"sql":"SELECT count(*)::BIGINT AS c FROM lake.main.events"}' \
		| python3 -c "import json,sys; print(json.load(sys.stdin)['data'][0]['c'])") || count=-1

	probe_stop_hub "${port}"

	if [[ "${count}" -eq "${expected}" ]]; then
		probe_record "hub_ducklake_parallel_http" "single_hub" "pass" "${expected} rows via parallel HTTP → DuckLake"
	else
		probe_record "hub_ducklake_parallel_http" "single_hub" "fail" "expected ${expected}, got ${count}"
	fi
}

# --- single hub: parallel HTTP with schema churn ---

probe_hub_schema_churn() {
	probe_fresh_lake "hub_churn"
	local port=9911
	probe_start_hub "${port}"
	local auth=(-H "Authorization: Bearer probe" -H "Content-Type: application/json")

	local w
	for ((w = 0; w < 4; w++)); do
		curl -sf -X POST "http://127.0.0.1:${port}/v1/tables/churn" "${auth[@]}" \
			-d "[{\"writer\":${w},\"field_${w}\":\"v${w}\"}]" >/dev/null 2>&1 &
	done
	wait

	local count cols
	count=$(curl -sf --max-time 10 -X POST "http://127.0.0.1:${port}/v1/query" "${auth[@]}" \
		-d '{"sql":"SELECT count(*)::BIGINT AS c FROM lake.main.churn"}' \
		| python3 -c "import json,sys; print(json.load(sys.stdin)['data'][0]['c'])") || count=-1
	cols=$(curl -sf --max-time 10 -X POST "http://127.0.0.1:${port}/v1/query" "${auth[@]}" \
		-d '{"sql":"SELECT count(*)::BIGINT AS c FROM information_schema.columns WHERE table_catalog='"'"'lake'"'"' AND table_name='"'"'churn'"'"'"}' \
		| python3 -c "import json,sys; print(json.load(sys.stdin)['data'][0]['c'])") || cols=-1

	probe_stop_hub "${port}"

	if [[ "${count}" -eq 4 && "${cols}" -ge 5 ]]; then
		probe_record "hub_ducklake_schema_churn_http" "single_hub" "pass" "4 rows, ${cols} columns after parallel wide-schema posts"
	else
		probe_record "hub_ducklake_schema_churn_http" "single_hub" "fail" "rows=${count} cols=${cols}"
	fi
}

# --- single hub: parallel OTLP ---

probe_hub_parallel_otlp() {
	probe_fresh_lake "hub_otlp"
	local port=9912
	probe_start_hub "${port}"
	local auth=(-H "Authorization: Bearer probe" -H "Content-Type: application/json")
	local writers=4 spans=8

	otlp_body() {
		local wid=$1 sid=$2
		python3 - <<PY
import json
wid, sid = int(${wid}), int(${sid})
print(json.dumps({"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":f"w{wid}"}}]},"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":f"{wid:08x}{sid:08x}"[:16],"name":"s","kind":1}]}]}]}))
PY
	}

	local wid sid
	for ((wid = 0; wid < writers; wid++)); do
		for ((sid = 0; sid < spans; sid++)); do
			otlp_body "${wid}" "${sid}" | curl -sf --max-time 5 -X POST \
				"http://127.0.0.1:${port}/otlp/v1/traces?transform=otlp-traces" \
				"${auth[@]}" -H "Content-Type: application/json" -d @- >/dev/null || true
		done
	done

	local expected=$((writers * spans))
	local count
	count=$(curl -sf -X POST "http://127.0.0.1:${port}/v1/query" "${auth[@]}" \
		-d '{"sql":"SELECT count(*)::BIGINT AS c FROM lake.main.otel_traces"}' \
		| python3 -c "import json,sys; print(json.load(sys.stdin)['data'][0]['c'])")

	probe_stop_hub "${port}"

	if [[ "${count}" -eq "${expected}" ]]; then
		probe_record "hub_ducklake_parallel_otlp" "single_hub" "pass" "${expected} OTLP spans via one hub → DuckLake"
	else
		probe_record "hub_ducklake_parallel_otlp" "single_hub" "fail" "expected ${expected}, got ${count}"
	fi
}

# --- sequential multi-process OTEL hubs (delegate) ---

probe_mp_sequential_otel_hubs() {
	if WORKDIR="${PROBE_WORK}/otel_cluster" RAWDUCK_CLUSTER_WRITERS=2 RAWDUCK_CLUSTER_SPANS=10 \
		"${SCRIPT_DIR}/run_ducklake_otel_cluster.sh" >/dev/null 2>&1; then
		probe_record "mp_sequential_otel_hubs" "multi_process" "pass" "2 writer processes × 10 OTLP spans → reader"
	else
		probe_record "mp_sequential_otel_hubs" "multi_process" "fail" "run_ducklake_otel_cluster.sh failed"
	fi
}

# --- run ---

echo "== DuckLake distribution probe matrix =="
probe_run sql_tests probe_sql_tests
probe_run mp_sequential_writers probe_mp_sequential_writers
probe_run mp_overlapping_writers probe_mp_overlapping_writers
probe_run mp_schema_evolution probe_mp_schema_evolution
probe_run mp_reader probe_mp_reader_after_writers

if [[ "${QUICK}" == true ]]; then
	probe_record "hub_ducklake_parallel_http" "single_hub" "skip" "run full matrix for hub HTTP probe"
	probe_record "hub_ducklake_schema_churn_http" "single_hub" "skip" "run full matrix for hub schema churn"
else
	probe_run hub_http run_timeout 60 probe_hub_parallel_http
	probe_run hub_churn run_timeout 60 probe_hub_schema_churn
	probe_run mp_dual_attach probe_mp_concurrent_persistent_attach
	probe_run hub_otlp run_timeout 90 probe_hub_parallel_otlp
	probe_run mp_otel_hubs run_timeout 120 probe_mp_sequential_otel_hubs
fi

probe_write_report

pass=0
fail=0
expected_fail=0
skipped=0
for r in "${PROBE_RESULTS[@]}"; do
	outcome=$(python3 -c "import json; print(json.loads('''${r}''')['outcome'])")
	case "${outcome}" in
		pass) pass=$((pass + 1)) ;;
		fail) fail=$((fail + 1)) ;;
		expected_fail) expected_fail=$((expected_fail + 1)) ;;
		skip) skipped=$((skipped + 1)) ;;
	esac
done

echo
echo "Summary: ${pass} pass, ${fail} fail, ${expected_fail} expected_fail, ${skipped} skipped (of ${#PROBE_RESULTS[@]} scenarios)"
if [[ "${fail}" -gt 0 ]]; then
	exit 1
fi
set -e
