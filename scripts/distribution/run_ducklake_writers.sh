#!/usr/bin/env bash
# Two DuckDB processes ingest into the same DuckLake catalog (file-backed).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DUCKDB="${DUCKDB:-${ROOT}/build/release/duckdb}"
EXT="${EXT:-${ROOT}/build/release/extension/rawduck/rawduck.duckdb_extension}"
WORKDIR="${WORKDIR:-${ROOT}/benchmark/work/dist_ducklake}"
LAKE="${LAKE:-${WORKDIR}/shared.ducklake}"
DATA="${DATA:-${WORKDIR}/data}"
ROWS="${RAWDUCK_DIST_ROWS:-50}"

if [[ ! -x "${DUCKDB}" ]]; then
	echo "Build release first: GEN=ninja make release" >&2
	exit 1
fi

mkdir -p "${DATA}" "${WORKDIR}"

setup_sql="
LOAD '${EXT}';
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:${LAKE}' AS lake (DATA_PATH '${DATA}');
"

write_sql() {
	local writer=$1
	local start=$2
	local end=$3
	local sql="${setup_sql}"
	local i payload
	for ((i = start; i < end; i++)); do
		payload="[{\"id\":${i},\"writer\":\"${writer}\",\"action\":\"evt\"}]"
		sql+="SELECT * FROM raw_ingest('lake.main.events', '${payload}');"
	done
	echo "${sql}"
}

duckdb_quiet() {
	"${DUCKDB}" -unsigned -batch "$@"
}

echo "== process A: rows 0..$((ROWS - 1)) =="
write_sql "procA" 0 "${ROWS}" | duckdb_quiet

echo "== process B: rows ${ROWS}..$((2 * ROWS - 1)) (background) =="
write_sql "procB" "${ROWS}" "$((2 * ROWS))" | duckdb_quiet &
pid_b=$!

# overlap: process A writes again while B runs
write_sql "procA" "$((2 * ROWS))" "$((3 * ROWS))" | duckdb_quiet &
pid_a2=$!

wait "${pid_b}"
wait "${pid_a2}"

expected=$((3 * ROWS))
count=$(
	duckdb_quiet -csv -noheader <<SQL
${setup_sql}
SELECT count(*) FROM lake.main.events;
SQL
)

writers=$(
	duckdb_quiet -csv -noheader <<SQL
${setup_sql}
SELECT count(DISTINCT writer) FROM lake.main.events;
SQL
)

echo "rows=${count} (expect ${expected}), distinct_writers=${writers} (expect 2)"
if [[ "${count}" -ne "${expected}" ]]; then
	echo "FAIL: row count mismatch" >&2
	exit 1
fi
if [[ "${writers}" -ne 2 ]]; then
	echo "FAIL: expected 2 writers" >&2
	exit 1
fi
echo "OK: DuckLake multi-process ingest"
