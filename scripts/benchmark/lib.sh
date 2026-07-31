#!/usr/bin/env bash
# Shared helpers for RawDuck benchmark scripts (bash — portable to remote Linux/macOS).

RAWDUCK_BENCH_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

bench_duckdb() {
	echo "${DUCKDB:-${RAWDUCK_BENCH_ROOT}/build/release/duckdb}"
}

bench_extension() {
	echo "${EXT:-${RAWDUCK_BENCH_ROOT}/build/release/extension/rawduck/rawduck.duckdb_extension}"
}

bench_data_dir() {
	echo "${BENCH_DATA:-${RAWDUCK_BENCH_ROOT}/benchmark/data}"
}

bench_work_dir() {
	echo "${BENCH_WORK:-${RAWDUCK_BENCH_ROOT}/benchmark/work}"
}

bench_results_dir() {
	echo "${BENCH_RESULTS:-${RAWDUCK_BENCH_ROOT}/benchmark/results}"
}

bench_require_build() {
	local duckdb ext
	duckdb="$(bench_duckdb)"
	ext="$(bench_extension)"
	if [[ ! -x "${duckdb}" ]]; then
		echo "Build release first: GEN=ninja make release" >&2
		return 1
	fi
	if [[ ! -f "${ext}" ]]; then
		echo "Missing extension: ${ext}" >&2
		return 1
	fi
}
