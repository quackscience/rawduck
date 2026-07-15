#!/usr/bin/env zsh
# Shared helpers for RawDuck benchmark scripts.

typeset -g RAWDUCK_BENCH_ROOT
RAWDUCK_BENCH_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

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

bench_git_meta() {
	git -C "${RAWDUCK_BENCH_ROOT}" rev-parse HEAD 2>/dev/null || echo "unknown"
	git -C "${RAWDUCK_BENCH_ROOT}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown"
}
