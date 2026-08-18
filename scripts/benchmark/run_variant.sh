#!/usr/bin/env bash
# VARIANT (DuckDB v1.5) vs RawDuck shredded tables.
#
# Same OTLP/JSON traces envelopes as run_otel.sh. Reports ingest, query, and
# on-disk size. VARIANT is measured as it exists in the pinned DuckDB (v1.5.5),
# not the v2.0 shredded-execution preview.
#
# Remote / new machine (post-build), from this branch:
#
#   git clone --recurse-submodules … && git checkout feat/variant-benchmark
#   GEN=ninja make release
#   ./scripts/benchmark/run_variant.sh --quick
#   ./scripts/benchmark/run_variant.sh --records 1000000 --runs 3
#
# Send back the JSON under benchmark/results/variant_*.json (host, git, and
# DuckDB version are already inside the file).
#
# Examples:
#   ./scripts/benchmark/run_variant.sh --quick
#   ./scripts/benchmark/run_variant.sh --records 1000000 --runs 3
set -euo pipefail

export PATH="/bin:/usr/bin:/usr/local/bin:${HOME}/.pyenv/shims:${PATH:-}"
PYTHON="${PYTHON:-$(command -v python3 || true)}"
if [[ -z "${PYTHON}" || ! -x "${PYTHON}" ]]; then
	echo "python3 not found (set PYTHON=...)" >&2
	exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/benchmark/lib.sh
source "${ROOT}/scripts/benchmark/lib.sh"

bench_require_build

export DUCKDB
export EXT
DUCKDB="$(bench_duckdb)"
EXT="$(bench_extension)"
export BENCH_DATA BENCH_WORK BENCH_RESULTS
BENCH_DATA="$(bench_data_dir)"
BENCH_WORK="$(bench_work_dir)"
BENCH_RESULTS="$(bench_results_dir)"

exec "${PYTHON}" "${ROOT}/scripts/benchmark/run_variant.py" "$@"
