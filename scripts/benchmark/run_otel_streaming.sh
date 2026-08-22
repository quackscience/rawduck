#!/usr/bin/env bash
# Realistic OTEL streaming ingestion benchmark: drives real OTLP/HTTP traffic
# (actual OpenTelemetry Python SDK, protobuf wire format) into RawDuck's
# raw_serve() HTTP API with concurrent exporter processes, instead of bulk-
# loading an NDJSON file (see run_otel.sh for that).
#
# Manages a dedicated venv (not the system python: PEP 668 externally-managed
# environments reject a plain `pip install`) for the OpenTelemetry SDK deps.
#
# Examples:
#   ./scripts/benchmark/run_otel_streaming.sh --quick
#   ./scripts/benchmark/run_otel_streaming.sh --workers 16 --spans-per-worker 60000
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/benchmark/lib.sh
source "${ROOT}/scripts/benchmark/lib.sh"

bench_require_build

VENV_DIR="$(bench_work_dir)/otel-streaming-venv"
REQS="${ROOT}/scripts/benchmark/otel_streaming_requirements.txt"

if [[ ! -x "${VENV_DIR}/bin/python3" ]]; then
	echo "Setting up OTel SDK venv at ${VENV_DIR} (one-time)..." >&2
	python3 -m venv "${VENV_DIR}"
	"${VENV_DIR}/bin/pip" install --quiet --upgrade pip
	"${VENV_DIR}/bin/pip" install --quiet -r "${REQS}"
fi

export OTEL_VENV_PYTHON="${VENV_DIR}/bin/python3"
export DUCKDB="$(bench_duckdb)"
export EXT="$(bench_extension)"
export BENCH_WORK="$(bench_work_dir)"
export BENCH_RESULTS="$(bench_results_dir)"
mkdir -p "${BENCH_WORK}" "${BENCH_RESULTS}"

exec "${VENV_DIR}/bin/python3" "${ROOT}/scripts/benchmark/run_otel_streaming.py" \
	--python "${OTEL_VENV_PYTHON}" "$@"
