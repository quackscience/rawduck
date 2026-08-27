#!/usr/bin/env bash
# Cross-system K8s logs benchmark: RawDuck vs OpenObserve vs ClickHouse
#
# Usage:
#   ./run_k8s_comparison.sh --quick              # 1M records smoke test
#   ./run_k8s_comparison.sh --records 10000000   # 10M records
#   ./run_k8s_comparison.sh --systems rawduck    # RawDuck only
#
# Prerequisites: See README.md for setup instructions

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

# Check RawDuck build
if [[ ! -f "$PROJECT_ROOT/build/release/duckdb" ]]; then
    echo "ERROR: RawDuck not built."
    echo "Run from project root: GEN=ninja make release"
    exit 1
fi

# Run benchmark from this directory
cd "$SCRIPT_DIR"
exec python3 run_k8s_comparison.py "$@"
