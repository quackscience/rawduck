#!/usr/bin/env bash
# Fully automated benchmark: tests each system in isolation
#
# Flow: RawDuck -> OpenObserve (start/test/stop) -> ClickHouse (start/test/stop)
#
# Usage:
#   ./run_full_benchmark.sh --quick              # 1M records
#   ./run_full_benchmark.sh --records 10000000   # 10M records

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SETUP_DIR="$SCRIPT_DIR/setup"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

# Parse args to extract records count for data generation
RECORDS=10000000
RUNS=3
for arg in "$@"; do
    if [[ "$arg" == "--quick" ]]; then
        RECORDS=1000000
    fi
done

# Check binaries
check_prereqs() {
    if [[ ! -f "$PROJECT_ROOT/build/release/duckdb" ]]; then
        echo "ERROR: RawDuck not built. Run: GEN=ninja make release"
        exit 1
    fi
    if [[ ! -f "$SETUP_DIR/openobserve/openobserve" ]]; then
        echo "ERROR: OpenObserve not installed. Run: ./setup/install_openobserve.sh"
        exit 1
    fi
    if [[ ! -f "$SETUP_DIR/clickhouse/clickhouse" ]]; then
        echo "ERROR: ClickHouse not installed. Run: ./setup/install_clickhouse.sh"
        exit 1
    fi
}

# Generate data once
generate_data() {
    echo "=== Generating test data ($RECORDS records) ==="
    cd "$SCRIPT_DIR"
    python3 -c "
import sys
sys.path.insert(0, '.')
import gen_k8s_logs
from pathlib import Path
data_dir = Path('data')
data_dir.mkdir(exist_ok=True)
data_path = data_dir / f'k8s_logs_{${RECORDS} // 1_000_000}m.ndjson'
if not data_path.exists():
    sys.argv = ['gen', '$RECORDS', str(data_path)]
    gen_k8s_logs.main()
else:
    print(f'Data already exists: {data_path}')
"
    echo ""
}

start_openobserve() {
    echo "Starting OpenObserve..."
    # Clean previous data for fresh benchmark
    rm -rf "$SETUP_DIR/openobserve_data"
    mkdir -p "$SETUP_DIR/openobserve_data"
    export ZO_ROOT_USER_EMAIL="root@example.com"
    export ZO_ROOT_USER_PASSWORD="Complexpass#123"
    export ZO_DATA_DIR="$SETUP_DIR/openobserve_data"
    export ZO_HTTP_PORT="5080"
    # Records are generated fresh at the start of the run, but RawDuck is benchmarked
    # first, so by the time OpenObserve ingests them they are already some hours old.
    # OpenObserve discards anything outside this window (handle_timestamp in
    # src/core/src/logs/ingest.rs); the defaults are 5h past / 24h future.
    export ZO_INGEST_ALLOWED_UPTO="48"
    export ZO_INGEST_ALLOWED_IN_FUTURE="48"

    "$SETUP_DIR/openobserve/openobserve" > "$SETUP_DIR/openobserve.log" 2>&1 &
    echo $! > "$SETUP_DIR/openobserve.pid"

    for i in {1..30}; do
        if curl -s http://localhost:5080/healthz >/dev/null 2>&1; then
            echo "OpenObserve ready (PID $(cat "$SETUP_DIR/openobserve.pid"))"
            return 0
        fi
        sleep 1
    done
    echo "OpenObserve failed to start"
    return 1
}

stop_openobserve() {
    if [[ -f "$SETUP_DIR/openobserve.pid" ]]; then
        kill "$(cat "$SETUP_DIR/openobserve.pid")" 2>/dev/null || true
        rm -f "$SETUP_DIR/openobserve.pid"
        echo "OpenObserve stopped"
    fi
}

start_clickhouse() {
    echo "Starting ClickHouse..."
    # Clean previous data for fresh benchmark
    rm -rf "$SETUP_DIR/store" "$SETUP_DIR/clickhouse_data" "$SETUP_DIR/metadata" "$SETUP_DIR/access"
    mkdir -p "$SETUP_DIR/clickhouse_data"
    cd "$SETUP_DIR"

    # Use embedded config (simpler, works reliably)
    "$SETUP_DIR/clickhouse/clickhouse" server > "$SETUP_DIR/clickhouse.log" 2>&1 &
    echo $! > "$SETUP_DIR/clickhouse.pid"
    cd "$SCRIPT_DIR"

    for i in {1..60}; do
        if curl -s http://127.0.0.1:8123/ping >/dev/null 2>&1; then
            echo "ClickHouse ready (PID $(cat "$SETUP_DIR/clickhouse.pid"))"
            return 0
        fi
        sleep 0.5
    done
    echo "ClickHouse failed to start. Check $SETUP_DIR/clickhouse.log"
    return 1
}

stop_clickhouse() {
    if [[ -f "$SETUP_DIR/clickhouse.pid" ]]; then
        kill "$(cat "$SETUP_DIR/clickhouse.pid")" 2>/dev/null || true
        rm -f "$SETUP_DIR/clickhouse.pid"
        echo "ClickHouse stopped"
    fi
}

# Main
check_prereqs
generate_data

cd "$SCRIPT_DIR"

echo ""
echo "=========================================="
echo "=== Phase 1: RawDuck (no service needed) ==="
echo "=========================================="
python3 run_k8s_comparison.py "$@" --systems rawduck

echo ""
echo "=========================================="
echo "=== Phase 2: OpenObserve ==="
echo "=========================================="
start_openobserve
trap stop_openobserve EXIT
python3 run_k8s_comparison.py "$@" --systems openobserve
stop_openobserve
# Clean OpenObserve data after benchmark
rm -rf "$SETUP_DIR/openobserve_data"
trap - EXIT

echo ""
echo "=========================================="
echo "=== Phase 3: ClickHouse ==="
echo "=========================================="
start_clickhouse
trap stop_clickhouse EXIT
python3 run_k8s_comparison.py "$@" --systems clickhouse
stop_clickhouse
# Clean ClickHouse data after benchmark
rm -rf "$SETUP_DIR/store" "$SETUP_DIR/clickhouse_data" "$SETUP_DIR/metadata" "$SETUP_DIR/access"
trap - EXIT

echo ""
echo "=========================================="
echo "=== All benchmarks complete ==="
echo "=========================================="
echo "Results in: $SCRIPT_DIR/results/"
