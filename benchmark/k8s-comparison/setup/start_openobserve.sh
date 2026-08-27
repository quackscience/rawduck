#!/usr/bin/env bash
# Start OpenObserve locally
#
# Usage: ./start_openobserve.sh [--background]
#
# Default credentials: root@example.com / Complexpass#123
# Web UI: http://localhost:5080
# API: http://localhost:5080/api

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="$SCRIPT_DIR/openobserve"
DATA_DIR="$SCRIPT_DIR/openobserve_data"
LOG_FILE="$SCRIPT_DIR/openobserve.log"
PID_FILE="$SCRIPT_DIR/openobserve.pid"

# Find binary
BINARY=$(find "$INSTALL_DIR" -name "openobserve" -type f -perm +111 2>/dev/null | head -1)
if [[ -z "$BINARY" ]]; then
    BINARY=$(find "$INSTALL_DIR" -name "openobserve" -type f 2>/dev/null | head -1)
fi

if [[ ! -f "$BINARY" ]]; then
    echo "OpenObserve not installed. Run: ./install_openobserve.sh"
    exit 1
fi

# Kill existing if running
if [[ -f "$PID_FILE" ]]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "Stopping existing OpenObserve (PID $OLD_PID)..."
        kill "$OLD_PID" 2>/dev/null || true
        sleep 2
    fi
    rm -f "$PID_FILE"
fi

mkdir -p "$DATA_DIR"

# Environment
export ZO_ROOT_USER_EMAIL="root@example.com"
export ZO_ROOT_USER_PASSWORD="Complexpass#123"
export ZO_DATA_DIR="$DATA_DIR"
export ZO_HTTP_PORT="5080"
export ZO_GRPC_PORT="5081"

echo "Starting OpenObserve..."
echo "  Data dir: $DATA_DIR"
echo "  Web UI: http://localhost:5080"
echo "  User: root@example.com"
echo "  Password: Complexpass#123"
echo ""

if [[ "${1:-}" == "--background" ]]; then
    nohup "$BINARY" > "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "Started in background (PID $(cat $PID_FILE))"
    echo "Logs: tail -f $LOG_FILE"

    # Wait for startup
    echo -n "Waiting for startup..."
    for i in {1..30}; do
        if curl -s http://localhost:5080/healthz >/dev/null 2>&1; then
            echo " ready!"
            exit 0
        fi
        sleep 1
        echo -n "."
    done
    echo " timeout (check logs)"
else
    exec "$BINARY"
fi
