#!/usr/bin/env bash
# Stop OpenObserve

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/openobserve.pid"

if [[ -f "$PID_FILE" ]]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "Stopping OpenObserve (PID $PID)..."
        kill "$PID"
        rm -f "$PID_FILE"
        echo "Stopped."
    else
        echo "Process not running."
        rm -f "$PID_FILE"
    fi
else
    # Try to find by name
    PIDS=$(pgrep -f "openobserve" 2>/dev/null || true)
    if [[ -n "$PIDS" ]]; then
        echo "Stopping OpenObserve processes: $PIDS"
        echo "$PIDS" | xargs kill 2>/dev/null || true
    else
        echo "OpenObserve not running."
    fi
fi
