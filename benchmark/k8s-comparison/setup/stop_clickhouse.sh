#!/usr/bin/env bash
# Stop ClickHouse server

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/clickhouse.pid"

if [[ -f "$PID_FILE" ]]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "Stopping ClickHouse (PID $PID)..."
        kill "$PID"
        rm -f "$PID_FILE"
        echo "Stopped."
    else
        echo "Process not running."
        rm -f "$PID_FILE"
    fi
else
    # Try to find by name
    PIDS=$(pgrep -f "clickhouse server" 2>/dev/null || true)
    if [[ -n "$PIDS" ]]; then
        echo "Stopping ClickHouse processes: $PIDS"
        echo "$PIDS" | xargs kill 2>/dev/null || true
    else
        echo "ClickHouse not running."
    fi
fi
