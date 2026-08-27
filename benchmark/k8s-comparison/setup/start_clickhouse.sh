#!/usr/bin/env bash
# Start ClickHouse server locally
#
# Usage: ./start_clickhouse.sh [--background]
#
# HTTP API: http://localhost:8123
# Native: localhost:9000

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="$SCRIPT_DIR/clickhouse"
DATA_DIR="$SCRIPT_DIR/clickhouse_data"
LOG_FILE="$SCRIPT_DIR/clickhouse.log"
PID_FILE="$SCRIPT_DIR/clickhouse.pid"

BINARY="$INSTALL_DIR/clickhouse"

if [[ ! -f "$BINARY" ]]; then
    echo "ClickHouse not installed. Run: ./install_clickhouse.sh"
    exit 1
fi

# Kill existing if running
if [[ -f "$PID_FILE" ]]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "Stopping existing ClickHouse (PID $OLD_PID)..."
        kill "$OLD_PID" 2>/dev/null || true
        sleep 2
    fi
    rm -f "$PID_FILE"
fi

mkdir -p "$DATA_DIR"

# Create minimal config
CONFIG_FILE="$SCRIPT_DIR/clickhouse_config.xml"
cat > "$CONFIG_FILE" << 'EOF'
<?xml version="1.0"?>
<clickhouse>
    <logger>
        <level>warning</level>
        <console>1</console>
    </logger>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <listen_host>127.0.0.1</listen_host>
    <max_concurrent_queries>100</max_concurrent_queries>
    <max_connections>100</max_connections>
    <mark_cache_size>5368709120</mark_cache_size>
    <path>DATADIR/</path>
    <tmp_path>DATADIR/tmp/</tmp_path>
    <user_files_path>DATADIR/user_files/</user_files_path>
    <format_schema_path>DATADIR/format_schemas/</format_schema_path>
</clickhouse>
EOF

# Replace DATADIR placeholder
sed -i.bak "s|DATADIR|$DATA_DIR|g" "$CONFIG_FILE" && rm -f "$CONFIG_FILE.bak"

echo "Starting ClickHouse server..."
echo "  Data dir: $DATA_DIR"
echo "  HTTP API: http://localhost:8123"
echo ""

if [[ "${1:-}" == "--background" ]]; then
    nohup "$BINARY" server --config-file="$CONFIG_FILE" > "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "Started in background (PID $(cat $PID_FILE))"
    echo "Logs: tail -f $LOG_FILE"

    # Wait for startup
    echo -n "Waiting for startup..."
    for i in {1..30}; do
        if curl -s http://localhost:8123/ping >/dev/null 2>&1; then
            echo " ready!"
            exit 0
        fi
        sleep 1
        echo -n "."
    done
    echo " timeout (check logs)"
else
    exec "$BINARY" server --config-file="$CONFIG_FILE"
fi
