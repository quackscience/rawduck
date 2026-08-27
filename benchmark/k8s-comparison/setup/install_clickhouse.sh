#!/usr/bin/env bash
# Install ClickHouse locally (no Docker)
#
# Usage: ./install_clickhouse.sh
#
# Downloads the official ClickHouse binary for your platform

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="$SCRIPT_DIR/clickhouse"

mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"

echo "Downloading ClickHouse..."

# Use official installer which handles platform detection
if command -v curl &>/dev/null; then
    curl -fsSL https://clickhouse.com/ | sh
else
    wget -qO- https://clickhouse.com/ | sh
fi

# The installer creates a ./clickhouse binary in the current directory
if [[ -f "./clickhouse" ]]; then
    chmod +x ./clickhouse
    echo ""
    echo "ClickHouse installed successfully!"
    echo "Binary: $INSTALL_DIR/clickhouse"
    echo ""
    echo "To start: ./start_clickhouse.sh"
else
    echo "Installation may have issues. Contents of $INSTALL_DIR:"
    ls -la "$INSTALL_DIR"
fi
