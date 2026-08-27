#!/usr/bin/env bash
# Install OpenObserve locally (no Docker)
#
# Usage: ./install_openobserve.sh
#
# Downloads the latest OpenObserve binary for your platform and extracts to ./openobserve/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="$SCRIPT_DIR/openobserve"

# Detect platform
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$ARCH" in
    x86_64) ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

case "$OS" in
    darwin) PLATFORM="darwin-$ARCH" ;;
    linux) PLATFORM="linux-$ARCH" ;;
    *) echo "Unsupported OS: $OS"; exit 1 ;;
esac

# Version (update manually for new releases)
VERSION="0.92.2"
echo "OpenObserve version: $VERSION"

# Download URL (official downloads site)
URL="https://downloads.openobserve.ai/releases/openobserve/v$VERSION/openobserve-v$VERSION-$PLATFORM.tar.gz"
TARBALL="$SCRIPT_DIR/openobserve-$VERSION-$PLATFORM.tar.gz"

echo "Downloading from: $URL"
mkdir -p "$INSTALL_DIR"

if command -v wget &>/dev/null; then
    wget -q --show-progress -O "$TARBALL" "$URL"
else
    curl -L --progress-bar -o "$TARBALL" "$URL"
fi

echo "Extracting to $INSTALL_DIR..."
tar -xzf "$TARBALL" -C "$INSTALL_DIR" --strip-components=0 2>/dev/null || tar -xzf "$TARBALL" -C "$INSTALL_DIR"
rm -f "$TARBALL"

# Find the binary (might be in a subdirectory)
BINARY=$(find "$INSTALL_DIR" -name "openobserve" -type f -perm +111 2>/dev/null | head -1)
if [[ -z "$BINARY" ]]; then
    BINARY=$(find "$INSTALL_DIR" -name "openobserve" -type f 2>/dev/null | head -1)
    chmod +x "$BINARY" 2>/dev/null || true
fi

if [[ -n "$BINARY" && -f "$BINARY" ]]; then
    echo ""
    echo "OpenObserve installed successfully!"
    echo "Binary: $BINARY"
    echo ""
    echo "To start: ./start_openobserve.sh"
else
    echo "Installation may have issues. Contents of $INSTALL_DIR:"
    ls -la "$INSTALL_DIR"
fi
