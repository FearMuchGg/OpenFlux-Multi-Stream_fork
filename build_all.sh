#!/bin/bash
# OpenFlux Cross-Platform Build Script
# Builds Linux and Windows AMD64 binaries

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "=== Building OpenFlux ==="
echo "Project directory: $PROJECT_DIR"
echo ""

# Clean old binaries
rm -f openflux-linux-amd64 openflux-windows-amd64.exe 2>/dev/null || true

# Build Linux AMD64
echo "🐧 Building for Linux AMD64..."
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -X main.Version=$(date +%Y%m%d)" \
    -trimpath \
    -o openflux-linux-amd64 \
    .

if [ $? -eq 0 ]; then
    echo "✅ openflux-linux-amd64 built successfully"
    ls -lh openflux-linux-amd64
else
    echo "❌ Failed to build Linux binary"
    exit 1
fi

echo ""

# Build Windows AMD64
echo "🪟 Building for Windows AMD64..."
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build \
    -ldflags="-s -w -X main.Version=$(date +%Y%m%d)" \
    -trimpath \
    -o openflux-windows-amd64.exe \
    .

if [ $? -eq 0 ]; then
    echo "✅ openflux-windows-amd64.exe built successfully"
    ls -lh openflux-windows-amd64.exe
else
    echo "❌ Failed to build Windows binary"
    exit 1
fi

echo ""
echo "=== Build Complete ==="
echo "Binaries ready in: $PROJECT_DIR"
echo ""
echo "Usage examples:"
echo "  Linux:   ./openflux-linux-amd64 --exit-node --transport yandex --urls \"DOC1,DOC2,DOC3\" --mode proxy"
echo "  Windows: .\\openflux-windows-amd64.exe --exit-node --transport yandex --urls \"DOC1,DOC2,DOC3\" --mode proxy"
