#!/usr/bin/env bash
# Build the flogo-datapipeline engine binary → ./bin/dp
set -euo pipefail
EX="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$EX/../.." && pwd)"
mkdir -p "$EX/bin"
echo "Building engine → bin/dp (from $REPO)"
( cd "$REPO" && CGO_ENABLED=1 go build -o "$EX/bin/dp" ./cmd/flogo-datapipeline )
echo "OK: $EX/bin/dp ($(ls -la "$EX/bin/dp" | awk '{print $5}') bytes)"
