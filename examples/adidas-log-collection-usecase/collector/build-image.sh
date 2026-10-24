#!/usr/bin/env bash
# Build both arm64 images and load them into the adidas-logs minikube cluster:
#   1. flogo-datapipeline:arm64-local  — the collector engine (native arm64 CGO build)
#   2. bwce-emitter:arm64-local        — the BWCE-format log workload
#
# The engine build runs inside a golang container (host module cache mounted),
# so no local Go toolchain or cross-compiler is required.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UC_DIR="$(cd "$HERE/.." && pwd)"
REPO_ROOT="$(cd "$UC_DIR/../.." && pwd)"
PROFILE="${MINIKUBE_PROFILE:-adidas-logs}"
GOIMG="${GO_IMAGE:-golang:1.25}"

COLLECTOR_IMG="flogo-datapipeline:arm64-local"
EMITTER_IMG="bwce-emitter:arm64-local"

echo ">> repo root:   $REPO_ROOT"
echo ">> profile:     $PROFILE"

echo ">> [1/4] building linux/arm64 engine binary (CGO) via $GOIMG"
mkdir -p /tmp/adidas-gocache
# The engine depends on the private module github.com/tibco/flogo-general, which
# the repo's go.work resolves via a local `replace` to the VS Code extension
# media. Mount that module at the same path so the build needs no credentials
# and no network, and keep Go workspace mode on (do NOT set GOWORK=off).
GENERAL_PATH="$(awk -F'=>' '/=>/{gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2}' "$REPO_ROOT/go.work" 2>/dev/null | head -1)"
echo ">> flogo-general local module: ${GENERAL_PATH:-<none>}"
# -tags tibco_hooks compiles in the TIBCO App Startup/Shutdown triggers the
# pipeline's "startup" trigger needs (see cmd/flogo-datapipeline/tibco_hooks.go).
docker run --rm \
  -v "$REPO_ROOT":/src \
  -v "$HOME/go/pkg/mod":/go/pkg/mod \
  -v /tmp/adidas-gocache:/gocache \
  -v "$HERE":/out \
  ${GENERAL_PATH:+-v "$GENERAL_PATH":"$GENERAL_PATH":ro} \
  -e CGO_ENABLED=1 -e GOCACHE=/gocache \
  -w /src "$GOIMG" \
  go build -tags tibco_hooks -trimpath -ldflags="-s -w" -o /out/flogo-dp-linux-arm64 ./cmd/flogo-datapipeline

echo ">> [2/4] building collector image $COLLECTOR_IMG"
docker build -t "$COLLECTOR_IMG" "$HERE"

echo ">> [3/4] building emitter image $EMITTER_IMG"
docker build -t "$EMITTER_IMG" "$UC_DIR/workload"

echo ">> [4/4] loading images into minikube profile $PROFILE"
minikube image load "$COLLECTOR_IMG" -p "$PROFILE"
minikube image load "$EMITTER_IMG" -p "$PROFILE"

echo ">> done. images available in cluster:"
echo "   - $COLLECTOR_IMG"
echo "   - $EMITTER_IMG"
