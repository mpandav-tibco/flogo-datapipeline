#!/usr/bin/env bash
# Runs the OTel metrics example against a local collector on :4317.
# Start a collector first (see README.md), then:  ./run.sh
set -euo pipefail
cd "$(dirname "$0")"
BIN="${DP_BIN:-../../flogo-datapipeline}"
exec "$BIN" -f otel-metrics.datapipeline
