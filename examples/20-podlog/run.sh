#!/usr/bin/env bash
# Run the pod-log collector example against the synthetic CRI tree in ./data/pods.
# Bounded mode drains the tree once and exits, writing NDJSON to ./out.
set -euo pipefail
cd "$(dirname "$0")"

BIN="${FLOGO_DATAPIPELINE_BIN:-../../flogo-datapipeline}"
if [[ ! -x "$BIN" ]]; then
  echo "Building flogo-datapipeline binary…"
  ( cd ../.. && CGO_ENABLED=0 go build -o flogo-datapipeline ./cmd/flogo-datapipeline/ )
fi

mkdir -p out
echo "Running podlog-collector (bounded drain)…"
"$BIN" -f podlog-collector.datapipeline

echo
echo "── Collected records (out/pod-logs.ndjson) ──"
cat out/pod-logs.ndjson 2>/dev/null || echo "(none)"
echo
echo "Record count: $(wc -l < out/pod-logs.ndjson 2>/dev/null || echo 0)"
echo "Position registry: out/positions.json"
