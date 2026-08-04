#!/usr/bin/env bash
# Run the pod-log collector example against the synthetic CRI tree in ./data/pods.
# The podlog source drains the tree once (bounded mode); this script stops the
# otherwise long-running engine as soon as the NDJSON output settles.
set -euo pipefail
cd "$(dirname "$0")"

BIN="${FLOGO_DATAPIPELINE_BIN:-../../flogo-datapipeline}"
if [[ ! -x "$BIN" ]]; then
  echo "Building flogo-datapipeline binary…"
  ( cd ../.. && CGO_ENABLED=0 go build -o flogo-datapipeline ./cmd/flogo-datapipeline/ )
fi

mkdir -p out
# Reset the previous run's output and position registry so the demo re-reads the
# whole synthetic tree each time (fromBeginning only applies when no position is
# recorded for a file).
rm -f out/pod-logs.ndjson out/positions.json
echo "Running podlog-collector (bounded drain)…"

# The podlog source drains the synthetic tree in bounded mode, but the engine is a
# long-running process (streaming-source model — it blocks until a termination
# signal), so run it in the background and stop it once the output settles.
"$BIN" -f podlog-collector.datapipeline >out/engine.log 2>&1 &
PID=$!
prev=-1
for _ in $(seq 1 30); do
  sleep 0.5
  cur=$(wc -l < out/pod-logs.ndjson 2>/dev/null || echo 0)
  if [[ "$cur" -gt 0 && "$cur" == "$prev" ]]; then break; fi
  prev=$cur
done
kill "$PID" 2>/dev/null || true
wait "$PID" 2>/dev/null || true

echo
echo "── Collected records (out/pod-logs.ndjson) ──"
cat out/pod-logs.ndjson 2>/dev/null || echo "(none)"
echo
echo "Record count: $(wc -l < out/pod-logs.ndjson 2>/dev/null || echo 0)"
echo "Position registry: out/positions.json"
