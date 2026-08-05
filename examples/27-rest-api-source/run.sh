#!/usr/bin/env bash
# run.sh — Start the local REST fixture API and the pipeline engine, wait for
# the one-shot pipeline run to complete, then print the result and tear
# everything down.
#
# The engine itself is a long-running Flogo app (it stays up hosting the timer
# trigger for any future scheduled runs), so it is started in the background
# and stopped once the pipeline has written its output — the same lifecycle a
# Kubernetes Job/CronJob wrapper would apply in production.
#
# Usage:
#   ./run.sh
#
# Prerequisites:
#   - flogo-datapipeline binary is on your PATH
#   - go (to build the fixture API) is on your PATH
#
# Expected output:
#   The pipeline pages through 25 fake orders (page size 10 -> 3 requests:
#   10 + 10 + 5 rows) from http://localhost:8089/orders and writes
#   out/orders.jsonl (25 lines).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
mkdir -p out

FIXTURE_BIN="$(mktemp -d)/fixture"
ENGINE_PID=""
FIXTURE_PID=""
cleanup() {
  [ -n "$ENGINE_PID" ] && kill "$ENGINE_PID" 2>/dev/null || true
  [ -n "$FIXTURE_PID" ] && kill "$FIXTURE_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "==> Building and starting the fixture API on :8089..."
go build -o "$FIXTURE_BIN" ./fixture
"$FIXTURE_BIN" -addr :8089 &
FIXTURE_PID=$!

for i in $(seq 1 30); do
  if curl -sf http://localhost:8089/health >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

echo "==> Starting the pipeline engine (runs once via the timer trigger's startDelay:0)..."
rm -f out/orders.jsonl
flogo-datapipeline -f orders-rest-to-jsonlines.datapipeline &
ENGINE_PID=$!

for i in $(seq 1 50); do
  if [ -s out/orders.jsonl ] && [ "$(wc -l < out/orders.jsonl)" -ge 25 ]; then
    break
  fi
  sleep 0.2
done

echo ""
echo "==> Pipeline finished. First 3 lines of out/orders.jsonl:"
head -n 3 out/orders.jsonl
echo "    Total lines: $(wc -l < out/orders.jsonl)"
