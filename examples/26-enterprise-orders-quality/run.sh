#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# run.sh — Example 26: Enterprise Orders Quality Gate
#
# The pipeline is fired by a timer that ticks once on startup, so the engine is
# a long-running process. This script starts it, waits for the single run to
# finish (by watching the log), prints a summary, and then stops the engine.
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_FILE="${SCRIPT_DIR}/enterprise-orders-quality.datapipeline"
SOURCE_FILE="${SCRIPT_DIR}/data/orders.csv"
CURATED_FILE="${SCRIPT_DIR}/out/curated-orders.jsonl"
REJECTED_FILE="${SCRIPT_DIR}/data/rejected-orders.csv"
LOG_FILE="$(mktemp -t ex26.XXXXXX)"

# ── Locate the engine binary ──────────────────────────────────────────────────
ENGINE=""
for candidate in \
    "${SCRIPT_DIR}/../../flogo-datapipeline" \
    "${SCRIPT_DIR}/../../flogo-datapipeline.exe" \
    "$(command -v flogo-datapipeline 2>/dev/null || true)"
do
    if [[ -x "$candidate" ]]; then ENGINE="$candidate"; break; fi
done
if [[ -z "$ENGINE" ]]; then
    echo "ERROR: flogo-datapipeline binary not found." >&2
    echo "       Build it:  cd ../../ && CGO_ENABLED=1 go build -o flogo-datapipeline ./cmd/flogo-datapipeline" >&2
    exit 1
fi

# ── Fresh output ──────────────────────────────────────────────────────────────
mkdir -p "${SCRIPT_DIR}/out"
rm -f "$CURATED_FILE" "$REJECTED_FILE" "${SCRIPT_DIR}/out/audit.jsonl"

# ── Start the engine, wait for the run to complete, then stop it ─────────────
echo "Running: ${ENGINE} -f ${PIPELINE_FILE}"
echo "──────────────────────────────────────────────────────────────────────────"
( cd "$SCRIPT_DIR" && "$ENGINE" -f "$PIPELINE_FILE" ) > "$LOG_FILE" 2>&1 &
ENGINE_PID=$!

# Wait up to 30s for the "pipeline run <status>" summary line to appear.
for _ in $(seq 60); do
    if grep -qE "pipeline run (completed|partial|failed) —" "$LOG_FILE"; then break; fi
    if ! kill -0 "$ENGINE_PID" 2>/dev/null; then break; fi
    sleep 0.5
done
kill "$ENGINE_PID" 2>/dev/null || true
wait "$ENGINE_PID" 2>/dev/null || true

grep -E "pipeline run (completed|partial|failed) —" "$LOG_FILE" | tail -1 || true
echo "──────────────────────────────────────────────────────────────────────────"

# ── Summarise ─────────────────────────────────────────────────────────────────
TOTAL=$(( $(wc -l < "$SOURCE_FILE") - 1 ))
REJECTED=0; [[ -f "$REJECTED_FILE" ]] && REJECTED=$(( $(wc -l < "$REJECTED_FILE") - 1 ))
CURATED=0;  [[ -f "$CURATED_FILE"  ]] && CURATED=$(wc -l < "$CURATED_FILE")

echo ""
echo "Results"
echo "======="
echo "  Source rows            : ${TOTAL}"
echo "  Rejected (quality gate): ${REJECTED}   → ${REJECTED_FILE}"
echo "  Curated (high-value)   : ${CURATED}   → ${CURATED_FILE}"
echo ""
if [[ -f "$CURATED_FILE" ]]; then
    echo "Curated feed:"
    cat "$CURATED_FILE"
    echo ""
fi
rm -f "$LOG_FILE"
echo "Done."
