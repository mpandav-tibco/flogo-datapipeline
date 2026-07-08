#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# run.sh — Example 03: Validation + Dead-Letter Queue
#
# Runs the orders-validate-dlq pipeline, then prints a summary showing how
# many rows passed validation and how many were routed to the DLQ.
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_FILE="${SCRIPT_DIR}/orders-validate-dlq.datapipeline"
DLQ_FILE="${SCRIPT_DIR}/data/orders_dlq.csv"
SOURCE_FILE="${SCRIPT_DIR}/data/orders.csv"

# ── Locate the engine binary ──────────────────────────────────────────────────
ENGINE=""
for candidate in \
    "${SCRIPT_DIR}/../../flogo-datapipeline" \
    "${SCRIPT_DIR}/../../flogo-datapipeline.exe" \
    "$(command -v flogo-datapipeline 2>/dev/null || true)"
do
    if [[ -x "$candidate" ]]; then
        ENGINE="$candidate"
        break
    fi
done

if [[ -z "$ENGINE" ]]; then
    echo "ERROR: flogo-datapipeline binary not found." >&2
    echo "       Build it first:  cd ../../ && go build -o flogo-datapipeline ./cmd/..." >&2
    exit 1
fi

# ── Clean up any previous DLQ output ─────────────────────────────────────────
if [[ -f "$DLQ_FILE" ]]; then
    echo "Removing previous DLQ file: $DLQ_FILE"
    rm -f "$DLQ_FILE"
fi

# ── Run the pipeline ──────────────────────────────────────────────────────────
echo ""
echo "Running: ${ENGINE} run ${PIPELINE_FILE}"
echo "──────────────────────────────────────────────────────────────────────────"
"$ENGINE" run "$PIPELINE_FILE"
echo "──────────────────────────────────────────────────────────────────────────"
echo ""

# ── Summarise results ─────────────────────────────────────────────────────────
# Count source rows (subtract 1 for the header line)
TOTAL_SOURCE=$(( $(wc -l < "$SOURCE_FILE") - 1 ))

if [[ -f "$DLQ_FILE" ]]; then
    # Subtract 1 for the DLQ header line
    DLQ_ROWS=$(( $(wc -l < "$DLQ_FILE") - 1 ))
else
    DLQ_ROWS=0
fi

GOOD_ROWS=$(( TOTAL_SOURCE - DLQ_ROWS ))

echo "Results"
echo "======="
echo "  Source rows  : ${TOTAL_SOURCE}"
echo "  Passed rules : ${GOOD_ROWS}   → forwarded to main sink"
echo "  Failed rules : ${DLQ_ROWS}   → routed to DLQ (${DLQ_FILE})"
echo ""

if [[ -f "$DLQ_FILE" && $DLQ_ROWS -gt 0 ]]; then
    echo "DLQ contents (${DLQ_FILE}):"
    echo "──────────────────────────────────────────────────────────────────────────"
    cat "$DLQ_FILE"
    echo ""
fi

echo "Done."
