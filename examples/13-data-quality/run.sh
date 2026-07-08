#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# run.sh — Example 13: Data Quality Expectations
#
# Runs the data-quality pipeline against financial_records.csv and prints a
# quality score summary. The 40-row dataset contains deliberate defects that
# push the quality ratio below the 0.95 threshold, triggering the report path.
#
# Usage:
#   bash run.sh
#
# Prerequisites:
#   Build the engine from the repository root:
#     cd ../../ && go build -o flogo-datapipeline ./cmd/...
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_FILE="${SCRIPT_DIR}/data-quality.datapipeline"
SOURCE_FILE="${SCRIPT_DIR}/data/financial_records.csv"

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

# ── Verify source data exists ─────────────────────────────────────────────────
if [[ ! -f "$SOURCE_FILE" ]]; then
    echo "ERROR: Source file not found: ${SOURCE_FILE}" >&2
    exit 1
fi

# Count source rows (subtract 1 for the header line)
TOTAL_SOURCE=$(( $(wc -l < "$SOURCE_FILE") - 1 ))

echo ""
echo "Example 13 — Data Quality Expectations"
echo "======================================="
echo "  Pipeline : ${PIPELINE_FILE}"
echo "  Source   : ${SOURCE_FILE} (${TOTAL_SOURCE} rows)"
echo "  Threshold: 0.95  (95%% of rows must satisfy ALL expectations)"
echo ""
echo "Expectations:"
echo "  1. record_id         — not_null"
echo "  2. transaction_amount — range [0.01, 1000000]"
echo "  3. currency          — in_set [USD, EUR, GBP, JPY, CAD]"
echo "  4. account_id        — not_null"
echo "  5. txn_date          — not_null"
echo ""
echo "Known defects in the sample data:"
echo "  Row 11  — record_id is null              (not_null violation)"
echo "  Row 16  — transaction_amount = 1200000   (range violation: above max)"
echo "  Row 21  — account_id is null             (not_null violation)"
echo "  Row 26  — currency = XBT                 (in_set violation)"
echo "  Row 31  — record_id is null              (not_null violation)"
echo "  Row 33  — transaction_amount = 0.00      (range violation: below min)"
echo "  Row 35  — account_id is null             (not_null violation)"
echo ""
echo "Expected quality score: 33/40 = 0.825  (below 0.95 threshold → report emitted)"
echo ""

# ── Run the pipeline ──────────────────────────────────────────────────────────
echo "Running: ${ENGINE} run ${PIPELINE_FILE}"
echo "──────────────────────────────────────────────────────────────────────────"
"$ENGINE" run "$PIPELINE_FILE"
PIPELINE_EXIT=$?
echo "──────────────────────────────────────────────────────────────────────────"
echo ""

# ── Summarise ─────────────────────────────────────────────────────────────────
if [[ $PIPELINE_EXIT -eq 0 ]]; then
    echo "Pipeline completed (exit 0)."
    echo ""
    echo "With onFail=report the engine:"
    echo "  - Wrote the quality report to the configured reportSinkType."
    echo "  - Continued processing — all 40 rows were forwarded to the null sink."
    echo "  - Did NOT abort the pipeline run."
    echo ""
    echo "To abort instead of reporting, change onFail to \"abort\" in the pipeline file."
else
    echo "Pipeline exited with code ${PIPELINE_EXIT}." >&2
    echo "If onFail were set to \"abort\" this would be the expected behaviour when" >&2
    echo "the quality score falls below the threshold." >&2
fi

echo "Done."
