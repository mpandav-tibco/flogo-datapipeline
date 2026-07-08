#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# run.sh — Example 11: Schema Evolution & Drift Handling
#
# Demonstrates two schemadrift policies against the same evolved CSV file:
#   1. additive — new columns (region, score) are accepted and forwarded
#   2. strict   — new columns cause the pipeline to fail fast
#
# Usage:
#   ./run.sh              # runs both pipelines and shows results
#   ./run.sh additive     # runs only the additive pipeline
#   ./run.sh strict       # runs only the strict pipeline
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
V1_FILE="${SCRIPT_DIR}/data/v1_records.csv"
V2_FILE="${SCRIPT_DIR}/data/v2_records.csv"
ADDITIVE_PIPELINE="${SCRIPT_DIR}/additive.datapipeline"
STRICT_PIPELINE="${SCRIPT_DIR}/strict.datapipeline"

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

# ── Helper: column count from CSV header ─────────────────────────────────────
csv_col_count() {
    local file="$1"
    head -1 "$file" | awk -F',' '{ print NF }'
}

csv_row_count() {
    local file="$1"
    # subtract header line
    echo $(( $(wc -l < "$file") - 1 ))
}

# ── Schema summary ────────────────────────────────────────────────────────────
V1_COLS=$(csv_col_count "$V1_FILE")
V2_COLS=$(csv_col_count "$V2_FILE")
V1_ROWS=$(csv_row_count "$V1_FILE")
V2_ROWS=$(csv_row_count "$V2_FILE")
V1_HEADER=$(head -1 "$V1_FILE")
V2_HEADER=$(head -1 "$V2_FILE")

echo ""
echo "Schema Drift Example — Data Summary"
echo "===================================="
echo ""
echo "  v1_records.csv  :  ${V1_ROWS} rows,  ${V1_COLS} columns  [${V1_HEADER}]"
echo "  v2_records.csv  :  ${V2_ROWS} rows,  ${V2_COLS} columns  [${V2_HEADER}]"
echo ""
echo "  Both pipelines declare only the original 3-column schema (id, name, status)."
echo "  v2_records.csv carries 2 extra columns: region, score."
echo ""

# ── Which pipelines to run ────────────────────────────────────────────────────
RUN_ADDITIVE=true
RUN_STRICT=true

if [[ "${1:-}" == "additive" ]]; then
    RUN_STRICT=false
elif [[ "${1:-}" == "strict" ]]; then
    RUN_ADDITIVE=false
fi

# ── Run: additive ─────────────────────────────────────────────────────────────
if $RUN_ADDITIVE; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Policy: ADDITIVE  (${ADDITIVE_PIPELINE})"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "Expected: pipeline succeeds; extra columns (region, score) are accepted"
    echo "          and appended to each row. No pipeline change required."
    echo ""
    echo "Running: ${ENGINE} run ${ADDITIVE_PIPELINE}"
    echo "------------------------------------------------------------------------"
    if "$ENGINE" run "$ADDITIVE_PIPELINE"; then
        echo "------------------------------------------------------------------------"
        echo "Result: SUCCESS — all ${V2_ROWS} rows processed; extra columns forwarded."
    else
        echo "------------------------------------------------------------------------"
        echo "Result: UNEXPECTED FAILURE (additive should succeed)."
    fi
    echo ""
fi

# ── Run: strict ───────────────────────────────────────────────────────────────
if $RUN_STRICT; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Policy: STRICT  (${STRICT_PIPELINE})"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "Expected: pipeline fails with a schema mismatch error because"
    echo "          v2_records.csv contains undeclared columns (region, score)."
    echo ""
    echo "Running: ${ENGINE} run ${STRICT_PIPELINE}"
    echo "------------------------------------------------------------------------"
    if "$ENGINE" run "$STRICT_PIPELINE" 2>&1; then
        echo "------------------------------------------------------------------------"
        echo "Result: PASSED (strict did NOT fail — re-check engine behaviour)."
    else
        echo "------------------------------------------------------------------------"
        echo "Result: FAILED as expected — schema mismatch detected and pipeline aborted."
    fi
    echo ""
fi

# ── Bonus: baseline run with v1 data (no drift at all) ────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Bonus: ADDITIVE policy against v1_records.csv (no drift scenario)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Expected: pipeline succeeds normally. Additive policy is a no-op when"
echo "          the incoming file exactly matches the declared schema."
echo ""

# Temporarily swap path in additive pipeline for v1 file
TMP_PIPELINE="$(mktemp --suffix=.datapipeline)"
sed 's|v2_records.csv|v1_records.csv|g' "$ADDITIVE_PIPELINE" > "$TMP_PIPELINE"

echo "Running: ${ENGINE} run <additive-v1-variant>"
echo "------------------------------------------------------------------------"
if "$ENGINE" run "$TMP_PIPELINE"; then
    echo "------------------------------------------------------------------------"
    echo "Result: SUCCESS — ${V1_ROWS} rows processed; schema exactly matched, no drift action taken."
else
    echo "------------------------------------------------------------------------"
    echo "Result: UNEXPECTED FAILURE."
fi

rm -f "$TMP_PIPELINE"
echo ""
echo "Done."
