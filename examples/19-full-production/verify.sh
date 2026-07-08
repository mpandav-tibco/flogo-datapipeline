#!/usr/bin/env bash
# verify.sh — Inspect the enriched claims database after running the pipeline.
#
# Usage:
#   ./verify.sh
#
# Prerequisites:
#   - sqlite3 must be on your PATH.
#   - Run ./setup.sh and ./run.sh at least once before running this script.
#
# Output:
#   1. Row count and status breakdown in claims_processed.
#   2. Sample rows showing the full enriched schema (claim_id, amount_usd, claimant_name, etc.).
#   3. Policy distribution (which policyholders have the most claims).
#   4. HWM watermark state.
#   5. Checkpoint state.
#   6. DLQ accounting (invalid rows in source minus valid rows in sink).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

SINK_DB="./data/claims_enriched.db"
SOURCE_DB="./data/claims.db"

if [ ! -f "$SINK_DB" ]; then
    echo "ERROR: $SINK_DB not found."
    echo "       Run ./setup.sh then ./run.sh before verifying."
    exit 1
fi

PROCESSED=$(sqlite3 "$SINK_DB" "SELECT COUNT(*) FROM claims_processed;" 2>/dev/null || echo 0)

if [ "$PROCESSED" -eq 0 ]; then
    echo "WARNING: claims_processed is empty — run ./run.sh first."
    exit 1
fi

echo "============================================================"
echo "  Verification — Full Production Pipeline"
echo "  Sink: $SINK_DB"
echo "============================================================"
echo ""

# ── 1. Count and status breakdown ────────────────────────────────────────────
echo "── 1. ROW COUNT & STATUS BREAKDOWN ─────────────────────────"
echo ""
sqlite3 -column -header "$SINK_DB" \
    "SELECT status, COUNT(*) AS claim_count, ROUND(AVG(amount_usd),2) AS avg_usd, ROUND(MIN(amount_usd),2) AS min_usd, ROUND(MAX(amount_usd),2) AS max_usd
     FROM claims_processed
     GROUP BY status
     ORDER BY claim_count DESC;"
echo ""
echo "    Total rows in claims_processed: $PROCESSED"
echo ""

# ── 2. Sample enriched rows ──────────────────────────────────────────────────
echo "── 2. SAMPLE ENRICHED ROWS (first 10) ──────────────────────"
echo ""
sqlite3 -column -header "$SINK_DB" \
    "SELECT claim_id, policy_id, amount_usd, claimant_name, claimant_email, status
     FROM claims_processed
     ORDER BY claim_id
     LIMIT 10;"
echo ""

# ── 3. Policy distribution ───────────────────────────────────────────────────
echo "── 3. POLICYHOLDER DISTRIBUTION ────────────────────────────"
echo ""
sqlite3 -column -header "$SINK_DB" \
    "SELECT policy_id, claimant_name, COUNT(*) AS claims, ROUND(SUM(amount_usd),2) AS total_usd
     FROM claims_processed
     GROUP BY policy_id
     ORDER BY total_usd DESC;"
echo ""

# ── 4. Enrichment quality check ──────────────────────────────────────────────
echo "── 4. ENRICHMENT QUALITY ────────────────────────────────────"
echo ""
UNKNOWN_NAME=$(sqlite3 "$SINK_DB" \
    "SELECT COUNT(*) FROM claims_processed WHERE claimant_name='UNKNOWN';")
UNKNOWN_EMAIL=$(sqlite3 "$SINK_DB" \
    "SELECT COUNT(*) FROM claims_processed WHERE claimant_email='UNKNOWN';")
echo "    Rows with claimant_name='UNKNOWN'  (lookup returned null): $UNKNOWN_NAME"
echo "    Rows with claimant_email='UNKNOWN' (lookup returned null): $UNKNOWN_EMAIL"

if [ "$UNKNOWN_NAME" -eq 0 ] && [ "$UNKNOWN_EMAIL" -eq 0 ]; then
    echo "    All rows successfully enriched with policyholder data."
else
    echo "    Some rows fell back to UNKNOWN — likely a network timeout during the lookup."
    echo "    Re-running the pipeline will re-attempt the lookup (rows already in sink"
    echo "    will be upserted with updated enrichment data if lookup succeeds next time)."
fi
echo ""

# ── 5. Amount conversion check ───────────────────────────────────────────────
echo "── 5. AMOUNT CONVERSION CHECK (cents → dollars) ─────────────"
echo ""
# Spot-check CLM-00000001: source amount=250000 cents → sink amount_usd=2500.00
SPOT_CHECK=$(sqlite3 "$SINK_DB" \
    "SELECT amount_usd FROM claims_processed WHERE claim_id='CLM-00000001';" 2>/dev/null || echo "N/A")
echo "    CLM-00000001 source amount: 250000 cents"
echo "    CLM-00000001 sink amount_usd: $SPOT_CHECK (expected: 2500.0)"

if [ "$SPOT_CHECK" = "2500.0" ] || [ "$SPOT_CHECK" = "2500" ]; then
    echo "    PASS — divide(amount, 100) map expression is working correctly."
else
    echo "    NOTE: Value differs from expected 2500.0 — check map transform expression."
fi
echo ""

# ── 6. DLQ accounting ────────────────────────────────────────────────────────
echo "── 6. DLQ ACCOUNTING ────────────────────────────────────────"
echo ""
SOURCE_TOTAL=$(sqlite3 "$SOURCE_DB" "SELECT COUNT(*) FROM raw_claims;" 2>/dev/null || echo "N/A")
echo "    Rows in source (raw_claims)     : $SOURCE_TOTAL"
echo "    Rows in sink   (claims_processed): $PROCESSED"
echo ""
echo "    Invalid rows routed to DLQ sink : $(( SOURCE_TOTAL - PROCESSED )) (expected: 20)"
echo ""
echo "    Invalid claim_id patterns in source:"
sqlite3 -column -header "$SOURCE_DB" \
    "SELECT claim_id, amount, status
     FROM raw_claims
     WHERE claim_id NOT LIKE 'CLM-________' OR LENGTH(claim_id) != 12
        OR amount < 1 OR amount > 10000000
     ORDER BY claim_id
     LIMIT 20;"
echo ""

# ── 7. HWM state ─────────────────────────────────────────────────────────────
echo "── 7. HIGH-WATER MARK STATE ─────────────────────────────────"
echo ""
HWM_FOUND=0
for f in ./watermarks/*.hwm ./watermarks/*.json; do
    if [ -f "$f" ]; then
        echo "    File: $f"
        echo "    Contents: $(cat "$f")"
        HWM_FOUND=1
        break
    fi
done
if [ "$HWM_FOUND" -eq 0 ]; then
    echo "    No watermark file found in ./watermarks/"
    echo "    This means the pipeline has not completed a run yet, or watermarks were deleted."
fi
echo ""

# ── 8. Checkpoint state ───────────────────────────────────────────────────────
echo "── 8. CHECKPOINT STATE ──────────────────────────────────────"
echo ""
CKPT_FILES=$(ls ./checkpoints/*.json 2>/dev/null | wc -l | tr -d ' ')
if [ "$CKPT_FILES" -gt 0 ]; then
    echo "    Checkpoint files in ./checkpoints/: $CKPT_FILES"
    ls -lh ./checkpoints/*.json 2>/dev/null | awk '{print "    " $NF " (" $5 ")"}'
    echo ""
    echo "    Latest checkpoint content:"
    LATEST=$(ls -t ./checkpoints/*.json 2>/dev/null | head -1)
    echo "    $(cat "$LATEST")"
else
    echo "    No checkpoint files found in ./checkpoints/"
fi
echo ""

echo "============================================================"
echo "  Verification complete."
echo "============================================================"
echo ""
echo "  To explore the data interactively:"
echo "    sqlite3 $SINK_DB"
echo "    sqlite3> SELECT * FROM claims_processed LIMIT 5;"
echo "    sqlite3> SELECT COUNT(*), status FROM claims_processed GROUP BY status;"
echo "    sqlite3> .quit"
