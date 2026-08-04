#!/usr/bin/env bash
# run.sh — Execute the full-production pipeline.
#
# Usage:
#   ./run.sh           # First run: processes all valid claims up to the HWM cutoff
#   ./run.sh           # Second run: HWM is already set; 0 new rows unless add-claims.sh was run
#
# Prerequisites:
#   - flogo-datapipeline binary must be on your PATH.
#   - Run ./setup.sh first to start PostgreSQL and seed both tables.
#   - Network access to jsonplaceholder.typicode.com is required for the lookup transform.
#     The lookup uses policy_id 1–10, which map to JSONPlaceholder user IDs 1–10.
#     If the network is unavailable, the lookup onError:null policy will substitute
#     claimant_name="UNKNOWN" and claimant_email="UNKNOWN" for all rows.
#
# What happens on the FIRST run:
#   1. HWM: no watermark exists yet → all rows in raw_claims are candidates.
#   2. Validate: 20 invalid rows (bad claim_id pattern or out-of-range amount) are
#      routed to the dlqSink (null — discarded in this example).
#   3. DQ: measures null-check and range compliance on the 80 remaining valid rows.
#      Expected quality ratio: 100% (all 80 valid rows pass both expectations).
#   4. Lookup: fetches policyholder name+email from JSONPlaceholder for policy_ids 1–10.
#      With cacheSize=5000, the same policy_id is only fetched once; subsequent rows
#      with the same policy_id are served from the in-memory LRU cache.
#   5. Map: divides amount (cents) by 100 to produce amount_usd; coalesces lookup fields.
#   6. Sink: upserts 80 rows into claims_processed using claim_id as the conflict key.
#   7. Checkpoint: written every 500 rows and every 30 seconds; preserved after success
#      (deleteOnSuccess:false) so you can inspect ./checkpoints/ after the run.
#   8. HWM watermark is updated to the maximum submitted_at seen in this batch.
#
# What happens on the SECOND run (without add-claims.sh):
#   1. HWM filter: the saved watermark is '2025-06-30 17:00:00' (last CLM-00000080).
#      ALL rows in raw_claims have submitted_at <= this value, so 0 rows pass the filter.
#   2. Pipeline exits immediately: rows_read=0, rows_written=0.
#
# What happens after add-claims.sh + a third run:
#   1. add-claims.sh inserts 20 new claims with submitted_at = '2025-07-15 ...'
#   2. HWM filter: only rows with submitted_at > '2025-06-30 17:00:00' pass → 20 rows.
#   3. The 20 new claims are validated, enriched, mapped, and upserted.
#
# Run flags:
#   VERBOSE=1 ./run.sh    — enable debug-level log output from the engine

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Guards ────────────────────────────────────────────────────────────────────
if ! docker compose exec -T postgres pg_isready -U postgres -d pipelinedemo >/dev/null 2>&1; then
    echo "ERROR: PostgreSQL is not reachable."
    echo "       Run ./setup.sh first to start the database and seed both tables."
    exit 1
fi

# ── Determine run number for informational output ────────────────────────────
HWM_FILE=""
for f in ./watermarks/*.hwm ./watermarks/*.json; do
    [ -f "$f" ] && HWM_FILE="$f" && break
done

if [ -z "$HWM_FILE" ]; then
    RUN_LABEL="FIRST RUN — no watermark exists; all 100 source rows are candidates"
else
    HWM_VALUE=$(cat "$HWM_FILE" 2>/dev/null | tr -d '"{} \n' | sed 's/.*://' || echo "unknown")
    RUN_LABEL="SUBSEQUENT RUN — watermark=${HWM_VALUE}; only rows AFTER this timestamp will be processed"
fi

echo "============================================================"
echo "  Full Production Pipeline — Insurance Claims ETL"
echo "============================================================"
echo ""
echo "  Pipeline : full-production.datapipeline"
echo "  Source   : pipelinedemo.raw_claims       (JDBC PostgreSQL)"
echo "  Sink     : pipelinedemo.claims_processed (JDBC PostgreSQL, upsert)"
echo "  DLQ sink : null (invalid claims discarded)"
echo "  Checkpoint: ./checkpoints/"
echo "  Watermark : ./watermarks/"
echo ""
echo "  $RUN_LABEL"
echo ""
echo "============================================================"
echo ""

# ── Run the pipeline ─────────────────────────────────────────────────────────
if [ "${VERBOSE:-0}" = "1" ]; then
    flogo-datapipeline -f full-production.datapipeline -v
else
    flogo-datapipeline -f full-production.datapipeline
fi

EXIT_CODE=$?

echo ""
echo "============================================================"
echo "  Pipeline finished (exit code: $EXIT_CODE)"
echo "============================================================"
echo ""

if [ "$EXIT_CODE" -ne 0 ]; then
    echo "  ERROR: Pipeline exited with code $EXIT_CODE."
    echo "         Check the log output above for details."
    echo "         If the run was interrupted, re-run this script."
    echo "         The checkpoint in ./checkpoints/ will enable resume from the last safe point."
    exit "$EXIT_CODE"
fi

# ── Post-run summary ─────────────────────────────────────────────────────────
PROCESSED_COUNT=$(docker compose exec -T postgres psql -U postgres -d pipelinedemo -tAc \
    "SELECT COUNT(*) FROM claims_processed;" 2>/dev/null || echo "0")

echo "  Claims processed (cumulative): $PROCESSED_COUNT"
echo ""
echo "  Status breakdown in claims_processed:"
docker compose exec -T postgres psql -U postgres -d pipelinedemo -c \
    "SELECT status, COUNT(*) AS count FROM claims_processed GROUP BY status ORDER BY count DESC;" \
    2>/dev/null || echo "  (sink table is empty)"
echo ""

# Show current HWM
for f in ./watermarks/*.hwm ./watermarks/*.json; do
    if [ -f "$f" ]; then
        echo "  Current watermark ($(basename "$f")):"
        echo "    $(cat "$f")"
        break
    fi
done

# Show latest checkpoint token
LATEST_CKPT=$(ls -t ./checkpoints/*.json 2>/dev/null | head -1 || true)
if [ -n "$LATEST_CKPT" ]; then
    echo ""
    echo "  Latest checkpoint: $LATEST_CKPT"
    echo "    (deleteOnSuccess:false — checkpoint preserved for inspection)"
fi

echo ""
echo "  Next steps:"
echo "    ./run.sh           — run again to confirm 0 new rows (HWM idempotence)"
echo "    ./add-claims.sh    — insert 20 new claims into the source DB"
echo "    ./run.sh           — run again to process only the 20 new claims"
echo "    ./verify.sh        — inspect the enriched output"
