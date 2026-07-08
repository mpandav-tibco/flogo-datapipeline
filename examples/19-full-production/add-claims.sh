#!/usr/bin/env bash
# add-claims.sh — Insert 20 new claims into claims.db with submitted_at timestamps
#                 AFTER the first-run HWM cutoff (2025-06-30 17:00:00).
#
# Purpose:
#   After running ./run.sh once, the HWM watermark is set to the maximum
#   submitted_at seen in the first batch ('2025-06-30 17:00:00').
#
#   This script inserts 20 new claims dated 2025-07-15, which are all AFTER
#   the watermark. Running ./run.sh a second time after this script will
#   demonstrate incremental processing — only the 20 new rows are consumed.
#
#   This simulates the production pattern where source systems continuously
#   append new rows and the pipeline runs on a schedule (cron, Flogo timer
#   trigger, or Kubernetes CronJob) processing only the delta each time.
#
# Usage:
#   ./add-claims.sh
#   ./run.sh           # Only processes the 20 new claims

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

SOURCE_DB="./data/claims.db"

if [ ! -f "$SOURCE_DB" ]; then
    echo "ERROR: $SOURCE_DB not found."
    echo "       Run ./setup.sh first."
    exit 1
fi

BEFORE_COUNT=$(sqlite3 "$SOURCE_DB" "SELECT COUNT(*) FROM raw_claims;")
echo "==> Adding 20 new claims to $SOURCE_DB (current row count: $BEFORE_COUNT)"
echo "    New claims have submitted_at = 2025-07-15 (after the HWM cutoff of 2025-06-30)"
echo ""

sqlite3 "$SOURCE_DB" <<'SQL'
-- 20 new valid claims — all submitted on 2025-07-15 (post-watermark)
-- claim_ids CLM-00000101 to CLM-00000120 to avoid conflicts with the first batch.
-- policy_id 1–10 so the lookup cache can be reused.
-- amounts in cents; all within the valid range (1 to 10,000,000).
INSERT INTO raw_claims VALUES
  ('CLM-00000101',  1,   320000, 'PENDING',      '2025-07-14', '2025-07-15 08:00:00', NULL),
  ('CLM-00000102',  2,  1450000, 'APPROVED',     '2025-07-14', '2025-07-15 08:15:00', 42),
  ('CLM-00000103',  3,   880000, 'UNDER_REVIEW', '2025-07-14', '2025-07-15 08:30:00', 17),
  ('CLM-00000104',  4,  3900000, 'REJECTED',     '2025-07-14', '2025-07-15 08:45:00', 9),
  ('CLM-00000105',  5,   120000, 'PENDING',      '2025-07-14', '2025-07-15 09:00:00', NULL),
  ('CLM-00000106',  6,  5800000, 'APPROVED',     '2025-07-14', '2025-07-15 09:15:00', 33),
  ('CLM-00000107',  7,   475000, 'PENDING',      '2025-07-14', '2025-07-15 09:30:00', NULL),
  ('CLM-00000108',  8,  1200000, 'UNDER_REVIEW', '2025-07-14', '2025-07-15 09:45:00', 22),
  ('CLM-00000109',  9,   960000, 'APPROVED',     '2025-07-14', '2025-07-15 10:00:00', 55),
  ('CLM-00000110', 10,   710000, 'PENDING',      '2025-07-14', '2025-07-15 10:15:00', NULL),
  ('CLM-00000111',  1,  2600000, 'REJECTED',     '2025-07-14', '2025-07-15 10:30:00', 12),
  ('CLM-00000112',  2,   165000, 'PENDING',      '2025-07-14', '2025-07-15 10:45:00', NULL),
  ('CLM-00000113',  3,  8200000, 'APPROVED',     '2025-07-14', '2025-07-15 11:00:00', 67),
  ('CLM-00000114',  4,   390000, 'UNDER_REVIEW', '2025-07-14', '2025-07-15 11:15:00', 19),
  ('CLM-00000115',  5,  4800000, 'PENDING',      '2025-07-14', '2025-07-15 11:30:00', NULL),
  ('CLM-00000116',  6,   545000, 'APPROVED',     '2025-07-14', '2025-07-15 11:45:00', 44),
  ('CLM-00000117',  7,  1380000, 'REJECTED',     '2025-07-14', '2025-07-15 12:00:00', 8),
  ('CLM-00000118',  8,   920000, 'PENDING',      '2025-07-14', '2025-07-15 12:15:00', NULL),
  ('CLM-00000119',  9,  3300000, 'UNDER_REVIEW', '2025-07-14', '2025-07-15 12:30:00', 30),
  ('CLM-00000120', 10,   250000, 'APPROVED',     '2025-07-14', '2025-07-15 12:45:00', 58);
SQL

AFTER_COUNT=$(sqlite3 "$SOURCE_DB" "SELECT COUNT(*) FROM raw_claims;")
NEW_COUNT=$(( AFTER_COUNT - BEFORE_COUNT ))

echo "    $NEW_COUNT new claims inserted."
echo "    Source database now has $AFTER_COUNT total rows."
echo ""
echo "    New claims date range:"
sqlite3 "$SOURCE_DB" \
    "SELECT '    submitted_at range: ' || MIN(submitted_at) || ' to ' || MAX(submitted_at)
     FROM raw_claims WHERE claim_id BETWEEN 'CLM-00000101' AND 'CLM-00000120';"
echo ""

# Show current watermark so the user can see the relationship
HWM_FILE=""
for f in ./watermarks/*.hwm ./watermarks/*.json; do
    [ -f "$f" ] && HWM_FILE="$f" && break
done

if [ -n "$HWM_FILE" ]; then
    echo "    Current HWM watermark: $(cat "$HWM_FILE")"
    echo "    New rows are AFTER the watermark — they will be picked up on the next run."
else
    echo "    NOTE: No watermark file found in ./watermarks/."
    echo "          Run ./run.sh at least once to establish the watermark before running add-claims.sh."
    echo "          The new rows will still be picked up, but so will all 100 original rows."
fi

echo ""
echo "==> Done. Run ./run.sh to process only the 20 new claims."
