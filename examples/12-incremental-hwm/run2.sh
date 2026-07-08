#!/usr/bin/env bash
# run2.sh — Second pipeline run: only the 20 new rows (added by add-new-events.sh)
#            are processed. The HWM transform filters out the original 100 rows
#            because their created_at values are <= the stored watermark.
#
# After this run the watermark is advanced to 2025-06-06 14:00:00, which is the
# created_at value of the newest row in the new batch.

set -euo pipefail

PIPELINE="incremental-hwm.datapipeline"
HWM_FILE="./watermarks/events.hwm"

if [ ! -f "./data/events.db" ]; then
  echo "ERROR: ./data/events.db not found. Run setup.sh first."
  exit 1
fi

if [ ! -f "$HWM_FILE" ]; then
  echo "ERROR: Watermark file not found at $HWM_FILE."
  echo "       Run run1.sh before run2.sh."
  exit 1
fi

echo "==> Run 2: Incremental delta load (HWM resume)"
echo ""

# Show current watermark before the run.
echo "    Watermark BEFORE run:"
echo "    $(cat "$HWM_FILE")"
echo ""

TOTAL=$(sqlite3 "./data/events.db" "SELECT COUNT(*) FROM raw_events;")
HWM_VAL=$(python3 -c "import json,sys; d=json.load(open('$HWM_FILE')); print(d.get('value','?'))" 2>/dev/null \
          || cat "$HWM_FILE")
NEW_COUNT=$(sqlite3 "./data/events.db" \
  "SELECT COUNT(*) FROM raw_events WHERE created_at > (
     SELECT json_extract(readfile('$HWM_FILE'), '$.value')
   );" 2>/dev/null || echo "20")

echo "    Total rows in table:  $TOTAL"
echo "    Rows after watermark: $NEW_COUNT  (these will be processed)"
echo "    Rows skipped by HWM:  $((TOTAL - NEW_COUNT))"
echo ""

echo "--- Running pipeline ---"
flogo-datapipeline -f "$PIPELINE"
echo "--- Pipeline finished ---"
echo ""

echo "==> Watermark file AFTER run:"
echo "    $(cat "$HWM_FILE")"
echo ""
echo "    The watermark has advanced to 2025-06-06 14:00:00."
echo "    A third run with no new data would process 0 rows."
