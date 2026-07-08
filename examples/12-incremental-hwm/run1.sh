#!/usr/bin/env bash
# run1.sh — First pipeline run: process all 100 events and save the watermark.
#
# Because no watermark file exists yet, the HWM transform has no lower bound
# and passes every row through. After the run, the watermark file will contain
# the ISO-8601 timestamp of the most recent row seen (2025-06-05 15:30:00).
#
# Subsequent runs (run2.sh) will only process rows with created_at strictly
# greater than that recorded value.

set -euo pipefail

PIPELINE="incremental-hwm.datapipeline"
HWM_FILE="./watermarks/events.hwm"

# Verify the database exists.
if [ ! -f "./data/events.db" ]; then
  echo "ERROR: ./data/events.db not found. Run setup.sh first."
  exit 1
fi

echo "==> Run 1: Full initial load (no watermark on record)"
echo ""

# Show current watermark state before the run.
if [ -f "$HWM_FILE" ]; then
  echo "    Current watermark: $(cat "$HWM_FILE")"
else
  echo "    Current watermark: <none — this is the first run>"
fi
echo ""

# Show how many rows are in the table.
ROW_COUNT=$(sqlite3 "./data/events.db" "SELECT COUNT(*) FROM raw_events;")
echo "    Rows in raw_events: $ROW_COUNT"
echo "    Expected rows processed: $ROW_COUNT  (all rows pass — no prior HWM)"
echo ""

echo "--- Running pipeline ---"
flogo-datapipeline -f "$PIPELINE"
echo "--- Pipeline finished ---"
echo ""

# Show the watermark that was written after the run.
echo "==> Watermark file written at: $HWM_FILE"
echo "    Content:"
cat "$HWM_FILE"
echo ""
echo "    The next run will only process rows with created_at > the value above."
echo ""
echo "    Next step: run ./add-new-events.sh to insert 20 new rows, then ./run2.sh"
