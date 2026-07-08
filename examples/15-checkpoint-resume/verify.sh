#!/usr/bin/env bash
# verify.sh — Inspect the checkpoint file and confirm resume semantics.
#
# PURPOSE
# -------
# This script does not run flogo-datapipeline.  It is a read-only diagnostic
# tool that parses and explains the checkpoint token so you can confirm exactly
# what state was saved and what the engine will skip on the next resume.
#
# Run this after interrupt.sh to see the mid-run token, or after resume.sh to
# see the final token written when the pipeline completes cleanly.
#
# OUTPUT
# ------
# For each .json file found in ./checkpoints/ the script prints:
#   - The raw JSON (pretty-printed if python3 is available)
#   - A human-readable interpretation of every field
#   - A prediction of what the engine will do on the next resume
#   - The percentage of the file already processed
#
# CHECKPOINT FILE FORMAT
# ----------------------
# {
#   "pipeline":     "checkpoint-resume",
#   "rowsProcessed": 2000,
#   "timestamp":    "2026-07-08T14:23:01Z",
#   "source":       "csv",
#   "sourcePath":   "./data/big_file.csv"
# }
#
# Field meanings:
#   pipeline       — matches the "name" field in the .datapipeline file
#   rowsProcessed  — number of source rows fully committed (past the sink)
#   timestamp      — wall-clock time the token was last written (UTC ISO-8601)
#   source         — source type (used to select the correct Skip() implementation)
#   sourcePath     — path the source was reading from (sanity check on resume)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CHECKPOINT_DIR="./checkpoints"
CSV_FILE="./data/big_file.csv"

echo "========================================"
echo " verify.sh — Checkpoint Inspection"
echo "========================================"
echo ""

# ------------------------------------------------------------------
# Check for checkpoint files
# ------------------------------------------------------------------
FOUND=$(find "$CHECKPOINT_DIR" -maxdepth 1 -name "*.json" 2>/dev/null | sort)
COUNT=$(echo "$FOUND" | grep -c '.' 2>/dev/null || echo 0)

if [ -z "$FOUND" ] || [ "$COUNT" -eq 0 ]; then
  echo "  No checkpoint files found in $CHECKPOINT_DIR/"
  echo ""
  echo "  To create one:"
  echo "    1. Run ./setup.sh     (generate big_file.csv)"
  echo "    2. Run ./interrupt.sh (start pipeline, kill it mid-run)"
  echo "    3. Re-run ./verify.sh"
  exit 0
fi

# ------------------------------------------------------------------
# Total rows in source (for percentage calculation)
# ------------------------------------------------------------------
if [ -f "$CSV_FILE" ]; then
  TOTAL_ROWS=$(tail -n +2 "$CSV_FILE" | wc -l | tr -d ' ')
else
  TOTAL_ROWS="?"
fi

# ------------------------------------------------------------------
# Parse and display each checkpoint file
# ------------------------------------------------------------------
for CHECKPOINT_FILE in $FOUND; do
  echo "========================================"
  echo " File: $CHECKPOINT_FILE"
  echo "========================================"
  echo ""

  # Raw JSON
  echo "  Raw JSON:"
  echo "  ----------------------------------------"
  if command -v python3 &>/dev/null; then
    python3 -m json.tool "$CHECKPOINT_FILE" | sed 's/^/  /'
  else
    cat "$CHECKPOINT_FILE" | sed 's/^/  /'
  fi
  echo "  ----------------------------------------"
  echo ""

  # Parse individual fields
  if command -v python3 &>/dev/null; then
    PIPELINE_NAME=$(python3 -c "import json; d=json.load(open('$CHECKPOINT_FILE')); print(d.get('pipeline','<missing>'))")
    ROWS_DONE=$(python3     -c "import json; d=json.load(open('$CHECKPOINT_FILE')); print(d.get('rowsProcessed', 0))")
    TIMESTAMP=$(python3     -c "import json; d=json.load(open('$CHECKPOINT_FILE')); print(d.get('timestamp','<missing>'))")
    SOURCE_TYPE=$(python3   -c "import json; d=json.load(open('$CHECKPOINT_FILE')); print(d.get('source','<missing>'))")
    SOURCE_PATH=$(python3   -c "import json; d=json.load(open('$CHECKPOINT_FILE')); print(d.get('sourcePath','<missing>'))")
  else
    PIPELINE_NAME=$(grep -o '"pipeline":"[^"]*"' "$CHECKPOINT_FILE" | cut -d'"' -f4 || echo "<missing>")
    ROWS_DONE=$(grep -o '"rowsProcessed":[0-9]*' "$CHECKPOINT_FILE" | grep -o '[0-9]*' || echo "0")
    TIMESTAMP=$(grep -o '"timestamp":"[^"]*"' "$CHECKPOINT_FILE" | cut -d'"' -f4 || echo "<missing>")
    SOURCE_TYPE=$(grep -o '"source":"[^"]*"' "$CHECKPOINT_FILE" | cut -d'"' -f4 || echo "<missing>")
    SOURCE_PATH=$(grep -o '"sourcePath":"[^"]*"' "$CHECKPOINT_FILE" | cut -d'"' -f4 || echo "<missing>")
  fi

  # Compute remaining rows and percentage
  if [ "$TOTAL_ROWS" != "?" ] && [ "$ROWS_DONE" -gt 0 ] 2>/dev/null; then
    REMAINING=$(( TOTAL_ROWS - ROWS_DONE ))
    PCT=$(awk "BEGIN { printf \"%.1f\", ($ROWS_DONE / $TOTAL_ROWS) * 100 }")
  else
    REMAINING="?"
    PCT="?"
  fi

  echo "  Field-by-field interpretation:"
  echo "  ----------------------------------------"
  printf "  %-20s %s\n"  "pipeline:"      "$PIPELINE_NAME"
  printf "  %-20s %s\n"  "rowsProcessed:" "$ROWS_DONE  <-- engine will call Skip($ROWS_DONE) on next start"
  printf "  %-20s %s\n"  "timestamp:"     "$TIMESTAMP"
  printf "  %-20s %s\n"  "source:"        "$SOURCE_TYPE"
  printf "  %-20s %s\n"  "sourcePath:"    "$SOURCE_PATH"
  echo "  ----------------------------------------"
  echo ""

  echo "  Progress summary:"
  printf "  %-30s %s\n" "Rows committed to sink:"   "$ROWS_DONE / $TOTAL_ROWS"
  printf "  %-30s %s\n" "Rows remaining:"            "$REMAINING"
  printf "  %-30s %s\n" "Completion:"                "${PCT}%"
  echo ""

  echo "  What happens on resume:"
  echo "  ----------------------------------------"
  echo "  1. Engine reads $CHECKPOINT_FILE"
  echo "  2. Source opens $SOURCE_PATH"
  echo "  3. Source calls Skip($ROWS_DONE) — reads and discards the first"
  echo "     $ROWS_DONE data rows without sending them through transforms or sink"
  echo "  4. Normal processing resumes from row $(( ROWS_DONE + 1 )) onward"
  echo "  5. Checkpoint continues firing every 1000 rows or 10 seconds"
  echo "  6. On clean finish: final token written, exit code 0"
  echo "  ----------------------------------------"
  echo ""
done

# ------------------------------------------------------------------
# Cross-check: confirm pipeline definition checkpoint config
# ------------------------------------------------------------------
PIPELINE_FILE="./checkpoint-resume.datapipeline"
if [ -f "$PIPELINE_FILE" ]; then
  echo "========================================"
  echo " Pipeline checkpoint config (from $PIPELINE_FILE)"
  echo "========================================"
  echo ""
  if command -v python3 &>/dev/null; then
    python3 -c "
import json
with open('$PIPELINE_FILE') as f:
    d = json.load(f)
cp = d.get('checkpoint', {})
print('  type            :', cp.get('type', '<missing>'))
print('  path            :', cp.get('path', '<missing>'))
print('  intervalRows    :', cp.get('intervalRows', '<missing>'), '  (write token every N rows read)')
print('  intervalSeconds :', cp.get('intervalSeconds', '<missing>'), '  (write token every N elapsed seconds)')
print('  deleteOnSuccess :', cp.get('deleteOnSuccess', '<missing>'), '  (True = delete file after clean finish; False = keep for inspection)')
"
  else
    grep -A 10 '"checkpoint"' "$PIPELINE_FILE" | sed 's/^/  /'
  fi
  echo ""
fi

echo "========================================"
echo " Done"
echo "========================================"
echo ""
