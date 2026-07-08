#!/usr/bin/env bash
# setup.sh — Prepare the working environment for the checkpoint-resume example.
#
# What this script does:
#   1. Creates ./data/ and ./checkpoints/ directories.
#   2. Generates ./data/big_file.csv — 5000 rows (id, data, value).
#      - If Python 3 is available:  runs data/generate_big_file.py (reproducible,
#        seeded RNG — same file every time).
#      - Fallback (bash only):      generates rows with $RANDOM so values differ
#        per run, which is still sufficient to demonstrate checkpointing.
#   3. Removes any stale checkpoint files from a previous run so the demo
#      always starts from scratch.
#   4. Prints a summary so you can confirm the dataset before running the pipeline.
#
# Prerequisites:
#   - Python 3 on PATH (recommended) OR bash with $RANDOM support (universal).
#   - flogo-datapipeline binary on PATH (needed for interrupt.sh / resume.sh).
#
# Run order:
#   1. ./setup.sh          — generate data, clear checkpoints
#   2. ./interrupt.sh      — start pipeline in background, kill it after 2 s
#   3. ./verify.sh         — inspect the checkpoint file
#   4. ./resume.sh         — re-run pipeline; it picks up from the checkpoint

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DATA_DIR="./data"
CHECKPOINT_DIR="./checkpoints"
CSV_FILE="$DATA_DIR/big_file.csv"
ROWS=5000

echo "========================================"
echo " Example 15 — Checkpoint & Resume Setup"
echo "========================================"
echo ""

# ------------------------------------------------------------------
# 1. Create directories
# ------------------------------------------------------------------
echo "--> Creating directories..."
mkdir -p "$DATA_DIR"
mkdir -p "$CHECKPOINT_DIR"
echo "    $DATA_DIR/          OK"
echo "    $CHECKPOINT_DIR/    OK"
echo ""

# ------------------------------------------------------------------
# 2. Remove stale checkpoint files
# ------------------------------------------------------------------
STALE=$(find "$CHECKPOINT_DIR" -maxdepth 1 -name "*.json" 2>/dev/null | wc -l | tr -d ' ')
if [ "$STALE" -gt 0 ]; then
  echo "--> Removing $STALE stale checkpoint file(s)..."
  rm -f "$CHECKPOINT_DIR"/*.json
  echo "    Done."
else
  echo "--> No stale checkpoints to remove."
fi
echo ""

# ------------------------------------------------------------------
# 3. Generate big_file.csv
# ------------------------------------------------------------------
echo "--> Generating $CSV_FILE ($ROWS rows)..."

if command -v python3 &>/dev/null; then
  echo "    Using Python 3 generator (reproducible, seed=42)..."
  python3 "$DATA_DIR/generate_big_file.py" "$ROWS"
else
  echo "    Python 3 not found — using bash fallback (values vary per run)..."
  {
    echo "id,data,value"
    for i in $(seq 1 $ROWS); do
      VALUE=$(( (RANDOM % 100) + 1 ))
      echo "$i,data_$i,$VALUE"
    done
  } > "$CSV_FILE"
  echo "    Written $CSV_FILE"
fi

echo ""

# ------------------------------------------------------------------
# 4. Summary
# ------------------------------------------------------------------
TOTAL=$(tail -n +2 "$CSV_FILE" | wc -l | tr -d ' ')
PASSING=$(tail -n +2 "$CSV_FILE" | awk -F',' '$3 > 50' | wc -l | tr -d ' ')
DROPPED=$(( TOTAL - PASSING ))

echo "========================================"
echo " Data summary"
echo "========================================"
echo "  File         : $CSV_FILE"
echo "  Total rows   : $TOTAL"
echo "  value > 50   : $PASSING  (will pass the filter transform)"
echo "  value <= 50  : $DROPPED  (will be dropped by the filter transform)"
echo "  Checkpoints  : $CHECKPOINT_DIR/ (empty — ready for a fresh run)"
echo ""
echo "  Checkpoint config (from checkpoint-resume.datapipeline):"
echo "    type            : file"
echo "    path            : $CHECKPOINT_DIR"
echo "    intervalRows    : 1000   (write a token every 1000 rows read)"
echo "    intervalSeconds : 10     (write a token every 10 seconds elapsed)"
echo "    deleteOnSuccess : false  (keep the file for inspection after a clean finish)"
echo ""
echo "========================================"
echo " Next steps"
echo "========================================"
echo "  ./interrupt.sh   — start the pipeline, kill it after 2 s to simulate a crash"
echo "  ./verify.sh      — inspect the checkpoint token that was written"
echo "  ./resume.sh      — re-run; the engine resumes from the saved row offset"
echo ""
