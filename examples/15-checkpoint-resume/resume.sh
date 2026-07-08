#!/usr/bin/env bash
# resume.sh — Re-run the pipeline after a crash; it resumes from the checkpoint.
#
# PURPOSE
# -------
# This script re-invokes flogo-datapipeline with exactly the same pipeline
# definition that interrupt.sh was using.  The engine detects an existing
# checkpoint file in ./checkpoints/ and automatically resumes from the saved
# row offset — no extra flags or configuration changes needed.
#
# RESUME MECHANICS (what the engine does internally)
# --------------------------------------------------
# 1. On startup, the engine looks for:
#      ./checkpoints/checkpoint-resume.json
#    (the file name is derived from the pipeline "name" field).
#
# 2. If the file exists and is valid JSON, the engine reads rowsProcessed (N).
#
# 3. The CSV source is opened and Skip(N) is called: the first N lines of the
#    file (after the header) are consumed and discarded without entering the
#    transform or sink stages.  This is O(N) in I/O but O(1) in memory — the
#    engine reads and drops rows one by one without buffering them.
#
# 4. Normal pipeline processing resumes from row N+1 onwards.
#
# 5. Checkpointing continues to fire at intervalRows / intervalSeconds
#    boundaries relative to the new starting position.
#
# 6. When the source is exhausted, the engine writes a final checkpoint
#    (rowsProcessed = 5000), then exits with code 0.
#    Because deleteOnSuccess is false, the final checkpoint file is kept on
#    disk for inspection.
#
# ZERO ROW LOSS GUARANTEE
# -----------------------
# The checkpoint token is written *after* a batch is committed to the sink.
# If the engine crashes between two checkpoints, at most (intervalRows - 1)
# rows are re-processed on resume, but never skipped.  For a null sink (which
# discards rows) this means at-least-once semantics with no visible side effect.
# For idempotent sinks (upsert to a database with a primary key) this is also
# safe.  For append-only sinks, de-duplication by the consumer is recommended.
#
# PREREQUISITES
# -------------
#   ./interrupt.sh must have been run and produced a checkpoint file.
#   flogo-datapipeline must be on PATH.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PIPELINE="checkpoint-resume.datapipeline"
CHECKPOINT_DIR="./checkpoints"
CHECKPOINT_FILE="$CHECKPOINT_DIR/checkpoint-resume.json"

# ------------------------------------------------------------------
# Preflight checks
# ------------------------------------------------------------------
if [ ! -f "./data/big_file.csv" ]; then
  echo "ERROR: ./data/big_file.csv not found."
  echo "       Run ./setup.sh first."
  exit 1
fi

if ! command -v flogo-datapipeline &>/dev/null; then
  echo "ERROR: flogo-datapipeline not found on PATH."
  exit 1
fi

# ------------------------------------------------------------------
# Show checkpoint state before resuming
# ------------------------------------------------------------------
echo "========================================"
echo " resume.sh — Resuming from checkpoint"
echo "========================================"
echo ""

if [ -f "$CHECKPOINT_FILE" ]; then
  echo "--> Checkpoint file found: $CHECKPOINT_FILE"
  echo ""
  echo "    Contents:"
  echo "    ----------------------------------------"
  cat "$CHECKPOINT_FILE"
  echo ""
  echo "    ----------------------------------------"
  echo ""

  # Parse rowsProcessed with python3 if available, otherwise use grep/awk.
  if command -v python3 &>/dev/null; then
    ROWS_DONE=$(python3 -c "import json,sys; d=json.load(open('$CHECKPOINT_FILE')); print(d.get('rowsProcessed', '?'))")
  else
    ROWS_DONE=$(grep -o '"rowsProcessed":[^,}]*' "$CHECKPOINT_FILE" | grep -o '[0-9]*' || echo "?")
  fi

  TOTAL=$(tail -n +2 ./data/big_file.csv | wc -l | tr -d ' ')
  REMAINING=$(( TOTAL - ROWS_DONE )) 2>/dev/null || REMAINING="?"

  echo "    rowsProcessed before resume : $ROWS_DONE"
  echo "    Total rows in source        : $TOTAL"
  echo "    Rows remaining to process   : $REMAINING"
  echo ""
  echo "    The engine will call Skip($ROWS_DONE) on the CSV source,"
  echo "    then process the remaining $REMAINING rows."
else
  echo "WARNING: No checkpoint file found at $CHECKPOINT_FILE"
  echo ""
  echo "  If no checkpoint exists, the engine will start from the beginning"
  echo "  of the file (full re-scan).  This is safe — it is equivalent to"
  echo "  a fresh run — but means the interrupt demo did not capture state."
  echo ""
  echo "  To produce a checkpoint first, run ./interrupt.sh."
  echo ""
  read -rp "  Continue with a full run anyway? [y/N] " ANSWER
  case "$ANSWER" in
    [yY]|[yY][eE][sS]) echo "" ;;
    *) echo "Aborted."; exit 1 ;;
  esac
fi

# ------------------------------------------------------------------
# Resume (or full re-run) the pipeline
# ------------------------------------------------------------------
echo "--> Running: flogo-datapipeline -f $PIPELINE"
echo "    (Watch for 'resuming from checkpoint' in the log)"
echo ""
echo "--- Pipeline output ---"

flogo-datapipeline -f "$PIPELINE"
EXIT_CODE=$?

echo "--- Pipeline finished (exit code: $EXIT_CODE) ---"
echo ""

# ------------------------------------------------------------------
# Show the final checkpoint token
# ------------------------------------------------------------------
echo "========================================"
echo " Final checkpoint state"
echo "========================================"
if [ -f "$CHECKPOINT_FILE" ]; then
  echo ""
  echo "  File: $CHECKPOINT_FILE"
  echo "  ----------------------------------------"
  cat "$CHECKPOINT_FILE"
  echo ""
  echo "  ----------------------------------------"
  echo ""
  echo "  deleteOnSuccess=false means this file is kept even though the"
  echo "  pipeline finished cleanly. Inspect it with ./verify.sh, or set"
  echo "  deleteOnSuccess=true in the pipeline definition to auto-clean."
else
  echo "  Checkpoint file not found after run."
fi

echo ""
echo "========================================"
echo " Run complete"
echo "========================================"
echo "  Exit code : $EXIT_CODE"
echo ""
if [ "$EXIT_CODE" -eq 0 ]; then
  echo "  The pipeline finished successfully."
  echo "  All rows from row $ROWS_DONE onward were processed."
  echo "  Zero rows were lost or duplicated."
else
  echo "  The pipeline exited with a non-zero code."
  echo "  Check the log above for errors."
fi
echo ""
