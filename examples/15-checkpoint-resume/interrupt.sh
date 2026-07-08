#!/usr/bin/env bash
# interrupt.sh — Start the pipeline in the background and kill it after 2 seconds.
#
# PURPOSE
# -------
# This script simulates an unexpected process termination mid-run: a power cut,
# OOM kill, Kubernetes pod eviction, or operator Ctrl-C on a long-running job.
#
# The pipeline reads 5000 rows with a batchSize of 500 and writes a checkpoint
# token every 1000 rows.  After ~2 seconds the engine will typically have
# completed at least one checkpoint interval, meaning the ./checkpoints/ directory
# will contain a JSON token recording how many rows were successfully committed.
#
# WHAT HAPPENS INSIDE THE ENGINE
# --------------------------------
# 1. Engine opens big_file.csv, begins reading batches of 500 rows.
# 2. After every 1000 rows read (intervalRows) OR 10 elapsed seconds
#    (intervalSeconds), the engine atomically writes a checkpoint token:
#
#      ./checkpoints/checkpoint-resume.json
#      {
#        "pipeline": "checkpoint-resume",
#        "rowsProcessed": <N>,
#        "timestamp": "<ISO-8601>",
#        "source": "csv",
#        "sourcePath": "./data/big_file.csv"
#      }
#
# 3. SIGTERM (sent by this script) causes the engine to flush the in-progress
#    batch, write one final checkpoint token, and then exit non-zero.
#
# AFTER THE KILL
# ---------------
# Run ./verify.sh to see the checkpoint token, then ./resume.sh to continue.
# The engine will Skip(rowsProcessed) rows at source open time and process
# only the remaining rows — zero row loss, zero row duplication.
#
# PREREQUISITES
# -------------
#   ./setup.sh must have been run first (data/big_file.csv must exist).
#   flogo-datapipeline must be on PATH.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PIPELINE="checkpoint-resume.datapipeline"
CSV_FILE="./data/big_file.csv"
CHECKPOINT_DIR="./checkpoints"
KILL_AFTER=2   # seconds to wait before sending SIGTERM

# ------------------------------------------------------------------
# Preflight checks
# ------------------------------------------------------------------
if [ ! -f "$CSV_FILE" ]; then
  echo "ERROR: $CSV_FILE not found."
  echo "       Run ./setup.sh first to generate the sample data."
  exit 1
fi

if ! command -v flogo-datapipeline &>/dev/null; then
  echo "ERROR: flogo-datapipeline not found on PATH."
  echo "       Build or install it, then re-run this script."
  exit 1
fi

# ------------------------------------------------------------------
# Show starting state
# ------------------------------------------------------------------
echo "========================================"
echo " interrupt.sh — Simulated crash demo"
echo "========================================"
echo ""
echo "  Pipeline  : $PIPELINE"
echo "  Source    : $CSV_FILE  ($(tail -n +2 "$CSV_FILE" | wc -l | tr -d ' ') rows)"
echo "  Checkpoints dir: $CHECKPOINT_DIR"
echo ""

EXISTING=$(find "$CHECKPOINT_DIR" -maxdepth 1 -name "*.json" 2>/dev/null | wc -l | tr -d ' ')
if [ "$EXISTING" -gt 0 ]; then
  echo "  WARNING: A checkpoint file already exists from a previous run:"
  for f in "$CHECKPOINT_DIR"/*.json; do
    echo "    $f"
    cat "$f"
    echo ""
  done
  echo "  The resume demo will use this file. Run ./setup.sh to start fresh."
  echo ""
fi

# ------------------------------------------------------------------
# Start the pipeline in the background
# ------------------------------------------------------------------
echo "--> Starting pipeline in background..."
echo "    (will send SIGTERM in $KILL_AFTER seconds)"
echo ""

flogo-datapipeline -f "$PIPELINE" &
PIPELINE_PID=$!
echo "    PID: $PIPELINE_PID"
echo ""

# ------------------------------------------------------------------
# Wait, then kill
# ------------------------------------------------------------------
echo "--> Sleeping $KILL_AFTER second(s)..."
sleep "$KILL_AFTER"

if kill -0 "$PIPELINE_PID" 2>/dev/null; then
  echo ""
  echo "--> Sending SIGTERM to PID $PIPELINE_PID (simulating crash)..."
  kill -TERM "$PIPELINE_PID"

  # Give the engine a moment to flush its in-progress checkpoint.
  sleep 1

  # Wait for the process to fully exit.
  wait "$PIPELINE_PID" 2>/dev/null || true
  echo "    Process exited."
else
  echo ""
  echo "    NOTE: Pipeline already exited before the kill signal."
  echo "    (The dataset may be too small or the machine too fast.)"
  echo "    A checkpoint was still written if intervalRows was reached."
fi

echo ""

# ------------------------------------------------------------------
# Show the checkpoint that was written
# ------------------------------------------------------------------
echo "========================================"
echo " Checkpoint file(s) written:"
echo "========================================"
WRITTEN=$(find "$CHECKPOINT_DIR" -maxdepth 1 -name "*.json" 2>/dev/null | wc -l | tr -d ' ')
if [ "$WRITTEN" -gt 0 ]; then
  for f in "$CHECKPOINT_DIR"/*.json; do
    echo ""
    echo "  File: $f"
    echo "  ----------------------------------------"
    cat "$f"
    echo ""
  done
else
  echo ""
  echo "  No checkpoint file found."
  echo "  The pipeline was killed before it could write its first checkpoint."
  echo "  Try increasing KILL_AFTER (currently $KILL_AFTER s) and re-run:"
  echo "    KILL_AFTER=5 ./interrupt.sh"
  echo ""
  echo "  Alternatively, increase batchSize or reduce the dataset size so"
  echo "  more rows are processed per second and a checkpoint is reached faster."
fi

echo ""
echo "========================================"
echo " Next steps"
echo "========================================"
echo "  ./verify.sh   — inspect the checkpoint token in detail"
echo "  ./resume.sh   — re-run the pipeline; it resumes from the saved offset"
echo ""
