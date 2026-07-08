#!/usr/bin/env bash
# run.sh — Run the Prometheus metrics demo pipeline.
#
# This script starts the pipeline in the background, waits briefly for the
# HTTP server to bind, then scrapes the /metrics endpoint twice (before the
# pipeline finishes and once more after) so you can see per-batch counters
# and the rows_per_second gauge.
#
# Usage:
#   chmod +x run.sh
#   ./run.sh
#
# Prerequisites:
#   - flogo-datapipeline binary on your PATH
#   - curl on your PATH (standard on macOS/Linux; Windows users: use Git Bash)
#   - Run from the 14-prometheus-metrics/ directory

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PIPELINE_FILE="prometheus-metrics.datapipeline"
METRICS_URL="http://localhost:2112/metrics"
LOG_FILE="pipeline.log"

echo "============================================================"
echo " Example 14 — Prometheus Metrics & Observability"
echo "============================================================"
echo ""
echo "Starting pipeline in background..."
echo "Logs will stream to: $LOG_FILE"
echo ""

# Start the pipeline in the background; capture PID
flogo-datapipeline -f "$PIPELINE_FILE" > "$LOG_FILE" 2>&1 &
PIPELINE_PID=$!

echo "Pipeline PID: $PIPELINE_PID"
echo ""

# Give the HTTP server a moment to bind
sleep 1

echo "------------------------------------------------------------"
echo " Live scrape #1 — pipeline still processing batches"
echo "------------------------------------------------------------"
echo ""
if curl -sf "$METRICS_URL" | grep -E "^flogo_pipeline"; then
    echo ""
else
    echo "(Metrics endpoint not yet up — pipeline may have finished very fast or failed to bind)"
    echo "Check $LOG_FILE for details."
fi

# Wait for the pipeline to finish
echo ""
echo "Waiting for pipeline to complete..."
wait "$PIPELINE_PID" || true

echo ""
echo "------------------------------------------------------------"
echo " Final scrape #2 — pipeline complete; cumulative totals"
echo "------------------------------------------------------------"
echo ""
if curl -sf "$METRICS_URL" | grep -E "^flogo_pipeline"; then
    echo ""
else
    echo "(Metrics endpoint closed — the pipeline has exited.)"
    echo "Tip: set a longer 'shutdownDelayMs' in the metrics config to keep the"
    echo "     endpoint alive after the pipeline finishes for a final scrape."
fi

echo "============================================================"
echo " Pipeline finished. Check pipeline.log for full engine output."
echo ""
echo " Key metrics to look for:"
echo "   flogo_pipeline_rows_read_total        — 500 (all rows from CSV)"
echo "   flogo_pipeline_rows_written_total      — 0   (null sink)"
echo "   flogo_pipeline_rows_filtered_total     — ~96 (value <= 0)"
echo "   flogo_pipeline_batch_duration_ms       — histogram of batch latency"
echo "   flogo_pipeline_rows_per_second         — throughput gauge"
echo "============================================================"
echo ""
echo "Tip: run the pipeline manually and keep the terminal open to watch"
echo "     metrics in real time from a second terminal:"
echo ""
echo "  Terminal 1: flogo-datapipeline -f $PIPELINE_FILE"
echo "  Terminal 2: watch -n1 'curl -s $METRICS_URL | grep flogo_pipeline'"
echo ""
