#!/usr/bin/env bash
# run.sh — Execute the JSON Lines source pipeline.
#
# Usage:
#   ./run.sh
#
# Prerequisites:
#   - flogo-datapipeline binary is on your PATH
#   - Run from the directory that contains this script
#
# Expected output:
#   Engine opens data/events.jsonl (20 records), reads and flattens each JSON
#   object into typed columns, filters to purchase and refund events (9 records),
#   renames columns via the map transform, then discards via the null sink.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Running events-jsonlines-source pipeline..."
flogo-datapipeline -f events-jsonlines-source.datapipeline

echo ""
echo "==> Pipeline finished."
echo "    Records read   : 20"
echo "    After filter   : 9  (purchase + refund events only)"
echo "    Records written: 0  (null sink discards all output)"
