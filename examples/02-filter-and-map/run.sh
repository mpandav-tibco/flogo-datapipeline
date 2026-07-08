#!/usr/bin/env bash
# run.sh — Execute the Filter & Map pipeline.
#
# Usage:
#   ./run.sh [--verbose]
#
# Prerequisites:
#   - flogo-datapipeline binary is on your PATH  (or built via: go build ./cmd/flogo-datapipeline)
#   - Run from the directory that contains this script
#
# Expected output:
#   Engine reads 30 rows from data/sales.csv.
#   Filter drops 11 rows (7 CANCELLED + 4 with amount <= 100).
#   Map reshapes the 19 surviving rows: region uppercased, amount converted
#   from cents to dollars.
#   Null sink discards all output rows; pipeline exits cleanly.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VERBOSE="${1:-}"

echo "==> Running filter-and-map pipeline..."
echo "    Source : data/sales.csv  (30 rows)"
echo "    Filter : amount > 100 AND status != 'CANCELLED'"
echo "    Map    : region → upper(), amount ÷ 100, rename fields"
echo "    Sink   : null (output discarded)"
echo ""

if [[ "$VERBOSE" == "--verbose" ]]; then
  flogo-datapipeline -f filter-and-map.datapipeline -v
else
  flogo-datapipeline -f filter-and-map.datapipeline
fi

echo ""
echo "==> Pipeline finished."
echo "    Rows read    : 30"
echo "    Rows dropped : 11  (7 CANCELLED, 4 with amount <= 100)"
echo "    Rows output  : 19"
