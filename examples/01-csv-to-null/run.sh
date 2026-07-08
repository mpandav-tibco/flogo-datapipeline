#!/usr/bin/env bash
# run.sh — Execute the CSV → Null Hello World pipeline.
#
# Usage:
#   ./run.sh
#
# Prerequisites:
#   - flogo-datapipeline binary is on your PATH
#   - Run from the directory that contains this script
#
# Expected output:
#   Engine starts, reads 20 rows from data/employees.csv, writes 0 rows
#   (null sink discards everything), then exits cleanly.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Running employees-to-null pipeline..."
flogo-datapipeline -f employees-to-null.datapipeline

echo ""
echo "==> Pipeline finished. 20 rows read, 0 rows written (null sink)."
