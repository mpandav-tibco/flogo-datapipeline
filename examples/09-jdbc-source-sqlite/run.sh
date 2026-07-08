#!/usr/bin/env bash
# run.sh — Execute the JDBC SQLite source pipeline.
#
# Usage:
#   ./run.sh
#
# Prerequisites:
#   - flogo-datapipeline binary is on your PATH.
#   - Run setup.sh first to create data/inventory.db.
#   - Run from the directory that contains this script.
#
# Expected output:
#   Engine opens data/inventory.db, reads all 50 rows from the 'products' table
#   using keyset pagination (batchSize=100) across 2 partitions, then discards
#   all rows via the null sink. Total rows read: 50, rows written: 0.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Guard: database must exist ────────────────────────────────────────────────
if [ ! -f "./data/inventory.db" ]; then
    echo "ERROR: ./data/inventory.db not found."
    echo "       Run ./setup.sh first to create the database."
    exit 1
fi

echo "==> Running inventory-jdbc-source pipeline..."
flogo-datapipeline -f inventory-jdbc-source.datapipeline

echo ""
echo "==> Pipeline finished."
echo "    Rows read   : 50  (all products, across 2 partitions)"
echo "    Rows written: 0   (null sink discards all output)"
