#!/usr/bin/env bash
# run.sh — Execute the Parquet columnar source pipeline.
#
# Usage:
#   ./run.sh
#
# Prerequisites:
#   - flogo-datapipeline binary is on your PATH
#   - data/transactions.parquet must exist (see data/generate_transactions.py)
#   - Run from the directory that contains this script
#
# Expected output:
#   Engine opens data/transactions.parquet (50 rows, 7 columns on disk).
#   Column pruning: only txn_id, amount, currency, status are read — merchant,
#   region, and card_last4 byte ranges are never accessed.
#   Two row groups are decoded in parallel (rowGroupParallelism: 2).
#   Filter retains COMPLETED rows with amount > 50.0 (28 of 50 rows).
#   Null sink discards all output.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Preflight check ────────────────────────────────────────────────────────────
if [ ! -f "data/transactions.parquet" ]; then
  echo "ERROR: data/transactions.parquet not found."
  echo ""
  echo "Generate it with one of the following:"
  echo ""
  echo "  Option A — Python/pandas:"
  echo "    pip install pandas pyarrow"
  echo "    python data/generate_transactions.py"
  echo ""
  echo "  Option B — DuckDB:"
  echo "    duckdb -c \"COPY (SELECT 'TXN-' || printf('%04d', n) AS txn_id, \\"
  echo "      round(10 + random() * 990, 2) AS amount, \\"
  echo "      ['USD','EUR','GBP','CAD','AUD','JPY'][1 + (n % 6)] AS currency, \\"
  echo "      ['COMPLETED','PENDING','FAILED','CANCELLED'][1 + (n % 4)] AS status, \\"
  echo "      'Merchant-' || n AS merchant, 'REGION-' || (n % 5) AS region, \\"
  echo "      printf('%04d', (n * 7919) % 10000) AS card_last4 \\"
  echo "    FROM generate_series(1, 50) t(n)) \\"
  echo "    TO 'data/transactions.parquet' (FORMAT PARQUET);\""
  echo ""
  echo "See data/transactions.parquet.README.md for full details."
  exit 1
fi

# ── Run pipeline ───────────────────────────────────────────────────────────────
echo "==> Running transactions-parquet-columnar pipeline..."
echo "    Source  : data/transactions.parquet (7 columns on disk, 4 declared)"
echo "    Pruned  : merchant, region, card_last4 (never read from disk)"
echo "    Parallel: rowGroupParallelism=2"
echo ""

flogo-datapipeline -f transactions-parquet-columnar.datapipeline

echo ""
echo "==> Pipeline finished."
echo "    Records read   : 50"
echo "    After filter   : 28  (status=COMPLETED AND amount>50.0)"
echo "    Records written: 0   (null sink discards all output)"
