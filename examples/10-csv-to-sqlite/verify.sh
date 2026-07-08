#!/usr/bin/env bash
# verify.sh — Queries the SQLite output database to confirm the pipeline ran correctly.
# Run this after executing the pipeline.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_PATH="${SCRIPT_DIR}/data/output.db"

if [ ! -f "${DB_PATH}" ]; then
  echo "ERROR: Database not found at ${DB_PATH}"
  echo "       Run setup.sh first, then execute the pipeline."
  exit 1
fi

echo "============================================================"
echo " Verification: customers_enriched (SQLite)"
echo "============================================================"

echo ""
echo "--- Row count by tier ---"
sqlite3 -column -header "${DB_PATH}" \
  "SELECT tier, COUNT(*) AS row_count FROM customers_enriched GROUP BY tier ORDER BY tier;"

echo ""
echo "--- Revenue stats ---"
sqlite3 -column -header "${DB_PATH}" \
  "SELECT COUNT(*) AS total_rows,
          ROUND(MIN(revenue_usd), 2) AS min_revenue,
          ROUND(MAX(revenue_usd), 2) AS max_revenue,
          ROUND(AVG(revenue_usd), 2) AS avg_revenue
   FROM customers_enriched;"

echo ""
echo "--- Country codes are upper-cased (sample) ---"
sqlite3 -column -header "${DB_PATH}" \
  "SELECT id, full_name, country, tier FROM customers_enriched LIMIT 10;"

echo ""
echo "--- PLATINUM customers ---"
sqlite3 -column -header "${DB_PATH}" \
  "SELECT id, full_name, email, revenue_usd FROM customers_enriched WHERE tier = 'PLATINUM';"

echo ""
echo "============================================================"
echo " Expected: 26 valid rows loaded (4 rows dropped by validate)"
echo "   Dropped: C011 (bad email), C023 (bad email),"
echo "            C027 (negative revenue), C028 (invalid tier DIAMOND)"
echo "============================================================"
