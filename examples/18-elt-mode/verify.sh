#!/usr/bin/env bash
# verify.sh — Spot-check the analytics database after the full ELT run.
# Run this after setup.sh, the pipeline, AND post-transform.sh have all completed.
#
# Queries performed:
#   1. Staging row count (should be 50 — the full CSV, no drops)
#   2. clean_sales category normalisation check
#   3. clean_sales region normalisation check
#   4. Currency conversion spot-check (JPY amounts in USD)
#   5. Top earners by region
#   6. Daily revenue summary

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_PATH="${SCRIPT_DIR}/data/analytics.db"

if [ ! -f "${DB_PATH}" ]; then
  echo "ERROR: Database not found at ${DB_PATH}"
  echo "       Run: ./setup.sh && flogo-datapipeline run 18-elt-mode.datapipeline && ./post-transform.sh"
  exit 1
fi

# Check that analytics tables exist (i.e. post-transform.sh was run).
TABLE_COUNT=$(sqlite3 "${DB_PATH}" \
  "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='clean_sales';" 2>/dev/null || echo "0")
if [ "${TABLE_COUNT}" -eq 0 ]; then
  echo "ERROR: clean_sales table not found — post-transform.sh has not been run yet."
  echo "       Run: ./post-transform.sh"
  exit 1
fi

echo "============================================================"
echo " Verification: ELT Mode — example 18"
echo " Database: ${DB_PATH}"
echo "============================================================"

echo ""
echo "--- [1] Staging table row count (expect: 50, no rows dropped) ---"
sqlite3 -column -header "${DB_PATH}" \
  "SELECT COUNT(*) AS staged_rows FROM raw_sales_staging;"

echo ""
echo "--- [2] Category normalisation — distinct categories in clean_sales ---"
echo "        (expect: exactly 3 categories: Electronics, Clothing, Food & Beverage)"
sqlite3 -column -header "${DB_PATH}" \
  "SELECT category, COUNT(*) AS rows
   FROM clean_sales
   GROUP BY category
   ORDER BY category;"

echo ""
echo "--- [3] Region normalisation — distinct regions in clean_sales ---"
echo "        (expect: exactly 3 regions: AMER, APAC, EMEA — all upper case)"
sqlite3 -column -header "${DB_PATH}" \
  "SELECT region, COUNT(*) AS rows
   FROM clean_sales
   GROUP BY region
   ORDER BY region;"

echo ""
echo "--- [4] Currency conversion spot-check ---"
echo "        (S003: 42000 JPY * 0.0067 = 281.40 USD)"
echo "        (S005: 210.75 GBP * 1.27 = 267.65 USD)"
sqlite3 -column -header "${DB_PATH}" \
  "SELECT id, raw_currency, raw_amount, amount_usd
   FROM clean_sales
   WHERE id IN ('S003','S005','S010','S021')
   ORDER BY id;"

echo ""
echo "--- [5] Top deal per region ---"
sqlite3 -column -header "${DB_PATH}" \
  "SELECT c.region, c.id, c.category, c.raw_currency, c.raw_amount, c.amount_usd
   FROM clean_sales c
   INNER JOIN (
     SELECT region, MAX(amount_usd) AS max_usd
     FROM clean_sales GROUP BY region
   ) m ON c.region = m.region AND c.amount_usd = m.max_usd
   ORDER BY c.region;"

echo ""
echo "--- [6] sales_by_region summary ---"
sqlite3 -column -header "${DB_PATH}" \
  "SELECT region, deal_count, total_revenue_usd, avg_deal_usd
   FROM sales_by_region;"

echo ""
echo "--- [7] sales_by_category summary ---"
sqlite3 -column -header "${DB_PATH}" \
  "SELECT category, deal_count, total_revenue_usd, avg_deal_usd
   FROM sales_by_category;"

echo ""
echo "--- [8] Daily revenue (first 5 and last 5 days) ---"
sqlite3 -column -header "${DB_PATH}" \
  "SELECT sale_date, deals, revenue_usd FROM daily_revenue LIMIT 5;"
echo "    ..."
sqlite3 -column -header "${DB_PATH}" \
  "SELECT sale_date, deals, revenue_usd FROM daily_revenue ORDER BY sale_date DESC LIMIT 5;"

echo ""
echo "============================================================"
echo " ELT verification complete."
echo " Key assertions:"
echo "   - 50 rows in raw_sales_staging (all loaded, no pipeline transforms)"
echo "   - 50 rows in clean_sales (SQL normalisation, no drops)"
echo "   - 3 canonical categories in clean_sales"
echo "   - 3 canonical regions (all upper case) in clean_sales"
echo "   - Currency conversion to USD applied in SQL"
echo "============================================================"
