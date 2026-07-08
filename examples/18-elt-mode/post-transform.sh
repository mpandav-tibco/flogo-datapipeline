#!/usr/bin/env bash
# post-transform.sh — Apply the SQL transformation layer after the ELT pipeline load.
#
# Usage:
#   ./post-transform.sh
#
# What this script does:
#   1. Verifies that the pipeline has already loaded data into analytics.db
#   2. Runs transform.sql inside SQLite to build clean_sales and all
#      aggregate/reporting tables from raw_sales_staging
#   3. Prints a summary of every output table so you can confirm the results
#
# In a production ELT workflow this step is typically executed by a scheduler
# (Airflow, dbt Cloud, TIBCO Flogo trigger) immediately after the pipeline
# finishes loading.  For dbt users: replace this script with `dbt run`.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_PATH="${SCRIPT_DIR}/data/analytics.db"
SQL_PATH="${SCRIPT_DIR}/transform.sql"

# ── Preflight checks ─────────────────────────────────────────────────────────

if ! command -v sqlite3 &>/dev/null; then
  echo "ERROR: sqlite3 not found on PATH."
  echo "       Install it (e.g. 'brew install sqlite', 'apt install sqlite3') and retry."
  exit 1
fi

if [ ! -f "${DB_PATH}" ]; then
  echo "ERROR: Database not found at ${DB_PATH}"
  echo "       Run setup.sh first, then execute the pipeline:"
  echo "         ./setup.sh"
  echo "         flogo-datapipeline run 18-elt-mode.datapipeline"
  exit 1
fi

if [ ! -f "${SQL_PATH}" ]; then
  echo "ERROR: transform.sql not found at ${SQL_PATH}"
  exit 1
fi

# Confirm the staging table has rows before proceeding.
STAGED_ROWS=$(sqlite3 "${DB_PATH}" "SELECT COUNT(*) FROM raw_sales_staging;" 2>/dev/null || echo "0")
if [ "${STAGED_ROWS}" -eq 0 ]; then
  echo "ERROR: raw_sales_staging is empty."
  echo "       Run the pipeline first:"
  echo "         flogo-datapipeline run 18-elt-mode.datapipeline"
  exit 1
fi

echo "============================================================"
echo " ELT Post-Load Transform"
echo " Database : ${DB_PATH}"
echo " SQL file : ${SQL_PATH}"
echo " Staged rows in raw_sales_staging: ${STAGED_ROWS}"
echo "============================================================"
echo ""
echo "==> Running transform.sql ..."
echo ""

sqlite3 "${DB_PATH}" < "${SQL_PATH}"

echo ""
echo "============================================================"
echo " Transform complete. Querying output tables ..."
echo "============================================================"

echo ""
echo "--- clean_sales (first 10 rows) ---"
sqlite3 -column -header "${DB_PATH}" \
  "SELECT id, region, category, raw_currency, raw_amount, amount_usd, sale_date
   FROM clean_sales
   ORDER BY sale_date, id
   LIMIT 10;"

echo ""
echo "--- sales_by_region (all regions, ordered by revenue) ---"
sqlite3 -column -header "${DB_PATH}" \
  "SELECT region, deal_count, total_revenue_usd, avg_deal_usd, min_deal_usd, max_deal_usd
   FROM sales_by_region;"

echo ""
echo "--- sales_by_category (all categories) ---"
sqlite3 -column -header "${DB_PATH}" \
  "SELECT category, deal_count, total_revenue_usd, avg_deal_usd
   FROM sales_by_category;"

echo ""
echo "--- sales_by_region_category (cross-tab) ---"
sqlite3 -column -header "${DB_PATH}" \
  "SELECT region, category, deal_count, total_revenue_usd
   FROM sales_by_region_category
   ORDER BY region, total_revenue_usd DESC;"

echo ""
echo "--- daily_revenue (time series) ---"
sqlite3 -column -header "${DB_PATH}" \
  "SELECT sale_date, deals, revenue_usd
   FROM daily_revenue;"

echo ""
echo "============================================================"
echo " Verification summary"
echo "============================================================"
sqlite3 "${DB_PATH}" \
  "SELECT 'raw_sales_staging' AS table_name, COUNT(*) AS rows FROM raw_sales_staging
   UNION ALL
   SELECT 'clean_sales',            COUNT(*) FROM clean_sales
   UNION ALL
   SELECT 'sales_by_region',        COUNT(*) FROM sales_by_region
   UNION ALL
   SELECT 'sales_by_category',      COUNT(*) FROM sales_by_category
   UNION ALL
   SELECT 'sales_by_region_category', COUNT(*) FROM sales_by_region_category
   UNION ALL
   SELECT 'daily_revenue',          COUNT(*) FROM daily_revenue;" \
  | column -t -s '|'

echo ""
echo "==> Post-transform complete."
echo "    Query analytics.db directly for ad-hoc analysis:"
echo "      sqlite3 ${DB_PATH}"
