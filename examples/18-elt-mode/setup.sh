#!/usr/bin/env bash
# setup.sh — Creates (or resets) the SQLite analytics database for example 18.
# Run this once before executing the pipeline.
#
# What it does:
#   1. Removes any existing analytics.db
#   2. Creates a fresh analytics.db
#   3. Creates the raw_sales_staging table that the pipeline loads into
#
# The analytics tables (clean_sales, sales_by_region, etc.) are created by
# transform.sql AFTER the pipeline finishes loading — that is the ELT pattern.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_PATH="${SCRIPT_DIR}/data/analytics.db"

if ! command -v sqlite3 &>/dev/null; then
  echo "ERROR: sqlite3 not found on PATH."
  echo "       Install it (e.g. 'brew install sqlite', 'apt install sqlite3') and retry."
  exit 1
fi

echo "==> Setting up SQLite analytics database at: ${DB_PATH}"

# Remove any previous database so every run starts clean.
if [ -f "${DB_PATH}" ]; then
  echo "    Removing existing database..."
  rm -f "${DB_PATH}"
fi

# Create the staging table.
# This table receives raw, un-transformed rows directly from the CSV source.
# Column types mirror the source exactly — no normalisation happens here.
sqlite3 "${DB_PATH}" <<'SQL'
CREATE TABLE raw_sales_staging (
  id           TEXT,
  region       TEXT,
  amount       REAL,
  currency     TEXT,
  sale_date    TEXT,
  raw_category TEXT
);
SQL

echo "    Table 'raw_sales_staging' created (raw landing zone, no constraints)."
echo ""
echo "==> Setup complete."
echo ""
echo "    Next steps:"
echo "      1. Run the pipeline to load raw data:"
echo "         flogo-datapipeline run 18-elt-mode.datapipeline"
echo ""
echo "      2. Run the SQL transform to build analytics tables:"
echo "         ./post-transform.sh"
echo ""
echo "    Or run the full ELT sequence in one command:"
echo "      ./setup.sh && flogo-datapipeline run 18-elt-mode.datapipeline && ./post-transform.sh"
