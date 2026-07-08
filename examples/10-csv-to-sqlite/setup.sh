#!/usr/bin/env bash
# setup.sh — Creates (or resets) the SQLite target database for example 10.
# Run this once before executing the pipeline.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_PATH="${SCRIPT_DIR}/data/output.db"

echo "==> Setting up SQLite database at: ${DB_PATH}"

# Remove any previous database so every run starts clean.
if [ -f "${DB_PATH}" ]; then
  echo "    Removing existing database..."
  rm -f "${DB_PATH}"
fi

# Create the target table.
sqlite3 "${DB_PATH}" <<'SQL'
CREATE TABLE customers_enriched (
  id          TEXT PRIMARY KEY,
  full_name   TEXT NOT NULL,
  email       TEXT NOT NULL,
  country     TEXT NOT NULL,
  revenue_usd REAL NOT NULL,
  tier        TEXT NOT NULL
);
SQL

echo "    Table 'customers_enriched' created."
echo ""
echo "==> Setup complete. Run the pipeline with:"
echo "    flogo-datapipeline run 10-csv-to-sqlite.datapipeline"
