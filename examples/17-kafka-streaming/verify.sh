#!/usr/bin/env bash
# verify.sh — Inspect the PostgreSQL database after the pipeline has run.
#
# Runs four queries against the sensor_readings table and prints the results:
#   1. Total row count (expect 16)
#   2. Row counts by location
#   3. Temperature range per sensor
#   4. The 5 most recently ingested rows (by ingested_at)
#
# Run AFTER:
#   ./setup.sh
#   ./produce.sh
#   flogo-datapipeline run kafka-streaming.datapipeline

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Base psql command targeting the postgres container.
PSQL=(docker compose exec -T postgres psql -U postgres -d pipelinedemo)

# ---------------------------------------------------------------------------
# Pre-flight
# ---------------------------------------------------------------------------
if ! docker compose exec -T postgres pg_isready -U postgres -d pipelinedemo >/dev/null 2>&1; then
  echo "ERROR: PostgreSQL is not reachable. Run ./setup.sh first."
  exit 1
fi

echo "========================================"
echo " Example 17 — Verify: Kafka → PostgreSQL"
echo "========================================"
echo ""
echo "  Database: pipelinedemo (table sensor_readings)"
echo ""

# ---------------------------------------------------------------------------
# Query 1 — Total rows
# ---------------------------------------------------------------------------
echo "---[ 1. Total rows in sensor_readings ]---"
"${PSQL[@]}" -c \
  "SELECT COUNT(*) AS total_rows FROM sensor_readings;"
echo ""
echo "  Expected: 16"
echo "  (20 messages published, 2 dropped by filter temperature>0,"
echo "   1 dropped by validate max:200, 1 dropped by validate notNull sensor_id)"
echo ""

# ---------------------------------------------------------------------------
# Query 2 — Rows by location
# ---------------------------------------------------------------------------
echo "---[ 2. Rows by location ]---"
"${PSQL[@]}" -c \
  "SELECT location, COUNT(*) AS row_count
   FROM sensor_readings
   GROUP BY location
   ORDER BY row_count DESC, location ASC;"
echo ""

# ---------------------------------------------------------------------------
# Query 3 — Temperature range per sensor
# ---------------------------------------------------------------------------
echo "---[ 3. Temperature stats per sensor ]---"
"${PSQL[@]}" -c \
  "SELECT
     sensor_id,
     location,
     COUNT(*)                           AS readings,
     ROUND(MIN(temperature)::numeric,1) AS min_temp,
     ROUND(MAX(temperature)::numeric,1) AS max_temp,
     ROUND(AVG(temperature)::numeric,2) AS avg_temp
   FROM sensor_readings
   GROUP BY sensor_id, location
   ORDER BY sensor_id ASC;"
echo ""

# ---------------------------------------------------------------------------
# Query 4 — 5 most recently ingested rows
# ---------------------------------------------------------------------------
echo "---[ 4. Latest 5 rows (by ingested_at) ]---"
"${PSQL[@]}" -c \
  "SELECT id, sensor_id, location, temperature, humidity, ts, ingested_at
   FROM sensor_readings
   ORDER BY ingested_at DESC, id DESC
   LIMIT 5;"
echo ""

# ---------------------------------------------------------------------------
# Confirmation
# ---------------------------------------------------------------------------
ROW_COUNT=$("${PSQL[@]}" -tAc "SELECT COUNT(*) FROM sensor_readings;")
echo "========================================"
if [ "$ROW_COUNT" -ge 16 ]; then
  echo " PASS — $ROW_COUNT rows in sensor_readings (expected >= 16)"
else
  echo " WARN — Only $ROW_COUNT rows found (expected 16)."
  echo "        Check that produce.sh ran and the pipeline completed."
fi
echo "========================================"
echo ""
