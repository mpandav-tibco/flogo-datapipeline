#!/usr/bin/env bash
# setup.sh — Prepare the environment for the Kafka streaming example.
#
# What this script does:
#   1. Creates ./data/ directory.
#   2. Removes any existing sensors.db so the demo starts clean.
#   3. Creates ./data/sensors.db with the sensor_readings table.
#   4. Prints a summary and next-step instructions.
#
# Prerequisites:
#   - sqlite3 CLI on PATH
#   - Docker + docker compose (to start Kafka via docker-compose.yml)
#   - flogo-datapipeline CLI on PATH
#
# Run order:
#   1. docker compose up -d   — start Kafka + Zookeeper
#   2. ./setup.sh             — create the SQLite target database
#   3. ./produce.sh           — publish 20 test messages to Kafka
#   4. flogo-datapipeline run kafka-streaming.datapipeline
#   5. ./verify.sh            — inspect the rows written to SQLite

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DATA_DIR="./data"
DB_FILE="$DATA_DIR/sensors.db"

echo "========================================"
echo " Example 17 — Kafka Streaming Setup"
echo "========================================"
echo ""

# ---------------------------------------------------------------------------
# 1. Create data directory
# ---------------------------------------------------------------------------
echo "--> Creating $DATA_DIR/ ..."
mkdir -p "$DATA_DIR"
echo "    OK"
echo ""

# ---------------------------------------------------------------------------
# 2. Check for sqlite3
# ---------------------------------------------------------------------------
if ! command -v sqlite3 &>/dev/null; then
  echo "ERROR: sqlite3 not found on PATH."
  echo ""
  echo "  Install on macOS  : brew install sqlite"
  echo "  Install on Ubuntu : apt-get install sqlite3"
  echo "  Install on Windows: winget install SQLite.SQLite"
  echo ""
  exit 1
fi
SQLITE_VERSION=$(sqlite3 --version | awk '{print $1}')
echo "--> sqlite3 $SQLITE_VERSION found"
echo ""

# ---------------------------------------------------------------------------
# 3. Drop and recreate sensors.db
# ---------------------------------------------------------------------------
if [ -f "$DB_FILE" ]; then
  echo "--> Removing existing $DB_FILE ..."
  rm -f "$DB_FILE"
  echo "    Done."
  echo ""
fi

echo "--> Creating $DB_FILE with sensor_readings table ..."

sqlite3 "$DB_FILE" <<'SQL'
-- sensor_readings: target table for the Kafka streaming pipeline.
--
-- Column notes:
--   sensor_id   — device identifier from the Kafka message JSON
--   location    — physical location tag (floor-1, roof, basement, etc.)
--   temperature — celsius float; filter drops <= 0, validate drops < -100 or > 200
--   humidity    — percentage float (informational, not validated in this example)
--   unit        — temperature unit label (always "C" in test data)
--   ts          — ISO-8601 timestamp from the producer (stored as TEXT in SQLite)
--   ingested_at — wall-clock time the row was written by the pipeline (default NOW)
--
-- No PRIMARY KEY constraint: onConflict:insert in the sink means duplicate
-- Kafka offsets (e.g. after a consumer restart) will produce duplicate rows.
-- Add a PRIMARY KEY on (sensor_id, ts) and switch to onConflict:ignore or
-- onConflict:upsert if idempotent loads are required.

CREATE TABLE IF NOT EXISTS sensor_readings (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  sensor_id   TEXT    NOT NULL,
  location    TEXT,
  temperature REAL    NOT NULL,
  humidity    REAL,
  unit        TEXT    DEFAULT 'C',
  ts          TEXT,
  ingested_at TEXT    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

-- Index for fast queries by sensor and time
CREATE INDEX IF NOT EXISTS idx_sr_sensor_ts ON sensor_readings (sensor_id, ts);
SQL

echo "    Created table: sensor_readings"
echo "    Created index: idx_sr_sensor_ts"
echo ""

# ---------------------------------------------------------------------------
# 4. Verify the schema
# ---------------------------------------------------------------------------
echo "--> Schema verification:"
sqlite3 "$DB_FILE" ".schema sensor_readings" | sed 's/^/    /'
echo ""

# ---------------------------------------------------------------------------
# 5. Check Docker / Kafka status (informational only)
# ---------------------------------------------------------------------------
echo "--> Kafka broker status:"
if command -v docker &>/dev/null && docker compose ps 2>/dev/null | grep -q kafka; then
  KAFKA_STATE=$(docker compose ps kafka 2>/dev/null | awk 'NR==2 {print $4}' || echo "unknown")
  echo "    Kafka container state: $KAFKA_STATE"
  if echo "$KAFKA_STATE" | grep -qi "running\|healthy\|up"; then
    echo "    Broker is UP — you can run ./produce.sh"
  else
    echo "    Broker is NOT yet running — start it with: docker compose up -d"
  fi
else
  echo "    Docker not available or kafka container not started."
  echo "    Start Kafka with: docker compose up -d"
fi
echo ""

# ---------------------------------------------------------------------------
# 6. Summary
# ---------------------------------------------------------------------------
echo "========================================"
echo " Setup complete"
echo "========================================"
echo ""
echo "  SQLite database : $DB_FILE"
echo "  Target table    : sensor_readings (0 rows)"
echo ""
echo "  Next steps:"
echo "    1. docker compose up -d"
echo "       # wait ~15 s for the broker to become healthy"
echo "    2. ./produce.sh"
echo "       # publishes 20 test messages to topic 'sensor-data'"
echo "    3. flogo-datapipeline run kafka-streaming.datapipeline"
echo "       # pipeline polls Kafka, filters, validates, writes to SQLite"
echo "    4. ./verify.sh"
echo "       # confirms rows in sensors.db"
echo ""
echo "  Expected final row count: 16 (4 messages dropped by transforms)"
echo ""
