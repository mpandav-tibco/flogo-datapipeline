#!/usr/bin/env bash
# setup.sh — Prepare the environment for the Kafka streaming example.
#
# What this script does:
#   1. Starts the local stack (Kafka + Zookeeper + PostgreSQL) via docker compose.
#   2. Waits for PostgreSQL to accept connections.
#   3. Creates the sensor_readings table in PostgreSQL (dropping any prior copy).
#   4. Prints a summary and next-step instructions.
#
# Prerequisites:
#   - Docker + Docker Compose (starts Kafka + Zookeeper + PostgreSQL)
#   - flogo-datapipeline CLI on PATH
#
# Run order:
#   1. ./setup.sh             — start the stack + create the Postgres target table
#   2. ./produce.sh           — publish 20 test messages to Kafka
#   3. flogo-datapipeline run kafka-streaming.datapipeline
#   4. ./verify.sh            — inspect the rows written to PostgreSQL

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo " Example 17 — Kafka Streaming Setup"
echo "========================================"
echo ""

# ---------------------------------------------------------------------------
# 1. Start the local stack (Kafka + Zookeeper + PostgreSQL)
# ---------------------------------------------------------------------------
echo "--> Starting Kafka + Zookeeper + PostgreSQL (docker compose up -d) ..."
docker compose up -d
echo "    OK"
echo ""

# ---------------------------------------------------------------------------
# 2. Wait for PostgreSQL to become ready
# ---------------------------------------------------------------------------
echo "--> Waiting for PostgreSQL to become ready ..."
until docker compose exec -T postgres pg_isready -U postgres -d pipelinedemo >/dev/null 2>&1; do
  sleep 1
done
echo "    PostgreSQL is ready."
echo ""

# ---------------------------------------------------------------------------
# 3. Drop and recreate the sensor_readings table
# ---------------------------------------------------------------------------
echo "--> Creating sensor_readings table in database pipelinedemo ..."

docker compose exec -T postgres psql -U postgres -d pipelinedemo <<'SQL'
-- sensor_readings: target table for the Kafka streaming pipeline.
--
-- Column notes:
--   sensor_id   — device identifier from the Kafka message JSON
--   location    — physical location tag (floor-1, roof, basement, etc.)
--   temperature — celsius float; filter drops <= 0, validate drops < -100 or > 200
--   humidity    — percentage float (informational, not validated in this example)
--   unit        — temperature unit label (always "C" in test data)
--   ts          — ISO-8601 timestamp from the producer (stored as TEXT)
--   ingested_at — wall-clock time the row was written by the pipeline (default NOW)
--
-- No natural unique key is enforced: onConflict:insert in the sink means
-- duplicate Kafka offsets (e.g. after a consumer restart) will produce
-- duplicate rows. Add a UNIQUE constraint on (sensor_id, ts) and switch to
-- onConflict:ignore or onConflict:upsert if idempotent loads are required.

DROP TABLE IF EXISTS sensor_readings;
CREATE TABLE sensor_readings (
  id          SERIAL  PRIMARY KEY,
  sensor_id   TEXT    NOT NULL,
  location    TEXT,
  temperature REAL    NOT NULL,
  humidity    REAL,
  unit        TEXT    DEFAULT 'C',
  ts          TEXT,
  ingested_at TEXT    DEFAULT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
);

-- Index for fast queries by sensor and time
CREATE INDEX idx_sr_sensor_ts ON sensor_readings (sensor_id, ts);
SQL

echo "    Created table: sensor_readings"
echo "    Created index: idx_sr_sensor_ts"
echo ""

# ---------------------------------------------------------------------------
# 4. Verify the schema
# ---------------------------------------------------------------------------
echo "--> Schema verification:"
docker compose exec -T postgres psql -U postgres -d pipelinedemo -c "\d sensor_readings" | sed 's/^/    /'
echo ""

# ---------------------------------------------------------------------------
# 5. Check Kafka broker status (informational only)
# ---------------------------------------------------------------------------
echo "--> Kafka broker status:"
if command -v docker &>/dev/null && docker compose ps 2>/dev/null | grep -q kafka; then
  KAFKA_STATE=$(docker compose ps kafka 2>/dev/null | awk 'NR==2 {print $4}' || echo "unknown")
  echo "    Kafka container state: $KAFKA_STATE"
  if echo "$KAFKA_STATE" | grep -qi "running\|healthy\|up"; then
    echo "    Broker is UP — you can run ./produce.sh"
  else
    echo "    Broker is starting — give it ~15 s, then run ./produce.sh"
  fi
else
  echo "    Docker not available or kafka container not started."
  echo "    Start the stack with: docker compose up -d"
fi
echo ""

# ---------------------------------------------------------------------------
# 6. Summary
# ---------------------------------------------------------------------------
echo "========================================"
echo " Setup complete"
echo "========================================"
echo ""
echo "  PostgreSQL      : postgres://postgres:postgres@localhost:5432/pipelinedemo"
echo "  Target table    : sensor_readings (0 rows)"
echo ""
echo "  Next steps:"
echo "    1. ./produce.sh"
echo "       # publishes 20 test messages to topic 'sensor-data'"
echo "       # (wait ~15 s after setup for the Kafka broker to become healthy)"
echo "    2. flogo-datapipeline run kafka-streaming.datapipeline"
echo "       # pipeline polls Kafka, filters, validates, writes to PostgreSQL"
echo "    3. ./verify.sh"
echo "       # confirms rows in the sensor_readings table"
echo ""
echo "  Expected final row count: 16 (4 messages dropped by transforms)"
echo ""
