#!/usr/bin/env bash
# produce.sh — Publish 20 sensor JSON messages to the "sensor-data" Kafka topic.
#
# What this script does:
#   1. Checks that the Kafka broker is reachable (localhost:9092).
#   2. Detects the best available producer tool (kafka-console-producer or
#      kafkacat/kcat) and uses it to send messages.
#   3. Publishes 20 messages total:
#        - 16 valid sensor readings (temperature 10–95 °C, humidity 30–85 %)
#        -  2 with temperature = 0   (will be dropped by the filter transform)
#        -  1 with temperature > 200 (will be dropped by the validate transform)
#        -  1 with no sensor_id      (will be dropped by the validate transform)
#
# Prerequisites (any one of):
#   Option A) kafka-console-producer on PATH
#             (ships with Apache Kafka / Confluent Platform)
#   Option B) kcat (formerly kafkacat) on PATH — brew install kcat
#             https://github.com/edenhill/kcat
#
# If neither tool is available the script prints a manual curl/docker
# alternative and exits cleanly.
#
# Run order:
#   1. ./setup.sh             (start the stack + create Postgres target)
#   2. ./produce.sh           (inject messages)
#   3. flogo-datapipeline run kafka-streaming.datapipeline

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

BROKER="localhost:9092"
TOPIC="sensor-data"

# ---------------------------------------------------------------------------
# ANSI colours
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GRN='\033[0;32m'
YLW='\033[1;33m'
NC='\033[0m'

echo "========================================"
echo " Example 17 — Kafka Message Producer"
echo "========================================"
echo ""
echo "  Broker : $BROKER"
echo "  Topic  : $TOPIC"
echo ""

# ---------------------------------------------------------------------------
# 1. Check broker reachability (best-effort — no nc on all platforms)
# ---------------------------------------------------------------------------
if command -v nc &>/dev/null; then
  if ! nc -z localhost 9092 2>/dev/null; then
    echo -e "${RED}ERROR${NC}: Cannot reach Kafka broker at $BROKER"
    echo ""
    echo "  Start the broker first:"
    echo "    docker compose up -d"
    echo "    docker compose ps    # wait for kafka to become healthy"
    echo ""
    exit 1
  fi
  echo -e "  Broker reachability : ${GRN}OK${NC}"
else
  echo -e "  ${YLW}WARNING${NC}: 'nc' not found — skipping broker reachability check"
fi
echo ""

# ---------------------------------------------------------------------------
# 2. 20 test messages
#    Format: one JSON object per line (kafka-console-producer --property
#    parse.key=false, kcat -P both expect one message per line).
#
#    Breakdown:
#      msg 01–16  valid readings — all 16 pass filter + validate → written to DB
#      msg 17–18  temperature = 0 exactly — dropped by filter (expr: temperature > 0)
#      msg 19     temperature = 250 → passes filter but fails validate max:200 → drop
#      msg 20     no sensor_id (empty string) → fails validate notNull:true → drop
# ---------------------------------------------------------------------------
MESSAGES=(
  '{"sensor_id":"S001","location":"floor-1","temperature":23.4,"humidity":55.2,"unit":"C","ts":"2026-07-08T08:00:00Z"}'
  '{"sensor_id":"S002","location":"floor-1","temperature":24.1,"humidity":58.0,"unit":"C","ts":"2026-07-08T08:00:05Z"}'
  '{"sensor_id":"S003","location":"floor-2","temperature":19.8,"humidity":62.3,"unit":"C","ts":"2026-07-08T08:00:10Z"}'
  '{"sensor_id":"S004","location":"floor-2","temperature":31.5,"humidity":44.7,"unit":"C","ts":"2026-07-08T08:00:15Z"}'
  '{"sensor_id":"S005","location":"roof",   "temperature":47.2,"humidity":30.1,"unit":"C","ts":"2026-07-08T08:00:20Z"}'
  '{"sensor_id":"S006","location":"roof",   "temperature":52.0,"humidity":28.5,"unit":"C","ts":"2026-07-08T08:00:25Z"}'
  '{"sensor_id":"S007","location":"basement","temperature":14.3,"humidity":78.9,"unit":"C","ts":"2026-07-08T08:00:30Z"}'
  '{"sensor_id":"S008","location":"basement","temperature":13.9,"humidity":81.4,"unit":"C","ts":"2026-07-08T08:00:35Z"}'
  '{"sensor_id":"S001","location":"floor-1","temperature":23.9,"humidity":54.8,"unit":"C","ts":"2026-07-08T08:01:00Z"}'
  '{"sensor_id":"S002","location":"floor-1","temperature":24.5,"humidity":57.2,"unit":"C","ts":"2026-07-08T08:01:05Z"}'
  '{"sensor_id":"S003","location":"floor-2","temperature":20.1,"humidity":61.5,"unit":"C","ts":"2026-07-08T08:01:10Z"}'
  '{"sensor_id":"S004","location":"floor-2","temperature":32.0,"humidity":43.9,"unit":"C","ts":"2026-07-08T08:01:15Z"}'
  '{"sensor_id":"S009","location":"server-room","temperature":18.5,"humidity":40.0,"unit":"C","ts":"2026-07-08T08:01:20Z"}'
  '{"sensor_id":"S010","location":"server-room","temperature":17.8,"humidity":41.2,"unit":"C","ts":"2026-07-08T08:01:25Z"}'
  '{"sensor_id":"S011","location":"office-a","temperature":22.0,"humidity":50.0,"unit":"C","ts":"2026-07-08T08:01:30Z"}'
  '{"sensor_id":"S012","location":"office-b","temperature":21.5,"humidity":52.0,"unit":"C","ts":"2026-07-08T08:01:35Z"}'
  '{"sensor_id":"S013","location":"outdoor", "temperature":0,"humidity":85.0,"unit":"C","ts":"2026-07-08T08:01:40Z"}'
  '{"sensor_id":"S014","location":"outdoor", "temperature":0,"humidity":84.0,"unit":"C","ts":"2026-07-08T08:01:45Z"}'
  '{"sensor_id":"S015","location":"furnace", "temperature":250,"humidity":10.0,"unit":"C","ts":"2026-07-08T08:01:50Z"}'
  '{"sensor_id":"",    "location":"unknown", "temperature":22.0,"humidity":50.0,"unit":"C","ts":"2026-07-08T08:01:55Z"}'
)

TOTAL=${#MESSAGES[@]}
VALID=16
FILTER_DROPPED=2
VALIDATE_DROPPED=2

echo "--> Preparing $TOTAL messages ($VALID valid, $FILTER_DROPPED dropped by filter, $VALIDATE_DROPPED dropped by validate)"
echo ""

# Build a newline-separated payload for stdin-based producers
PAYLOAD=""
for msg in "${MESSAGES[@]}"; do
  PAYLOAD="${PAYLOAD}${msg}"$'\n'
done

# ---------------------------------------------------------------------------
# 3. Detect producer tool and send
# ---------------------------------------------------------------------------
send_via_console_producer() {
  echo "$PAYLOAD" | kafka-console-producer.sh \
    --bootstrap-server "$BROKER" \
    --topic "$TOPIC" \
    2>/dev/null
}

send_via_kcat() {
  echo "$PAYLOAD" | kcat \
    -b "$BROKER" \
    -t "$TOPIC" \
    -P \
    2>/dev/null
}

send_via_kafkacat() {
  echo "$PAYLOAD" | kafkacat \
    -b "$BROKER" \
    -t "$TOPIC" \
    -P \
    2>/dev/null
}

send_via_docker_exec() {
  # Fallback: run kafka-console-producer inside the running kafka container
  echo "$PAYLOAD" | docker exec -i kafka \
    kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic "$TOPIC" \
    2>/dev/null
}

SENT=false

if command -v kafka-console-producer.sh &>/dev/null; then
  echo "--> Using kafka-console-producer.sh (Apache Kafka / Confluent Platform)"
  send_via_console_producer && SENT=true
elif command -v kcat &>/dev/null; then
  echo "--> Using kcat"
  send_via_kcat && SENT=true
elif command -v kafkacat &>/dev/null; then
  echo "--> Using kafkacat"
  send_via_kafkacat && SENT=true
elif docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^kafka$'; then
  echo "--> Using kafka-console-producer.sh inside running Docker container 'kafka'"
  send_via_docker_exec && SENT=true
fi

if [ "$SENT" = "false" ]; then
  echo -e "${YLW}WARNING${NC}: No Kafka producer tool found and 'kafka' container is not running."
  echo ""
  echo "  Option A — Install kcat:"
  echo "    brew install kcat          # macOS"
  echo "    apt-get install kafkacat   # Debian/Ubuntu"
  echo ""
  echo "  Option B — Produce via the Docker container:"
  echo "    docker compose up -d"
  echo "    # then re-run this script"
  echo ""
  echo "  Option C — Copy-paste one message manually:"
  echo "    docker exec -it kafka kafka-console-producer.sh \\"
  echo "      --bootstrap-server localhost:9092 --topic sensor-data"
  echo '    > {"sensor_id":"S001","location":"floor-1","temperature":23.4,"humidity":55.2,"unit":"C","ts":"2026-07-08T08:00:00Z"}'
  echo ""
  exit 1
fi

echo ""
echo -e "${GRN}Success${NC} — $TOTAL messages published to topic '$TOPIC'"
echo ""
echo "  Expected pipeline behaviour:"
echo "    Messages published  : $TOTAL"
printf "    Dropped by filter   : $FILTER_DROPPED  (temperature = 0 — msg 17, 18)\n"
printf "    Dropped by validate : $VALIDATE_DROPPED  (temperature > 200 — msg 19; empty sensor_id — msg 20)\n"
printf "    Written to Postgres : $VALID  (db pipelinedemo  table: sensor_readings)\n"
echo ""
echo "  Next step:"
echo "    flogo-datapipeline run kafka-streaming.datapipeline"
echo ""
