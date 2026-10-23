#!/usr/bin/env bash
# S2: Kafka -> Postgres (COPY) with Redis checkpoints and commitMode
# "checkpoint". kill -9 mid-stream, restart, verify no loss / bounded dupes,
# then restart again and verify nothing (incl. a trailing undecodable
# message) is redelivered.
source "$(dirname "$0")/lib.sh"
N=${N:-300000}; BAD=${BAD:-20}; INTERVAL=${INTERVAL:-20000}
TOPIC="e2e-orders-$(date +%s)"; TABLE=s2_orders
log "S2 Kafka crash/restart: $N valid + $BAD bad messages, topic $TOPIC"
docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$TOPIC" --partitions 3 --replication-factor 1 >/dev/null
"$H" produce -broker "$KAFKA_BROKER" -topic "$TOPIC" -n "$N" -bad "$BAD"
psql_q "DROP TABLE IF EXISTS $TABLE" >/dev/null 2>&1
f=$(app s2 "{ \"name\": \"s2\", \"mode\": \"etl\",
	\"source\": { \"type\": \"kafka\", \"brokers\": \"$KAFKA_BROKER\", \"topic\": \"$TOPIC\", \"groupId\": \"g-$TOPIC\",
		\"startOffset\": \"earliest\", \"commitMode\": \"checkpoint\", \"pollTimeoutMs\": 1000, \"batchSize\": 2000 },
	\"transforms\": [{ \"type\": \"map\", \"fields\": { \"id\": \"=\$id\", \"sensor_id\": \"=\$sensor_id\", \"temperature\": \"=\$temperature\" },
		\"fieldOrder\": [\"id\",\"sensor_id\",\"temperature\"] }],
	\"sink\": { \"type\": \"jdbc\", \"driver\": \"postgres\", \"dsn\": \"$PG_DSN\", \"table\": \"$TABLE\", \"autoMigrate\": true, \"batchSize\": 1000 },
	\"performance\": { \"batchSize\": 2000 },
	\"checkpoint\": { \"type\": \"redis\", \"addr\": \"$REDIS_ADDR\", \"key\": \"s2-$TOPIC\", \"intervalRows\": $INTERVAL, \"intervalSeconds\": 5 } }")
count() { tcount "$TABLE"; }

start_app "$f" "$WORK/logs/s2-run1.log"
while [ "$(count)" -lt $((N * 2 / 5)) ]; do sleep 0.3; done
kill -9 "$PID"; wait "$PID" 2>/dev/null || true
atkill=$(count)
result "S2 run1: kill -9 at $atkill rows in Postgres"

start_app "$f" "$WORK/logs/s2-run2.log"
prev=-1; stable=0
while [ $stable -lt 15 ]; do c=$(count); [ "$c" = "$prev" ] && stable=$((stable + 1)) || stable=0; prev=$c; sleep 1; done
stop_app
total=$(count); distinct=$(psql_q "SELECT count(DISTINCT id) FROM $TABLE")
missing=$(psql_q "SELECT $N - count(DISTINCT id) FROM $TABLE WHERE id::numeric BETWEEN 0 AND $((N - 1))")
skipped=$(cat "$WORK"/logs/s2-run*.log | grep -c "skipping undecodable" || true)
result "S2 after restart: rows=$total distinct=$distinct missing=$missing duplicates=$((total - distinct)) (bound: checkpoint interval $INTERVAL + in-flight) | undecodable skipped (both runs)=$skipped"

start_app "$f" "$WORK/logs/s2-run3.log"
sleep 12; stop_app
after=$(count); redelivered=$(grep -c "skipping undecodable" "$WORK/logs/s2-run3.log" || true)
result "S2 run3 (idle restart): rows $total -> $after (want unchanged) | undecodable redelivered=$redelivered (want 0)"
