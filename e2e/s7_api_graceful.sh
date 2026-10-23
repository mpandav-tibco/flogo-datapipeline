#!/usr/bin/env bash
# S7: a streaming Kafka pipeline started on boot must not block startup (the
# management API answers while it runs), and SIGTERM mid-stream must shut it
# down cleanly with no loss: restart consumes the rest, every id present.
source "$(dirname "$0")/lib.sh"
N=${N:-200000}; TOPIC="e2e-graceful-$(date +%s)"; TABLE=s7_orders; API=127.0.0.1:18888
log "S7 API during streaming + graceful SIGTERM"
docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$TOPIC" --partitions 3 --replication-factor 1 >/dev/null
"$H" produce -broker "$KAFKA_BROKER" -topic "$TOPIC" -n "$N" >/dev/null
psql_q "DROP TABLE IF EXISTS $TABLE" >/dev/null 2>&1
f=$(app s7 "{ \"name\": \"s7\", \"mode\": \"etl\",
	\"source\": { \"type\": \"kafka\", \"brokers\": \"$KAFKA_BROKER\", \"topic\": \"$TOPIC\", \"groupId\": \"g-$TOPIC\",
		\"startOffset\": \"earliest\", \"commitMode\": \"checkpoint\", \"pollTimeoutMs\": 500, \"batchSize\": 2000 },
	\"transforms\": [{ \"type\": \"map\", \"fields\": { \"id\": \"=\$id\" }, \"fieldOrder\": [\"id\"] }],
	\"sink\": { \"type\": \"jdbc\", \"driver\": \"postgres\", \"dsn\": \"$PG_DSN\", \"table\": \"$TABLE\", \"autoMigrate\": true, \"batchSize\": 1000 },
	\"performance\": { \"batchSize\": 2000 },
	\"checkpoint\": { \"type\": \"redis\", \"addr\": \"$REDIS_ADDR\", \"key\": \"s7-$TOPIC\", \"intervalRows\": 20000, \"intervalSeconds\": 5 } }")
export PIPELINE_API_ADDR=$API
start_app "$f" "$WORK/logs/s7-run1.log"
for i in $(seq 1 50); do curl -sf "http://$API/v1/health" >/dev/null && break; sleep 0.2; done
health=$(curl -s -o /dev/null -w "%{http_code}" "http://$API/v1/health" || true)
while [ "$(tcount $TABLE)" -lt $((N / 2)) ]; do sleep 0.3; done
stop_app # SIGTERM mid-stream
atstop=$(tcount $TABLE)
shut=$(grep -o "shutdown: .*" "$WORK/logs/s7-run1.log" | head -1 || true)
status=$(grep -o "pipeline run [a-z]* —" "$WORK/logs/s7-run1.log" | tail -1 || true)
start_app "$f" "$WORK/logs/s7-run2.log"
prev=-1; stable=0
while [ $stable -lt 12 ]; do c=$(tcount $TABLE); [ "$c" = "$prev" ] && stable=$((stable + 1)) || stable=0; prev=$c; sleep 1; done
stop_app
unset PIPELINE_API_ADDR
total=$(tcount $TABLE); distinct=$(psql_q "SELECT count(DISTINCT id) FROM $TABLE")
result "S7 API /v1/health during streaming run: HTTP $health (want 200) | SIGTERM at $atstop rows: '$status' / '$shut' | after restart: rows=$total distinct=$distinct (want $N) duplicates=$((total - distinct))"
