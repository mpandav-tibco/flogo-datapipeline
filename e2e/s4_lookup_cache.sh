#!/usr/bin/env bash
# S4: lookup enrichment caching against the mock API.
#  A) batch CSV, 200k rows / 5000 customers (500 missing): HTTP calls + runtime
#     per caching variant, and exact null-enrichment counts.
#  B) streaming Kafka: phase 1, then the missing customers appear upstream
#     (/heal), then phase 2 — counts phase-2 rows still null-enriched (stale).
# VARIANTS: space-separated "label=json-options" (json merged into lookup cfg).
source "$(dirname "$0")/lib.sh"
VARIANTS=${VARIANTS:-'default={} no-negative-cache={"cacheNotFound":false}'}
[ -f "$WORK/data/orders_200k.csv" ] || "$H" gen -rows 200000 -customers 5000 -out "$WORK/data/orders_200k.csv"
cols='[{"name":"id","type":"int"},{"name":"customer_id","type":"int"},{"name":"amount","type":"int"},{"name":"status","type":"string"},{"name":"region","type":"string"},{"name":"note","type":"string"}]'
lookup_json() { # extra-json
	python3 -c "import json,sys; d={'type':'lookup','keyField':'customer_id','url':'http://$MOCK_ADDR/customers/{customer_id}','outputFields':['tier'],'cacheSize':10000,'concurrency':8}; d.update(json.loads(sys.argv[1])); print(json.dumps(d))" "$1"
}
for v in $VARIANTS; do
	label=${v%%=*}; extra=${v#*=}; lk=$(lookup_json "$extra"); tl=$(echo "$label" | tr - _)

	log "S4A batch lookup [$label] $extra"
	table="s4a_$tl"; psql_q "DROP TABLE IF EXISTS $table" >/dev/null 2>&1; mock_reset
	f=$(app "s4a-$label" "{ \"name\": \"s4a-$label\", \"mode\": \"etl\",
		\"source\": { \"type\": \"csv\", \"path\": \"$WORK/data/orders_200k.csv\", \"hasHeader\": true, \"columns\": $cols },
		\"transforms\": [ $lk, { \"type\": \"map\", \"fields\": { \"id\": \"=\$id\", \"customer_id\": \"=\$customer_id\", \"tier\": \"=\$tier\" }, \"fieldOrder\": [\"id\",\"customer_id\",\"tier\"] } ],
		\"sink\": { \"type\": \"jdbc\", \"driver\": \"postgres\", \"dsn\": \"$PG_DSN\", \"table\": \"$table\", \"autoMigrate\": true, \"batchSize\": 20000 },
		\"performance\": { \"batchSize\": 5000 }, \"checkpoint\": { \"type\": \"none\" } }")
	start_app "$f" "$WORK/logs/s4a-$label.log"; st=$(wait_run "$WORK/logs/s4a-$label.log" 900); stop_app
	nulls=$(psql_q "SELECT count(*) FROM $table WHERE tier IS NULL")
	sleep 0.5 # let the mock settle before reading counters
	result "S4A [$label] $st | HTTP $(mock_stats) | null-tier rows=$nulls (want 20000)"

	log "S4B streaming lookup staleness [$label]"
	topic="e2e-lookup-$tl-$(date +%s)"; table="s4b_$tl"
	docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$topic" --partitions 1 --replication-factor 1 >/dev/null
	psql_q "DROP TABLE IF EXISTS $table" >/dev/null 2>&1; mock_reset
	f=$(app "s4b-$label" "{ \"name\": \"s4b-$label\", \"mode\": \"etl\",
		\"source\": { \"type\": \"kafka\", \"brokers\": \"$KAFKA_BROKER\", \"topic\": \"$topic\", \"groupId\": \"g-$topic\", \"startOffset\": \"earliest\", \"pollTimeoutMs\": 500 },
		\"transforms\": [ $lk, { \"type\": \"map\", \"fields\": { \"id\": \"=\$id\", \"customer_id\": \"=\$customer_id\", \"tier\": \"=\$tier\" }, \"fieldOrder\": [\"id\",\"customer_id\",\"tier\"] } ],
		\"sink\": { \"type\": \"jdbc\", \"driver\": \"postgres\", \"dsn\": \"$PG_DSN\", \"table\": \"$table\", \"autoMigrate\": true, \"batchSize\": 1000 },
		\"performance\": { \"batchSize\": 2000 }, \"checkpoint\": { \"type\": \"none\" } }")
	start_app "$f" "$WORK/logs/s4b-$label.log"
	"$H" produce -broker "$KAFKA_BROKER" -topic "$topic" -n 50000 >/dev/null
	until [ "$(tcount $table)" -ge 50000 ]; do sleep 0.5; done
	curl -s -X POST "http://$MOCK_ADDR/heal" >/dev/null; sleep ${HEAL_WAIT:-65} # > default notFoundTtlMs (60s)
	"$H" produce -broker "$KAFKA_BROKER" -topic "$topic" -n 50000 -start 50000 >/dev/null
	until [ "$(tcount $table)" -ge 100000 ]; do sleep 0.5; done
	stop_app
	stale=$(psql_q "SELECT count(*) FROM $table WHERE id::numeric >= 50000 AND customer_id::numeric % 10 = 0 AND tier IS NULL")
	result "S4B [$label] customers appeared upstream mid-stream: phase-2 rows still null-enriched (stale)=$stale of 5000 (want 0: phase 2 starts ${HEAL_WAIT:-65}s after the heal, past the 60s default notFoundTtlMs) | HTTP $(mock_stats)"
done
