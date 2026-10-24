#!/usr/bin/env bash
# S5: REST sink against the mock /ingest (429 + Retry-After: 1 on every 7th
# call). Every row must arrive exactly once; concurrency should overlap waits.
source "$(dirname "$0")/lib.sh"
ROWS=${ROWS:-500}
"$H" gen -rows "$ROWS" -customers 50 -out "$WORK/data/orders_rest.csv"
cols='[{"name":"id","type":"int"},{"name":"customer_id","type":"int"},{"name":"amount","type":"int"},{"name":"status","type":"string"},{"name":"region","type":"string"},{"name":"note","type":"string"}]'
for v in "perRow:1" "perRow:8" "batch:1"; do
	mode=${v%%:*}; conc=${v#*:}; name="s5-$mode-c$conc"
	log "S5 REST sink mode=$mode concurrency=$conc"
	mock_reset
	f=$(app "$name" "{ \"name\": \"$name\", \"mode\": \"etl\",
		\"source\": { \"type\": \"csv\", \"path\": \"$WORK/data/orders_rest.csv\", \"hasHeader\": true, \"columns\": $cols },
		\"sink\": { \"type\": \"rest\", \"url\": \"http://$MOCK_ADDR/ingest\", \"mode\": \"$mode\", \"concurrency\": $conc,
			\"maxRetries\": 5, \"retryBackoffMs\": 200 },
		\"performance\": { \"batchSize\": 100 }, \"checkpoint\": { \"type\": \"none\" } }")
	start_app "$f" "$WORK/logs/$name.log"; st=$(wait_run "$WORK/logs/$name.log" 900); stop_app
	result "S5 $mode concurrency=$conc: $st | HTTP $(mock_stats) (want distinctIngested=$ROWS duplicateIngested=0)"
done
