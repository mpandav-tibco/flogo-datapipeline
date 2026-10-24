#!/usr/bin/env bash
# S1: CSV (1M rows) -> filter -> map -> Postgres, bulkMode auto (COPY) vs insert.
source "$(dirname "$0")/lib.sh"
ROWS=${ROWS:-1000000}
[ -f "$WORK/data/orders_1m.csv" ] || "$H" gen -rows "$ROWS" -out "$WORK/data/orders_1m.csv" -customers 5000
log "S1 Postgres bulk load: COPY vs multi-row INSERT"
for mode in auto insert; do
	table="s1_$mode"
	psql_q "DROP TABLE IF EXISTS $table" >/dev/null 2>&1
	f=$(app "s1-$mode" "{
		\"name\": \"s1-$mode\", \"mode\": \"etl\",
		\"source\": { \"type\": \"csv\", \"path\": \"$WORK/data/orders_1m.csv\", \"hasHeader\": true,
			\"columns\": [{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"customer_id\",\"type\":\"int\"},{\"name\":\"amount\",\"type\":\"int\"},
				{\"name\":\"status\",\"type\":\"string\"},{\"name\":\"region\",\"type\":\"string\"},{\"name\":\"note\",\"type\":\"string\"}] },
		\"transforms\": [
			{ \"type\": \"filter\", \"expr\": \"status != 'VOID'\" },
			{ \"type\": \"map\", \"fields\": { \"id\": \"=\$id\", \"customer_id\": \"=\$customer_id\", \"amount\": \"=\$amount\", \"region\": \"=\$region\", \"note\": \"=upper(\$note)\" },
			  \"fieldOrder\": [\"id\",\"customer_id\",\"amount\",\"region\",\"note\"] } ],
		\"sink\": { \"type\": \"jdbc\", \"driver\": \"postgres\", \"dsn\": \"$PG_DSN\", \"table\": \"$table\",
			\"autoMigrate\": true, \"batchSize\": 20000, \"bulkMode\": \"$mode\" },
		\"performance\": { \"batchSize\": 5000 }, \"checkpoint\": { \"type\": \"none\" } }")
	start_app "$f" "$WORK/logs/s1-$mode.log"
	st=$(wait_run "$WORK/logs/s1-$mode.log" 600)
	stop_app
	got=$(psql_q "SELECT count(*)||' rows, sum(amount)='||sum(amount)||', md5='||md5(string_agg(id||note, ',' ORDER BY id)) FROM $table")
	result "S1 bulkMode=$mode: $st | peakRSS=$(peak_rss_mb "$WORK/logs/s1-$mode.log")MB | table: $got"
done
expect=$(awk -F, 'NR>1 && $4!="VOID" {n++; s+=$3} END {print n" rows, sum(amount)="s}' "$WORK/data/orders_1m.csv")
result "S1 expected (from source file): $expect"
