#!/usr/bin/env bash
# S1b: COPY default edge cases — constraint violations (per-row fallback + DLQ)
# and an updatable view as the target (INSERT works; COPY cannot target views).
source "$(dirname "$0")/lib.sh"
"$H" gen -rows 20000 -out "$WORK/data/orders_20k.csv" -customers 500
log "S1b Postgres COPY edge cases"
pipeline() { # name table mode
	echo "{ \"name\": \"$1\", \"mode\": \"etl\",
		\"source\": { \"type\": \"csv\", \"path\": \"$WORK/data/orders_20k.csv\", \"hasHeader\": true,
			\"columns\": [{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"customer_id\",\"type\":\"int\"},{\"name\":\"amount\",\"type\":\"int\"},
				{\"name\":\"status\",\"type\":\"string\"},{\"name\":\"region\",\"type\":\"string\"},{\"name\":\"note\",\"type\":\"string\"}] },
		\"transforms\": [{ \"type\": \"map\", \"fields\": { \"id\": \"=\$id\", \"amount\": \"=\$amount\" }, \"fieldOrder\": [\"id\",\"amount\"] }],
		\"sink\": { \"type\": \"jdbc\", \"driver\": \"postgres\", \"dsn\": \"$PG_DSN\", \"table\": \"$2\", \"batchSize\": 1000, \"bulkMode\": \"$3\" },
		\"dlq\": { \"type\": \"jsonlines\", \"options\": { \"path\": \"$WORK/logs/$1.dlq.jsonl\" } },
		\"performance\": { \"batchSize\": 1000 }, \"checkpoint\": { \"type\": \"none\" } }"
}
for mode in auto insert; do
	# (a) 1% of rows violate CHECK (amount < 990): batch fails -> per-row isolation -> DLQ.
	psql_q "DROP TABLE IF EXISTS s1b_chk_$mode; CREATE TABLE s1b_chk_$mode (id int, amount int CHECK (amount < 990))" >/dev/null
	rm -f "$WORK/logs/s1b-chk-$mode.dlq.jsonl"
	f=$(app "s1b-chk-$mode" "$(pipeline s1b-chk-$mode s1b_chk_$mode $mode)")
	start_app "$f" "$WORK/logs/s1b-chk-$mode.log"; st=$(wait_run "$WORK/logs/s1b-chk-$mode.log" 900); stop_app
	result "S1b constraint bulkMode=$mode: $st | table=$(psql_q "SELECT count(*) FROM s1b_chk_$mode") dlq=$(wc -l <"$WORK/logs/s1b-chk-$mode.dlq.jsonl" 2>/dev/null | tr -d ' ')"

	# (b) Updatable view as the target.
	psql_q "DROP VIEW IF EXISTS s1b_view_$mode; DROP TABLE IF EXISTS s1b_base_$mode; CREATE TABLE s1b_base_$mode (id int, amount int); CREATE VIEW s1b_view_$mode AS SELECT id, amount FROM s1b_base_$mode" >/dev/null
	f=$(app "s1b-view-$mode" "$(pipeline s1b-view-$mode s1b_view_$mode $mode)")
	start_app "$f" "$WORK/logs/s1b-view-$mode.log"; st=$(wait_run "$WORK/logs/s1b-view-$mode.log" 900); stop_app
	err=$(grep -o 'cannot copy[^"]*\|COPY[^"]\{0,80\}' "$WORK/logs/s1b-view-$mode.log" | head -1)
	result "S1b view target bulkMode=$mode: $st | rows via view=$(psql_q "SELECT count(*) FROM s1b_base_$mode") ${err:+| error: $err}"
done
