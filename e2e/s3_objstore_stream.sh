#!/usr/bin/env bash
# S3: MinIO objects -> Postgres. Streaming (default for csv/tsv/jsonl) vs
# download:true spooling: wall time, peak RSS, peak temp disk, exact rows.
# Objects are produced by the harness and uploaded under e2e/ in dp-test-bucket.
source "$(dirname "$0")/lib.sh"
log "S3 object storage: streaming vs spool"
s3cfg="\"bucket\": \"dp-test-bucket\", \"region\": \"us-east-1\", \"endpoint\": \"$S3_ENDPOINT\", \"forcePathStyle\": true, \"accessKeyId\": \"minioadmin\", \"secretAccessKey\": \"minioadmin\""
cols='[{"name":"id","type":"int"},{"name":"customer_id","type":"int"},{"name":"amount","type":"int"},{"name":"status","type":"string"},{"name":"region","type":"string"},{"name":"note","type":"string"}]'
for fmt in csv tsv jsonlines; do
	prefix=$fmt; [ $fmt = jsonlines ] && prefix=jsonl
	expect=$([ $fmt = csv ] && echo 1600000 || echo 400000)
	extra=""; [ $fmt != jsonlines ] && extra=", \"hasHeader\": true, \"columns\": $cols"
	[ $fmt = jsonlines ] && extra=", \"fields\": [\"id\",\"customer_id\",\"amount\",\"status\",\"region\",\"note\"]"
	for dl in false true; do
		name="s3-$fmt-dl$dl"; table="s3_${fmt}_dl$dl"
		psql_q "DROP TABLE IF EXISTS $table" >/dev/null 2>&1
		f=$(app "$name" "{ \"name\": \"$name\", \"mode\": \"etl\",
			\"source\": { \"type\": \"s3\", $s3cfg, \"prefix\": \"e2e/$prefix/\", \"format\": \"$fmt\", \"download\": $dl $extra },
			\"transforms\": [{ \"type\": \"map\", \"fields\": { \"id\": \"=\$id\", \"amount\": \"=\$amount\" }, \"fieldOrder\": [\"id\",\"amount\"] }],
			\"sink\": { \"type\": \"jdbc\", \"driver\": \"postgres\", \"dsn\": \"$PG_DSN\", \"table\": \"$table\", \"autoMigrate\": true, \"batchSize\": 20000 },
			\"performance\": { \"batchSize\": 5000 }, \"checkpoint\": { \"type\": \"none\" } }")
		start_app "$f" "$WORK/logs/$name.log"; st=$(wait_run "$WORK/logs/$name.log" 900); stop_app
		got=$(psql_q "SELECT count(*)||' rows, distinct ids '||count(DISTINCT id::numeric) FROM $table")
		result "S3 $fmt download=$dl: $st | peakRSS=$(peak_rss_mb "$WORK/logs/$name.log")MB peakTmp=$(peak_tmp_mb "$WORK/logs/$name.log")MB | $got (want $expect)"
	done
done
