#!/usr/bin/env bash
# S3b: restart MinIO while an object is being streamed. A lookup against the
# mock API slows processing so the object read spans the restart.
source "$(dirname "$0")/lib.sh"
log "S3b MinIO restart mid-stream"
s3cfg="\"bucket\": \"dp-test-bucket\", \"region\": \"us-east-1\", \"endpoint\": \"$S3_ENDPOINT\", \"forcePathStyle\": true, \"accessKeyId\": \"minioadmin\", \"secretAccessKey\": \"minioadmin\""
cols='[{"name":"id","type":"int"},{"name":"customer_id","type":"int"},{"name":"amount","type":"int"},{"name":"status","type":"string"},{"name":"region","type":"string"},{"name":"note","type":"string"}]'
for dl in ${MODES:-false true}; do
	name="s3b-dl$dl"; table="s3b_dl$dl"
	psql_q "DROP TABLE IF EXISTS $table" >/dev/null 2>&1
	f=$(app "$name" "{ \"name\": \"$name\", \"mode\": \"etl\",
		\"source\": { \"type\": \"s3\", $s3cfg, \"prefix\": \"e2e/csv/part-0\", \"format\": \"csv\", \"download\": $dl, \"hasHeader\": true, \"columns\": $cols },
		\"transforms\": [
			{ \"type\": \"lookup\", \"keyField\": \"customer_id\", \"url\": \"http://$MOCK_ADDR/customers/{customer_id}\", \"outputFields\": [\"tier\"], \"cacheSize\": 1, \"concurrency\": 4 },
			{ \"type\": \"map\", \"fields\": { \"id\": \"=\$id\", \"tier\": \"=\$tier\" }, \"fieldOrder\": [\"id\",\"tier\"] } ],
		\"sink\": { \"type\": \"jdbc\", \"driver\": \"postgres\", \"dsn\": \"$PG_DSN\", \"table\": \"$table\", \"autoMigrate\": true, \"batchSize\": 1000 },
		\"performance\": { \"batchSize\": 1000, \"sourceBufferBatches\": 1, \"maxMemoryMB\": 1 }, \"checkpoint\": { \"type\": \"none\" } }")
	start_app "$f" "$WORK/logs/$name.log"
	until [ "$(tcount $table)" -gt 5000 ]; do sleep 0.3; done
	docker restart -t 0 "$MINIO_CONTAINER" >/dev/null
	st=$(wait_run "$WORK/logs/$name.log" 900); stop_app
	err=$(grep -o "source read: [^\"]\{0,160\}" "$WORK/logs/$name.log" | head -1 || true)
	resumed=$(grep -c "stream resumed" "$WORK/logs/$name.log" || true)
	result "S3b download=$dl, MinIO restarted mid-run: $st | rows=$(psql_q "SELECT count(*) FROM $table") (want 400000) | stream resumptions=$resumed ${err:+| $err}"
	until docker exec "$MINIO_CONTAINER" sh -c 'curl -sf http://localhost:9000/minio/health/ready' >/dev/null 2>&1; do sleep 1; done
done
