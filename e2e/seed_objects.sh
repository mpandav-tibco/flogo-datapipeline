#!/usr/bin/env bash
# Seeds the MinIO objects the S3/S3b scenarios read (idempotent): under
# dp-test-bucket/e2e/ — csv/part-0..3.csv (4 x 400k rows, ids 0..1.6M),
# tsv/part-0.tsv and jsonl/part-0.jsonl (400k rows each).
source "$(dirname "$0")/lib.sh"
NET="${COMPOSE_NETWORK:-flogo-datapipeline_default}"
mc() {
	docker run --rm --network "$NET" -v "$WORK/objs:/objs" --entrypoint sh minio/minio:latest -c \
		"mc alias set l http://minio:9000 minioadmin minioadmin >/dev/null && $*"
}
if mc "mc stat l/dp-test-bucket/e2e/csv/part-3.csv" >/dev/null 2>&1; then
	exit 0
fi
log "seeding MinIO objects for S3/S3b"
mkdir -p "$WORK/objs/csv" "$WORK/objs/tsv" "$WORK/objs/jsonl"
for i in 0 1 2 3; do
	"$H" gen -format csv -rows 400000 -start $((i * 400000)) -out "$WORK/objs/csv/part-$i.csv"
done
"$H" gen -format tsv -rows 400000 -out "$WORK/objs/tsv/part-0.tsv"
"$H" gen -format jsonl -rows 400000 -out "$WORK/objs/jsonl/part-0.jsonl"
mc "mc mb -p l/dp-test-bucket >/dev/null && mc cp -r /objs/ l/dp-test-bucket/e2e/ >/dev/null"
rm -rf "$WORK/objs"
