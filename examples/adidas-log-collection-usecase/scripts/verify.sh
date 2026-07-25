#!/usr/bin/env bash
# Verify the log-collection pipeline end to end: collector pods (one per node),
# sample BW-app logs, collector reconciliation, and the NDJSON objects landed in
# the S3 (MinIO) bucket.
set -euo pipefail

PROFILE="${MINIKUBE_PROFILE:-adidas-logs}"
K="kubectl --context $PROFILE"
BUCKET="${BUCKET:-adidas-logs}"
MINIO_CONTAINER="${MINIO_CONTAINER:-miniobench}"

line() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

line "nodes"
$K get nodes -o wide

line "collector DaemonSet (one pod per node)"
$K -n logging get pods -o wide

line "BW-app workload pods (6 namespaces x 2)"
$K get pods -A -l app.kubernetes.io/part-of=bw-apps -o wide | head -20

line "sample BW-app console log (order-service)"
$K -n team-alpha logs -l app.kubernetes.io/name=order-service --tail=12 --prefix 2>/dev/null | head -12

line "collector logs (reconciliation + S3 writes)"
for p in $($K -n logging get pods -l app.kubernetes.io/name=adidas-log-collector -o name); do
  echo "--- $p ---"
  $K -n logging logs "$p" --tail=18 2>/dev/null | grep -iE 'podlog|reconcil|written|processed|s3|error|flush|started' | tail -18
done

line "S3 bucket '$BUCKET' — object count + tree (top 30)"
docker exec "$MINIO_CONTAINER" sh -c \
  "mc alias set localminio http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; \
   echo 'total objects:'; mc ls --recursive localminio/$BUCKET 2>/dev/null | wc -l; \
   mc ls --recursive localminio/$BUCKET 2>/dev/null | head -30"

line "sample enriched NDJSON record from S3 (first object, first line)"
docker exec "$MINIO_CONTAINER" sh -c \
  "mc alias set localminio http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; \
   OBJ=\$(mc ls --recursive localminio/$BUCKET 2>/dev/null | awk '{print \$NF}' | head -1); \
   echo \"object: \$OBJ\"; mc cat localminio/$BUCKET/\$OBJ 2>/dev/null | head -1" \
  | python3 -m json.tool 2>/dev/null || \
  docker exec "$MINIO_CONTAINER" sh -c \
    "mc alias set localminio http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; \
     OBJ=\$(mc ls --recursive localminio/$BUCKET 2>/dev/null | awk '{print \$NF}' | head -1); \
     mc cat localminio/$BUCKET/\$OBJ 2>/dev/null | head -1"

echo
echo "Tip: open the MinIO console at http://localhost:9001 (minioadmin/minioadmin) to browse '$BUCKET'."
