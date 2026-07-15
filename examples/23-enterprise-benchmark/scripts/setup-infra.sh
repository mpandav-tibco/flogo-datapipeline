#!/usr/bin/env bash
# Provision the backends the benchmark needs — PostgreSQL, Kafka (KRaft), MinIO/S3 —
# apply the warehouse schema, and ensure the S3 bucket. Idempotent: reuses any
# already-running container and leaves reachable ports as they are.
#
#   ./scripts/setup-infra.sh
#
# Seeding the backends from the generated dataset is a separate step
# (python3 harness/seed.py) so provisioning stays fast and repeatable.
set -uo pipefail
EX="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$EX"
export MINIO_AK="${MINIO_AK:-minioadmin}" MINIO_SK="${MINIO_SK:-minioadmin}"
MC="${UC_MC:-$(command -v mc || echo /tmp/mc)}"

port_up() { (exec 3<>"/dev/tcp/localhost/$1") 2>/dev/null && exec 3>&-; }

ensure() { # ensure <name> <docker run args...>
  local name="$1"; shift
  if docker ps --format '{{.Names}}' | grep -qx "$name"; then
    echo "  $name already running"
  elif docker ps -a --format '{{.Names}}' | grep -qx "$name"; then
    echo "  starting existing $name"; docker start "$name" >/dev/null
  else
    echo "  creating $name"; docker run -d --name "$name" "$@" >/dev/null
  fi
}

echo "[1/5] PostgreSQL :5433 (warehouse)"
port_up 5433 && echo "  :5433 already reachable" || \
  ensure pgbench -p 5433:5432 -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=warehouse postgres:16-alpine

echo "[2/5] Kafka :9092 (KRaft, single node)"
port_up 9092 && echo "  :9092 already reachable" || \
  ensure kafkabench -p 9092:9092 \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    apache/kafka-native:4.1.0

echo "[3/5] MinIO :9000 (S3, console :9001)"
port_up 9000 && echo "  :9000 already reachable" || \
  ensure miniobench -p 9000:9000 -p 9001:9001 \
    -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
    minio/minio server /data --console-address :9001

echo "[4/5] waiting for readiness"
for _ in $(seq 1 60); do port_up 5433 && port_up 9092 && port_up 9000 && break; sleep 1; done
docker exec pgbench sh -c 'until pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done' 2>/dev/null || true
# Ensure the S3 bucket exists (no-op if mc is unavailable — the seed step retries).
if [ -x "$MC" ] || command -v "$MC" >/dev/null 2>&1; then
  "$MC" alias set local "http://localhost:9000" "$MINIO_AK" "$MINIO_SK" >/dev/null 2>&1 || true
  "$MC" mb --ignore-existing local/datalake >/dev/null 2>&1 && echo "  bucket local/datalake ready" || true
else
  echo "  (mc client not found — create bucket 'datalake' before the S3 cases)"
fi

echo "[5/5] applying warehouse schema"
docker exec -i pgbench psql -U postgres -d warehouse < sql/schema.sql >/dev/null && echo "  schema applied (orders_raw, orders_sink, orders_clean, orders_rejects)"

echo
echo "Infrastructure ready:"
echo "  PostgreSQL  localhost:5433  db=warehouse  user=postgres  pass=bench"
echo "  Kafka       localhost:9092  (KRaft)"
echo "  MinIO/S3    http://localhost:9000  bucket=datalake  (minioadmin/minioadmin)"
