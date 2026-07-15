#!/usr/bin/env bash
# Phase 3 driver: wait for the background seed to finish, then run the full
# 53M matrix + ELT + reliability rounds and generate reports — all unattended.
# Launch under caffeinate so the machine stays awake for the multi-hour run.
#
#   caffeinate -i ./harness/run_phase3.sh > out/phase3.log 2>&1 &
set -uo pipefail
EX="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$EX"
export MINIO_AK="${MINIO_AK:-minioadmin}" MINIO_SK="${MINIO_SK:-minioadmin}"
mkdir -p out

echo "[phase3] waiting for seed to complete ($(date '+%H:%M:%S'))"
for _ in $(seq 1 720); do            # up to ~3h of 15s polls
  grep -q "seeding done" out/seed.log 2>/dev/null && break
  sleep 15
done
RAW=$(docker exec pgbench psql -U postgres -d warehouse -tAc "SELECT count(*) FROM orders_raw;" 2>/dev/null | tr -d '[:space:]')
echo "[phase3] seed done — orders_raw=$RAW ($(date '+%H:%M:%S'))"

# Ensure the enrichment mock is up for the lookup ETL case.
if ! curl -s localhost:9099/enrich/EMEA >/dev/null 2>&1; then
  echo "[phase3] starting lookup mock"
  python3 harness/lookup_mock.py 9099 >out/lookup_mock.log 2>&1 &
  sleep 1
fi

echo "[phase3] === MATRIX (17 EL + 11 ETL + 2 ENT) $(date '+%H:%M:%S') ==="
python3 -u harness/run_matrix.py > out/matrix_full.log 2>&1
echo "[phase3] matrix done $(date '+%H:%M:%S')"

echo "[phase3] === ELT round $(date '+%H:%M:%S') ==="
python3 -u harness/run_elt.py > out/elt_full.log 2>&1
echo "[phase3] elt done $(date '+%H:%M:%S')"

echo "[phase3] === Enterprise reliability $(date '+%H:%M:%S') ==="
python3 -u harness/run_enterprise.py > out/enterprise_full.log 2>&1
echo "[phase3] enterprise done $(date '+%H:%M:%S')"

echo "[phase3] === Reports ==="
python3 harness/gen_reports.py > out/reports.log 2>&1
echo "[phase3] COMPLETE $(date '+%H:%M:%S')"
