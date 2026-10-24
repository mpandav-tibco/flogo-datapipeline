#!/usr/bin/env bash
# End-to-end benchmark driver: infra -> build -> data -> seed -> matrix -> ELT
# -> reliability -> reports. Each stage is idempotent; re-run any stage on its
# own with the individual scripts/harness commands documented in README.md.
#
#   ./scripts/run-benchmark.sh              # full 53,000,000-row standard run
#   ./scripts/run-benchmark.sh 1000000      # 1M-row quick pass
set -euo pipefail
EX="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$EX"
ROWS="${1:-53000000}"
mkdir -p out
export MINIO_AK="${MINIO_AK:-minioadmin}" MINIO_SK="${MINIO_SK:-minioadmin}"

echo "==> [1/8] infra (postgres, kafka, minio)"
./scripts/setup-infra.sh

echo "==> [2/8] build single-binary engine"
./scripts/build.sh

echo "==> [3/8] generate dataset ($ROWS rows, every format)"
./scripts/gen-data.sh "$ROWS"

echo "==> [4/8] regenerate pipeline definitions"
python3 harness/gen_pipes.py

echo "==> [5/8] seed postgres + kafka + s3"
python3 harness/seed.py pg kafka s3

echo "==> [6/8] enrichment mock (for the lookup ETL case)"
python3 harness/lookup_mock.py 9099 >out/lookup_mock.log 2>&1 &
MOCK_PID=$!
trap 'kill "$MOCK_PID" 2>/dev/null || true' EXIT
sleep 1

echo "==> [7/8] run matrix + ELT + reliability + graph"
caffeinate -i python3 harness/run_matrix.py 2>&1 | tee out/matrix.log
caffeinate -i python3 harness/run_elt.py 2>&1 | tee out/elt.log
caffeinate -i python3 harness/run_enterprise.py 2>&1 | tee out/enterprise.log
caffeinate -i python3 harness/run_graph.py 2>&1 | tee out/graph.log

echo "==> [8/8] generate reports"
python3 harness/gen_comparison.py
python3 harness/gen_reports.py

echo
echo "Done. See SUMMARY.md and reports/*.md"
