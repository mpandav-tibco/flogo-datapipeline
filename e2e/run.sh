#!/usr/bin/env bash
# Runtime end-to-end suite: builds the real binary + harness, starts the mock
# API, and runs every scenario against the docker-compose.test.yml stack.
#   make test-integration-up      # once: start Postgres, Kafka, MinIO, Redis, ...
#   e2e/run.sh                    # all scenarios
#   e2e/run.sh s2 s4              # a subset
# Results are appended to $E2E_WORK/results.md (default /tmp/dp-e2e).
set -euo pipefail
cd "$(dirname "$0")/.."
source e2e/lib.sh
go build -o "$WORK/bin/flogo-datapipeline" ./cmd/flogo-datapipeline
go build -o "$WORK/bin/harness" ./e2e/harness
if ! curl -sf "http://$MOCK_ADDR/stats" >/dev/null; then
	"$WORK/bin/harness" mock -addr "$MOCK_ADDR" -latency 2ms -missing-every 10 -throttle-every 7 >"$WORK/logs/mock.log" 2>&1 &
	sleep 1
fi
bash e2e/seed_objects.sh
echo "## Run $(date -u +%FT%TZ) ($(git rev-parse --short HEAD))" >>"$RESULTS"
scenarios=("$@")
[ ${#scenarios[@]} -eq 0 ] && scenarios=(s1 s1b s2 s3 s3b s4 s5 s6 s7)
for s in "${scenarios[@]}"; do
	script=$(ls e2e/${s}_*.sh | head -1)
	echo ">>> $script"
	bash "$script"
done
echo "results: $RESULTS"
