#!/usr/bin/env bash
# Shared helpers for the runtime end-to-end scenarios. Requires the live stack
# from `make test-integration-up` and the binaries built by e2e/run.sh.
set -euo pipefail

E2E_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$E2E_DIR/.." && pwd)"
WORK="${E2E_WORK:-/tmp/dp-e2e}"
BIN="$WORK/bin/flogo-datapipeline"
H="$WORK/bin/harness"
PG_CONTAINER="${PG_CONTAINER:-flogo-datapipeline-postgres-1}"
PG_DSN="${PG_DSN:-postgres://postgres:FlogoPass123@127.0.0.1:15432/dp?sslmode=disable}"
KAFKA_BROKER="${KAFKA_BROKER:-localhost:9092}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-flogo-datapipeline-kafka-1}"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
MINIO_CONTAINER="${MINIO_CONTAINER:-flogo-datapipeline-minio-1}"
REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:16379}"
MOCK_ADDR="${MOCK_ADDR:-127.0.0.1:18080}"
RESULTS="$WORK/results.md"
mkdir -p "$WORK/bin" "$WORK/logs" "$WORK/data" "$WORK/apps"

log() { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
result() { echo "$*" | tee -a "$RESULTS"; }

psql_q() { docker exec -i "$PG_CONTAINER" psql -U postgres -d dp -tAq -c "$1"; }

# app NAME PIPELINE_JSON -> writes $WORK/apps/NAME.datapipeline (one run at start)
app() {
	local name=$1 pipeline=$2 out="$WORK/apps/$1.datapipeline"
	cat >"$out" <<EOF
{
  "name": "$name", "type": "flogo:app", "version": "1.0.0", "appModel": "1.1.1",
  "imports": ["github.com/mpandav-tibco/flogo-datapipeline/action/pipeline", "github.com/project-flogo/contrib/trigger/timer"],
  "triggers": [{ "id": "t", "ref": "github.com/project-flogo/contrib/trigger/timer",
    "settings": { "repeating": true, "startDelay": 0, "interval": 86400000 },
    "handlers": [{ "settings": {}, "action": { "ref": "github.com/mpandav-tibco/flogo-datapipeline/action/pipeline",
      "settings": { "pipelineRef": "res://pipeline:$name" } } }] }],
  "resources": [{ "id": "pipeline:$name", "data": $pipeline }]
}
EOF
	echo "$out"
}

# start_app APPFILE LOGFILE -> starts the binary in the background, sets $PID.
# RSS is sampled every 200ms into LOGFILE.rss; TMPDIR is a private dir whose
# peak size is sampled into LOGFILE.tmp.
start_app() {
	local appfile=$1 logf=$2
	export APP_TMP="$WORK/tmp/$(basename "$logf" .log)"
	rm -rf "$APP_TMP" && mkdir -p "$APP_TMP"
	: >"$logf.rss"
	: >"$logf.tmp"
	TMPDIR="$APP_TMP" "$BIN" -f "$appfile" >"$logf" 2>&1 &
	PID=$!
	(
		while kill -0 "$PID" 2>/dev/null; do
			ps -o rss= -p "$PID" 2>/dev/null >>"$logf.rss" || true
			du -sk "$APP_TMP" 2>/dev/null | cut -f1 >>"$logf.tmp" || true
			sleep 0.2
		done
	) &
}

# wait_run LOGFILE TIMEOUT_S -> waits for the run to finish; prints status.
wait_run() {
	local logf=$1 timeout=$2 i=0
	while [ $i -lt $((timeout * 5)) ]; do
		if grep -q "pipeline run completed\|pipeline run failed\|pipeline run partial" "$logf" 2>/dev/null; then
			grep -o "pipeline run [a-z]*.*duration=[0-9a-zµ.]*" "$logf" | tail -1
			return 0
		fi
		sleep 0.2
		i=$((i + 1))
	done
	echo "TIMEOUT after ${timeout}s"
	return 1
}

stop_app() {
	kill "$PID" 2>/dev/null || true
	wait "$PID" 2>/dev/null || true
}

peak_rss_mb() { sort -n "$1.rss" | tail -1 | awk '{printf "%.0f", $1/1024}'; }
peak_tmp_mb() { sort -n "$1.tmp" | tail -1 | awk '{printf "%.1f", $1/1024}'; }
run_secs() { grep -o "duration=[0-9a-zµ.]*" "$1" | tail -1 | cut -d= -f2; }

mock_stats() { curl -s "http://$MOCK_ADDR/stats"; }
mock_reset() { curl -s -X POST "http://$MOCK_ADDR/reset" >/dev/null; }
