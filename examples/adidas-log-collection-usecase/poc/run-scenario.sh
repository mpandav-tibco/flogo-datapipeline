#!/usr/bin/env bash
# Run one POC scenario end to end: deploy the verifiable BW-app workload,
# optionally inject a fault, wait until every emitter's events are verified in
# S3, record collector CPU/memory, and save the evidence under poc/results/<name>/.
#
#   poc/run-scenario.sh <name> <workload args...> [-- <fault> <at-seconds> [<arg>]]
#
# Faults:
#   collector-kill  <t> <node>   SIGKILL the collector container on <node> (crash)
#   collector-roll  <t>          rolling restart of the DaemonSet (SIGTERM, upgrade)
#   s3-outage       <t> <secs>   stop the S3 endpoint (MinIO) for <secs>
#   node-restart    <t> <node>   restart a whole k3d node (kubelet, runtime, pods)
#
# Example:
#   poc/run-scenario.sh s2 --namespaces 6 --apps 5 --rate 10 --total 12000 -- collector-kill 120 k3d-adidas-poc-agent-0
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER="${CLUSTER:-adidas-poc}"
K="kubectl --context k3d-$CLUSTER"
MINIO="${MINIO_CONTAINER:-miniobench}"
TIMEOUT="${TIMEOUT:-1800}"
NAME="$1"; shift
WL=(); FAULT=()
while [ $# -gt 0 ]; do
  if [ "$1" = "--" ]; then shift; FAULT=("$@"); break; fi
  WL+=("$1"); shift
done
OUT="$HERE/results/$NAME"; rm -rf "$OUT"; mkdir -p "$OUT"
ts() { date '+%H:%M:%S'; }
log() { echo "[$(ts)] $NAME: $*" | tee -a "$OUT/run.log"; }

# Start from a clean slate: no objects of an earlier run of this scenario.
# (The MinIO image has no grep/awk, so filter the listing here.)
for d in $(docker exec "$MINIO" sh -c "mc alias set l http://localhost:9000 minioadmin minioadmin >/dev/null && mc ls l/${BUCKET:-adidas-logs}/" | awk '{print $NF}' | grep "^$NAME-team-"); do
  docker exec "$MINIO" mc rm -r --force "l/${BUCKET:-adidas-logs}/$d" >/dev/null
done
python3 "$HERE/workload.py" --prefix "$NAME" "${WL[@]}" > "$OUT/workload.yaml"
EXPECT=$(grep -cE '^kind: (Deployment|Job)' "$OUT/workload.yaml")
REPL=$(printf '%s\n' "${WL[@]}" | awk 'p{print;exit} /--replicas/{p=1}'); EXPECT=$((EXPECT * ${REPL:-1}))
log "workload: ${WL[*]} → $EXPECT emitter pods; fault: ${FAULT[*]:-none}"

# Resource sampler: collector pods + nodes every 10s.
(
  echo "time,pod,node,cpu_m,mem_mi" > "$OUT/collector_top.csv"
  while true; do
    t=$(date +%s)
    $K -n logging top pods --no-headers 2>/dev/null | while read -r p c m; do
      n=$($K -n logging get pod "$p" -o jsonpath='{.spec.nodeName}' 2>/dev/null)
      echo "$t,$p,$n,${c%m},${m%Mi}" >> "$OUT/collector_top.csv"
    done
    sleep 10
  done
) &
SAMPLER=$!
trap 'kill $SAMPLER 2>/dev/null' EXIT

START=$(date +%s)
$K apply -f "$OUT/workload.yaml" >/dev/null
log "workload applied"

fault() {
  local kind="$1" at="$2" arg="${3:-}"
  sleep "$at"
  case "$kind" in
  collector-kill)
    cid=$(docker exec "$arg" crictl ps --name collector -q | head -1)
    pid=$(docker exec "$arg" crictl inspect --output go-template --template '{{.info.pid}}' "$cid")
    docker exec "$arg" kill -9 "$pid"
    log "FAULT collector-kill: kill -9 of the collector process (pid $pid, container $cid) on $arg — no shutdown handling" ;;
  collector-roll)
    $K -n logging rollout restart daemonset/adidas-log-collector >/dev/null
    log "FAULT collector-roll: rolling restart of the DaemonSet started" ;;
  s3-outage)
    docker stop "$MINIO" >/dev/null; log "FAULT s3-outage: S3 endpoint DOWN for ${arg}s"
    sleep "$arg"; docker start "$MINIO" >/dev/null; log "FAULT s3-outage: S3 endpoint back UP" ;;
  node-restart)
    docker restart "$arg" >/dev/null; log "FAULT node-restart: node $arg restarted" ;;
  esac
}
if [ ${#FAULT[@]} -gt 0 ]; then
  fault "${FAULT[@]}" &
  FAULTPID=$!
fi

# Wait until every expected emitter instance is verified complete in S3.
sleep 60
while true; do
  [ -n "${FAULTPID:-}" ] && kill -0 "$FAULTPID" 2>/dev/null && { sleep 20; continue; }
  if python3 "$HERE/verify.py" --prefix "$NAME" --expect-instances "$EXPECT" --quiet > "$OUT/verify_progress.txt" 2>&1; then
    break
  fi
  log "$(tail -1 "$OUT/verify_progress.txt")"
  if [ $(( $(date +%s) - START )) -gt "$TIMEOUT" ]; then log "TIMEOUT waiting for complete delivery"; break; fi
  sleep 30
done
END=$(date +%s)
python3 "$HERE/verify.py" --prefix "$NAME" --expect-instances "$EXPECT" --json "$OUT/verify.json" > "$OUT/verify.txt" 2>&1
RC=$?
kill $SAMPLER 2>/dev/null
log "verified in $((END - START))s — $(python3 -c "import json;d=json.load(open('$OUT/verify.json'));print('OK' if d['ok'] else 'FAILED', 'events=%d missing=%d gaps=%d dup=%d multiline_broken=%d dlq=%d p95_latency=%ss' % (d['distinct_events_delivered'], d['missing_events'], d['gaps_in_unfinished'], d['duplicate_records'], d['broken_multiline'], d['records_in_dlq'], d['latency_s']['p95']))")"
echo "{\"duration_s\": $((END - START)), \"expected_instances\": $EXPECT}" > "$OUT/run.json"

# Collector evidence: restarts, reconciliation lines, metrics.
$K -n logging get pods -o wide > "$OUT/collector_pods.txt"
for p in $($K -n logging get pods -o name); do
  n=${p#pod/}
  $K -n logging logs "$n" --all-containers --prefix 2>/dev/null | grep -E "reconciliation|ERROR|WARN|panic" | tail -40 > "$OUT/collector_$n.log" || true
  $K get --raw "/api/v1/namespaces/logging/pods/$n:9095/proxy/metrics" 2>/dev/null | grep -E "^(flogo_|podlog_)" > "$OUT/metrics_$n.txt" || true
done

# Clean up the scenario's workload.
$K delete ns -l "poc-scenario=$NAME" --wait=false >/dev/null 2>&1
exit $RC
