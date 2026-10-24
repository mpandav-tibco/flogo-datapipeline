#!/usr/bin/env bash
# Scale scenario: a synthetic writer on every node simulates many containers
# (kubelet-style rotation + gzip, stdout/stderr, stack traces, pod churn); the
# collector ships them; the verifier proves every event arrived.
#
#   poc/run-scale.sh <name> [KEY=VALUE writer settings...] [-- s3-outage <at> <secs>]
#
# Writer settings (see synthlog.py): CONTAINERS RATE TOTAL ROTATE_BYTES MAX_FILES
# CHURN DELETE_AFTER NAMESPACES. Collector config overrides come from the
# environment of this script (S3_KEY, SPOOL_DIR, COLLECTOR_IMG) — config only.
#
#   S3_KEY='{namespace}/{YYYY}/{MM}/{DD}/{HH}/{node}-{mm}-{uuid}.ndjson' \
#     poc/run-scale.sh s8 CONTAINERS=400 RATE=8 TOTAL=3000
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER="${CLUSTER:-adidas-poc}"
K="kubectl --context k3d-$CLUSTER"
MINIO="${MINIO_CONTAINER:-miniobench}"
BUCKET="${BUCKET:-adidas-logs}"
ROOT=/var/log/synthpods
TIMEOUT="${TIMEOUT:-2400}"
NAME="$1"; shift
WENV=(); FAULT=()
while [ $# -gt 0 ]; do
  if [ "$1" = "--" ]; then shift; FAULT=("$@"); break; fi
  WENV+=("$1"); shift
done
OUT="$HERE/results/$NAME"; rm -rf "$OUT"; mkdir -p "$OUT"
log() { echo "[$(date '+%H:%M:%S')] $NAME: $*" | tee -a "$OUT/run.log"; }
nodes() { k3d node list -o json | python3 -c "import json,sys;[print(x['name']) for x in json.load(sys.stdin) if x['role'] in ('server','agent') and '$CLUSTER' in x['name']]"; }

# Clean slate: writer + collector stopped FIRST (a draining collector would
# upload a previous run's data after the cleanup), then synthetic logs and S3.
$K -n logging delete daemonset synthlog adidas-log-collector --ignore-not-found --wait=true >/dev/null 2>&1
$K -n logging wait --for=delete pod -l 'app.kubernetes.io/name=adidas-log-collector' --timeout=120s >/dev/null 2>&1
$K -n logging wait --for=delete pod -l app=synthlog --timeout=120s >/dev/null 2>&1
for n in $(nodes); do docker exec "$n" sh -c "rm -rf $ROOT/*" ; done
for d in $(docker exec "$MINIO" sh -c "mc alias set l http://localhost:9000 minioadmin minioadmin >/dev/null && mc ls l/$BUCKET/" | awk '{print $NF}' | grep "^$NAME-team-"); do
  docker exec "$MINIO" mc rm -r --force "l/$BUCKET/$d" >/dev/null
done
SYNTH_ROOT=$ROOT "$HERE/deploy-collector.sh" reset > "$OUT/deploy.log" 2>&1 || { log "collector deploy FAILED (see deploy.log)"; exit 1; }
log "collector: image=${COLLECTOR_IMG:-flogo-datapipeline:poc} S3_KEY=${S3_KEY:-<default>} SPOOL_DIR=${SPOOL_DIR-<default>}; writer: ${WENV[*]}; fault: ${FAULT[*]:-none}"

# Writer DaemonSet (synthlog.py from a ConfigMap, Python from the emitter image).
$K -n logging create configmap synthlog --from-file="$HERE/synthlog.py" --dry-run=client -o yaml | $K apply -f - >/dev/null
ENVY="            - { name: PREFIX, value: \"$NAME\" }"
for kv in "${WENV[@]}"; do ENVY="$ENVY
            - { name: ${kv%%=*}, value: \"${kv#*=}\" }"; done
cat <<YAML | $K apply -f - >/dev/null
apiVersion: apps/v1
kind: DaemonSet
metadata: { name: synthlog, namespace: logging }
spec:
  selector: { matchLabels: { app: synthlog } }
  template:
    metadata: { labels: { app: synthlog } }
    spec:
      tolerations: [ { operator: Exists } ]
      containers:
        - name: writer
          image: bwce-emitter:poc
          imagePullPolicy: Never
          command: ["python3", "-u", "/synth/synthlog.py"]
          env:
            - { name: ROOT, value: "$ROOT" }
            - name: NODE_NAME
              valueFrom: { fieldRef: { fieldPath: spec.nodeName } }
$ENVY
          volumeMounts:
            - { name: logs, mountPath: $ROOT }
            - { name: code, mountPath: /synth }
      volumes:
        - { name: logs, hostPath: { path: $ROOT, type: DirectoryOrCreate } }
        - { name: code, configMap: { name: synthlog } }
YAML
START=$(date +%s)
log "writers started"

# Resource sampler (collector + writer) every 10 s.
(
  echo "time,pod,node,cpu_m,mem_mi" > "$OUT/collector_top.csv"
  while true; do
    t=$(date +%s)
    $K -n logging top pods --no-headers 2>/dev/null | grep adidas-log-collector | while read -r p c m; do
      n=$($K -n logging get pod "$p" -o jsonpath='{.spec.nodeName}' 2>/dev/null)
      echo "$t,$p,$n,${c%m},${m%Mi}" >> "$OUT/collector_top.csv"
    done
    sleep 10
  done
) &
SAMPLER=$!
trap 'kill $SAMPLER 2>/dev/null' EXIT

if [ ${#FAULT[@]} -gt 0 ] && [ "${FAULT[0]}" = "s3-outage" ]; then
  ( sleep "${FAULT[1]}"; docker stop "$MINIO" >/dev/null; log "FAULT s3-outage: S3 DOWN for ${FAULT[2]}s"
    sleep "${FAULT[2]}"; docker start "$MINIO" >/dev/null; log "FAULT s3-outage: S3 back UP" ) &
  FAULTPID=$!
fi

# Wait for every writer to finish; its last log line reports how many
# containers it simulated (incl. churn replacements) = expected instances.
EXPECT=0; FAULTPID=${FAULTPID:-}
while true; do
  done_lines=$($K -n logging logs -l app=synthlog --tail=5 2>/dev/null | grep -c "^synthlog:")
  if [ "$done_lines" -ge "$(nodes | wc -l)" ]; then
    EXPECT=$($K -n logging logs -l app=synthlog --tail=5 2>/dev/null | grep "^synthlog:" | sed 's/.*containers=\([0-9]*\).*/\1/' | paste -sd+ - | bc)
    break
  fi
  sleep 15
done
log "writers done after $(( $(date +%s) - START ))s: $($K -n logging logs -l app=synthlog --tail=5 2>/dev/null | grep '^synthlog:' | tr '\n' ' ')"
[ -n "${FAULTPID:-}" ] && wait "$FAULTPID"  # not the sampler: it never exits
# Then wait until the verifier proves complete delivery.
while true; do
  if python3 "$HERE/verify.py" --prefix "$NAME" --expect-instances "$EXPECT" --quiet > "$OUT/verify_progress.txt" 2>&1; then break; fi
  log "$(tail -1 "$OUT/verify_progress.txt")"
  [ $(( $(date +%s) - START )) -gt "$TIMEOUT" ] && { log "TIMEOUT waiting for complete delivery"; break; }
  sleep 45
done
END=$(date +%s)
python3 "$HERE/verify.py" --prefix "$NAME" --expect-instances "$EXPECT" --json "$OUT/verify.json" > "$OUT/verify.txt" 2>&1
kill $SAMPLER 2>/dev/null
log "verified in $((END - START))s — $(python3 -c "import json;d=json.load(open('$OUT/verify.json'));print('OK' if d['ok'] else 'FAILED', 'instances=%d/%d events=%d missing=%d gaps=%d dup=%d multiline_broken=%d dlq=%d s3_objects=%d p95_latency=%ss' % (d['finished_instances'], d['expected_instances'], d['distinct_events_delivered'], d['missing_events'], d['gaps_in_unfinished'], d['duplicate_records'], d['broken_multiline'], d['records_in_dlq'], d['s3_objects'], d['latency_s']['p95']))")"
TOTALW=$(printf '%s\n' "${WENV[@]}" | sed -n 's/^TOTAL=//p'); TOTALW=${TOTALW:-3000}
echo "{\"duration_s\": $((END - START)), \"expected_instances\": $EXPECT, \"per_instance\": $TOTALW}" > "$OUT/run.json"
$K -n logging get pods -o wide > "$OUT/collector_pods.txt"
for p in $($K -n logging get pods -l app.kubernetes.io/name=adidas-log-collector -o name); do
  n=${p#pod/}
  $K get --raw "/api/v1/namespaces/logging/pods/$n:9095/proxy/metrics" 2>/dev/null | grep -E "^(flogo_|podlog_)" > "$OUT/metrics_$n.txt" || true
  $K -n logging logs "$n" 2>/dev/null | grep -E "ERROR|WARN|spool|reconciliation" | tail -40 > "$OUT/collector_$n.log" || true
done
$K -n logging delete daemonset synthlog --wait=false >/dev/null 2>&1
