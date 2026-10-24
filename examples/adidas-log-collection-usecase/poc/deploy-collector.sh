#!/usr/bin/env bash
# Deploy the collector DaemonSet onto the POC cluster from the example's own
# manifests. Only per-environment CONFIG changes (ConfigMap values + image) —
# the pipeline definition is used exactly as committed.
#
#   poc/deploy-collector.sh            deploy / roll out
#   poc/deploy-collector.sh reset      also wipe node-local collector state
#                                      (positions, checkpoints, DLQ) first
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UC="$(cd "$HERE/.." && pwd)"
CLUSTER="${CLUSTER:-adidas-poc}"
K="kubectl --context k3d-$CLUSTER"
COLLECTOR_IMG="${COLLECTOR_IMG:-flogo-datapipeline:poc}"
S3_ENDPOINT="${S3_ENDPOINT:-http://host.k3d.internal:9000}"
CHECKPOINT_SECONDS="${CHECKPOINT_SECONDS:-30}"
# Optional overrides (config only — the pipeline file never changes):
#   SYNTH_ROOT  also mount this node directory and collect it (scale tests)
#   S3_KEY      object layout, e.g. per namespace
#   SPOOL_DIR   "" or none disables the durable spool
export SYNTH_ROOT="${SYNTH_ROOT:-}" COLLECTOR_IMG

if [[ "${1:-}" == "reset" ]]; then
  $K -n logging delete daemonset adidas-log-collector --ignore-not-found --wait=true >/dev/null
  for n in $(k3d node list -o json | python3 -c "import json,sys;[print(x['name']) for x in json.load(sys.stdin) if x['role'] in ('server','agent') and '$CLUSTER' in x['name']]"); do
    docker exec "$n" rm -rf /var/lib/flogo-datapipeline
  done
fi

$K apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Namespace
metadata: { name: logging, labels: { purpose: log-collection } }
YAML
$K apply -f "$UC/k8s/20-collector-secret.yaml" >/dev/null
$K apply -f "$UC/k8s/25-collector-config.yaml" >/dev/null
$K -n logging patch configmap adidas-log-collector-config --type merge \
  -p "{\"data\":{\"S3_ENDPOINT\":\"$S3_ENDPOINT\",\"CLUSTER_NAME\":\"k3d-$CLUSTER\",\"ENVIRONMENT\":\"poc\",\"CHECKPOINT_SECONDS\":\"$CHECKPOINT_SECONDS\"}}" >/dev/null
cfg() { $K -n logging patch configmap adidas-log-collector-config --type merge -p "{\"data\":{\"$1\":\"$2\"}}" >/dev/null; }
[ -n "$SYNTH_ROOT" ] && cfg POD_LOGS_ROOT "$SYNTH_ROOT"
[ -n "${S3_KEY+x}" ] && cfg S3_KEY "$S3_KEY"
if [ -n "${SPOOL_DIR+x}" ]; then
  # Empty env values fall back to the pipeline default, so "off" is spelled none.
  if [ -z "$SPOOL_DIR" ] || [ "$SPOOL_DIR" = none ]; then cfg SPOOL_DIR none; cfg SPOOL_DLQ_DIR none
  else cfg SPOOL_DIR "$SPOOL_DIR"; cfg SPOOL_DLQ_DIR "$SPOOL_DIR-dlq"; fi
fi
$K -n logging create configmap adidas-log-collector-pipeline \
  --from-file="$UC/adidas-log-collector.datapipeline" --dry-run=client -o yaml | $K apply -f - >/dev/null
python3 "$HERE/patch_ds.py" < "$UC/k8s/40-collector-daemonset.yaml" | $K apply -f - >/dev/null
$K -n logging rollout restart daemonset/adidas-log-collector >/dev/null
$K -n logging rollout status daemonset/adidas-log-collector --timeout=180s
$K -n logging get pods -o wide
