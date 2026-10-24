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
$K -n logging create configmap adidas-log-collector-pipeline \
  --from-file="$UC/adidas-log-collector.datapipeline" --dry-run=client -o yaml | $K apply -f - >/dev/null
sed -e "s#image: flogo-datapipeline:arm64-local#image: $COLLECTOR_IMG#" -e "s#imagePullPolicy: IfNotPresent#imagePullPolicy: Never#" \
  "$UC/k8s/40-collector-daemonset.yaml" | $K apply -f - >/dev/null
$K -n logging rollout restart daemonset/adidas-log-collector >/dev/null
$K -n logging rollout status daemonset/adidas-log-collector --timeout=180s
$K -n logging get pods -o wide
