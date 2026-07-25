#!/usr/bin/env bash
# Deploy the full adidas log-collection use case onto the local minikube cluster.
# Idempotent: safe to re-run. Set SKIP_BUILD=1 to skip the image build/load.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UC_DIR="$(cd "$HERE/.." && pwd)"
PROFILE="${MINIKUBE_PROFILE:-adidas-logs}"
K="kubectl --context $PROFILE"
BUCKET="${BUCKET:-adidas-logs}"
MINIO_CONTAINER="${MINIO_CONTAINER:-miniobench}"

echo "== [1/6] ensure minikube profile '$PROFILE' is running =="
if ! minikube status -p "$PROFILE" >/dev/null 2>&1; then
  minikube start -p "$PROFILE" --nodes=2 --container-runtime=containerd --driver=docker --cpus=2 --memory=2800
fi
$K get nodes -o wide

echo "== [2/6] ensure MinIO bucket '$BUCKET' exists on container '$MINIO_CONTAINER' =="
docker exec "$MINIO_CONTAINER" sh -c \
  "mc alias set localminio http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; mc mb --ignore-existing localminio/$BUCKET" \
  || echo "  WARN: could not create bucket via mc — create '$BUCKET' manually if it is missing."

echo "== [3/6] build + load arm64 images (SKIP_BUILD=1 to skip) =="
if [[ "${SKIP_BUILD:-}" != "1" ]]; then
  bash "$UC_DIR/collector/build-image.sh"
fi

echo "== [4/6] apply namespaces, secret, config, and BW-app workload =="
$K apply -f "$UC_DIR/k8s/00-namespaces.yaml"
$K apply -f "$UC_DIR/k8s/20-collector-secret.yaml"
$K apply -f "$UC_DIR/k8s/25-collector-config.yaml"
$K apply -f "$UC_DIR/k8s/10-workload.yaml"

echo "== [5/6] (re)create the pipeline ConfigMap from the .datapipeline =="
$K -n logging create configmap adidas-log-collector-pipeline \
  --from-file="$UC_DIR/adidas-log-collector.datapipeline" \
  --dry-run=client -o yaml | $K apply -f -

echo "== [6/6] deploy the collector DaemonSet =="
$K apply -f "$UC_DIR/k8s/40-collector-daemonset.yaml"

echo "-- waiting for workload + collector to become ready --"
for ns in team-alpha team-beta team-gamma team-delta team-epsilon team-zeta; do
  $K -n "$ns" wait --for=condition=Available deploy --all --timeout=150s || true
done
$K -n logging rollout status daemonset/adidas-log-collector --timeout=180s || true

echo
echo "Deployed. Next: bash $HERE/verify.sh"
