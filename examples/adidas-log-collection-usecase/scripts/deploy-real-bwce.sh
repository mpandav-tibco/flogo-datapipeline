#!/usr/bin/env bash
# Deploy the REAL licensed TIBCO BWCE 6.12 app (Rest_Sample_Multiple_Endpoints)
# into the cluster as an additional workload, so the collector captures genuine
# BW6 console logs. Optional add-on to scripts/deploy.sh — requires a BWCE license.
#
# BWCE 6.12 is x86-64 only; on the arm64 minikube nodes it runs via Rosetta. The
# image is built with an arm64 MANIFEST but amd64 CONTENT (buildx) so containerd
# accepts it. The license (.bin) is mounted at the BW6 default path /home/bwce/license.
#   docs: https://docs.tibco.com/.../incorporating-tibco-license-file-for-BWCE.htm
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UC_DIR="$(cd "$HERE/.." && pwd)"
PROFILE="${MINIKUBE_PROFILE:-adidas-logs}"
K="kubectl --context $PROFILE"

# Inputs (override via env). Defaults match this repo's environment.
BWCE_DOCKER_KIT="${BWCE_DOCKER_KIT:-$HOME/tibco_home/bw6/6.12/bw/6.12/docker}"
EAR_FILE="${EAR_FILE:-$HOME/Downloads/work/tibco/tibco_platform/bwce-ears/Rest_Sample_Multiple_Endpoints.application_1.0.0.ear}"
LICENSE_FILE="${LICENSE_FILE:-$HOME/Downloads/work/tibco_license/tibco-license_ANY.bin}"

BASE_IMG="bwce-base:6.12-amd64"
CLUSTER_IMG="bwce-reqresp:cluster"

echo ">> [1/5] build amd64 BWCE base image (if missing)"
if ! docker image inspect "$BASE_IMG" >/dev/null 2>&1; then
  docker build --platform linux/amd64 -t "$BASE_IMG" "$BWCE_DOCKER_KIT"
else
  echo "   $BASE_IMG already present"
fi

echo ">> [2/5] build the app image (arm64 manifest / amd64 content, via buildx)"
cp "$EAR_FILE" "$UC_DIR/workload/bwce-app/Rest_Sample_Multiple_Endpoints.application_1.0.0.ear"
docker buildx build --platform linux/arm64 --load -t "$CLUSTER_IMG" "$UC_DIR/workload/bwce-app"

echo ">> [3/5] load image into minikube"
minikube image load "$CLUSTER_IMG" -p "$PROFILE"

echo ">> [4/5] create license Secret (mounted at /home/bwce/license)"
$K create namespace team-bwce --dry-run=client -o yaml | $K apply -f -
$K -n team-bwce create secret generic tibco-license --from-file="$LICENSE_FILE" \
  --dry-run=client -o yaml | $K apply -f -

echo ">> [5/5] deploy the real BWCE app"
$K apply -f "$UC_DIR/k8s/15-bwce-real-app.yaml"
$K -n team-bwce rollout status deploy/rest-sample --timeout=180s || true

echo
echo "Deployed. The app boots under Rosetta (a few minutes). Watch license activation:"
echo "  $K -n team-bwce logs -l app.kubernetes.io/name=rest-sample -f | grep -i licens"
echo "Then its logs land in S3 under team-bwce/ (bash scripts/verify.sh)."
