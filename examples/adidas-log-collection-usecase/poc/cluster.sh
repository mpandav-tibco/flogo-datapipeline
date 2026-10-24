#!/usr/bin/env bash
# 3-node k3d (k3s/containerd) cluster for the adidas POC, plus the S3 target.
#
#   poc/cluster.sh up       create the cluster, build + import images, start MinIO
#   poc/cluster.sh images   rebuild + re-import the collector/emitter images only
#   poc/cluster.sh down     delete the cluster
#
# Kubelet rotates container logs at CONTAINER_LOG_MAX_SIZE (default 1Mi, far
# below the 10Mi default) so log rotation happens constantly under load — the
# collector must follow it without losing a line.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UC="$(cd "$HERE/.." && pwd)"
REPO="$(cd "$UC/../.." && pwd)"
CLUSTER="${CLUSTER:-adidas-poc}"
K3S_IMAGE="${K3S_IMAGE:-rancher/k3s:v1.30.6-k3s1}"
LOG_MAX_SIZE="${CONTAINER_LOG_MAX_SIZE:-1Mi}"
LOG_MAX_FILES="${CONTAINER_LOG_MAX_FILES:-5}"
MINIO="${MINIO_CONTAINER:-miniobench}"
BUCKET="${BUCKET:-adidas-logs}"
COLLECTOR_IMG="${COLLECTOR_IMG:-flogo-datapipeline:poc}"
EMITTER_IMG="${EMITTER_IMG:-bwce-emitter:poc}"

build_images() {
  echo ">> building the collector engine (linux/$(docker info --format '{{.Architecture}}' | sed 's/aarch64/arm64/;s/x86_64/amd64/'), static, CGO off)"
  local arch; arch=$(docker info --format '{{.Architecture}}' | sed 's/aarch64/arm64/;s/x86_64/amd64/')
  (cd "$REPO" && CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -tags tibco_hooks -trimpath -ldflags "-s -w" \
    -o "$UC/collector/flogo-dp-linux-arm64" ./cmd/flogo-datapipeline)
  docker build -q -t "$COLLECTOR_IMG" "$UC/collector" >/dev/null
  docker build -q -t "$EMITTER_IMG" "$UC/workload" >/dev/null
  k3d image import -c "$CLUSTER" "$COLLECTOR_IMG" "$EMITTER_IMG"
}

minio_up() {
  if ! docker ps --format '{{.Names}}' | grep -qx "$MINIO"; then
    docker start "$MINIO" >/dev/null 2>&1 || docker run -d --name "$MINIO" -p 9000:9000 -p 9001:9001 \
      -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio:latest server /data --console-address :9001 >/dev/null
  fi
  for _ in $(seq 1 30); do curl -sf http://localhost:9000/minio/health/live >/dev/null && break; sleep 1; done
  docker exec "$MINIO" sh -c "mc alias set l http://localhost:9000 minioadmin minioadmin >/dev/null && mc mb --ignore-existing l/$BUCKET >/dev/null"
}

case "${1:-up}" in
up)
  minio_up
  if ! k3d cluster list -o json | grep -q "\"name\":\"$CLUSTER\""; then
    k3d cluster create "$CLUSTER" --servers 1 --agents 2 --image "$K3S_IMAGE" --wait \
      --k3s-arg "--disable=traefik@server:*" \
      --k3s-arg "--kubelet-arg=container-log-max-size=$LOG_MAX_SIZE@server:*;agent:*" \
      --k3s-arg "--kubelet-arg=container-log-max-files=$LOG_MAX_FILES@server:*;agent:*"
  fi
  build_images
  kubectl --context "k3d-$CLUSTER" get nodes -o wide
  ;;
images) build_images ;;
down) k3d cluster delete "$CLUSTER" ;;
*) echo "usage: $0 up|images|down" >&2; exit 2 ;;
esac
