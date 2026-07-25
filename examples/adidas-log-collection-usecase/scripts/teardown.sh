#!/usr/bin/env bash
# Tear down the use case. By default removes the k8s resources only.
#   FULL=1     also delete the minikube profile (whole cluster)
#   BUCKET=1   also empty+remove the S3 bucket on the host MinIO
set -euo pipefail

PROFILE="${MINIKUBE_PROFILE:-adidas-logs}"
K="kubectl --context $PROFILE"
BUCKET_NAME="${BUCKET_NAME:-adidas-logs}"
MINIO_CONTAINER="${MINIO_CONTAINER:-miniobench}"

echo "== deleting k8s resources =="
$K delete ns logging team-alpha team-beta team-gamma team-delta team-epsilon team-zeta --ignore-not-found

if [[ "${BUCKET:-}" == "1" ]]; then
  echo "== emptying + removing bucket '$BUCKET_NAME' =="
  docker exec "$MINIO_CONTAINER" sh -c \
    "mc alias set localminio http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; \
     mc rb --force localminio/$BUCKET_NAME" || true
fi

if [[ "${FULL:-}" == "1" ]]; then
  echo "== deleting minikube profile '$PROFILE' =="
  minikube delete -p "$PROFILE"
fi

echo "done."
