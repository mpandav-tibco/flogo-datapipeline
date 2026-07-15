#!/usr/bin/env bash
# Stop and remove the benchmark containers. Add --volumes to also drop the
# PostgreSQL/MinIO data volumes (a fresh backend on next setup).
#
#   ./scripts/teardown-infra.sh            # remove containers
#   ./scripts/teardown-infra.sh --volumes  # remove containers + volumes
set -uo pipefail
RM_ARGS="-f"
[ "${1:-}" = "--volumes" ] && RM_ARGS="-f -v"
for c in pgbench kafkabench miniobench; do
  if docker ps -a --format '{{.Names}}' | grep -qx "$c"; then
    echo "  removing $c"; docker rm $RM_ARGS "$c" >/dev/null 2>&1 || true
  fi
done
echo "Infrastructure torn down."
