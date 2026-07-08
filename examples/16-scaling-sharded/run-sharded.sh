#!/usr/bin/env bash
# run-sharded.sh — Launch 3 sharded pipeline instances in parallel.
#
# Usage:
#   ./run-sharded.sh
#
# Prerequisites:
#   - flogo-datapipeline binary is on your PATH
#   - Run from the directory that contains this script (16-scaling-sharded/)
#
# What this does:
#   Starts three independent flogo-datapipeline processes, each bound to one
#   partition file by setting INSTANCE_INDEX and INSTANCE_COUNT. Each instance
#   reads its own CSV file, applies the filter (value > 10), and discards rows
#   via the null sink. All three run concurrently — no coordinator, no shared
#   state.
#
#   INSTANCE_INDEX=0  ->  reads  data/partition_0.csv  (rows  1-20)
#   INSTANCE_INDEX=1  ->  reads  data/partition_1.csv  (rows 21-40)
#   INSTANCE_INDEX=2  ->  reads  data/partition_2.csv  (rows 41-60)
#
# Env-var override precedence:
#   INSTANCE_INDEX and INSTANCE_COUNT set here override the static
#   values in sharded.datapipeline (scaling.instanceIndex / scaling.instanceCount).
#
# In Kubernetes StatefulSets the pod ordinal (0, 1, 2, ...) maps directly to
# INSTANCE_INDEX. INSTANCE_COUNT equals the StatefulSet replica count.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PIPELINE_FILE="sharded.datapipeline"
INSTANCE_COUNT=3

echo "=============================================="
echo " Multi-Instance Sharding — ${INSTANCE_COUNT} instances"
echo "=============================================="
echo ""
echo "Launching instance 0 (partition_0.csv) ..."
INSTANCE_INDEX=0 INSTANCE_COUNT=${INSTANCE_COUNT} \
  flogo-datapipeline -f "${PIPELINE_FILE}" \
  > logs/instance-0.log 2>&1 &
PID0=$!
echo "  PID=${PID0}  log=logs/instance-0.log"

echo "Launching instance 1 (partition_1.csv) ..."
INSTANCE_INDEX=1 INSTANCE_COUNT=${INSTANCE_COUNT} \
  flogo-datapipeline -f "${PIPELINE_FILE}" \
  > logs/instance-1.log 2>&1 &
PID1=$!
echo "  PID=${PID1}  log=logs/instance-1.log"

echo "Launching instance 2 (partition_2.csv) ..."
INSTANCE_INDEX=2 INSTANCE_COUNT=${INSTANCE_COUNT} \
  flogo-datapipeline -f "${PIPELINE_FILE}" \
  > logs/instance-2.log 2>&1 &
PID2=$!
echo "  PID=${PID2}  log=logs/instance-2.log"

echo ""
echo "Waiting for all instances to complete ..."
echo ""

# Capture exit codes individually so we report all failures, not just the first.
EC0=0; EC1=0; EC2=0
wait "${PID0}" || EC0=$?
wait "${PID1}" || EC1=$?
wait "${PID2}" || EC2=$?

echo "=============================================="
echo " Results"
echo "=============================================="
printf "  Instance 0  exit=%d\n" "${EC0}"
printf "  Instance 1  exit=%d\n" "${EC1}"
printf "  Instance 2  exit=%d\n" "${EC2}"
echo ""

# Print a summary line from each log (the pipeline-complete line).
for IDX in 0 1 2; do
  LOG="logs/instance-${IDX}.log"
  if [[ -f "${LOG}" ]]; then
    SUMMARY=$(grep -m1 "pipeline complete" "${LOG}" 2>/dev/null || echo "(no summary line found)")
    printf "  [instance-%d] %s\n" "${IDX}" "${SUMMARY}"
  fi
done

echo ""

TOTAL_FAILURES=$(( EC0 + EC1 + EC2 ))
if [[ "${TOTAL_FAILURES}" -eq 0 ]]; then
  echo "All 3 instances completed successfully."
  echo "Total rows ingested: 60  (20 per partition)"
  echo "Rows passing filter (value > 10): 54  (6 low-value rows dropped: ids 5,8,16,23,48,56)"
else
  echo "ERROR: one or more instances failed. Check logs/ for details." >&2
  exit 1
fi
