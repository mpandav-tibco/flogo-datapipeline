#!/usr/bin/env bash
# Run the full adidas POC scenario matrix on the 3-node cluster and write the
# summary table (poc/results/SUMMARY.md). Each scenario is self-verifying:
# poc/run-scenario.sh waits until every emitted event is proven in S3.
#
#   poc/cluster.sh up && poc/run-all.sh            # all scenarios
#   poc/run-all.sh s4 c4                           # a subset
#
# Scenarios (fixed collector unless noted):
#   s1  steady state: 12 namespaces x 5 BW apps (60 pods), ~1,560 events/s
#   s2  collector crash: SIGKILL the collector on a node mid-stream
#   s2r rolling upgrade of the collector DaemonSet (SIGTERM) mid-stream
#   s3  S3 outage: object store down for 120 s mid-stream
#   s4  log-rotation stress: 6 pods at ~1,000 events/s each, kubelet rotating at 1 MiB
#   s5  short-lived pods: 40 Jobs that log and are deleted 5 s after finishing
#   s6  node restart: a worker node (kubelet + runtime + its pods) restarts mid-stream
#   s7  long S3 outage while kubelet keeps rotating (fixed rotation size/count):
#       each pod's log rotates ~3 times while the collector is behind, so kubelet
#       gzips rotations the collector has not read yet
#   c2  CONTROL: s2 on the pre-fix collector (master before this work)
#   c4  CONTROL: s4 on the pre-fix collector
#   c7  CONTROL: s7 on the collector without compressed-rotation support
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER="${CLUSTER:-adidas-poc}"
FIXED_IMG="${COLLECTOR_IMG:-flogo-datapipeline:poc}"
CONTROL_IMG="${CONTROL_IMG:-flogo-datapipeline:poc-master}"
NOGZ_IMG="${NOGZ_IMG:-flogo-datapipeline:poc-nogz}"
AGENT0="k3d-$CLUSTER-agent-0"
AGENT1="k3d-$CLUSTER-agent-1"
ALL=(s1 s2 s2r s3 s4 s5 s6 s7 c2 c4 c7)
SEL=("$@"); [ ${#SEL[@]} -eq 0 ] && SEL=("${ALL[@]}")
current=""

collector() { # (re)deploy the given collector image with clean node state
  [ "$current" = "$1" ] && return
  COLLECTOR_IMG="$1" "$HERE/deploy-collector.sh" reset >/dev/null
  current="$1"
}

run() { "$HERE/run-scenario.sh" "$@" | grep -E "verified in|TIMEOUT|FAULT" ; }

for s in "${SEL[@]}"; do
  case "$s" in
  s1)  collector "$FIXED_IMG";   run s1  --namespaces 12 --apps 5 --rate 10 --total 12000 ;;
  s2)  collector "$FIXED_IMG";   run s2  --namespaces 6 --apps 5 --rate 10 --total 8000 -- collector-kill 120 "$AGENT0" ;;
  s2r) collector "$FIXED_IMG";   run s2r --namespaces 6 --apps 5 --rate 10 --total 8000 -- collector-roll 120 ;;
  s3)  collector "$FIXED_IMG";   run s3  --namespaces 6 --apps 5 --rate 10 --total 8000 -- s3-outage 90 120 ;;
  s4)  collector "$FIXED_IMG";   run s4  --namespaces 3 --apps 2 --rate 400 --total 150000 ;;
  s5)  collector "$FIXED_IMG";   run s5  --namespaces 4 --apps 10 --rate 50 --total 3000 --kind job ;;
  s6)  collector "$FIXED_IMG";   run s6  --namespaces 6 --apps 5 --rate 10 --total 8000 -- node-restart 120 "$AGENT1" ;;
  s7)  collector "$FIXED_IMG";   run s7  --namespaces 3 --apps 2 --rate 30 --total 30000 -- s3-outage 90 90 ;;
  c7)  collector "$NOGZ_IMG";    TIMEOUT=900 run c7 --namespaces 3 --apps 2 --rate 30 --total 30000 -- s3-outage 90 90 ;;
  c2)  collector "$CONTROL_IMG"; TIMEOUT=900 run c2 --namespaces 6 --apps 5 --rate 10 --total 8000 -- collector-kill 120 "$AGENT0" ;;
  c4)  collector "$CONTROL_IMG"; TIMEOUT=600 run c4 --namespaces 3 --apps 2 --rate 400 --total 150000 ;;
  *) echo "unknown scenario $s" >&2 ;;
  esac
done
python3 "$HERE/summary.py" > "$HERE/results/SUMMARY.md"
cat "$HERE/results/SUMMARY.md"
