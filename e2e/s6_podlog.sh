#!/usr/bin/env bash
# S6: pod-log tailer against a live CRI log file whose lines are written in
# several chunks with pauses (the writer flushes mid-line). Every message must
# arrive whole, exactly once; the persisted offset must equal the file size.
source "$(dirname "$0")/lib.sh"
LINES=${LINES:-300}
root="$WORK/podlogs"; rm -rf "$root"; mkdir -p "$root/team-a_orders-7f9_uid1/app"
logf="$root/team-a_orders-7f9_uid1/app/0.log"; : >"$logf"
out="$WORK/logs/s6-out.jsonl"; pos="$WORK/logs/s6-positions.json"; rm -f "$out" "$pos"
f=$(app s6 "{ \"name\": \"s6\", \"mode\": \"etl\",
	\"source\": { \"type\": \"podlog\", \"root\": \"$root\", \"node\": \"n1\", \"positionFile\": \"$pos\", \"fromBeginning\": true, \"pollMs\": 50 },
	\"sink\": { \"type\": \"jsonlines\", \"path\": \"$out\" },
	\"performance\": { \"batchSize\": 50 }, \"checkpoint\": { \"type\": \"none\" } }")
log "S6 podlog: $LINES lines, each written in 3 chunks"
start_app "$f" "$WORK/logs/s6.log"
sleep 2
python3 - "$logf" "$LINES" <<'PY'
import sys, time
path, n = sys.argv[1], int(sys.argv[2])
with open(path, "a", buffering=1) as f:
    for i in range(n):
        line = f"2026-07-14T10:00:{i%60:02d}.000Z stdout F message-{i:04d} " + "x" * 120 + "\n"
        a, b = len(line) // 3, 2 * len(line) // 3
        for part in (line[:a], line[a:b], line[b:]):
            f.write(part); f.flush(); time.sleep(0.004)
PY
sleep 3
stop_app
got=$(python3 -c "
import json,sys
msgs=[json.loads(l).get('message','') for l in open('$out') if l.strip()]
want=['message-%04d '%i + 'x'*120 for i in range($LINES)]
print('records=%d exact=%d missing=%d corrupted=%d dup=%d' % (len(msgs), sum(1 for m in msgs if m in set(want)), len(set(want)-set(msgs)), sum(1 for m in msgs if m not in set(want)), len(msgs)-len(set(msgs))))")
size=$(stat -f %z "$logf"); saved=$(python3 -c "import json; d=json.load(open('$pos')); print(max(int(v.get('offset', v.get('Offset',0))) for v in (d.values() if isinstance(d,dict) else d)))" 2>/dev/null || echo "?")
result "S6 podlog chunked writes: $got (want records=$LINES exact=$LINES) | saved offset=$saved file size=$size"
