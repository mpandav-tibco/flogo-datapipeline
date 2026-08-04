#!/usr/bin/env bash
# add-new-events.sh — Simulate new activity arriving after the first pipeline run.
#
# Inserts 20 new events with created_at timestamps on 2025-06-06, which is
# strictly after the high-water mark recorded by run1.sh (2025-06-05 15:30:00).
#
# These 20 rows represent the "delta" that the second pipeline run (run2.sh)
# should pick up — demonstrating that only new data is processed once a
# watermark has been established.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

HWM_FILE="./watermarks/events.hwm"

if ! docker compose exec -T postgres pg_isready -U postgres -d pipelinedemo >/dev/null 2>&1; then
  echo "ERROR: PostgreSQL is not reachable. Run ./setup.sh first."
  exit 1
fi

if [ ! -f "$HWM_FILE" ]; then
  echo "ERROR: Watermark file not found at $HWM_FILE."
  echo "       Run run1.sh before adding new events."
  exit 1
fi

echo "==> Current watermark:"
cat "$HWM_FILE"
echo ""

BEFORE=$(docker compose exec -T postgres psql -U postgres -d pipelinedemo -tAc "SELECT COUNT(*) FROM raw_events;")
echo "    Rows before insert: $BEFORE"

docker compose exec -T postgres psql -U postgres -d pipelinedemo <<'SQL'
INSERT INTO raw_events (event_type, user_id, session_id, page, duration_ms, status, created_at) VALUES
  ('page_view',  1061, 'sess-ag1', '/home',       143,  'ok',      '2025-06-06 08:00:00'),
  ('click',      1061, 'sess-ag1', '/home',        18,  'ok',      '2025-06-06 08:01:10'),
  ('api_call',   1062, 'sess-ag2', '/api/search', 355,  'ok',      '2025-06-06 08:30:00'),
  ('page_view',  1063, 'sess-ag3', '/products',   215,  'ok',      '2025-06-06 09:00:00'),
  ('checkout',   1063, 'sess-ag3', '/checkout',   490,  'ok',      '2025-06-06 09:01:30'),
  ('page_view',  1064, 'sess-ag4', '/cart',       188,  'ok',      '2025-06-06 09:30:00'),
  ('api_call',   1064, 'sess-ag4', '/api/orders', 305,  'ok',      '2025-06-06 09:31:00'),
  ('page_view',  1065, 'sess-ag5', '/blog',       200,  'ok',      '2025-06-06 10:00:00'),
  ('click',      1065, 'sess-ag5', '/blog',        13,  'ok',      '2025-06-06 10:01:00'),
  ('api_call',   1066, 'sess-ag6', '/api/user',   275,  'ok',      '2025-06-06 10:30:00'),
  ('checkout',   1066, 'sess-ag6', '/checkout',   520,  'error',   '2025-06-06 10:31:45'),
  ('page_view',  1067, 'sess-ag7', '/home',       152,  'ok',      '2025-06-06 11:00:00'),
  ('click',      1067, 'sess-ag7', '/products',    16,  'ok',      '2025-06-06 11:01:05'),
  ('api_call',   1068, 'sess-ag8', '/api/search', 420,  'timeout', '2025-06-06 11:30:00'),
  ('page_view',  1069, 'sess-ag9', '/products',   195,  'ok',      '2025-06-06 12:00:00'),
  ('checkout',   1069, 'sess-ag9', '/checkout',   505,  'ok',      '2025-06-06 12:01:50'),
  ('api_call',   1070, 'sess-ah0', '/api/orders', 340,  'ok',      '2025-06-06 12:30:00'),
  ('page_view',  1071, 'sess-ah1', '/home',       160,  'ok',      '2025-06-06 13:00:00'),
  ('click',      1071, 'sess-ah1', '/home',        14,  'ok',      '2025-06-06 13:01:00'),
  ('page_view',  1072, 'sess-ah2', '/cart',       178,  'ok',      '2025-06-06 14:00:00');
SQL

AFTER=$(docker compose exec -T postgres psql -U postgres -d pipelinedemo -tAc "SELECT COUNT(*) FROM raw_events;")
NEW=$((AFTER - BEFORE))

echo "    Rows after insert:  $AFTER  (+$NEW new rows)"
echo ""
echo "    New rows span: 2025-06-06 08:00:00 → 2025-06-06 14:00:00"
echo "    All new timestamps are AFTER the recorded watermark."
echo ""
echo "    Next step: run ./run2.sh — the pipeline will process only these $NEW new rows."
