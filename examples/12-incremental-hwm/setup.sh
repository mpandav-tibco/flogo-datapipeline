#!/usr/bin/env bash
# setup.sh — Start PostgreSQL and seed the events table.
#
# This script:
#   1. Starts the local PostgreSQL container (docker compose up -d).
#   2. Waits for the database to accept connections.
#   3. Creates the raw_events table (dropping any previous copy first).
#   4. Inserts 100 events spread across five days in the past, simulating a
#      historical dataset that existed before the pipeline was first deployed.
#      50 of those rows have older timestamps (the "pre-existing backlog");
#      the other 50 have more recent timestamps (the "full initial load").
#
# After running this script you have 100 rows ready. Run run1.sh to perform
# the first pipeline pass and save the high-water mark.
#
# Prerequisites:
#   Docker + Docker Compose  (the postgres service is defined in docker-compose.yml)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

mkdir -p "./watermarks"

echo "==> Starting PostgreSQL (docker compose up -d) ..."
docker compose up -d

echo "==> Waiting for PostgreSQL to become ready ..."
until docker compose exec -T postgres pg_isready -U postgres -d pipelinedemo >/dev/null 2>&1; do
  sleep 1
done
echo "    PostgreSQL is ready."

echo "==> Creating schema in database pipelinedemo ..."

docker compose exec -T postgres psql -U postgres -d pipelinedemo <<'SQL'
DROP TABLE IF EXISTS raw_events;
CREATE TABLE raw_events (
    event_id    SERIAL  PRIMARY KEY,
    event_type  TEXT    NOT NULL,
    user_id     INTEGER NOT NULL,
    session_id  TEXT    NOT NULL,
    page        TEXT    NOT NULL,
    duration_ms INTEGER NOT NULL,
    status      TEXT    NOT NULL CHECK(status IN ('ok','error','timeout')),
    created_at  TEXT    NOT NULL   -- ISO-8601 datetime stored as TEXT (string HWM comparison)
);
SQL

echo "==> Inserting 50 older events (2025-06-01 to 2025-06-03) ..."

docker compose exec -T postgres psql -U postgres -d pipelinedemo <<'SQL'
INSERT INTO raw_events (event_type, user_id, session_id, page, duration_ms, status, created_at) VALUES
  ('page_view',  1001, 'sess-aa1', '/home',       142,  'ok',      '2025-06-01 08:01:00'),
  ('page_view',  1002, 'sess-aa2', '/products',   238,  'ok',      '2025-06-01 08:03:22'),
  ('click',      1001, 'sess-aa1', '/home',        18,  'ok',      '2025-06-01 08:04:10'),
  ('page_view',  1003, 'sess-aa3', '/cart',       195,  'ok',      '2025-06-01 08:15:00'),
  ('checkout',   1003, 'sess-aa3', '/checkout',   520,  'ok',      '2025-06-01 08:16:44'),
  ('page_view',  1004, 'sess-aa4', '/home',       110,  'ok',      '2025-06-01 09:00:00'),
  ('click',      1004, 'sess-aa4', '/products',    22,  'ok',      '2025-06-01 09:01:05'),
  ('api_call',   1005, 'sess-aa5', '/api/search', 340,  'ok',      '2025-06-01 09:30:00'),
  ('api_call',   1005, 'sess-aa5', '/api/search', 510,  'timeout', '2025-06-01 09:31:02'),
  ('page_view',  1006, 'sess-aa6', '/blog',       175,  'ok',      '2025-06-01 10:00:00'),
  ('page_view',  1007, 'sess-aa7', '/home',       133,  'ok',      '2025-06-01 10:05:00'),
  ('click',      1007, 'sess-aa7', '/products',    15,  'ok',      '2025-06-01 10:06:11'),
  ('checkout',   1007, 'sess-aa7', '/checkout',   610,  'error',   '2025-06-01 10:07:35'),
  ('page_view',  1008, 'sess-aa8', '/home',       205,  'ok',      '2025-06-01 11:00:00'),
  ('api_call',   1008, 'sess-aa8', '/api/user',   290,  'ok',      '2025-06-01 11:02:00'),
  ('page_view',  1009, 'sess-aa9', '/products',   160,  'ok',      '2025-06-02 08:00:00'),
  ('click',      1009, 'sess-aa9', '/products',    12,  'ok',      '2025-06-02 08:01:22'),
  ('page_view',  1010, 'sess-ab0', '/cart',       180,  'ok',      '2025-06-02 08:30:00'),
  ('checkout',   1010, 'sess-ab0', '/checkout',   455,  'ok',      '2025-06-02 08:31:10'),
  ('api_call',   1011, 'sess-ab1', '/api/orders', 320,  'ok',      '2025-06-02 09:00:00'),
  ('page_view',  1012, 'sess-ab2', '/home',       145,  'ok',      '2025-06-02 09:15:00'),
  ('page_view',  1013, 'sess-ab3', '/blog',       220,  'ok',      '2025-06-02 09:45:00'),
  ('click',      1013, 'sess-ab3', '/blog',        10,  'ok',      '2025-06-02 09:46:00'),
  ('api_call',   1014, 'sess-ab4', '/api/search', 410,  'ok',      '2025-06-02 10:00:00'),
  ('api_call',   1014, 'sess-ab4', '/api/search', 980,  'timeout', '2025-06-02 10:01:45'),
  ('page_view',  1015, 'sess-ab5', '/products',   198,  'ok',      '2025-06-02 11:00:00'),
  ('checkout',   1015, 'sess-ab5', '/checkout',   530,  'ok',      '2025-06-02 11:02:18'),
  ('page_view',  1016, 'sess-ab6', '/home',       167,  'ok',      '2025-06-02 12:00:00'),
  ('click',      1016, 'sess-ab6', '/home',        25,  'ok',      '2025-06-02 12:00:50'),
  ('api_call',   1017, 'sess-ab7', '/api/user',   275,  'ok',      '2025-06-02 13:00:00'),
  ('page_view',  1018, 'sess-ab8', '/cart',       185,  'ok',      '2025-06-03 08:00:00'),
  ('checkout',   1018, 'sess-ab8', '/checkout',   600,  'error',   '2025-06-03 08:01:55'),
  ('page_view',  1019, 'sess-ab9', '/home',       152,  'ok',      '2025-06-03 08:30:00'),
  ('api_call',   1019, 'sess-ab9', '/api/orders', 305,  'ok',      '2025-06-03 09:00:00'),
  ('page_view',  1020, 'sess-ac0', '/products',   211,  'ok',      '2025-06-03 09:30:00'),
  ('click',      1020, 'sess-ac0', '/products',    14,  'ok',      '2025-06-03 09:30:55'),
  ('page_view',  1021, 'sess-ac1', '/blog',       190,  'ok',      '2025-06-03 10:00:00'),
  ('api_call',   1022, 'sess-ac2', '/api/search', 360,  'ok',      '2025-06-03 10:15:00'),
  ('checkout',   1022, 'sess-ac2', '/checkout',   490,  'ok',      '2025-06-03 10:16:30'),
  ('page_view',  1023, 'sess-ac3', '/home',       130,  'ok',      '2025-06-03 11:00:00'),
  ('click',      1023, 'sess-ac3', '/home',        19,  'ok',      '2025-06-03 11:01:00'),
  ('api_call',   1024, 'sess-ac4', '/api/user',   280,  'ok',      '2025-06-03 11:30:00'),
  ('page_view',  1025, 'sess-ac5', '/products',   175,  'ok',      '2025-06-03 12:00:00'),
  ('checkout',   1025, 'sess-ac5', '/checkout',   540,  'ok',      '2025-06-03 12:01:45'),
  ('page_view',  1026, 'sess-ac6', '/cart',       160,  'ok',      '2025-06-03 13:00:00'),
  ('click',      1026, 'sess-ac6', '/cart',        11,  'ok',      '2025-06-03 13:00:40'),
  ('api_call',   1027, 'sess-ac7', '/api/orders', 390,  'timeout', '2025-06-03 14:00:00'),
  ('page_view',  1028, 'sess-ac8', '/home',       148,  'ok',      '2025-06-03 14:30:00'),
  ('page_view',  1029, 'sess-ac9', '/blog',       202,  'ok',      '2025-06-03 15:00:00'),
  ('api_call',   1030, 'sess-ad0', '/api/search', 315,  'ok',      '2025-06-03 15:30:00');
SQL

echo "==> Inserting 50 more recent events (2025-06-04 to 2025-06-05) ..."

docker compose exec -T postgres psql -U postgres -d pipelinedemo <<'SQL'
INSERT INTO raw_events (event_type, user_id, session_id, page, duration_ms, status, created_at) VALUES
  ('page_view',  1031, 'sess-ad1', '/home',       139,  'ok',      '2025-06-04 08:00:00'),
  ('click',      1031, 'sess-ad1', '/home',        16,  'ok',      '2025-06-04 08:01:00'),
  ('page_view',  1032, 'sess-ad2', '/products',   222,  'ok',      '2025-06-04 08:15:00'),
  ('checkout',   1032, 'sess-ad2', '/checkout',   480,  'ok',      '2025-06-04 08:16:30'),
  ('api_call',   1033, 'sess-ad3', '/api/user',   265,  'ok',      '2025-06-04 09:00:00'),
  ('page_view',  1034, 'sess-ad4', '/cart',       195,  'ok',      '2025-06-04 09:30:00'),
  ('checkout',   1034, 'sess-ad4', '/checkout',   560,  'error',   '2025-06-04 09:31:55'),
  ('page_view',  1035, 'sess-ad5', '/blog',       185,  'ok',      '2025-06-04 10:00:00'),
  ('api_call',   1035, 'sess-ad5', '/api/search', 330,  'ok',      '2025-06-04 10:02:00'),
  ('api_call',   1036, 'sess-ad6', '/api/orders', 400,  'ok',      '2025-06-04 10:30:00'),
  ('page_view',  1037, 'sess-ad7', '/home',       155,  'ok',      '2025-06-04 11:00:00'),
  ('click',      1037, 'sess-ad7', '/products',    20,  'ok',      '2025-06-04 11:01:10'),
  ('page_view',  1038, 'sess-ad8', '/products',   210,  'ok',      '2025-06-04 11:30:00'),
  ('checkout',   1038, 'sess-ad8', '/checkout',   515,  'ok',      '2025-06-04 11:31:40'),
  ('api_call',   1039, 'sess-ad9', '/api/user',   285,  'ok',      '2025-06-04 12:00:00'),
  ('page_view',  1040, 'sess-ae0', '/home',       165,  'ok',      '2025-06-04 12:30:00'),
  ('click',      1040, 'sess-ae0', '/home',        13,  'ok',      '2025-06-04 12:31:00'),
  ('api_call',   1041, 'sess-ae1', '/api/search', 370,  'timeout', '2025-06-04 13:00:00'),
  ('page_view',  1042, 'sess-ae2', '/blog',       195,  'ok',      '2025-06-04 13:30:00'),
  ('checkout',   1042, 'sess-ae2', '/checkout',   500,  'ok',      '2025-06-04 13:32:00'),
  ('api_call',   1043, 'sess-ae3', '/api/orders', 295,  'ok',      '2025-06-04 14:00:00'),
  ('page_view',  1044, 'sess-ae4', '/products',   178,  'ok',      '2025-06-04 14:30:00'),
  ('click',      1044, 'sess-ae4', '/products',    17,  'ok',      '2025-06-04 14:31:05'),
  ('page_view',  1045, 'sess-ae5', '/cart',       192,  'ok',      '2025-06-04 15:00:00'),
  ('checkout',   1045, 'sess-ae5', '/checkout',   545,  'ok',      '2025-06-04 15:01:50'),
  ('page_view',  1046, 'sess-ae6', '/home',       144,  'ok',      '2025-06-05 08:00:00'),
  ('api_call',   1046, 'sess-ae6', '/api/user',   310,  'ok',      '2025-06-05 08:01:30'),
  ('page_view',  1047, 'sess-ae7', '/products',   230,  'ok',      '2025-06-05 08:30:00'),
  ('click',      1047, 'sess-ae7', '/products',    21,  'ok',      '2025-06-05 08:31:00'),
  ('api_call',   1048, 'sess-ae8', '/api/search', 350,  'ok',      '2025-06-05 09:00:00'),
  ('page_view',  1049, 'sess-ae9', '/cart',       205,  'ok',      '2025-06-05 09:30:00'),
  ('checkout',   1049, 'sess-ae9', '/checkout',   475,  'ok',      '2025-06-05 09:31:20'),
  ('api_call',   1050, 'sess-af0', '/api/orders', 380,  'ok',      '2025-06-05 10:00:00'),
  ('page_view',  1051, 'sess-af1', '/home',       158,  'ok',      '2025-06-05 10:30:00'),
  ('click',      1051, 'sess-af1', '/home',        14,  'ok',      '2025-06-05 10:31:00'),
  ('page_view',  1052, 'sess-af2', '/blog',       215,  'ok',      '2025-06-05 11:00:00'),
  ('api_call',   1052, 'sess-af2', '/api/search', 345,  'timeout', '2025-06-05 11:01:30'),
  ('checkout',   1053, 'sess-af3', '/checkout',   510,  'ok',      '2025-06-05 11:30:00'),
  ('page_view',  1054, 'sess-af4', '/products',   182,  'ok',      '2025-06-05 12:00:00'),
  ('click',      1054, 'sess-af4', '/products',    16,  'ok',      '2025-06-05 12:01:00'),
  ('api_call',   1055, 'sess-af5', '/api/user',   270,  'ok',      '2025-06-05 12:30:00'),
  ('page_view',  1056, 'sess-af6', '/home',       135,  'ok',      '2025-06-05 13:00:00'),
  ('checkout',   1056, 'sess-af6', '/checkout',   530,  'error',   '2025-06-05 13:01:40'),
  ('api_call',   1057, 'sess-af7', '/api/orders', 410,  'ok',      '2025-06-05 13:30:00'),
  ('page_view',  1058, 'sess-af8', '/cart',       175,  'ok',      '2025-06-05 14:00:00'),
  ('click',      1058, 'sess-af8', '/cart',        12,  'ok',      '2025-06-05 14:00:45'),
  ('page_view',  1059, 'sess-af9', '/blog',       198,  'ok',      '2025-06-05 14:30:00'),
  ('api_call',   1059, 'sess-af9', '/api/search', 325,  'ok',      '2025-06-05 14:31:30'),
  ('checkout',   1060, 'sess-ag0', '/checkout',   495,  'ok',      '2025-06-05 15:00:00'),
  ('page_view',  1060, 'sess-ag0', '/home',       150,  'ok',      '2025-06-05 15:30:00');
SQL

echo ""
echo "==> Database ready."
docker compose exec -T postgres psql -U postgres -d pipelinedemo -tAc \
  "SELECT COUNT(*) || ' rows in raw_events' FROM raw_events;"
echo ""
echo "    Timestamp range:"
docker compose exec -T postgres psql -U postgres -d pipelinedemo -tAc \
  "SELECT '    earliest: ' || MIN(created_at) FROM raw_events;"
docker compose exec -T postgres psql -U postgres -d pipelinedemo -tAc \
  "SELECT '    latest:   ' || MAX(created_at) FROM raw_events;"
echo ""
echo "    (No watermark file exists yet — run1.sh will process all 100 rows)"
