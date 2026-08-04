#!/usr/bin/env bash
# setup.sh — Start PostgreSQL and seed both tables for the full-production example.
#
# Usage:
#   chmod +x setup.sh
#   ./setup.sh
#
# Prerequisites:
#   - Docker + Docker Compose  (the postgres service is defined in docker-compose.yml).
#   - Run from the directory containing this script (19-full-production/).
#
# What this script does:
#   1. Starts the local PostgreSQL container (docker compose up -d).
#   2. Waits for the database to accept connections.
#   3. Creates the raw_claims source table (dropping any previous copy first).
#   4. Creates the empty claims_processed sink table (the pipeline populates it).
#   5. Inserts 100 raw claims into raw_claims:
#        - 80 valid claims (CLM-00000001 to CLM-00000080):
#            claim_id format: CLM-XXXXXXXX (exactly 8 digits)
#            amount: 100 to 9,000,000 (stored as cents; pipeline divides by 100)
#            status: PENDING | APPROVED | REJECTED | UNDER_REVIEW
#            policy_id: 1–10 (maps to JSONPlaceholder user IDs 1–10)
#            submitted_at: 2025-01-01 to 2025-06-30 (first load window)
#        - 10 intentionally invalid claims (to exercise the validate + DLQ path):
#            bad_id_01..bad_id_05: claim_id fails the ^CLM-[0-9]{8}$ pattern
#            BAD-00000001..05: claim_id prefix wrong
#        - 10 out-of-range amount claims (amount=0 or amount>10,000,000)
#   6. Resets the watermark and checkpoint directories.
#
#   Both tables live in the single PostgreSQL database "pipelinedemo".

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Directories ──────────────────────────────────────────────────────────────
mkdir -p watermarks checkpoints

# Remove stale run artifacts so this script is idempotent
rm -f ./watermarks/*.hwm ./watermarks/*.json
rm -f ./checkpoints/*.json

echo "==> Starting PostgreSQL (docker compose up -d) ..."
docker compose up -d

echo "==> Waiting for PostgreSQL to become ready ..."
until docker compose exec -T postgres pg_isready -U postgres -d pipelinedemo >/dev/null 2>&1; do
  sleep 1
done
echo "    PostgreSQL is ready."

echo "==> Creating source table raw_claims in database pipelinedemo ..."

docker compose exec -T postgres psql -U postgres -d pipelinedemo <<'SQL'
-- ── Source schema ────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS raw_claims;
CREATE TABLE raw_claims (
    claim_id     TEXT    NOT NULL,   -- Surrogate key; format CLM-XXXXXXXX
    policy_id    INTEGER NOT NULL,   -- FK to policyholder (1–10 for JSONPlaceholder)
    amount       INTEGER NOT NULL,   -- Claim amount in CENTS to avoid float precision issues
    status       TEXT    NOT NULL,   -- PENDING | APPROVED | REJECTED | UNDER_REVIEW
    incident_date TEXT   NOT NULL,   -- Date of the insured incident  (YYYY-MM-DD)
    submitted_at TEXT    NOT NULL,   -- ISO-8601 datetime row entered the system
    adjuster_id  INTEGER             -- Internal adjuster; may be NULL for PENDING claims
);

-- ── Index on submitted_at for efficient HWM filtering ───────────────────────
CREATE INDEX idx_raw_claims_submitted_at ON raw_claims(submitted_at);

-- ────────────────────────────────────────────────────────────────────────────
-- BATCH 1: 80 VALID CLAIMS (CLM-00000001 to CLM-00000080)
-- Spread across 2025-01-01 to 2025-06-30 with policy_id 1–10.
-- amounts stored in cents (divide by 100 gives USD value).
-- ────────────────────────────────────────────────────────────────────────────
INSERT INTO raw_claims VALUES
  ('CLM-00000001', 3,   250000, 'PENDING',      '2025-01-02', '2025-01-03 08:14:22', NULL),
  ('CLM-00000002', 7,  1800000, 'APPROVED',     '2025-01-05', '2025-01-06 09:02:11', 42),
  ('CLM-00000003', 1,   750000, 'UNDER_REVIEW', '2025-01-07', '2025-01-08 10:33:45', 17),
  ('CLM-00000004', 5,  3200000, 'REJECTED',     '2025-01-10', '2025-01-11 11:00:00', 9),
  ('CLM-00000005', 2,   100000, 'PENDING',      '2025-01-12', '2025-01-13 08:45:30', NULL),
  ('CLM-00000006', 8,  5500000, 'APPROVED',     '2025-01-14', '2025-01-15 14:22:17', 33),
  ('CLM-00000007', 4,   420000, 'PENDING',      '2025-01-16', '2025-01-17 09:11:00', NULL),
  ('CLM-00000008', 6,  1100000, 'UNDER_REVIEW', '2025-01-18', '2025-01-19 13:05:44', 22),
  ('CLM-00000009', 10,  900000, 'APPROVED',     '2025-01-20', '2025-01-21 15:30:00', 55),
  ('CLM-00000010', 9,   680000, 'PENDING',      '2025-01-22', '2025-01-23 10:00:00', NULL),
  ('CLM-00000011', 3,  2400000, 'REJECTED',     '2025-01-25', '2025-01-26 08:20:00', 12),
  ('CLM-00000012', 1,   150000, 'PENDING',      '2025-01-28', '2025-01-29 09:45:00', NULL),
  ('CLM-00000013', 7,  7800000, 'APPROVED',     '2025-02-01', '2025-02-02 11:15:22', 67),
  ('CLM-00000014', 5,   340000, 'UNDER_REVIEW', '2025-02-03', '2025-02-04 08:55:10', 19),
  ('CLM-00000015', 2,  4600000, 'PENDING',      '2025-02-05', '2025-02-06 12:00:00', NULL),
  ('CLM-00000016', 8,   510000, 'APPROVED',     '2025-02-08', '2025-02-09 10:30:00', 44),
  ('CLM-00000017', 4,  1250000, 'REJECTED',     '2025-02-10', '2025-02-11 14:10:33', 8),
  ('CLM-00000018', 6,   870000, 'PENDING',      '2025-02-13', '2025-02-14 09:00:00', NULL),
  ('CLM-00000019', 10, 3100000, 'UNDER_REVIEW', '2025-02-15', '2025-02-16 11:45:00', 30),
  ('CLM-00000020', 9,   200000, 'APPROVED',     '2025-02-18', '2025-02-19 13:20:00', 58),
  ('CLM-00000021', 3,  6200000, 'PENDING',      '2025-02-20', '2025-02-21 08:30:00', NULL),
  ('CLM-00000022', 1,   490000, 'REJECTED',     '2025-02-24', '2025-02-25 10:15:00', 3),
  ('CLM-00000023', 7,  2000000, 'APPROVED',     '2025-02-26', '2025-02-27 12:40:00', 71),
  ('CLM-00000024', 5,   130000, 'PENDING',      '2025-03-01', '2025-03-02 09:05:00', NULL),
  ('CLM-00000025', 2,  8900000, 'UNDER_REVIEW', '2025-03-03', '2025-03-04 14:00:00', 26),
  ('CLM-00000026', 8,   610000, 'APPROVED',     '2025-03-06', '2025-03-07 08:50:00', 39),
  ('CLM-00000027', 4,  3800000, 'PENDING',      '2025-03-08', '2025-03-09 10:00:00', NULL),
  ('CLM-00000028', 6,   780000, 'REJECTED',     '2025-03-11', '2025-03-12 11:30:00', 14),
  ('CLM-00000029', 10, 1400000, 'APPROVED',     '2025-03-13', '2025-03-14 13:00:00', 62),
  ('CLM-00000030', 9,   320000, 'PENDING',      '2025-03-16', '2025-03-17 09:20:00', NULL),
  ('CLM-00000031', 3,  5700000, 'UNDER_REVIEW', '2025-03-18', '2025-03-19 10:45:00', 21),
  ('CLM-00000032', 1,   940000, 'APPROVED',     '2025-03-21', '2025-03-22 14:30:00', 50),
  ('CLM-00000033', 7,   270000, 'PENDING',      '2025-03-24', '2025-03-25 08:15:00', NULL),
  ('CLM-00000034', 5,  4100000, 'REJECTED',     '2025-03-27', '2025-03-28 11:00:00', 6),
  ('CLM-00000035', 2,  1700000, 'APPROVED',     '2025-03-30', '2025-03-31 09:45:00', 74),
  ('CLM-00000036', 8,   560000, 'PENDING',      '2025-04-01', '2025-04-02 08:00:00', NULL),
  ('CLM-00000037', 4,  9000000, 'UNDER_REVIEW', '2025-04-03', '2025-04-04 13:15:00', 35),
  ('CLM-00000038', 6,   380000, 'APPROVED',     '2025-04-06', '2025-04-07 10:30:00', 47),
  ('CLM-00000039', 10, 2300000, 'PENDING',      '2025-04-08', '2025-04-09 09:00:00', NULL),
  ('CLM-00000040', 9,   730000, 'REJECTED',     '2025-04-11', '2025-04-12 12:00:00', 11),
  ('CLM-00000041', 3,  1050000, 'APPROVED',     '2025-04-14', '2025-04-15 14:45:00', 63),
  ('CLM-00000042', 1,   460000, 'PENDING',      '2025-04-17', '2025-04-18 08:30:00', NULL),
  ('CLM-00000043', 7,  6800000, 'UNDER_REVIEW', '2025-04-20', '2025-04-21 11:00:00', 28),
  ('CLM-00000044', 5,   195000, 'APPROVED',     '2025-04-23', '2025-04-24 09:30:00', 52),
  ('CLM-00000045', 2,  3400000, 'PENDING',      '2025-04-25', '2025-04-26 08:00:00', NULL),
  ('CLM-00000046', 8,   825000, 'REJECTED',     '2025-04-28', '2025-04-29 13:20:00', 16),
  ('CLM-00000047', 4,  1900000, 'APPROVED',     '2025-05-01', '2025-05-02 10:15:00', 69),
  ('CLM-00000048', 6,   410000, 'PENDING',      '2025-05-04', '2025-05-05 09:00:00', NULL),
  ('CLM-00000049', 10, 7200000, 'UNDER_REVIEW', '2025-05-07', '2025-05-08 14:00:00', 36),
  ('CLM-00000050', 9,   640000, 'APPROVED',     '2025-05-10', '2025-05-11 11:30:00', 57),
  ('CLM-00000051', 3,  2700000, 'PENDING',      '2025-05-13', '2025-05-14 08:45:00', NULL),
  ('CLM-00000052', 1,   510000, 'REJECTED',     '2025-05-16', '2025-05-17 10:00:00', 4),
  ('CLM-00000053', 7,  4900000, 'APPROVED',     '2025-05-19', '2025-05-20 12:30:00', 72),
  ('CLM-00000054', 5,   175000, 'PENDING',      '2025-05-22', '2025-05-23 09:15:00', NULL),
  ('CLM-00000055', 2,  8100000, 'UNDER_REVIEW', '2025-05-25', '2025-05-26 13:45:00', 31),
  ('CLM-00000056', 8,   690000, 'APPROVED',     '2025-05-28', '2025-05-29 08:00:00', 45),
  ('CLM-00000057', 4,  3300000, 'PENDING',      '2025-05-31', '2025-06-01 10:30:00', NULL),
  ('CLM-00000058', 6,   855000, 'REJECTED',     '2025-06-02', '2025-06-03 11:15:00', 20),
  ('CLM-00000059', 10, 1600000, 'APPROVED',     '2025-06-05', '2025-06-06 14:00:00', 60),
  ('CLM-00000060', 9,   295000, 'PENDING',      '2025-06-08', '2025-06-09 09:30:00', NULL),
  ('CLM-00000061', 3,  5200000, 'UNDER_REVIEW', '2025-06-10', '2025-06-11 10:00:00', 24),
  ('CLM-00000062', 1,   980000, 'APPROVED',     '2025-06-13', '2025-06-14 12:15:00', 53),
  ('CLM-00000063', 7,   215000, 'PENDING',      '2025-06-16', '2025-06-17 08:30:00', NULL),
  ('CLM-00000064', 5,  4400000, 'REJECTED',     '2025-06-19', '2025-06-20 11:00:00', 7),
  ('CLM-00000065', 2,  1550000, 'APPROVED',     '2025-06-22', '2025-06-23 13:30:00', 75),
  ('CLM-00000066', 8,   370000, 'PENDING',      '2025-06-25', '2025-06-26 09:00:00', NULL),
  ('CLM-00000067', 4,  9800000, 'UNDER_REVIEW', '2025-06-27', '2025-06-28 14:30:00', 37),
  ('CLM-00000068', 6,   620000, 'APPROVED',     '2025-06-28', '2025-06-29 10:45:00', 48),
  ('CLM-00000069', 10, 2100000, 'PENDING',      '2025-06-29', '2025-06-30 08:00:00', NULL),
  ('CLM-00000070', 9,   810000, 'REJECTED',     '2025-06-29', '2025-06-30 09:15:00', 13),
  ('CLM-00000071', 3,  1300000, 'APPROVED',     '2025-06-29', '2025-06-30 10:30:00', 64),
  ('CLM-00000072', 1,   445000, 'PENDING',      '2025-06-29', '2025-06-30 11:45:00', NULL),
  ('CLM-00000073', 7,  6400000, 'UNDER_REVIEW', '2025-06-30', '2025-06-30 12:00:00', 29),
  ('CLM-00000074', 5,   230000, 'APPROVED',     '2025-06-30', '2025-06-30 13:00:00', 54),
  ('CLM-00000075', 2,  3700000, 'PENDING',      '2025-06-30', '2025-06-30 14:00:00', NULL),
  ('CLM-00000076', 8,   760000, 'REJECTED',     '2025-06-30', '2025-06-30 14:30:00', 18),
  ('CLM-00000077', 4,  1850000, 'APPROVED',     '2025-06-30', '2025-06-30 15:00:00', 70),
  ('CLM-00000078', 6,   590000, 'PENDING',      '2025-06-30', '2025-06-30 15:30:00', NULL),
  ('CLM-00000079', 10, 7600000, 'UNDER_REVIEW', '2025-06-30', '2025-06-30 16:00:00', 38),
  ('CLM-00000080', 9,   350000, 'APPROVED',     '2025-06-30', '2025-06-30 17:00:00', 59);

-- ────────────────────────────────────────────────────────────────────────────
-- BATCH 2: 10 INVALID CLAIM IDs — fail the ^CLM-[0-9]{8}$ pattern rule.
-- These will be caught by the validate transform and routed to the DLQ sink.
-- ────────────────────────────────────────────────────────────────────────────
INSERT INTO raw_claims VALUES
  ('bad_id_01',    1,  500000, 'PENDING',      '2025-03-05', '2025-03-05 08:00:00', NULL),
  ('bad_id_02',    2,  300000, 'APPROVED',     '2025-03-10', '2025-03-10 09:00:00', 10),
  ('bad_id_03',    3,  750000, 'REJECTED',     '2025-04-01', '2025-04-01 10:00:00', 15),
  ('BAD-00000001', 4,  120000, 'PENDING',      '2025-04-15', '2025-04-15 11:00:00', NULL),
  ('BAD-00000002', 5,  890000, 'APPROVED',     '2025-05-01', '2025-05-01 08:30:00', 20),
  ('CLM-1234',     6,  400000, 'UNDER_REVIEW', '2025-05-10', '2025-05-10 09:00:00', 25),
  ('CLM-ABCDEFGH', 7,  670000, 'PENDING',      '2025-05-20', '2025-05-20 10:00:00', NULL),
  ('CLAIM-001',    8,  230000, 'APPROVED',     '2025-06-01', '2025-06-01 11:00:00', 30),
  ('bad_id_04',    9,  555000, 'REJECTED',     '2025-06-10', '2025-06-10 12:00:00', 35),
  ('bad_id_05',   10,  810000, 'PENDING',      '2025-06-20', '2025-06-20 13:00:00', NULL);

-- ────────────────────────────────────────────────────────────────────────────
-- BATCH 3: 10 OUT-OF-RANGE AMOUNTS — fail the amount min=1 max=10000000 rule.
-- These will also be caught by the validate transform and routed to the DLQ.
-- ────────────────────────────────────────────────────────────────────────────
INSERT INTO raw_claims VALUES
  ('CLM-00000091', 1,         0, 'PENDING',      '2025-02-14', '2025-02-14 08:00:00', NULL),
  ('CLM-00000092', 2,        -1, 'APPROVED',     '2025-03-01', '2025-03-01 09:00:00', 5),
  ('CLM-00000093', 3, 10000001, 'REJECTED',     '2025-03-15', '2025-03-15 10:00:00', 10),
  ('CLM-00000094', 4, 50000000, 'PENDING',      '2025-04-01', '2025-04-01 11:00:00', NULL),
  ('CLM-00000095', 5,        0, 'UNDER_REVIEW', '2025-04-20', '2025-04-20 08:30:00', 15),
  ('CLM-00000096', 6, 99999999, 'APPROVED',     '2025-05-05', '2025-05-05 09:00:00', 20),
  ('CLM-00000097', 7,       -500, 'PENDING',    '2025-05-18', '2025-05-18 10:00:00', NULL),
  ('CLM-00000098', 8, 10500000, 'REJECTED',     '2025-06-02', '2025-06-02 11:00:00', 25),
  ('CLM-00000099', 9,        0, 'APPROVED',     '2025-06-15', '2025-06-15 12:00:00', 30),
  ('CLM-00000100', 10,       -1, 'PENDING',     '2025-06-28', '2025-06-28 13:00:00', NULL);
SQL

echo ""
echo "==> Source table seeded successfully."
echo ""
ROW_COUNT=$(docker compose exec -T postgres psql -U postgres -d pipelinedemo -tAc "SELECT COUNT(*) FROM raw_claims;")
VALID_COUNT=$(docker compose exec -T postgres psql -U postgres -d pipelinedemo -tAc "SELECT COUNT(*) FROM raw_claims WHERE claim_id LIKE 'CLM-________' AND LENGTH(claim_id)=12 AND amount >= 1 AND amount <= 10000000;")
INVALID_COUNT=$(( ROW_COUNT - VALID_COUNT ))
echo "    Total claims inserted : $ROW_COUNT"
echo "    Valid claims           : $VALID_COUNT  (will be enriched and upserted)"
echo "    Invalid claims         : $INVALID_COUNT (will be routed to DLQ)"
echo ""
echo "    Status breakdown (all rows):"
docker compose exec -T postgres psql -U postgres -d pipelinedemo -c \
    "SELECT status, COUNT(*) AS count FROM raw_claims GROUP BY status ORDER BY status;"
echo ""
echo "    Date range:"
docker compose exec -T postgres psql -U postgres -d pipelinedemo -tAc \
    "SELECT '    earliest submitted_at: ' || MIN(submitted_at) FROM raw_claims;"
docker compose exec -T postgres psql -U postgres -d pipelinedemo -tAc \
    "SELECT '    latest   submitted_at: ' || MAX(submitted_at) FROM raw_claims;"

# ── Create sink table (empty target) in the same pipelinedemo database ───────
echo ""
echo "==> Creating sink table claims_processed in database pipelinedemo ..."

docker compose exec -T postgres psql -U postgres -d pipelinedemo <<'SQL'
DROP TABLE IF EXISTS claims_processed;
CREATE TABLE claims_processed (
    claim_id        TEXT    NOT NULL PRIMARY KEY,
    policy_id       INTEGER NOT NULL,
    amount_usd      REAL    NOT NULL,   -- amount / 100 (dollars, not cents)
    claimant_name   TEXT    NOT NULL,   -- from HTTP lookup; 'UNKNOWN' if lookup fails
    claimant_email  TEXT    NOT NULL,   -- from HTTP lookup; 'UNKNOWN' if lookup fails
    status          TEXT    NOT NULL,
    processed_at    TEXT    NOT NULL    -- copied from submitted_at
);
SQL

echo "    Sink table 'claims_processed' created (empty — will be populated by the pipeline)."
echo ""
echo "==> Setup complete."
echo ""
echo "    Next steps:"
echo "      ./run.sh               — run the pipeline (first pass: processes all 80 valid claims)"
echo "      ./run.sh               — run again       (HWM skips rows already processed)"
echo "      ./add-claims.sh        — insert 20 new claims after the HWM cutoff"
echo "      ./run.sh               — run again       (only the 20 new claims are processed)"
echo "      ./verify.sh            — inspect the enriched database"
