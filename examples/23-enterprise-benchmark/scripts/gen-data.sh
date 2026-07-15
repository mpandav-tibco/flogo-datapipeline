#!/usr/bin/env bash
# Generate the canonical orders dataset in every file format from a single
# deterministic CSV (seed 42, reproducible byte-for-byte).
#
#   ./scripts/gen-data.sh              # 53,000,000 rows (~5 GB, benchmark standard)
#   ./scripts/gen-data.sh 100000       # smaller, for a quick smoke run
set -euo pipefail
EX="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$EX/../.." && pwd)"
cd "$EX"
ROWS="${1:-53000000}"
mkdir -p data

echo "[1/3] CSV      ($ROWS rows) -> data/orders.csv"
( cd "$REPO" && CGO_ENABLED=1 go run ./examples/23-enterprise-benchmark/harness/datagen/orders-csv "$ROWS" "$EX/data/orders.csv" )

echo "[2/3] JSON Lines            -> data/orders.jsonl"
( cd "$REPO" && CGO_ENABLED=1 go run ./examples/23-enterprise-benchmark/harness/datagen/csv-to-jsonl "$EX/data/orders.csv" "$EX/data/orders.jsonl" )

echo "[3/3] Parquet (via engine)  -> data/orders.parquet"
[ -x bin/dp ] || ./scripts/build.sh
python3 harness/seed.py parquet

echo
echo "Dataset ready:"
ls -lh data/orders.csv data/orders.jsonl data/orders.parquet 2>/dev/null | awk '{print "  "$9"  "$5}'
