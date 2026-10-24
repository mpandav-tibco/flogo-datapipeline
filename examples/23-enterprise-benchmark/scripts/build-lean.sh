#!/usr/bin/env bash
# Build LEAN, per-pipeline binaries with the import-driven `dp-build` tool (the
# datapipeline analogue of `flogo build`) and compare their sizes to the
# all-in-one benchmark binary.
#
# Each lean binary links ONLY the components its pipeline references — derived
# automatically from the app by cmd/dp-build (see build/imports.go). It is
# CGO-free unless the app uses the sqlite driver, and ships as one small static
# file: the real per-app deployment footprint that matters for TCO.
set -euo pipefail
EX="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$EX/../.." && pwd)"
cd "$REPO"
GO="${GO:-go}"
OUT="$EX/bin/lean"
mkdir -p "$OUT"

echo "Building the import-driven builder (cmd/dp-build)…"
CGO_ENABLED=1 "$GO" build -o "$EX/bin/dp-build" ./cmd/dp-build

# Representative pipelines spanning every sink type + a full ETL chain.
CASES=(el05_csv_null el02_csv_postgres el04_csv_s3 el14_kafka_parquet etl04_csv_full_s3)

echo "Building lean binaries from their .datapipeline apps…"
for c in "${CASES[@]}"; do
  ( cd "$EX" && ./bin/dp-build -f "pipes/$c.datapipeline" -o "bin/lean/dp-$c" )
done

# The all-in-one size is measured on a stripped build, like the lean binaries
# (dp-build links with -s -w), so the two are comparable.
CGO_ENABLED=1 "$GO" build -trimpath -ldflags "-s -w" -o "$EX/bin/dp-aio-stripped" ./cmd/flogo-datapipeline

echo
printf "%-24s %7s   %s\n" "binary" "size" "links only"
printf "%-24s %7s   %s\n" "------------------------" "-------" "----------------------------"
sz() { du -m "$1" 2>/dev/null | cut -f1; }
printf "%-24s %6sM   %s\n" "dp-el05_csv_null"     "$(sz "$OUT/dp-el05_csv_null")"     "csv + null"
printf "%-24s %6sM   %s\n" "dp-el02_csv_postgres" "$(sz "$OUT/dp-el02_csv_postgres")" "csv + jdbc + lib/pq"
printf "%-24s %6sM   %s\n" "dp-el04_csv_s3"       "$(sz "$OUT/dp-el04_csv_s3")"       "csv + s3 + parquet"
printf "%-24s %6sM   %s\n" "dp-el14_kafka_parquet" "$(sz "$OUT/dp-el14_kafka_parquet")" "kafka + parquet"
printf "%-24s %6sM   %s\n" "dp-etl04_csv_full_s3" "$(sz "$OUT/dp-etl04_csv_full_s3")" "csv + 3 transforms + s3"
printf "%-24s %6sM   %s\n" "dp (all-in-one)" "$(sz "$EX/bin/dp-aio-stripped")" "EVERY source, sink, driver"

# Machine-readable sizes for the report generator (reports/tco.md).
python3 - "$OUT" "$EX/bin/dp-aio-stripped" "$EX/harness/binsizes.json" "${CASES[@]}" <<'PY'
import json, os, sys
out, mono, dest = sys.argv[1], sys.argv[2], sys.argv[3]
cases = sys.argv[4:]
def mb(p):
    return round(os.path.getsize(p) / 1048576.0, 1) if os.path.exists(p) else None
data = {c: mb(os.path.join(out, "dp-" + c)) for c in cases}
data["all_in_one"] = mb(mono)
json.dump(data, open(dest, "w"), indent=1)
print("\nwrote", dest)
PY
