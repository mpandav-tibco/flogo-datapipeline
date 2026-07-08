#!/usr/bin/env bash
# run.sh — Execute the HTTP Lookup Enrichment pipeline.
#
# Usage:
#   ./run.sh
#
# Prerequisites:
#   - flogo-datapipeline binary is on your PATH
#   - Active internet connection (calls jsonplaceholder.typicode.com)
#   - Run from the directory that contains this script
#
# What happens:
#   1. Reads 10 rows from data/users.csv
#   2. For each unique user_id, sends GET https://jsonplaceholder.typicode.com/users/{user_id}
#      - Up to 5 concurrent requests
#      - Results are cached (LRU, capacity 100) so duplicate keys hit the cache
#      - Requests that exceed 3 000 ms timeout or return an error produce null fields
#   3. Maps the combined row (source + API fields) into the output shape
#   4. Discards rows via the null sink (no file written)
#
# Expected output:
#   Engine starts, enriches 10 rows with username/email/phone/website from the
#   JSONPlaceholder API, logs per-row output, then exits cleanly.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Running http-lookup-enrich pipeline..."
flogo-datapipeline -f http-lookup-enrich.datapipeline

echo ""
echo "==> Pipeline finished. 10 rows read, enriched via HTTP lookup, 0 rows written (null sink)."
