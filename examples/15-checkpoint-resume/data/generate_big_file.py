#!/usr/bin/env python3
"""
generate_big_file.py — Generate data/big_file.csv with 5000 rows.

Each row has three columns:
  id    — sequential integer starting at 1
  data  — a label string of the form "data_<id>"
  value — a pseudo-random integer in the range [1, 100]

The PRNG seed is fixed (42) so the file is reproducible across runs.
Approximately half the rows have value > 50 and will pass the pipeline filter;
the remainder will be dropped by the filter transform.

Usage:
  python3 generate_big_file.py          # writes to ./data/big_file.csv
  python3 generate_big_file.py 10000    # override row count
"""

import csv
import os
import random
import sys

ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
SEED = 42
OUT  = os.path.join(os.path.dirname(__file__), "big_file.csv")


def main() -> None:
    rng = random.Random(SEED)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)

    with open(OUT, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["id", "data", "value"])
        for i in range(1, ROWS + 1):
            writer.writerow([i, f"data_{i}", rng.randint(1, 100)])

    # Quick stats
    above = sum(1 for _ in open(OUT) if True)  # re-count properly below
    with open(OUT, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    total   = len(rows)
    passing = sum(1 for r in rows if int(r["value"]) > 50)
    dropped = total - passing

    print(f"Written {OUT}")
    print(f"  Total rows : {total}")
    print(f"  value > 50 : {passing}  (pass filter — processed by sink)")
    print(f"  value <= 50: {dropped}  (dropped by filter transform)")


if __name__ == "__main__":
    main()
