#!/usr/bin/env python3
"""Seed the backends from data/orders.csv via the engine's _prep_ pipes and
materialise data/orders.parquet, so every source format holds the same rows.

  python3 harness/seed.py                 # parquet + pg + kafka + s3
  python3 harness/seed.py parquet kafka   # a subset
"""
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
EXAMPLE = os.path.dirname(HERE)
REPO = os.path.dirname(os.path.dirname(EXAMPLE))
BIN = os.path.join(EXAMPLE, "bin", "dp")
PIPES = os.path.join(EXAMPLE, "pipes")
env = dict(os.environ, MINIO_AK=os.environ.get("MINIO_AK", "minioadmin"),
           MINIO_SK=os.environ.get("MINIO_SK", "minioadmin"))


def run_prep(name):
    r = lib.run_pipe(BIN, os.path.join(PIPES, name + ".datapipeline"),
                     env=env, cwd=EXAMPLE, max_seconds=7200)
    print("%-24s %-10s processed=%d written=%d dur=%dms %s" % (
        name, r["status"], r["processed"], r["written"], r["duration_ms"], r["err"]))
    return r


def main():
    sel = sys.argv[1:] or ["parquet", "pg", "kafka", "s3"]
    if "parquet" in sel:
        run_prep("_prep_orders_parquet")
    if "pg" in sel:
        subprocess.run(["docker", "exec", "pgbench", "psql", "-U", "postgres", "-d", "warehouse",
                        "-c", "TRUNCATE orders_raw;"], capture_output=True)
        run_prep("_prep_load_pg")
    if "kafka" in sel:
        subprocess.run(["go", "run", "./harness/datagen/kafka-topic", "localhost:9092", "orders"],
                       cwd=EXAMPLE, capture_output=True)
        run_prep("_prep_load_kafka")
    if "s3" in sel:
        run_prep("_prep_load_s3")
    print("seeding done")


if __name__ == "__main__":
    main()
