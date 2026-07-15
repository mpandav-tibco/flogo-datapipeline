#!/usr/bin/env python3
"""ELT round: push the transform down into PostgreSQL and run it against the
full orders_raw table (already loaded by the EL/_prep stage).

This is the "T-in-the-target" half of ELT — the pipeline engine only lands raw
rows; the heavy set-based transforms (curate, rollup, dedup) execute inside the
database where they belong. We time each statement server-side and record the
resulting row counts, then write elt_results.json for the report generator.

Runs SQL through `docker exec pgbench psql` so it needs no Python driver.
"""
import json
import os
import subprocess
import time

HERE = os.path.dirname(os.path.abspath(__file__))
EXAMPLE = os.path.dirname(HERE)
CONTAINER = os.environ.get("PG_CONTAINER", "pgbench")
DB = os.environ.get("PG_DB", "warehouse")
USER = os.environ.get("PG_USER", "postgres")

# Each case: (id, description, result_table, ddl). The DDL materialises a new
# table from orders_raw so the row count is verifiable after the run.
CASES = [
    ("elt01_curate", "Filter + normalise + derive net_amount in-database", "orders_curated", """
        DROP TABLE IF EXISTS orders_curated;
        CREATE TABLE orders_curated AS
        SELECT order_id,
               upper(region)                              AS region_code,
               category,
               quantity,
               round((quantity * unit_price)::numeric, 2) AS net_amount,
               status,
               order_date::date                           AS order_date
        FROM   orders_raw
        WHERE  status <> 'CANCELLED' AND unit_price > 0;
    """),
    ("elt02_rollup", "GROUP BY region x category revenue aggregate", "orders_rollup", """
        DROP TABLE IF EXISTS orders_rollup;
        CREATE TABLE orders_rollup AS
        SELECT upper(region)                                AS region_code,
               category,
               count(*)                                     AS orders,
               sum(quantity)                                AS units,
               round(sum(quantity * unit_price)::numeric,2) AS revenue
        FROM   orders_raw
        WHERE  status <> 'CANCELLED'
        GROUP  BY 1, 2;
    """),
    ("elt03_dedup", "Window de-duplication (latest row per order_id)", "orders_dedup", """
        DROP TABLE IF EXISTS orders_dedup;
        CREATE TABLE orders_dedup AS
        SELECT DISTINCT ON (order_id) *
        FROM   orders_raw
        ORDER  BY order_id, order_date DESC;
    """),
]


def psql(sql):
    return subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "psql", "-U", USER, "-d", DB,
         "-v", "ON_ERROR_STOP=1", "-tAc", sql],
        capture_output=True, text=True)


def count(table):
    r = psql("SELECT count(*) FROM %s;" % table)
    try:
        return int(r.stdout.strip())
    except ValueError:
        return -1


def main():
    src_rows = count("orders_raw")
    print("ELT round — orders_raw has %s rows" % f"{src_rows:,}")
    results = {}
    for cid, desc, table, ddl in CASES:
        t0 = time.time()
        r = psql(ddl)
        dur = int((time.time() - t0) * 1000)
        if r.returncode != 0:
            print("  %-14s FAILED: %s" % (cid, (r.stderr or r.stdout).strip()[:200]))
            results[cid] = {"desc": desc, "status": "failed", "duration_ms": dur,
                            "rows": 0, "err": (r.stderr or r.stdout).strip()[:300]}
            continue
        rows = count(table)
        rps = round(src_rows / (dur / 1000)) if dur else 0
        results[cid] = {"desc": desc, "status": "completed", "duration_ms": dur,
                        "rows": rows, "table": table, "scanned": src_rows, "rows_per_sec": rps}
        print("  %-14s %-45s %10s rows  %7dms  scan=%s/s"
              % (cid, desc, f"{rows:,}", dur, f"{rps:,}"))
    with open(os.path.join(HERE, "elt_results.json"), "w") as f:
        json.dump(results, f, indent=1)
    print("wrote elt_results.json")


if __name__ == "__main__":
    main()
