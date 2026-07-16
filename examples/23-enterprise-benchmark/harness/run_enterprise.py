#!/usr/bin/env python3
"""Enterprise reliability round — two showcases that go beyond raw throughput:

  ent01  DLQ isolation      Bad rows are routed to a queryable reject table while
                            good rows still land in the sink. Nothing is lost:
                            good + rejected == processed.

  ent02  Checkpoint/resume  A long run is killed mid-flight; a second run resumes
                            from the last committed row-checkpoint instead of
                            restarting, so the total work done across both runs
                            equals the dataset (no rows reprocessed, none skipped).

Results are written to enterprise_results.json for the report generator.
"""
import glob
import json
import os
import subprocess
import sys
import time

import lib

HERE = os.path.dirname(os.path.abspath(__file__))
EXAMPLE = os.path.dirname(HERE)
REPO = os.path.dirname(os.path.dirname(EXAMPLE))
MONO = os.environ.get("UC_BIN")
DP_BUILD = os.path.join(EXAMPLE, "bin", "dp-build")
LEAN_DIR = os.path.join(EXAMPLE, "bin", "lean")
PIPES = os.path.join(EXAMPLE, "pipes")
CONTAINER = os.environ.get("PG_CONTAINER", "pgbench")
DB = os.environ.get("PG_DB", "warehouse")
USER = os.environ.get("PG_USER", "postgres")
CKPT_DIR = os.path.join(EXAMPLE, "checkpoints", "ent02")


def psql(sql):
    r = subprocess.run(["docker", "exec", "-i", CONTAINER, "psql", "-U", USER, "-d", DB,
                        "-tAc", sql], capture_output=True, text=True)
    return r.stdout.strip()


def pg_count(table):
    try:
        return int(psql("SELECT count(*) FROM %s;" % table))
    except ValueError:
        return -1


def env():
    e = dict(os.environ)
    e.setdefault("MINIO_AK", "minioadmin")
    e.setdefault("MINIO_SK", "minioadmin")
    return e


def ckpt_rows():
    """Return the max rowsProcessed across any persisted checkpoint token."""
    best = 0
    for p in glob.glob(os.path.join(CKPT_DIR, "*.ckpt.json")):
        try:
            with open(p) as f:
                best = max(best, int(json.load(f).get("rowsProcessed", 0)))
        except (OSError, ValueError, json.JSONDecodeError):
            pass
    return best


def run_ent01():
    print("== ent01  DLQ isolation ==")
    psql("TRUNCATE orders_rejects;")
    pipe = os.path.join(PIPES, "ent01_dlq_isolation.datapipeline")
    binp = MONO or lib.ensure_lean_bin(DP_BUILD, pipe, LEAN_DIR)
    r = lib.run_pipe(binp, pipe,
                     env=env(), cwd=EXAMPLE, max_seconds=int(os.environ.get("ENT_MAX_SECONDS", "3600")))
    good = r["written"]
    rejected = pg_count("orders_rejects")
    processed = r["processed"]
    conserved = (good + rejected == processed) if processed else False
    print("   processed=%s  good=%s  rejected=%s  conserved=%s"
          % (f"{processed:,}", f"{good:,}", f"{rejected:,}", conserved))
    return {"processed": processed, "good": good, "rejected": rejected,
            "duration_ms": r["duration_ms"], "rows_per_sec": r["rows_per_sec"],
            "conserved": conserved, "status": r["status"]}


def run_ent02(kill_seconds):
    print("== ent02  Checkpoint / resume ==")
    # clean slate so the first run starts from row 0
    for p in glob.glob(os.path.join(CKPT_DIR, "*")):
        os.remove(p)
    pipe = os.path.join(PIPES, "ent02_checkpoint_resume.datapipeline")
    binp = MONO or lib.ensure_lean_bin(DP_BUILD, pipe, LEAN_DIR)

    # Run 1: interrupted after kill_seconds by the harness watchdog.
    t0 = time.time()
    r1 = lib.run_pipe(binp, pipe, env=env(), cwd=EXAMPLE, max_seconds=kill_seconds)
    committed = ckpt_rows()
    interrupted = r1["status"] != "completed"
    print("   run1: status=%s  wall=%ds  checkpoint committed=%s rows"
          % (r1["status"], int(time.time() - t0), f"{committed:,}"))

    # Run 2: resume from the checkpoint, run to completion.
    r2 = lib.run_pipe(binp, pipe, env=env(), cwd=EXAMPLE,
                      max_seconds=int(os.environ.get("ENT_MAX_SECONDS", "3600")))
    print("   run2: status=%s  processed=%s  written=%s  dur=%dms"
          % (r2["status"], f"{r2['processed']:,}", f"{r2['written']:,}", r2["duration_ms"]))
    return {"run1_status": r1["status"], "run1_committed": committed, "interrupted": interrupted,
            "run2_status": r2["status"], "run2_processed": r2["processed"],
            "run2_written": r2["written"], "run2_duration_ms": r2["duration_ms"]}


def main():
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if not MONO:
        lib.ensure_dp_build(REPO, DP_BUILD)
    kill_seconds = int(os.environ.get("ENT02_KILL_SECONDS", "8"))
    out = {}
    if which in ("all", "ent01"):
        out["ent01_dlq_isolation"] = run_ent01()
    if which in ("all", "ent02"):
        out["ent02_checkpoint_resume"] = run_ent02(kill_seconds)
    with open(os.path.join(HERE, "enterprise_results.json"), "w") as f:
        json.dump(out, f, indent=1)
    print("wrote enterprise_results.json")


if __name__ == "__main__":
    main()
