#!/usr/bin/env python3
"""Run the benchmark matrix and capture per-case throughput into results.json.

The flogo-datapipeline engine runs a pipeline as a daemon (a timer trigger fires
it once at startup, then the engine idles). So for each case we start the binary,
read stdout until the "pipeline run completed" line (which carries the row counts
and engine-measured duration), record it, and terminate the process.

Run from the example directory. Auto-detects which backends are reachable and
skips cases needing a backend that is down, so the file-only cases run with zero
infrastructure.

  python3 harness/run_matrix.py                      # all reachable cases
  python3 harness/run_matrix.py el01_csv_parquet ...  # a subset
"""
import json
import os
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
EXAMPLE = os.path.dirname(HERE)
REPO = os.path.dirname(os.path.dirname(EXAMPLE))
PIPES = os.path.join(EXAMPLE, "pipes")
OUT = os.path.join(EXAMPLE, "out")
# Default: each pipe runs on its OWN lean binary compiled by dp-build (the exact
# artifact you would deploy). Set UC_BIN to force one monolith binary instead.
MONO = os.environ.get("UC_BIN")
DP_BUILD = os.path.join(EXAMPLE, "bin", "dp-build")
LEAN_DIR = os.path.join(EXAMPLE, "bin", "lean")
MC = os.environ.get("UC_MC", os.path.join(EXAMPLE, "bin", "mc"))
RESULTS = os.path.join(HERE, "results.json")
CASES = os.path.join(HERE, "cases.json")
MAX_SECONDS = int(os.environ.get("UC_MAX_SECONDS", "3600"))  # per-case safety cap

os.chdir(EXAMPLE)
os.makedirs(OUT, exist_ok=True)
env = dict(os.environ, MINIO_AK=os.environ.get("MINIO_AK", "minioadmin"),
           MINIO_SK=os.environ.get("MINIO_SK", "minioadmin"))
cases = json.load(open(CASES))


def _port_open(host, port):
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


PG_UP = subprocess.run(["docker", "exec", "pgbench", "pg_isready", "-U", "postgres"],
                       capture_output=True).returncode == 0
KAFKA_UP = _port_open("localhost", 9092)
S3_UP = _port_open("localhost", 9000)
BACKEND_UP = {"jdbc": PG_UP, "postgres": PG_UP, "kafka": KAFKA_UP, "s3": S3_UP, "s3-sink": S3_UP}


def needs(name):
    d = json.load(open(os.path.join(PIPES, name + ".datapipeline")))["resources"][0]["data"]
    req = set()
    for t in (d["source"]["type"], d["sink"]["type"]):
        if t in ("jdbc", "postgres", "kafka", "s3", "s3-sink"):
            req.add(t)
    return req


def pg(sql):
    subprocess.run(["docker", "exec", "pgbench", "psql", "-U", "postgres", "-d", "warehouse", "-c", sql],
                   capture_output=True)


def reset(defpath):
    """Reset the sink target so each case starts clean and reconciles exactly."""
    snk = json.load(open(defpath))["resources"][0]["data"]["sink"]
    t = snk["type"]
    if t == "jdbc" and PG_UP:
        pg("TRUNCATE %s;" % snk["table"])
    elif t == "parquet":
        try:
            os.remove(snk["path"])
        except OSError:
            pass
    elif t == "s3-sink" and S3_UP:
        subprocess.run([MC, "rm", "--force", "local/%s/%s" % (snk["bucket"], snk["key"])],
                       capture_output=True)


def stage(name):
    """Kafka-source cases get a fresh consumer group so 'earliest' re-reads the
    whole topic; the committed pipe stays pristine."""
    p = os.path.join(PIPES, name + ".datapipeline")
    d = json.load(open(p))
    src = d["resources"][0]["data"]["source"]
    if src.get("type") == "kafka":
        src["options"]["groupId"] = "g_%s_%d" % (name, int(time.time() * 1000))
        tmp = os.path.join(OUT, "_run_%s.datapipeline" % name)
        json.dump(d, open(tmp, "w"))
        return tmp
    return p


def reclaim(defpath):
    """Drop multi-GB per-case outputs so disk stays bounded across the matrix."""
    snk = json.load(open(defpath))["resources"][0]["data"]["sink"]
    if snk["type"] == "parquet":
        try:
            os.remove(snk["path"])
        except OSError:
            pass
    elif snk["type"] == "kafka" and KAFKA_UP and snk.get("topic") != "orders":
        subprocess.run(["go", "run", "./harness/datagen/kafka-topic", "localhost:9092", snk["topic"]],
                       capture_output=True)


LINE_MAX = int(os.environ.get("UC_MAX_SECONDS", "3600"))


def run(name):
    binp = MONO or lib.ensure_lean_bin(DP_BUILD, os.path.join(PIPES, name + ".datapipeline"), LEAN_DIR)
    p = stage(name)
    reset(p)
    res = lib.run_pipe(binp, p, env=env, cwd=EXAMPLE, max_seconds=MAX_SECONDS)
    reclaim(p)
    return res


def main():
    sel = sys.argv[1:] or list(cases.keys())
    if not MONO and not os.path.exists(DP_BUILD):
        print("building dp-build (import-driven lean builder)…")
        subprocess.run(["go", "build", "-o", DP_BUILD, "./cmd/dp-build"], cwd=REPO, check=True)
    try:
        results = json.load(open(RESULTS))
    except (OSError, ValueError):
        results = {}
    print("binary: %s" % (("monolith " + MONO) if MONO else "lean per-pipe (dp-build)"))
    print("backends: postgres=%s kafka=%s s3=%s\n" % (
        "up" if PG_UP else "DOWN", "up" if KAFKA_UP else "DOWN", "up" if S3_UP else "DOWN"))
    for name in sel:
        req = needs(name)
        missing = [b for b in req if not BACKEND_UP.get(b)]
        if missing:
            print("%-32s %-4s SKIPPED    needs %s (down)" % (name, cases[name]["cat"], ",".join(sorted(req))))
            continue
        res = run(name)
        results[name] = dict(cases[name], **res)
        json.dump(results, open(RESULTS, "w"), indent=1)  # persist after every case
        print("%-32s %-4s %-10s proc=%8d wr=%8d dur=%7dms thr=%9d/s rss=%6.0fMB cpu=%4.1fc %s" % (
            name, cases[name]["cat"], res["status"], res["processed"], res["written"],
            res["duration_ms"], res["rows_per_sec"], res.get("peak_rss_mb", 0),
            res.get("cpu_cores", 0), res["err"]))
    json.dump(results, open(RESULTS, "w"), indent=1)
    print("\nDONE -> %s" % RESULTS)


if __name__ == "__main__":
    main()
