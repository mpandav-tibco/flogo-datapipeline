#!/usr/bin/env python3
"""Run the enterprise GRAPH (DAG) benchmark and capture per-pipe throughput.

These pipelines exercise the streaming graph runner (action/pipeline/graphexec)
— the engine takes it over only for a NON-REDUCIBLE topology (a typed edge or
fan-out/fan-in >= 2) that the single-chain linear engine cannot represent. Every
pipe here does ONE source read and fans the stream out to several sinks in a
single pass, which is the whole point: the linear engine would need one full
pass per destination.

For each pipe we:
  * reset every sink target (all fan-out branches) so the run reconciles clean,
  * build (once, cached) a lean per-pipe binary with dp-build — the same artifact
    you would deploy — unless UC_BIN forces a monolith,
  * run it to completion, capturing the engine's own `graph run complete` line
    (nodes/edges/read/written) plus the `pipeline run completed` counts,
  * sample peak RSS and CPU for the single process, and
  * measure each output branch so the fan-out is auditable.

Results land in graph_results.json for the report generator.

  python3 harness/run_graph.py                          # all reachable pipes
  python3 harness/run_graph.py graph01_route_region_parquet ...   # a subset
"""
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
EXAMPLE = os.path.dirname(HERE)
REPO = os.path.dirname(os.path.dirname(EXAMPLE))
PIPES = os.path.join(EXAMPLE, "pipes")
OUT = os.path.join(EXAMPLE, "out")
MONO = os.environ.get("UC_BIN")
DP_BUILD = os.path.join(EXAMPLE, "bin", "dp-build")
LEAN_DIR = os.path.join(EXAMPLE, "bin", "lean")
MC = os.environ.get("UC_MC", os.path.join(EXAMPLE, "bin", "mc"))
RESULTS = os.path.join(HERE, "graph_results.json")
MAX_SECONDS = int(os.environ.get("UC_MAX_SECONDS", "3600"))

os.chdir(EXAMPLE)
os.makedirs(OUT, exist_ok=True)
env = dict(os.environ, MINIO_AK=os.environ.get("MINIO_AK", "minioadmin"),
           MINIO_SK=os.environ.get("MINIO_SK", "minioadmin"))

# name -> (enterprise pattern, linear-equivalent passes the linear engine needs)
GRAPHS = {
    "graph01_route_region_parquet": ("regional fan-out router (5-way)", 5),
    "graph02_valreject_parquet": ("valid / reject split (DLQ)", 2),
    "graph03_tee_audit_parquet": ("tee + high-value audit", 2),
    "graph04_route_region_s3": ("regional fan-out router (5-way) -> S3", 5),
    "graph05_multi_consolidate": ("multi-source consolidation (3 feeds -> filter/map/validate -> 6 sinks)", 6),
}

_GRAPH_LINE = re.compile(
    r"graph run complete\s+.*?nodes=(\d+) edges=(\d+) read=(\d+) written=(\d+)")


def port_open(host, port):
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


S3_UP = port_open("localhost", 9000)
BACKEND_UP = {"s3": S3_UP, "s3-sink": S3_UP}


def load(name):
    with open(os.path.join(PIPES, name + ".datapipeline")) as f:
        return json.load(f)


def graph_sinks(data):
    """Every sink target in a graph pipe: the default/linear sink (the graph's
    plain `sink` node resolves to it) plus each self-contained xsink node that
    carries its own `config`. Returns a list of (type, config) tuples."""
    sinks = []
    if isinstance(data.get("sink"), dict):
        sinks.append((data["sink"]["type"], data["sink"]))
    for n in data.get("graph", {}).get("nodes", []):
        if n.get("kind") == "sink" and isinstance(n.get("config"), dict):
            sinks.append((n["type"], n["config"]))
    return sinks


def needs(name):
    req = set()
    for stype, _ in graph_sinks(load(name)["resources"][0]["data"]):
        if stype in BACKEND_UP:
            req.add(stype)
    return req


def reset(name):
    """Clear every fan-out branch so the run starts clean and reconciles exactly."""
    for stype, cfg in graph_sinks(load(name)["resources"][0]["data"]):
        if stype == "parquet" and cfg.get("path"):
            try:
                os.remove(cfg["path"])
            except OSError:
                pass
        elif stype == "s3-sink" and cfg.get("key") and S3_UP:
            subprocess.run([MC, "rm", "--force",
                            "local/%s/%s" % (cfg.get("bucket", "datalake"), cfg["key"])],
                           capture_output=True)


def branch_report(name):
    """Measure each output branch (rows are engine-counted; here we record the
    persisted byte size so the fan-out is auditable in the report)."""
    branches = []
    for stype, cfg in graph_sinks(load(name)["resources"][0]["data"]):
        if stype == "parquet" and cfg.get("path"):
            tag = os.path.basename(cfg["path"]).replace(name + "_", "").replace(".parquet", "")
            sz = os.path.getsize(cfg["path"]) if os.path.exists(cfg["path"]) else 0
            branches.append({"branch": tag, "type": "parquet", "bytes": sz})
        elif stype == "s3-sink" and cfg.get("key") and S3_UP:
            tag = os.path.basename(cfg["key"]).replace(".parquet", "")
            out = subprocess.run(
                [MC, "ls", "--json", "local/%s/%s" % (cfg.get("bucket", "datalake"), cfg["key"])],
                capture_output=True, text=True).stdout.strip()
            sz = 0
            for ln in out.splitlines():
                try:
                    sz = int(json.loads(ln).get("size", 0))
                    break
                except (ValueError, json.JSONDecodeError):
                    pass
            branches.append({"branch": tag, "type": "s3", "bytes": sz})
        elif stype == "null":
            branches.append({"branch": "dlq", "type": "null", "bytes": 0})
    return branches


def run_graph_pipe(binp, pipe_path):
    """Run one graph pipe; capture full stdout to a log (to read the engine's
    `graph run complete` line), parse the completion counts, and read the child's
    kernel rusage (peak RSS + CPU) via os.wait4 — which needs no process
    enumeration, so it survives sandboxes that block ps/sysmond."""
    log_path = os.path.join(OUT, os.path.basename(pipe_path).replace(".datapipeline", ".graph.log"))
    logf = open(log_path, "w")
    proc = subprocess.Popen([binp, "-f", pipe_path], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, bufsize=1, env=env, cwd=EXAMPLE)
    res, t0 = None, time.time()
    graph_meta = {}
    timed_out = threading.Event()

    def _watchdog():
        proc.terminate()
        timed_out.set()
    wd = threading.Timer(MAX_SECONDS, _watchdog)
    wd.daemon = True
    wd.start()
    maxrss_bytes, cpu_s = 0, 0.0
    try:
        for line in proc.stdout:
            logf.write(line)
            g = _GRAPH_LINE.search(line)
            if g:
                graph_meta = {"nodes": int(g.group(1)), "edges": int(g.group(2)),
                              "read": int(g.group(3)), "written": int(g.group(4))}
            m = lib._LINE.search(line)
            if m:
                dur = int(m.group(7)) if m.group(7) else int((time.time() - t0) * 1000)
                processed = int(m.group(2))
                res = {"processed": processed, "written": int(m.group(3)), "failed": int(m.group(4)),
                       "dropped": int(m.group(5) or 0), "routed": int(m.group(6) or 0),
                       "duration_ms": dur,
                       "rows_per_sec": round(processed / (dur / 1000)) if dur else 0,
                       "status": "completed" if m.group(1) == "completed" else "failed"}
                break
    finally:
        wd.cancel()
        proc.terminate()
        # Reap the child ourselves so we get its rusage straight from the kernel
        # (ru_maxrss is bytes on macOS). This avoids ps/pgrep, which the sandbox
        # blocks ("sysmond service not found"). Mark the Popen reaped afterwards.
        try:
            _, _, ru = os.wait4(proc.pid, 0)
            maxrss_bytes = ru.ru_maxrss
            cpu_s = ru.ru_utime + ru.ru_stime
            proc.returncode = 0
        except (ChildProcessError, OSError):
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
        logf.close()
    if res is None:
        res = {"processed": 0, "written": 0, "failed": 0, "dropped": 0, "routed": 0,
               "duration_ms": 0, "rows_per_sec": 0,
               "status": "timeout" if timed_out.is_set() else "no-output"}
    res["peak_rss_mb"] = round(maxrss_bytes / 1048576.0, 1)
    res["cpu_seconds"] = round(cpu_s, 1)
    wall = res["duration_ms"] / 1000.0
    res["cpu_cores"] = round(cpu_s / wall, 2) if wall > 0 else 0.0
    res["graph"] = graph_meta
    res["graph_engaged"] = bool(graph_meta)
    return res


def main():
    sel = sys.argv[1:] or list(GRAPHS.keys())
    if not MONO and not os.path.exists(DP_BUILD):
        print("building dp-build (import-driven lean builder)…")
        subprocess.run(["go", "build", "-o", DP_BUILD, "./cmd/dp-build"], cwd=REPO, check=True)
    try:
        results = json.load(open(RESULTS))
    except (OSError, ValueError):
        results = {}
    print("binary: %s" % (("monolith " + MONO) if MONO else "lean per-pipe (dp-build)"))
    print("backends: s3=%s\n" % ("up" if S3_UP else "DOWN"))
    for name in sel:
        base = name[:-2] if name.endswith("_v") and name[:-2] in GRAPHS else name
        if base not in GRAPHS:
            print("%-34s UNKNOWN pipe" % name)
            continue
        pattern, passes = GRAPHS[base]
        missing = [b for b in needs(name) if not BACKEND_UP.get(b)]
        if missing:
            print("%-34s SKIPPED  needs %s (down)" % (name, ",".join(sorted(missing))))
            continue
        pipe = os.path.join(PIPES, name + ".datapipeline")
        binp = MONO or lib.ensure_lean_bin(DP_BUILD, pipe, LEAN_DIR)
        reset(name)
        res = run_graph_pipe(binp, pipe)
        res["pattern"] = pattern
        res["linear_passes"] = passes
        res["branches"] = branch_report(name)
        results[name] = res
        json.dump(results, open(RESULTS, "w"), indent=1)
        gm = res["graph"]
        print("%-34s %-9s read=%9d written=%9d dur=%7dms thr=%9d/s rss=%6.0fMB cpu=%4.1fc graph=%s%s" % (
            name, res["status"], gm.get("read", res["processed"]), res["written"],
            res["duration_ms"], res["rows_per_sec"], res.get("peak_rss_mb", 0),
            res.get("cpu_cores", 0), "yes" if res["graph_engaged"] else "NO",
            "" if res["graph_engaged"] else "  (!! ran linear — not a graph run)"))
    json.dump(results, open(RESULTS, "w"), indent=1)
    print("\nDONE -> %s" % RESULTS)


if __name__ == "__main__":
    main()
