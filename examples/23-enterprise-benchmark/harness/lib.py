"""Shared helper: run one flogo-datapipeline pipe to completion.

The engine runs a pipeline as a daemon (timer trigger fires it once, then the
engine idles). We start the process, read stdout until the "pipeline run
completed/failed" line (which carries the row counts and engine-measured
duration), then terminate it.
"""
import re
import subprocess
import threading
import time

_LINE = re.compile(
    r"run (completed|failed) .*?processed=(\d+) written=(\d+) failed=(\d+)"
    r"(?: dropped=(\d+))?(?: routed=(\d+))?(?: duration=(\d+)ms)?")


def run_pipe(binary, pipe_path, env=None, cwd=None, max_seconds=3600):
    """Run one pipe; return a result dict with counts, duration_ms, rows_per_sec, status.

    A watchdog thread force-terminates the daemon at ``max_seconds`` so a silent
    hang (no stdout) is still capped — the blocking ``for line in proc.stdout``
    loop cannot check the clock on its own.
    """
    proc = subprocess.Popen([binary, "-f", pipe_path], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, bufsize=1, env=env, cwd=cwd)
    res, t0 = None, time.time()
    timed_out = threading.Event()

    def _watchdog():
        proc.terminate()
        timed_out.set()

    wd = threading.Timer(max_seconds, _watchdog)
    wd.daemon = True
    wd.start()
    try:
        for line in proc.stdout:
            m = _LINE.search(line)
            if m:
                dur = int(m.group(7)) if m.group(7) else int((time.time() - t0) * 1000)
                processed = int(m.group(2))
                res = {
                    "processed": processed, "written": int(m.group(3)), "failed": int(m.group(4)),
                    "dropped": int(m.group(5) or 0), "routed": int(m.group(6) or 0),
                    "duration_ms": dur, "rows_per_sec": round(processed / (dur / 1000)) if dur else 0,
                    "status": "completed" if m.group(1) == "completed" else "failed", "err": "",
                }
                break
    finally:
        wd.cancel()
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
    if res is None and timed_out.is_set():
        res = {"processed": 0, "written": 0, "failed": 0, "dropped": 0, "routed": 0,
               "duration_ms": 0, "rows_per_sec": 0, "status": "timeout",
               "err": "exceeded %ds cap" % max_seconds}
    if res is None:
        res = {"processed": 0, "written": 0, "failed": 0, "dropped": 0, "routed": 0,
               "duration_ms": 0, "rows_per_sec": 0, "status": "no-output", "err": "no completion line"}
    return res
