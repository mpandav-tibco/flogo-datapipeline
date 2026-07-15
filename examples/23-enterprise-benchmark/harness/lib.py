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


def _parse_cputime(s):
    """Parse a `ps` cputime string (MM:SS.ss / HH:MM:SS / DD-HH:MM:SS) to seconds."""
    s = s.strip()
    if not s:
        return 0.0
    days = 0
    if "-" in s:
        d, s = s.split("-", 1)
        days = int(d)
    sec = 0.0
    for part in s.split(":"):
        sec = sec * 60 + float(part)
    return days * 86400 + sec


def _sample_resources(pid, stop_evt, state):
    """Poll RSS (KiB) and cumulative CPU time for pid; keep peak RSS and last cputime.

    state is a dict shared with the caller: {"peak_rss_kib": int, "cpu_s": float}.
    """
    while not stop_evt.is_set():
        try:
            out = subprocess.run(["ps", "-o", "rss=,cputime=", "-p", str(pid)],
                                 capture_output=True, text=True).stdout.strip()
            if out:
                parts = out.split()
                kib = int(parts[0])
                if kib > state["peak_rss_kib"]:
                    state["peak_rss_kib"] = kib
                if len(parts) > 1:
                    state["cpu_s"] = _parse_cputime(parts[1])
        except (ValueError, OSError, IndexError):
            pass
        stop_evt.wait(0.2)


_LINE = re.compile(
    r"run (completed|failed) .*?processed=(\d+) written=(\d+) failed=(\d+)"
    r"(?: dropped=(\d+))?(?: routed=(\d+))?(?: duration=(\d+)ms)?")


def run_pipe(binary, pipe_path, env=None, cwd=None, max_seconds=3600):
    """Run one pipe; return a result dict with counts, duration_ms, rows_per_sec,
    peak_rss_mb, cpu_seconds, cpu_cores, status.

    A watchdog thread force-terminates the daemon at ``max_seconds`` so a silent
    hang (no stdout) is still capped — the blocking ``for line in proc.stdout``
    loop cannot check the clock on its own. A second thread samples the process
    RSS and cumulative CPU time so we can report the memory + CPU footprint
    (the TCO metrics) for this single-instance run.
    """
    proc = subprocess.Popen([binary, "-f", pipe_path], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, bufsize=1, env=env, cwd=cwd)
    res, t0 = None, time.time()
    timed_out = threading.Event()

    stop_sampler = threading.Event()
    rstate = {"peak_rss_kib": 0, "cpu_s": 0.0}
    sampler = threading.Thread(target=_sample_resources, args=(proc.pid, stop_sampler, rstate))
    sampler.daemon = True
    sampler.start()

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
        stop_sampler.set()
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
    peak_rss_mb = round(rstate["peak_rss_kib"] / 1024.0, 1)
    cpu_seconds = round(rstate["cpu_s"], 1)
    if res is None and timed_out.is_set():
        res = {"processed": 0, "written": 0, "failed": 0, "dropped": 0, "routed": 0,
               "duration_ms": 0, "rows_per_sec": 0, "status": "timeout",
               "err": "exceeded %ds cap" % max_seconds}
    if res is None:
        res = {"processed": 0, "written": 0, "failed": 0, "dropped": 0, "routed": 0,
               "duration_ms": 0, "rows_per_sec": 0, "status": "no-output", "err": "no completion line"}
    res["peak_rss_mb"] = peak_rss_mb
    res["cpu_seconds"] = cpu_seconds
    # Average CPU cores utilised over the wall-clock run (>1.0 = multi-core).
    wall_s = res["duration_ms"] / 1000.0
    res["cpu_cores"] = round(cpu_seconds / wall_s, 2) if wall_s > 0 else 0.0
    return res
