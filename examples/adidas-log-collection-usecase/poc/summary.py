#!/usr/bin/env python3
"""Summarise poc/results/<scenario>/ into a Markdown table (stdout)."""
import csv
import collections
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.path.join(HERE, "results")
TITLES = {
    "s1": "Steady state — 60 BW-app pods, 12 namespaces",
    "s2": "Collector crash (SIGKILL) mid-stream",
    "s2r": "Collector rolling upgrade (SIGTERM) mid-stream",
    "s3": "S3 outage (120 s) mid-stream",
    "s4": "Log-rotation stress — ~1,000 events/s/pod, rotation at 1 MiB",
    "s5": "Short-lived pods — 40 Jobs deleted 5 s after finishing",
    "s6": "Worker node restart mid-stream",
    "s7": "S3 outage while kubelet rotates + gzips unread logs (~3 rotations/pod)",
    "c7": "CONTROL: s7 without compressed-rotation support",
    "c2": "CONTROL: collector crash on the pre-fix collector",
    "c4": "CONTROL: rotation stress on the pre-fix collector",
    "s8": "Scale — 400 containers/node (1,200 total), ~3,200 events/s/node, rotation+gzip, pod churn",
    "s9": "Scale + S3 outage (180 s) while 25% of pods are deleted — durable spool on",
    "c9": "CONTROL: s9 with the spool disabled",
}


def load(p, d=None):
    try:
        return json.load(open(p))
    except (OSError, ValueError):
        return d


def top(path):
    peak = collections.defaultdict(lambda: [0, 0, 0, 0])
    try:
        for r in csv.DictReader(open(path)):
            p = peak[r["node"]]
            c, m = int(r["cpu_m"] or 0), int(r["mem_mi"] or 0)
            p[0], p[1] = max(p[0], c), max(p[1], m)
            p[2] += c
            p[3] += 1
    except (OSError, ValueError, KeyError):
        return None, None, None
    if not peak:
        return None, None, None
    return (max(v[0] for v in peak.values()), max(v[1] for v in peak.values()),
            max(v[2] / v[3] for v in peak.values() if v[3]))


def main():
    rows = []
    for s in ["s1", "s2", "s2r", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "c2", "c4", "c7", "c9"]:
        d = os.path.join(RES, s)
        v = load(os.path.join(d, "verify.json"))
        if not v:
            continue
        run = load(os.path.join(d, "run.json"), {})
        cpu_peak, mem_peak, cpu_avg = top(os.path.join(d, "collector_top.csv"))
        events = v["distinct_events_delivered"]
        try:
            per_pod = int(re.search(r'EMIT_TOTAL, value: "(\d+)"', open(os.path.join(d, "workload.yaml")).read()).group(1))
        except (OSError, AttributeError):
            per_pod = run.get("per_instance") or 0  # scale scenarios (run-scale.sh)
        emitted = per_pod * (run.get("expected_instances") or 0)
        undelivered = max(0, emitted - events) if emitted else 0
        dur = run.get("duration_s") or 0
        lost = v["missing_events"] + v["gaps_in_unfinished"]
        rows.append("| %s | %s | %s | %s | %s | **%s** | **%s** | %s | %s | %s | %s | %s | %s |" % (
            s, TITLES.get(s, s), "✅ PASS" if v["ok"] else "❌ FAIL",
            f"{emitted:,}", f"{events:,}", f"{undelivered:,}", f"{lost:,}", f"{v['duplicate_records']:,}",
            f"{v['broken_multiline']:,}", f"{v['records_in_dlq']:,}",
            "%.0fs" % v["latency_s"]["p95"] if v["latency_s"]["p95"] is not None else "-",
            "%dm / %dm" % (cpu_avg, cpu_peak) if cpu_peak is not None else "-",
            "%d Mi" % mem_peak if mem_peak is not None else "-"))
    print("| # | Scenario | Result | Events emitted | Events delivered | Not delivered | Gaps (lost mid-stream) | Duplicates | Broken multiline | DLQ | p95 latency | Collector CPU avg / peak (per node) | Collector peak RSS |")
    print("|---|----------|--------|---------------:|-----------------:|--------------:|-----------------------:|-----------:|-----------------:|----:|------------:|------------------------------------:|-------------------:|")
    print("\n".join(rows))


if __name__ == "__main__":
    main()
