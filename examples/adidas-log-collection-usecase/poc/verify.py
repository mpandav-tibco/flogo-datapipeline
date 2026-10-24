#!/usr/bin/env python3
"""Verify end-to-end delivery of a POC scenario: every log event the BW-app
emitters wrote must be in S3 (or the node-local DLQ), exactly once or as an
identifiable duplicate, with multiline stack traces intact.

  poc/verify.py --prefix s1 [--expect-instances 60] [--json out.json]

Ground truth comes from the emitter itself: every event carries
[seq=<n> inst=<id>] (inst = one emitter process) and the last event of a
finished emitter is "EMIT_DONE total=<n>". For each (namespace, pod, inst):
  missing    = sequence numbers 1..N absent from S3 + DLQ (N = total when the
               emitter finished, else the highest seq seen: gaps only)
  duplicates = records delivered more than once (at-least-once replays)
  multiline  = stack-trace events whose stitched record lacks lines
Exit code 0 only when every expected instance finished and nothing is missing
or broken.
"""
import argparse
import collections
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys

SEQ = re.compile(r"\[seq=(\d+) inst=([0-9a-f]+)(?: lines=(\d+))?\]")
DONE = re.compile(r"EMIT_DONE total=(\d+)")
LEVELS = {"TRACE", "DEBUG", "INFO", "WARN", "WARNING", "ERROR", "FATAL"}


def sh(cmd, check=True):
    return subprocess.run(cmd, shell=True, check=check, capture_output=True, text=True).stdout


def parse_ts(s):
    if not s:
        return None
    s = s.strip().replace("Z", "+00:00")
    m = re.match(r"(.*\.\d{6})\d*(\+.*)$", s)  # trim nanoseconds to micro
    if m:
        s = m.group(1) + m.group(2)
    try:
        return dt.datetime.fromisoformat(s)
    except ValueError:
        return None


def pct(vals, p):
    if not vals:
        return None
    vals = sorted(vals)
    return vals[min(len(vals) - 1, int(round(p / 100.0 * (len(vals) - 1))))]


def fetch(prefix, minio, bucket, cache, cluster):
    """Mirror the scenario's objects + node DLQs into cache; return object metadata."""
    mc = "docker exec %s sh -c 'mc alias set l http://localhost:9000 minioadmin minioadmin >/dev/null && %%s'" % minio
    listing = sh(mc % ("mc ls --recursive --json l/%s/" % bucket), check=False)
    objs, all_keys = {}, []
    for line in listing.splitlines():
        try:
            o = json.loads(line)
        except ValueError:
            continue
        key = o.get("key", "")
        all_keys.append(key)
        if o.get("type") == "file" and key.startswith(prefix + "-team-"):
            objs[key] = o
    remote = "/tmp/verify-%s" % prefix
    sh(mc % ("rm -rf %s && mkdir -p %s" % (remote, remote)))
    nss = sorted({k.split("/", 1)[0] for k in objs})
    for ns in nss:
        sh(mc % ("mc mirror -q --overwrite l/%s/%s %s/%s >/dev/null" % (bucket, ns, remote, ns)))
    if os.path.isdir(cache):
        shutil.rmtree(cache)
    os.makedirs(cache)
    if nss:
        sh("docker cp %s:%s/. %s" % (minio, remote, cache))
    sh(mc % ("rm -rf %s" % remote), check=False)
    dlq = []
    # DLQ shipped to S3 (_dlq/...): all scenarios share it; filtered by namespace below.
    dlq_dir = os.path.join(cache, "_dlq")
    if any(k.startswith("_dlq/") for k in all_keys):
        sh(mc % ("rm -rf %s/_dlq && mc mirror -q --overwrite l/%s/_dlq %s/_dlq >/dev/null 2>&1 || true" % (remote, bucket, remote)), check=False)
        sh("docker cp %s:%s/_dlq %s 2>/dev/null || true" % (minio, remote, dlq_dir), check=False)
        sh(mc % ("rm -rf %s" % remote), check=False)
        for dp, _, files in os.walk(dlq_dir):
            for fn in files:
                if fn.endswith(".ndjson"):
                    with open(os.path.join(dp, fn)) as f:
                        dlq += [l for l in f if l.strip()]
    nodes = sh("k3d node list -o json")
    for n in json.loads(nodes):
        if cluster in n["name"] and n["role"] in ("server", "agent"):
            out = sh("docker exec %s sh -c 'cat /var/lib/flogo-datapipeline/dlq/*.ndjson 2>/dev/null'" % n["name"], check=False)
            dlq += [l for l in out.splitlines() if l.strip()]
    return objs, dlq


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--expect-instances", type=int, default=0, help="emitter processes expected to have finished")
    ap.add_argument("--minio", default="miniobench")
    ap.add_argument("--bucket", default="adidas-logs")
    ap.add_argument("--cluster", default="adidas-poc")
    ap.add_argument("--cache", default=None)
    ap.add_argument("--json", default=None)
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()
    cache = a.cache or os.path.join("/tmp", "adidas-poc-verify", a.prefix)
    objs, dlq_lines = fetch(a.prefix, a.minio, a.bucket, cache, a.cluster)

    inst = collections.defaultdict(lambda: {"seqs": collections.Counter(), "total": None, "ns": None, "pod": None})
    broken_multiline, bad_fields, no_seq = [], 0, 0
    latencies, records, objects, manifest_mismatch = [], 0, 0, 0
    services = collections.Counter()

    def account(rec, mtime, source):
        nonlocal bad_fields, no_seq, records
        records += 1
        raw = rec.get("raw_log") or rec.get("message") or ""
        m = SEQ.search(raw)
        if not m:
            no_seq += 1
            return
        seq, iid, lines = int(m.group(1)), m.group(2), m.group(3)
        k = (rec.get("namespace"), rec.get("pod"), iid)
        st = inst[k]
        st["ns"], st["pod"] = k[0], k[1]
        st["seqs"][seq] += 1
        d = DONE.search(raw)
        if d:
            st["total"] = int(d.group(1))
        if lines is not None and raw.count("\n") != int(lines):
            broken_multiline.append({"pod": k[1], "seq": seq, "want_lines": int(lines) + 1, "got_lines": raw.count("\n") + 1})
        ts = parse_ts(rec.get("@timestamp"))
        if source == "s3":
            if ts is None or rec.get("log_level") not in LEVELS or not isinstance(rec.get("severity"), int) \
                    or not rec.get("index_hint") or not rec.get("idempotency_key"):
                bad_fields += 1
            services[rec.get("service_name")] += 1
            if ts is not None and mtime is not None:
                latencies.append((mtime - ts).total_seconds())

    for key, meta in objs.items():
        path = os.path.join(cache, key)
        if key.endswith(".manifest.json"):
            try:
                man = json.load(open(path))
                with open(path[: -len(".manifest.json")]) as f:
                    if sum(1 for _ in f) != man.get("record_count"):
                        manifest_mismatch += 1
            except (OSError, ValueError):
                manifest_mismatch += 1
            continue
        objects += 1
        mtime = parse_ts(meta.get("lastModified"))
        with open(path) as f:
            for line in f:
                if line.strip():
                    account(json.loads(line), mtime, "s3")
    dlq_records = 0
    for line in dlq_lines:
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if str(rec.get("namespace", "")).startswith(a.prefix + "-team-"):
            dlq_records += 1
            account(rec, None, "dlq")

    finished = missing = gaps = dups = 0
    unfinished = []
    for k, st in inst.items():
        top = st["total"] or max(st["seqs"])
        present = set(st["seqs"])
        miss = top - len([s for s in present if s <= top])
        if st["total"]:
            finished += 1
            missing += miss
        else:
            gaps += miss
            unfinished.append({"pod": st["pod"], "max_seq": max(st["seqs"]), "gaps": miss})
        dups += sum(c - 1 for c in st["seqs"].values() if c > 1)
    events = sum(len(st["seqs"]) for st in inst.values())
    lat = {p: pct(latencies, p) for p in (50, 95, 99, 100)}
    ok = (missing == 0 and gaps == 0 and not broken_multiline and bad_fields == 0 and manifest_mismatch == 0
          and (not a.expect_instances or finished >= a.expect_instances))
    summary = {
        "scenario": a.prefix, "ok": ok,
        "instances": len(inst), "finished_instances": finished, "expected_instances": a.expect_instances,
        "distinct_events_delivered": events, "records_in_s3": records - dlq_records, "records_in_dlq": dlq_records,
        "s3_objects": objects, "missing_events": missing, "gaps_in_unfinished": gaps,
        "duplicate_records": dups, "broken_multiline": len(broken_multiline), "records_without_seq": no_seq,
        "bad_field_records": bad_fields, "manifest_mismatches": manifest_mismatch,
        "latency_s": {"p50": lat[50], "p95": lat[95], "p99": lat[99], "max": lat[100]},
        "unfinished": unfinished[:20], "broken_multiline_samples": broken_multiline[:5],
        "service_names": dict(services.most_common(12)),
    }
    if a.json:
        os.makedirs(os.path.dirname(os.path.abspath(a.json)), exist_ok=True)
        json.dump(summary, open(a.json, "w"), indent=1)
    if not a.quiet:
        print(json.dumps({k: v for k, v in summary.items() if k not in ("unfinished", "service_names")}, indent=1))
    else:
        print("%s: instances=%d finished=%d/%s events=%d missing=%d gaps=%d dup=%d s3=%d dlq=%d multiline_broken=%d" % (
            a.prefix, len(inst), finished, a.expect_instances or "?", events, missing, gaps, dups,
            records - dlq_records, dlq_records, len(broken_multiline)))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
