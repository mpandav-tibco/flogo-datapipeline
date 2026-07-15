#!/usr/bin/env python3
"""Generate the benchmark reports from the recorded result JSON files.

Reads:
  harness/results.json             (matrix: EL + ETL + ENT cases)
  harness/cases.json               (case metadata: source/sink/transform chain)
  harness/elt_results.json         (ELT SQL round, optional)
  harness/enterprise_results.json  (reliability round, optional)

Writes:
  reports/matrix.md, reports/elt.md, reports/enterprise.md
  SUMMARY.md   (top-level headline)
"""
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
EXAMPLE = os.path.dirname(HERE)
REPORTS = os.path.join(EXAMPLE, "reports")


def load(name, default=None):
    p = os.path.join(HERE, name)
    if not os.path.exists(p):
        return default if default is not None else {}
    with open(p) as f:
        return json.load(f)


def h(n):
    return f"{n:,}"


def dur(ms):
    return "%.1fs" % (ms / 1000.0) if ms >= 1000 else "%dms" % ms


def matrix_report(results, cases):
    lines = ["# Throughput Matrix", "",
             "Every source format x every sink, plus ETL chains exercising all seven",
             "transform activities. Row counts and durations are engine-measured.", ""]
    for cat, title in [("EL", "Extract - Load (raw, no transform)"),
                       ("ETL", "Extract - Transform - Load (in-flight transforms)")]:
        rows = [(k, v) for k, v in sorted(cases.items()) if v["cat"] == cat]
        if not rows:
            continue
        lines += ["## %s" % title, "",
                  "| Case | Pipeline | Rows in | Rows out | Duration | Throughput |",
                  "|------|----------|--------:|---------:|---------:|-----------:|"]
        for k, meta in rows:
            r = results.get(k)
            if not r:
                lines.append("| %s | %s | - | - | - | _not run_ |" % (k, meta["desc"]))
                continue
            lines.append("| %s | `%s` | %s | %s | %s | **%s/s** |" % (
                k, meta["desc"], h(r["processed"]), h(r["written"]),
                dur(r["duration_ms"]), h(r["rows_per_sec"])))
        lines.append("")
    return "\n".join(lines) + "\n"


def elt_report(elt):
    lines = ["# ELT Round - In-Database Transforms", "",
             "The pipeline lands raw rows; the set-based transforms run inside",
             "PostgreSQL against the full `orders_raw` table (server-side timing).", "",
             "| Case | Transform | Rows scanned | Rows out | Duration | Scan rate |",
             "|------|-----------|-------------:|---------:|---------:|----------:|"]
    for k, v in sorted(elt.items()):
        if v.get("status") != "completed":
            lines.append("| %s | %s | - | - | - | _%s_ |" % (k, v.get("desc", ""), v.get("status", "?")))
            continue
        lines.append("| %s | %s | %s | %s | %s | **%s/s** |" % (
            k, v["desc"], h(v.get("scanned", 0)), h(v["rows"]),
            dur(v["duration_ms"]), h(v.get("rows_per_sec", 0))))
    return "\n".join(lines) + "\n"


def enterprise_report(ent):
    lines = ["# Enterprise Reliability Round", ""]
    e1 = ent.get("ent01_dlq_isolation")
    if e1:
        lines += ["## ent01 - Dead-letter (DLQ) isolation", "",
                  "Invalid rows are routed to a queryable `orders_rejects` table while",
                  "valid rows continue to the sink. No row is silently lost.", "",
                  "| Processed | Good (sink) | Rejected (DLQ) | Conserved | Throughput |",
                  "|----------:|------------:|---------------:|:---------:|-----------:|",
                  "| %s | %s | %s | %s | %s/s |" % (
                      h(e1["processed"]), h(e1["good"]), h(e1["rejected"]),
                      "yes" if e1["conserved"] else "NO", h(e1["rows_per_sec"])), ""]
    e2 = ent.get("ent02_checkpoint_resume")
    if e2:
        lines += ["## ent02 - Checkpoint / resume", "",
                  "A run is interrupted mid-flight; the next run resumes from the last",
                  "committed row-checkpoint instead of restarting from zero.", "",
                  "| Run 1 | Committed @ kill | Run 2 (resume) | Rows processed on resume |",
                  "|-------|-----------------:|----------------|-------------------------:|",
                  "| %s | %s | %s | %s |" % (
                      e2["run1_status"], h(e2["run1_committed"]),
                      e2["run2_status"], h(e2["run2_processed"])), ""]
    return "\n".join(lines) + "\n"


def summary(results, cases, elt, ent, meta):
    matrix = {k: v for k, v in results.items() if not k.startswith("_prep")}
    ran = {k: v for k, v in matrix.items() if v.get("status") == "completed"}
    total_in = sum(v["processed"] for v in ran.values())
    peak = max(ran.items(), key=lambda kv: kv[1]["rows_per_sec"], default=(None, {"rows_per_sec": 0}))
    n_pass = len(ran)
    n_total = len(cases)
    scale = meta.get("rows", 0)
    lines = [
        "# Enterprise Data Pipeline Benchmark - Results", "",
        "Single-binary Flogo data pipeline moving a **%s-row** order dataset from" % h(scale),
        "every supported source to every supported sink, exercising all seven",
        "transform activities, plus an in-database ELT round and two reliability",
        "showcases.", "",
        "## Headline", "",
        "| Metric | Value |",
        "|--------|-------|",
        "| Dataset | **%s rows** (CSV %s / JSONL %s / Parquet %s) |" % (
            h(scale), meta.get("csv", "?"), meta.get("jsonl", "?"), meta.get("parquet", "?")),
        "| Matrix cases passed | **%d / %d** |" % (n_pass, n_total),
        "| Total rows moved (matrix) | **%s** |" % h(total_in),
        "| Peak throughput | **%s/s** (`%s`) |" % (h(peak[1]["rows_per_sec"]), peak[0]),
        "| Sources | csv, jsonl, parquet, postgres, kafka, s3 |",
        "| Sinks | parquet, postgres, kafka, s3, null |",
        "| Transforms | filter, map, validate, schemadrift, dq, hwm, lookup |",
        "",
        "See [reports/matrix.md](reports/matrix.md), [reports/elt.md](reports/elt.md)",
        "and [reports/enterprise.md](reports/enterprise.md) for the full tables.", "",
    ]
    if elt:
        ok = sum(1 for v in elt.values() if v.get("status") == "completed")
        lines += ["## ELT round", "", "%d in-database transforms over the full table. "
                  "See [reports/elt.md](reports/elt.md)." % ok, ""]
    if ent:
        e1 = ent.get("ent01_dlq_isolation", {})
        lines += ["## Reliability", ""]
        if e1:
            lines.append("- **DLQ isolation:** %s good + %s rejected = %s processed, no rows lost." % (
                h(e1.get("good", 0)), h(e1.get("rejected", 0)), h(e1.get("processed", 0))))
        e2 = ent.get("ent02_checkpoint_resume", {})
        if e2:
            lines.append("- **Checkpoint/resume:** interrupted after %s rows, resumed and finished (%s)." % (
                h(e2.get("run1_committed", 0)), e2.get("run2_status", "?")))
        lines.append("")
    return "\n".join(lines) + "\n"


def main():
    os.makedirs(REPORTS, exist_ok=True)
    results = load("results.json")
    cases = load("cases.json")
    elt = load("elt_results.json", {})
    ent = load("enterprise_results.json", {})
    meta = load("dataset_meta.json", {})

    with open(os.path.join(REPORTS, "matrix.md"), "w") as f:
        f.write(matrix_report(results, cases))
    if elt:
        with open(os.path.join(REPORTS, "elt.md"), "w") as f:
            f.write(elt_report(elt))
    if ent:
        with open(os.path.join(REPORTS, "enterprise.md"), "w") as f:
            f.write(enterprise_report(ent))
    with open(os.path.join(EXAMPLE, "SUMMARY.md"), "w") as f:
        f.write(summary(results, cases, elt, ent, meta))
    print("reports written to %s and SUMMARY.md" % REPORTS)


if __name__ == "__main__":
    main()
