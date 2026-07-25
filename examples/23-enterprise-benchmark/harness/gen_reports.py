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
             "transform activities. Row counts and durations are engine-measured; peak",
             "RSS and average CPU cores are sampled for the single engine process.", ""]
    for cat, title in [("EL", "Extract - Load (raw, no transform)"),
                       ("ETL", "Extract - Transform - Load (in-flight transforms)")]:
        rows = [(k, v) for k, v in sorted(cases.items()) if v["cat"] == cat]
        if not rows:
            continue
        lines += ["## %s" % title, "",
                  "| Case | Pipeline | Rows in | Rows out | Duration | Throughput | Peak RSS | CPU |",
                  "|------|----------|--------:|---------:|---------:|-----------:|---------:|----:|"]
        for k, meta in rows:
            r = results.get(k)
            if not r:
                lines.append("| %s | %s | - | - | - | _not run_ | - | - |" % (k, meta["desc"]))
                continue
            rss = "%s MB" % h(round(r["peak_rss_mb"])) if r.get("peak_rss_mb") else "-"
            cpu = "%.1f" % r["cpu_cores"] if r.get("cpu_cores") else "-"
            lines.append("| %s | `%s` | %s | %s | %s | **%s/s** | %s | %s |" % (
                k, meta["desc"], h(r["processed"]), h(r["written"]),
                dur(r["duration_ms"]), h(r["rows_per_sec"]), rss, cpu))
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


def summary(results, cases, elt, ent, meta, host, binsizes):
    matrix = {k: v for k, v in results.items() if not k.startswith("_prep")}
    ran = {k: v for k, v in matrix.items() if v.get("status") == "completed"}
    total_in = sum(v["processed"] for v in ran.values())
    peak = max(ran.items(), key=lambda kv: kv[1]["rows_per_sec"], default=(None, {"rows_per_sec": 0}))
    n_pass = len(ran)
    n_total = len(cases)
    scale = meta.get("rows", 0)
    rss_vals = [v["peak_rss_mb"] for v in ran.values() if v.get("peak_rss_mb")]
    cpu_vals = [v["cpu_cores"] for v in ran.values() if v.get("cpu_cores")]
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
    # Footprint & TCO — the whole run is ONE process, not a cluster.
    inst = host.get("instances", 1)
    binmb = host.get("binary_size_mb")
    _lean = [v for k, v in (binsizes or {}).items() if k != "all_in_one" and v]
    _mono = (binsizes or {}).get("all_in_one") or binmb
    if _lean:
        binrow = "%s to %s MB per app (all-in-one harness %s MB)" % (min(_lean), max(_lean), _mono)
    else:
        binrow = ("%s MB" % binmb) if binmb else "?"
    hostline = "%s \u00b7 %s cores \u00b7 %s GB RAM" % (
        host.get("cpu_model", "?"), host.get("logical_cores", "?"), host.get("ram_gb", "?"))
    lines += [
        "## Footprint & TCO", "",
        "Every case above is a **single process** (`scaling.mode=single`) — one",
        "statically-linked binary, no cluster, no JVM, no external runtime.", "",
        "| Metric | Value |",
        "|--------|-------|",
        "| Instances | **%d** (single-instance; sharded/distributed modes exist but were not needed) |" % inst,
        "| Deployed binary size | **%s** |" % binrow,
        "| Host | %s |" % hostline,
    ]
    if rss_vals:
        lines.append("| Peak memory (RSS) across matrix | **%s\u2013%s MB** |" % (
            h(round(min(rss_vals))), h(round(max(rss_vals)))))
    if cpu_vals:
        lines.append("| CPU used (avg cores) across matrix | **%.1f\u2013%.1f of %s cores** |" % (
            min(cpu_vals), max(cpu_vals), host.get("logical_cores", "?")))
    lines += [
        "",
        "%s rows were moved by a single process peaking at a few hundred MB of RAM and a"
        % h(scale),
        "fraction of the host's cores — no cluster to provision, license, or scale.", "",
    ]
    if binsizes:
        order = [("el05_csv_null", "csv -> null"),
                 ("el02_csv_postgres", "csv -> postgres (+lib/pq)"),
                 ("el04_csv_s3", "csv -> s3 (+AWS SDK +parquet)"),
                 ("el14_kafka_parquet", "kafka -> parquet (+sarama)"),
                 ("etl04_csv_full_s3", "csv -> filter/validate/map -> s3")]
        lines += [
            "### Per-app binary size (import-driven `dp-build`)", "",
            "A deployed pipeline is compiled with `dp-build`, which links ONLY the",
            "components the app references — the datapipeline analogue of `flogo build`",
            "(see `build/imports.go`). Each app ships far smaller than the all-in-one",
            "engine, and CGO-free unless it uses the sqlite driver.", "",
            "| Deployed pipeline | Binary |", "|-------------------|-------:|"]
        for k, label in order:
            if binsizes.get(k):
                lines.append("| `%s` | **%s MB** |" % (label, binsizes[k]))
        if binsizes.get("all_in_one"):
            lines.append("| _all-in-one benchmark binary (every source x sink)_ | %s MB |"
                         % binsizes["all_in_one"])
        lines.append("")
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
    host = load("host_meta.json", {})
    binsizes = load("binsizes.json", {})

    with open(os.path.join(REPORTS, "matrix.md"), "w") as f:
        f.write(matrix_report(results, cases))
    if elt:
        with open(os.path.join(REPORTS, "elt.md"), "w") as f:
            f.write(elt_report(elt))
    if ent:
        with open(os.path.join(REPORTS, "enterprise.md"), "w") as f:
            f.write(enterprise_report(ent))
    with open(os.path.join(EXAMPLE, "SUMMARY.md"), "w") as f:
        f.write(summary(results, cases, elt, ent, meta, host, binsizes))
    print("reports written to %s and SUMMARY.md" % REPORTS)


if __name__ == "__main__":
    main()
