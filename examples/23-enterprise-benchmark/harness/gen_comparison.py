#!/usr/bin/env python3
"""Compare the current benchmark results against the POC baseline.

Reads the current results (harness/*.json) and the POC baseline
(harness/baseline_poc/*.json) and writes:

  reports/improvements.md    per-case before/after: throughput, duration, peak
                             RSS, CPU cores, CPU-seconds, rows per CPU-second
  reports/improvements.csv   the same, one row per case (for charts/slides)

  python3 harness/gen_comparison.py
"""
import csv
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
EXAMPLE = os.path.dirname(HERE)
REPORTS = os.path.join(EXAMPLE, "reports")
BASE = os.path.join(HERE, "baseline_poc")


def load(path, default=None):
    if not os.path.exists(path):
        return {} if default is None else default
    with open(path) as f:
        return json.load(f)


def num(v):
    return v if isinstance(v, (int, float)) and v > 0 else None


def h(n):
    return "-" if n is None else f"{int(round(n)):,}"


def secs(ms):
    return "-" if not ms else "%.1fs" % (ms / 1000.0)


def mb(v):
    return "-" if v is None else "%d MB" % round(v)


def cores(v):
    return "-" if v is None else "%.1f" % v


def speedup(old, new):
    """Throughput / efficiency ratio, as '5.8x' or '+12%' / '-4%'."""
    if not old or not new:
        return "-"
    r = new / old
    if r >= 1.5:
        return "**%.1fx**" % r
    return "%+.0f%%" % ((r - 1) * 100)


def delta_pct(old, new, lower_is_better=True):
    if old is None or new is None or old == 0:
        return "-"
    d = (new - old) * 100.0 / old
    s = "%+.0f%%" % d
    good = d < -5 if lower_is_better else d > 5
    return "**%s**" % s if good else s


def eff(row):
    """Rows processed per CPU-second (higher = cheaper per row)."""
    cpu = num(row.get("cpu_seconds"))
    rows = num(row.get("processed"))
    return rows / cpu if cpu and rows else None


def case_rows(old, new, keys):
    out = []
    for k in keys:
        o, n = old.get(k) or {}, new.get(k) or {}
        if not n:
            continue
        out.append({
            "case": k, "pipeline": n.get("desc") or n.get("pattern") or "",
            "rows_in": num(n.get("processed")), "rows_out": num(n.get("written")),
            "rows_out_before": num(o.get("written")),
            "dur_before_ms": num(o.get("duration_ms")), "dur_after_ms": num(n.get("duration_ms")),
            "thr_before": num(o.get("rows_per_sec")), "thr_after": num(n.get("rows_per_sec")),
            "rss_before": num(o.get("peak_rss_mb")), "rss_after": num(n.get("peak_rss_mb")),
            "cores_before": num(o.get("cpu_cores")), "cores_after": num(n.get("cpu_cores")),
            "cpus_before": num(o.get("cpu_seconds")), "cpus_after": num(n.get("cpu_seconds")),
            "eff_before": eff(o), "eff_after": eff(n),
            "status": n.get("status", ""),
        })
    return out


def table(rows):
    lines = [
        "| Case | Pipeline | Rows out | Duration | Throughput (rows/s) | Speed-up | Peak RSS | CPU cores | CPU time | Rows per CPU-s |",
        "|------|----------|---------:|---------:|--------------------:|---------:|---------:|----------:|---------:|---------------:|",
    ]
    for r in rows:
        same = r["rows_out"] == r["rows_out_before"] or r["rows_out_before"] is None
        lines.append("| %s | `%s` | %s%s | %s → %s | %s → **%s** | %s | %s → %s (%s) | %s → %s | %s → %s (%s) | %s → %s (%s) |" % (
            r["case"], r["pipeline"], h(r["rows_out"]), "" if same else " ⚠",
            secs(r["dur_before_ms"]), secs(r["dur_after_ms"]),
            h(r["thr_before"]), h(r["thr_after"]), speedup(r["thr_before"], r["thr_after"]),
            mb(r["rss_before"]), mb(r["rss_after"]), delta_pct(r["rss_before"], r["rss_after"]),
            cores(r["cores_before"]), cores(r["cores_after"]),
            "%.0fs" % r["cpus_before"] if r["cpus_before"] else "-",
            "%.0fs" % r["cpus_after"] if r["cpus_after"] else "-",
            delta_pct(r["cpus_before"], r["cpus_after"]),
            h(r["eff_before"]), h(r["eff_after"]), speedup(r["eff_before"], r["eff_after"]),
        ))
    return lines


def rng(vals, fmt):
    vals = [v for v in vals if v is not None]
    return "-" if not vals else fmt(min(vals)) + " – " + fmt(max(vals))


def geomean(ratios):
    ratios = [r for r in ratios if r and r > 0]
    if not ratios:
        return None
    p = 1.0
    for r in ratios:
        p *= r
    return p ** (1.0 / len(ratios))


def main():
    old_m, new_m = load(os.path.join(BASE, "results.json")), load(os.path.join(HERE, "results.json"))
    old_g, new_g = load(os.path.join(BASE, "graph_results.json")), load(os.path.join(HERE, "graph_results.json"))
    old_e, new_e = load(os.path.join(BASE, "elt_results.json")), load(os.path.join(HERE, "elt_results.json"))
    old_r, new_r = load(os.path.join(BASE, "enterprise_results.json")), load(os.path.join(HERE, "enterprise_results.json"))
    old_b, new_b = load(os.path.join(BASE, "binsizes.json")), load(os.path.join(HERE, "binsizes.json"))
    host = load(os.path.join(HERE, "host_meta.json"))
    meta = load(os.path.join(HERE, "dataset_meta.json"))

    keys = sorted(new_m)
    el = case_rows(old_m, new_m, [k for k in keys if new_m[k].get("cat") == "EL"])
    etl = case_rows(old_m, new_m, [k for k in keys if new_m[k].get("cat") == "ETL"])
    ent = case_rows(old_m, new_m, [k for k in keys if new_m[k].get("cat") == "ENT"])
    graph = case_rows(old_g, new_g, [k for k in sorted(new_g) if not k.endswith("_v")])
    allm = el + etl + ent

    def pick(rows, f):
        return [r[f] for r in rows]

    def med(vals):
        vals = sorted(v for v in vals if v is not None)
        return vals[len(vals) // 2] if vals else None

    passed = sum(1 for k in new_m if new_m[k].get("status") == "completed")
    same_rows = all(r["rows_out"] == r["rows_out_before"] for r in allm + graph if r["rows_out_before"] is not None)
    peak_old = max(old_m.items(), key=lambda kv: kv[1].get("rows_per_sec") or 0)
    peak_new = max(new_m.items(), key=lambda kv: kv[1].get("rows_per_sec") or 0)
    tot_old = sum(r["dur_before_ms"] or 0 for r in allm) / 1000.0
    tot_new = sum(r["dur_after_ms"] or 0 for r in allm) / 1000.0
    cpu_old = sum(r["cpus_before"] or 0 for r in allm if r["cpus_after"])
    cpu_new = sum(r["cpus_after"] or 0 for r in allm if r["cpus_before"])
    gm_thr = geomean([r["thr_after"] / r["thr_before"] for r in allm if r["thr_before"] and r["thr_after"]])
    faster = [r for r in allm if r["thr_before"] and r["thr_after"] and r["thr_after"] / r["thr_before"] >= 1.05]
    slower = [r for r in allm if r["thr_before"] and r["thr_after"] and r["thr_after"] / r["thr_before"] <= 0.95]
    rss_lower = [r for r in allm if r["rss_before"] and r["rss_after"] and r["rss_after"] < r["rss_before"] * 0.95]

    L = ["# Benchmark Improvements — POC baseline vs current engine", "",
         "Same host, same 53,000,000-row dataset, same pipelines, same lean per-pipe",
         "binaries (`dp-build`). **Before** is the original POC run (the published",
         "baseline in `harness/baseline_poc/`); **after** is the current `master`",
         "(unified execution engine + the performance/correctness work of PR #62).", "",
         "Columns: duration and throughput are engine-measured; peak RSS and CPU are",
         "sampled for the single engine process. *CPU time* is total CPU-seconds the",
         "process consumed; *rows per CPU-s* = rows processed ÷ CPU-seconds — the cost",
         "efficiency (higher is cheaper). Bold marks a material improvement.", "",
         "## Headline", "",
         "| Metric | POC baseline | Current | Change |",
         "|--------|-------------:|--------:|-------:|",
         "| Dataset | 53,000,000 rows | 53,000,000 rows | same |",
         "| Matrix cases passed | 30 / 30 | %d / %d | %s |" % (passed, len(new_m), "same" if passed == len(new_m) else "⚠"),
         "| Output row counts | — | %s | correctness parity |" % ("identical in every case" if same_rows else "⚠ differ, see ⚠ rows"),
         "| Peak throughput | %s/s (`%s`) | %s/s (`%s`) | %s |" % (
             h(peak_old[1]["rows_per_sec"]), peak_old[0], h(peak_new[1]["rows_per_sec"]), peak_new[0],
             speedup(peak_old[1]["rows_per_sec"], peak_new[1]["rows_per_sec"])),
         "| Total matrix wall time (30 cases) | %s | %s | **%.1fx faster** |" % (
             "%.0f min" % (tot_old / 60), "%.0f min" % (tot_new / 60), tot_old / tot_new if tot_new else 0),
         "| Total CPU-seconds (cases with CPU data) | %s | %s | %s |" % (h(cpu_old), h(cpu_new), delta_pct(cpu_old, cpu_new)),
         "| Throughput, geometric mean across 30 cases | — | — | **%.2fx** |" % (gm_thr or 0),
         "| Cases faster (≥5%%) / same / slower (≥5%%) | — | — | %d / %d / %d |" % (
             len(faster), len(allm) - len(faster) - len(slower), len(slower)),
         "| Cases with lower peak memory | — | — | %d of %d |" % (len(rss_lower), len(allm)),
         "| Peak memory (RSS) across matrix | %s | %s | |" % (
             rng(pick(allm, "rss_before"), lambda v: "%d" % v) + " MB", rng(pick(allm, "rss_after"), lambda v: "%d" % v) + " MB"),
         "| Peak memory (RSS), median of 30 cases | %s | %s | %s |" % (
             mb(med(pick(allm, "rss_before"))), mb(med(pick(allm, "rss_after"))),
             delta_pct(med(pick(allm, "rss_before")), med(pick(allm, "rss_after")))),
         "| CPU used (avg cores) across matrix | %s of %s | %s of %s | |" % (
             rng(pick(allm, "cores_before"), lambda v: "%.1f" % v), host.get("logical_cores", "?"),
             rng(pick(allm, "cores_after"), lambda v: "%.1f" % v), host.get("logical_cores", "?")),
         "| Host | %s · %s cores · %s GB | same | |" % (host.get("cpu_model", "?"), host.get("logical_cores", "?"), host.get("ram_gb", "?")),
         ""]

    # Category roll-ups.
    L += ["## By category", "",
          "| Category | Cases | Throughput (geo-mean speed-up) | Best case | Peak RSS (median before → after) |",
          "|----------|------:|-------------------------------:|-----------|----------------------------------:|"]

    for name, rows in (("Extract-Load", el), ("Extract-Transform-Load", etl), ("Enterprise (DLQ, resume)", ent), ("Graph / DAG fan-out", graph)):
        if not rows:
            continue
        ratios = [(r["thr_after"] / r["thr_before"], r) for r in rows if r["thr_before"] and r["thr_after"]]
        best = max(ratios, key=lambda x: x[0]) if ratios else None
        L.append("| %s | %d | %s | %s | %s → %s |" % (
            name, len(rows), "%.2fx" % geomean([x[0] for x in ratios]) if ratios else "-",
            "`%s` %.1fx" % (best[1]["case"], best[0]) if best else "-",
            mb(med(pick(rows, "rss_before"))), mb(med(pick(rows, "rss_after")))))
    L.append("")

    L += ["## Extract - Load (raw, no transform)", ""] + table(el) + [""]
    L += ["## Extract - Transform - Load (in-flight transforms)", ""] + table(etl) + [""]
    if ent:
        L += ["## Enterprise reliability cases (matrix)", ""] + table(ent) + [""]
    if graph:
        L += ["## Graph (DAG) fan-out / fan-in", "",
              "One source read routed to N sinks in a single pass (graph05: 3 sources → 6 sinks).", ""] + table(graph) + [""]

    if new_r.get("ent01_dlq_isolation") or new_r.get("ent02_checkpoint_resume"):
        o1, n1 = old_r.get("ent01_dlq_isolation", {}), new_r.get("ent01_dlq_isolation", {})
        o2, n2 = old_r.get("ent02_checkpoint_resume", {}), new_r.get("ent02_checkpoint_resume", {})
        L += ["## Reliability round", "",
              "| Scenario | POC baseline | Current |", "|----------|-------------:|--------:|",
              "| DLQ isolation: good + rejected = processed | %s + %s = %s (%s) | %s + %s = %s (%s) |" % (
                  h(o1.get("good")), h(o1.get("rejected")), h(o1.get("processed")), "conserved" if o1.get("conserved") else "?",
                  h(n1.get("good")), h(n1.get("rejected")), h(n1.get("processed")), "conserved" if n1.get("conserved") else "⚠"),
              "| DLQ isolation duration / throughput | %s / %s/s | %s / %s/s (%s) |" % (
                  secs(o1.get("duration_ms")), h(o1.get("rows_per_sec")), secs(n1.get("duration_ms")), h(n1.get("rows_per_sec")),
                  speedup(o1.get("rows_per_sec"), n1.get("rows_per_sec"))),
              "| Checkpoint/resume: rows committed before interruption | %s | %s |" % (h(o2.get("run1_committed")), h(n2.get("run1_committed"))),
              "| Checkpoint/resume: resumed run | %s, %s processed in %s | %s, %s processed in %s |" % (
                  o2.get("run2_status", "-"), h(o2.get("run2_processed")), secs(o2.get("run2_duration_ms")),
                  n2.get("run2_status", "-"), h(n2.get("run2_processed")), secs(n2.get("run2_duration_ms"))),
              ""]

    ab = load(os.path.join(HERE, "ab_validation.json"))
    if ab:
        L += ["## Same-day A/B validation (cases at or below the POC numbers)", "",
              "Cases that did not improve against the months-old POC run were re-run",
              "back-to-back against binaries built from the pre-change engine, on the same",
              "day, infra and data (%s). This separates environment drift from engine" % ab.get("_note", "rounds listed per case"),
              "behaviour: where both engines land at the same number, the gap to the POC",
              "baseline is environmental.", "",
              "| Case | Pre-change engine (rows/s) | Current engine (rows/s) | Change | Peak RSS pre → current |",
              "|------|---------------------------:|------------------------:|-------:|-----------------------:|"]
        for k in sorted(x for x in ab if not x.startswith("_")):
            o, n = ab[k]["premerge"], ab[k]["current"]
            mo = sum(r["rows_per_sec"] for r in o) / len(o)
            mn = sum(r["rows_per_sec"] for r in n) / len(n)
            ro = max(r.get("peak_rss_mb") or 0 for r in o)
            rn = max(r.get("peak_rss_mb") or 0 for r in n)
            L.append("| %s | %s | %s | %s | %s → %s |" % (
                k, " / ".join(h(r["rows_per_sec"]) for r in o), " / ".join(h(r["rows_per_sec"]) for r in n),
                speedup(mo, mn), mb(ro), mb(rn)))
        L.append("")

    if new_e:
        L += ["## ELT round (runs inside PostgreSQL)", "",
              "Pushed-down SQL; timings reflect the database, not the engine.", "",
              "| Case | What | Before | After |", "|------|------|-------:|------:|"]
        for k in sorted(new_e):
            L.append("| %s | %s | %s | %s |" % (k, new_e[k].get("desc", ""), secs(old_e.get(k, {}).get("duration_ms")), secs(new_e[k].get("duration_ms"))))
        L.append("")

    if new_b:
        L += ["## Deployed binary size (lean per-app `dp-build`)", "",
              "| Deployed pipeline | POC baseline | Current |", "|-------------------|-------------:|--------:|"]
        labels = {"el05_csv_null": "csv -> null", "el02_csv_postgres": "csv -> postgres (+lib/pq)",
                  "el04_csv_s3": "csv -> s3 (+AWS SDK +parquet)", "el14_kafka_parquet": "kafka -> parquet (+sarama)",
                  "etl04_csv_full_s3": "csv -> filter/validate/map -> s3", "all_in_one": "all-in-one binary (every source x sink)"}
        for k, lab in labels.items():
            if k in new_b:
                L.append("| `%s` | %s MB | %s MB |" % (lab, old_b.get(k, "-"), new_b[k]))
        L.append("")

    L += ["## Where the gains come from", "",
          "- **Postgres sinks** default to `COPY` (with automatic INSERT fallback and per-row DLQ",
          "  isolation), instead of batched multi-row INSERTs — the 3.5–18x jdbc-sink speed-ups.",
          "- **One streaming executor**: the transform chain runs as its own fused stage, overlapped",
          "  with source reads and sink writes (bounded channels, back-pressure), for linear pipelines",
          "  and DAGs alike.",
          "- **Positional column binding** in filter / router / validate / map / dq / join — no per-row",
          "  map allocation — lower CPU per row and lower memory.",
          "- **Lookup** negative caching (not-found keys are not re-fetched) and a bounded LRU.",
          "- **DQ** gate evaluates expectations on bound columns.",
          "- **Graph/DAG** nodes buffer sink writes and share the memory budget.",
          "",
          "## Notes", "",
          "- The jdbc-source cases page with keyset pagination on `orders_raw.order_id`; the index is",
          "  part of `sql/schema.sql` (both runs had it).",
          "- Reading the 1.6 GiB Parquet object from S3 (`el16`, `el17`) peaks at ~800 MB RSS on both",
          "  the old and the new engine in a same-day A/B (see [engine-unification.md](engine-unification.md));",
          "  the POC baseline read a differently-seeded object, so its lower RSS is not comparable.",
          "- The checkpoint/resume interruption is time-based, so the faster engine has committed",
          "  more rows (and resumes from further along) when it is interrupted.",
          "- Single-run numbers on a laptop: expect ±3–5% run-to-run noise; treat changes inside that",
          "  band as parity. Kafka-sink and postgres-source throughput also drift with broker/table",
          "  state between days — the same-day A/B above shows both engines at the same number, and",
          "  `el17` (s3 -> postgres) measured 281k/s in the matrix and 313k/s on an immediate re-run.",
          "- Lean binaries are ~1 MB larger than in the POC, almost entirely from the newer Go",
          "  toolchain and dependency updates: the pre-change engine built today with the same",
          "  toolchain is 8.73 MB for `csv -> null` vs 8.84 MB now (+0.1 MB for this work). All",
          "  sizes are stripped builds (`-s -w`).",
          ""]

    os.makedirs(REPORTS, exist_ok=True)
    with open(os.path.join(REPORTS, "improvements.md"), "w") as f:
        f.write("\n".join(L))
    fields = ["category", "case", "pipeline", "rows_in", "rows_out", "dur_before_ms", "dur_after_ms", "thr_before", "thr_after",
              "rss_before", "rss_after", "cores_before", "cores_after", "cpus_before", "cpus_after", "eff_before", "eff_after", "status"]
    with open(os.path.join(REPORTS, "improvements.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for cat, rows in (("EL", el), ("ETL", etl), ("ENT", ent), ("GRAPH", graph)):
            for r in rows:
                w.writerow(dict(r, category=cat))
    print("wrote reports/improvements.md and reports/improvements.csv")


if __name__ == "__main__":
    main()
