#!/usr/bin/env python3
"""Generate enterprise GRAPH (DAG) pipelines that exercise the streaming graph
runner (action/pipeline/graphexec) rather than the linear engine.

The graph runner takes over only for a NON-REDUCIBLE topology — a typed edge
(condition / otherwise / error) or fan-out/fan-in >= 2 — that the single-chain
linear engine cannot represent. These pipelines model the enterprise patterns
customers actually build:

  * graph01  regional fan-out routing   : one source read, routed to 5 regional
                                          data-lake sinks (4 conditions + default)
  * graph02  valid / reject split (DLQ) : good rows to the lake, bad rows diverted
                                          to a dead-letter sink
  * graph03  tee + audit                : every row to the lake AND high-value
                                          rows teed to an audit sink

Each is emitted for parquet file sinks (infra-free throughput) and, for the
regional router, an s3 (MinIO) variant that hits live object storage.

Usage:  python3 harness/gen_graph_pipes.py [--source PATH] [--suffix S] [--rows-note N]
"""
import argparse
import json
import os

EX = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PIPES = os.path.join(EX, "pipes")

COLS = [
    {"name": "order_id", "type": "string"}, {"name": "customer_id", "type": "string"},
    {"name": "region", "type": "string"}, {"name": "product", "type": "string"},
    {"name": "category", "type": "string"}, {"name": "quantity", "type": "int"},
    {"name": "unit_price", "type": "float"}, {"name": "status", "type": "string"},
    {"name": "order_date", "type": "string"}, {"name": "email", "type": "string"},
]

ACTION = "github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"
BASE_IMPORTS = [
    ACTION,
    "github.com/mpandav-tibco/flogo-datapipeline/source/csv",
    "github.com/mpandav-tibco/flogo-datapipeline/transform/filter",
    "github.com/project-flogo/contrib/trigger/timer",
]
PARQUET_IMPORTS = ["github.com/mpandav-tibco/flogo-datapipeline/sink/parquet"]
NULL_IMPORTS = ["github.com/mpandav-tibco/flogo-datapipeline/sink/null"]
S3_IMPORTS = [
    "github.com/mpandav-tibco/flogo-datapipeline/objstore/s3",
    "github.com/mpandav-tibco/flogo-datapipeline/sink/objstore",
    "github.com/mpandav-tibco/flogo-datapipeline/sink/parquet",
]
# Multi-source consolidation: read the three systems-of-record formats and apply
# a filter -> map -> validate chain.
MULTI_SOURCE_IMPORTS = [
    "github.com/mpandav-tibco/flogo-datapipeline/source/csv",
    "github.com/mpandav-tibco/flogo-datapipeline/source/jsonlines",
    "github.com/mpandav-tibco/flogo-datapipeline/source/parquet",
]
MULTI_TX_IMPORTS = [
    "github.com/mpandav-tibco/flogo-datapipeline/transform/filter",
    "github.com/mpandav-tibco/flogo-datapipeline/transform/mapper",
    "github.com/mpandav-tibco/flogo-datapipeline/transform/validate",
]
TIMER = "github.com/project-flogo/contrib/trigger/timer"

REGIONS = ["NORTH", "CENTRAL", "EAST", "SOUTH"]  # WEST + anything unmatched -> default sink


def source(path):
    return {"type": "csv", "path": path, "delimiter": ",", "hasHeader": True, "columns": COLS}


def msource(src_type, path):
    """Source config for one of the three record-system formats."""
    if src_type == "csv":
        return source(path)
    if src_type == "jsonlines":
        return {"type": "jsonlines", "path": path}
    if src_type == "parquet":
        return {"type": "parquet", "path": path, "batchSize": 10000}
    raise ValueError("unknown source type %r" % src_type)


def xsource(node_id, src_type, cfg):
    """A self-contained graph source node carrying its own config (extra feed)."""
    return {"id": node_id, "kind": "source", "type": src_type, "ports": {"out": ["rows"]}, "config": cfg}


def parquet_opts(path):
    return {"path": path, "compression": "snappy", "rowGroupSize": 100000}


def s3_opts(key):
    return {
        "bucket": "datalake", "region": "us-east-1", "endpoint": "http://localhost:9000",
        "forcePathStyle": True, "accessKeyIdEnv": "MINIO_AK", "secretAccessKeyEnv": "MINIO_SK",
        "key": key, "format": "parquet",
    }


def sink_full(sink_type, opts):
    """A linear def.sink: type + flat options."""
    return dict({"type": sink_type}, **opts)


def xsink(node_id, sink_type, opts):
    """A self-contained graph sink node carrying its own flat-options config."""
    return {"id": node_id, "kind": "sink", "type": sink_type, "ports": {"in": ["in"]}, "config": opts}


def app(name, imports, data):
    return {
        "name": name, "type": "flogo:app", "version": "1.0.0", "appModel": "1.1.1",
        "imports": imports,
        "triggers": [{
            "id": "bench-timer", "ref": "github.com/project-flogo/contrib/trigger/timer",
            "settings": {"repeating": True, "startDelay": 0, "interval": 86400000},
            "handlers": [{"settings": {}, "action": {"ref": ACTION,
                          "settings": {"pipelineRef": "res://pipeline:%s" % name}}}],
        }],
        "resources": [{"id": "pipeline:%s" % name, "data": dict(data, name=name)}],
    }


def perf():
    return {
        "mode": "etl",
        "performance": {"batchSize": 5000, "maxMemoryMB": 1024},
        "scaling": {"mode": "single"},
        "checkpoint": {"type": "none"},
    }


def write(a):
    p = os.path.join(PIPES, a["name"] + ".datapipeline")
    with open(p, "w") as f:
        json.dump(a, f, indent=1)
    print("wrote", os.path.relpath(p, EX))


# ── graph 01: regional fan-out routing ───────────────────────────────────────
def graph_region(name, src, opts_for, sink_type, sink_imports):
    """source -> filter(drop cancelled) -> route by region to 5 destinations.

    4 condition edges (NORTH/CENTRAL/EAST/SOUTH) + 1 otherwise edge (default sink
    = WEST + anything unmatched). Non-reducible (fan-out 5 + typed edges).
    """
    default_opts = opts_for("west")
    nodes = [
        {"id": "source", "kind": "source", "type": "csv", "ports": {"out": ["rows"]}},
        {"id": "t0", "kind": "transform", "type": "filter", "ports": {"in": ["in"], "out": ["out"]}},
        {"id": "sink", "kind": "sink", "type": sink_type, "ports": {"in": ["in"]}},
    ]
    edges = [["source:rows", "t0:in"]]
    for r in REGIONS:
        nid = "xsink-" + r.lower()
        nodes.append(xsink(nid, sink_type, opts_for(r.lower())))
        edges.append({"from": "t0:out", "to": nid + ":in", "type": "expression",
                      "value": "region = '%s'" % r})
    edges.append({"from": "t0:out", "to": "sink:in", "type": "otherwise"})
    data = dict(perf(),
                source=source(src),
                transforms=[{"type": "filter", "expr": "status != 'CANCELLED'"}],
                sink=sink_full(sink_type, default_opts),
                graph={"nodes": nodes, "edges": edges})
    return app(name, BASE_IMPORTS + sink_imports, data)


# ── graph 02: valid / reject split (DLQ quarantine) ──────────────────────────
def graph_valreject(name, src):
    """source -> filter(drop cancelled) -> data-quality split: rows that pass the
    business rule land in the valid parquet partition; rows that fail (an
    implausible unit_price outlier) are quarantined to a persisted reject
    partition for review. valid + reject == processed (nothing lost).
    Non-reducible (condition + otherwise divert)."""
    valid = "unit_price <= 95000 AND quantity <= 1000"
    nodes = [
        {"id": "source", "kind": "source", "type": "csv", "ports": {"out": ["rows"]}},
        {"id": "t0", "kind": "transform", "type": "filter", "ports": {"in": ["in"], "out": ["out"]}},
        {"id": "sink", "kind": "sink", "type": "parquet", "ports": {"in": ["in"]}},
        xsink("xsink-reject", "parquet", parquet_opts("./out/%s_reject.parquet" % name)),
    ]
    edges = [
        ["source:rows", "t0:in"],
        {"from": "t0:out", "to": "sink:in", "type": "expression", "value": valid},
        {"from": "t0:out", "to": "xsink-reject:in", "type": "otherwise"},
    ]
    data = dict(perf(),
                source=source(src),
                transforms=[{"type": "filter", "expr": "status != 'CANCELLED'"}],
                sink=sink_full("parquet", parquet_opts("./out/%s_valid.parquet" % name)),
                graph={"nodes": nodes, "edges": edges})
    return app(name, BASE_IMPORTS + PARQUET_IMPORTS, data)


# ── graph 03: tee + audit ────────────────────────────────────────────────────
def graph_tee(name, src):
    """source -> filter -> tee: EVERY row to the parquet lake (success edge) AND
    high-value rows (unit_price > 90000, the top ~10%) teed to a separate audit
    sink. Non-reducible (success + condition tee on the same out-port)."""
    nodes = [
        {"id": "source", "kind": "source", "type": "csv", "ports": {"out": ["rows"]}},
        {"id": "t0", "kind": "transform", "type": "filter", "ports": {"in": ["in"], "out": ["out"]}},
        {"id": "sink", "kind": "sink", "type": "parquet", "ports": {"in": ["in"]}},
        xsink("xsink-audit", "parquet", parquet_opts("./out/%s_audit.parquet" % name)),
    ]
    edges = [
        ["source:rows", "t0:in"],
        ["t0:out", "sink:in"],  # success: every row
        {"from": "t0:out", "to": "xsink-audit:in", "type": "expression", "value": "unit_price > 90000"},
    ]
    data = dict(perf(),
                source=source(src),
                transforms=[{"type": "filter", "expr": "status != 'CANCELLED'"}],
                sink=sink_full("parquet", parquet_opts("./out/%s_all.parquet" % name)),
                graph={"nodes": nodes, "edges": edges})
    return app(name, BASE_IMPORTS + PARQUET_IMPORTS, data)


# ── graph 05: multi-source consolidation (fan-in + multi-transform + fan-out) ─
def graph_multi(name, sources):
    """The canonical enterprise ETL DAG: consolidate several systems of record
    (different formats), clean + enrich + validate in flight, and route to many
    destinations — all in a single streaming pass.

      N sources (csv + jsonl + parquet)  --fan-in-->  filter (drop cancelled)
        -> map (enrich: region_code=UPPER(region), net_amount=qty*price, lower(email))
        -> validate (split)  --pass-->  route by region to 5 regional lake sinks
                             --reject (error edge)-->  a quarantine sink

    Multi-source (fan-in >= 2), multi-transform (filter/map/validate), multi-sink
    (5 regional + 1 reject). Non-reducible on every axis, so the graph runner
    takes over; the linear engine cannot express any of the three.
    """
    prim_type, prim_path = sources[0]
    nodes = [{"id": "source", "kind": "source", "type": prim_type, "ports": {"out": ["rows"]}}]
    edges = [["source:rows", "t0:in"]]
    for i, (st, sp) in enumerate(sources[1:], start=2):
        nid = "xsrc-%d" % i
        nodes.append(xsource(nid, st, msource(st, sp)))
        edges.append(["%s:rows" % nid, "t0:in"])  # fan-in to the filter
    nodes += [
        {"id": "t0", "kind": "transform", "type": "filter", "ports": {"in": ["in"], "out": ["out"]}},
        {"id": "t1", "kind": "transform", "type": "map", "ports": {"in": ["in"], "out": ["out"]}},
        {"id": "t2", "kind": "transform", "type": "validate", "ports": {"in": ["in"], "out": ["out"]}},
        {"id": "sink", "kind": "sink", "type": "parquet", "ports": {"in": ["in"]}},  # west/default (pass)
    ]
    edges += [["t0:out", "t1:in"], ["t1:out", "t2:in"]]
    # PASS rows: route by the enriched region_code to a regional sink each.
    for r in REGIONS:
        nid = "xsink-" + r.lower()
        nodes.append(xsink(nid, "parquet", parquet_opts("./out/%s_%s.parquet" % (name, r.lower()))))
        edges.append({"from": "t2:out", "to": nid + ":in", "type": "expression",
                      "value": "region_code = '%s'" % r})
    edges.append({"from": "t2:out", "to": "sink:in", "type": "otherwise"})  # WEST + unmatched
    # REJECT rows (validation failures) ride the error edge to a quarantine sink.
    nodes.append(xsink("xsink-reject", "parquet", parquet_opts("./out/%s_reject.parquet" % name)))
    edges.append({"from": "t2:out", "to": "xsink-reject:in", "type": "error"})

    fields = {
        "order_id": "=$order_id", "customer_id": "=$customer_id",
        "region_code": "=upper(=$region)", "product": "=$product", "category": "=$category",
        "quantity": "=$quantity", "net_amount": "=multiply(=$quantity, =$unit_price)",
        "status": "=$status", "order_date": "=$order_date", "email": "=lower(=$email)",
    }
    field_order = ["order_id", "customer_id", "region_code", "product", "category",
                   "quantity", "net_amount", "status", "order_date", "email"]
    data = dict(perf(),
                source=msource(prim_type, prim_path),
                transforms=[
                    {"type": "filter", "expr": "status != 'CANCELLED'"},
                    {"type": "map", "fieldOrder": field_order, "fields": fields},
                    # QA gate: quantity must be a plausible line-item size; rejects ride
                    # the graph error edge to the quarantine sink (onViolation is ignored
                    # on the split path — the edge decides).
                    {"type": "validate", "rules": [{"field": "quantity", "notNull": True, "max": 950}]},
                ],
                sink=sink_full("parquet", parquet_opts("./out/%s_west.parquet" % name)),
                graph={"nodes": nodes, "edges": edges})
    imports = [ACTION] + MULTI_SOURCE_IMPORTS + MULTI_TX_IMPORTS + PARQUET_IMPORTS + [TIMER]
    return app(name, imports, data)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="./data/orders.csv")
    ap.add_argument("--suffix", default="")
    args = ap.parse_args()
    s = args.suffix
    src = args.source

    def pq_key(name):
        return lambda tag: parquet_opts("./out/%s_%s.parquet" % (name, tag))

    def s3_key(name):
        return lambda tag: s3_opts("graph/%s/%s.parquet" % (name, tag))

    n01 = "graph01_route_region_parquet" + s
    write(graph_region(n01, src, pq_key(n01), "parquet", PARQUET_IMPORTS))

    n04 = "graph04_route_region_s3" + s
    write(graph_region(n04, src, s3_key(n04), "s3-sink", S3_IMPORTS))

    write(graph_valreject("graph02_valreject_parquet" + s, src))
    write(graph_tee("graph03_tee_audit_parquet" + s, src))

    # graph05: multi-source consolidation. Full run reads the three real formats
    # (three systems of record); the _v validation variant reuses the 2M CSV
    # subset three times (only that subset is generated small).
    if s:
        msrc = [("csv", src), ("csv", src), ("csv", src)]
    else:
        msrc = [("csv", "./data/orders.csv"),
                ("jsonlines", "./data/orders.jsonl"),
                ("parquet", "./data/orders.parquet")]
    write(graph_multi("graph05_multi_consolidate" + s, msrc))


if __name__ == "__main__":
    main()
