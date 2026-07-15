#!/usr/bin/env python3
"""Generate every benchmark pipeline definition into ../pipes/.

Each pipe is a self-contained Flogo app (timer trigger -> datapipeline action),
so the monolithic flogo-datapipeline engine fires it once at startup. The harness
starts the process, reads the "pipeline run completed" line from stdout, then
terminates it (the engine idles after the single run).

Coverage:
  - prep    : load PostgreSQL / Kafka / S3 and materialise Parquet from the CSV
  - EL  (17): every source (csv, parquet, jsonlines, postgres, kafka, s3)
              to every sink (parquet, postgres, kafka, s3, null)
  - ETL (11): all seven transforms -- filter, map, validate, schemadrift,
              dq, hwm, lookup -- in streaming single-pass pipelines
  - ENT  (2): DLQ isolation + checkpoint/resume (enterprise reliability)
"""
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
EXAMPLE = os.path.dirname(HERE)
PIPES = os.path.join(EXAMPLE, "pipes")
os.makedirs(PIPES, exist_ok=True)

PG_DSN = "postgres://postgres:bench@localhost:5433/warehouse?sslmode=disable"
BROKERS = ["localhost:9092"]
S3 = {
    "bucket": "datalake", "region": "us-east-1", "endpoint": "http://localhost:9000",
    "forcePathStyle": True, "accessKeyIdEnv": "MINIO_AK", "secretAccessKeyEnv": "MINIO_SK",
}
COLS = [("order_id", "string"), ("customer_id", "string"), ("region", "string"),
        ("product", "string"), ("category", "string"), ("quantity", "int"),
        ("unit_price", "float"), ("status", "string"), ("order_date", "string"),
        ("email", "string")]


def cols():
    return [{"name": n, "type": t} for n, t in COLS]


# ---------- sources ----------
def src_csv(path="./data/orders.csv"):
    return {"type": "csv", "path": path, "delimiter": ",", "hasHeader": True, "columns": cols()}


def src_parquet(path="./data/orders.parquet"):
    return {"type": "parquet", "path": path, "columns": cols(), "rowGroupParallelism": 4}


def src_jsonl(path="./data/orders.jsonl"):
    return {"type": "jsonlines", "path": path, "columns": cols()}


def src_pg():
    return {"type": "jdbc", "driver": "postgres", "dsn": PG_DSN, "table": "orders_raw",
            "keyColumn": "order_id", "batchSize": 5000, "partitions": 4}


def src_kafka(group):
    # Batch drain-to-end: stopAtCurrentEnd snapshots each partition's high-water
    # mark at open and stops once every partition reaches it (the source's native
    # config key is "stopAtCurrentEnd" -> kc.stopAtEnd).
    return {"type": "kafka", "options": {"brokers": BROKERS, "topic": "orders", "mode": "batch",
            "groupId": group, "startOffset": "earliest", "stopAtCurrentEnd": True, "batchSize": 5000}}


def src_s3(prefix="lake/"):
    o = dict(S3); o["prefix"] = prefix; o["format"] = "parquet"
    return {"type": "s3", "options": o}


# ---------- sinks ----------
def snk_parquet(path):
    return {"type": "parquet", "path": path, "compression": "snappy", "rowGroupSize": 100000}


def snk_pg(table="orders_sink"):
    return {"type": "jdbc", "driver": "postgres", "dsn": PG_DSN, "table": table,
            "batchSize": 5000, "onConflict": "insert"}


def snk_kafka(topic):
    return {"type": "kafka", "brokers": BROKERS, "topic": topic, "ackMode": "leader", "serializer": "json"}


def snk_s3(key):
    s = dict(S3); s["type"] = "s3-sink"; s["key"] = key; s["format"] = "parquet"
    return s


def snk_null():
    return {"type": "null"}


# ---------- transforms ----------
FILTER = {"type": "filter", "expr": "status != 'CANCELLED' AND unit_price > 0"}

MAP_CLEAN = {
    "type": "map",
    "fieldOrder": ["order_id", "customer_id", "region_code", "product", "category",
                   "quantity", "net_amount", "status", "order_date", "email"],
    "fields": {
        "order_id": "=$order_id", "customer_id": "=$customer_id",
        "region_code": "=upper($.region)", "product": "=$product", "category": "=$category",
        "quantity": "=$quantity", "net_amount": "=multiply($.quantity, $.unit_price)",
        "status": "=$status", "order_date": "=$order_date", "email": "=lower($.email)",
    },
}

VALIDATE = {
    "type": "validate",
    "rules": [
        {"field": "order_id", "notNull": True, "pattern": "^ORD-[0-9]+$"},
        {"field": "quantity", "notNull": True, "min": 1, "max": 1000},
        {"field": "unit_price", "notNull": True, "min": 0.01, "max": 100000},
        {"field": "status", "in": ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED"]},
        {"field": "email", "pattern": "^[^@]+@[^@]+\\.[^@]+$"},
    ],
    "onViolation": "route", "errorSinkType": "null", "errorSinkConfig": {},
}

SCHEMADRIFT = {"type": "schemadrift", "policy": "additive"}

DQ = {
    "type": "dq",
    "expectations": [
        {"type": "expectColumnNotNull", "column": "order_id", "threshold": 1.0},
        {"type": "expectColumnValuesBetween", "column": "unit_price", "min": 0.01, "max": 100000},
        {"type": "expectColumnValuesInSet", "column": "status",
         "values": ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED"]},
    ],
    "qualityThreshold": 0.95, "onFail": "warn",
}

HWM = {"type": "hwm", "column": "order_date", "backend": "file",
       "backendConfig": {"path": "./watermarks/orders.hwm"}}

# lookup enriches on low-cardinality region -> the LRU cache is warm after a
# handful of misses, so a mock local endpoint keeps 53M rows fast.
LOOKUP = {"type": "lookup", "url": "http://localhost:9099/enrich/{region}", "method": "GET",
          "keyField": "region", "outputFields": ["region_name", "region_tier"],
          "concurrency": 8, "cacheSize": 100, "timeoutMs": 3000, "onError": "null"}


# ---------- app envelope ----------
def app(name, data):
    return {
        "name": name, "type": "flogo:app", "version": "1.0.0", "appModel": "1.1.1",
        "imports": [
            "github.com/mpandav-tibco/flogo-datapipeline/action/pipeline",
            "github.com/project-flogo/contrib/trigger/timer",
        ],
        "triggers": [{
            "id": "bench-timer",
            "ref": "github.com/project-flogo/contrib/trigger/timer",
            "settings": {"repeating": True, "startDelay": 0, "interval": 86400000},
            "handlers": [{"settings": {}, "action": {
                "ref": "github.com/mpandav-tibco/flogo-datapipeline/action/pipeline",
                "settings": {"pipelineRef": "res://pipeline:" + name}}}],
        }],
        "resources": [{"id": "pipeline:" + name, "data": dict(data, name=name)}],
    }


def write(name, source, transforms, sink, mode="el", extra=None):
    data = {"mode": mode, "source": source, "transforms": transforms, "sink": sink,
            "performance": {"batchSize": 5000, "maxMemoryMB": 1024},
            "scaling": {"mode": "single"}, "checkpoint": {"type": "none"}}
    if extra:
        data.update(extra)
    with open(os.path.join(PIPES, name + ".datapipeline"), "w") as f:
        json.dump(app(name, data), f, indent=1)
    if not name.startswith("_prep"):
        cat = "EL" if name.startswith("el") else "ENT" if name.startswith("ent") else "ETL"
        snk = "s3" if sink["type"] == "s3-sink" else sink["type"]
        trs = [t["type"] for t in transforms]
        chain = source["type"] + (" -> " + "/".join(trs) + " -> " if trs else " -> ") + snk
        CASE_META[name] = {"cat": cat, "desc": chain, "source": source["type"],
                           "sink": snk, "transforms": trs}


CASE_META = {}


def main():
    # ---- prep (materialise every source format + seed backends) ----
    write("_prep_orders_parquet", src_csv(), [], snk_parquet("./data/orders.parquet"))
    write("_prep_load_pg", src_csv(), [], snk_pg("orders_raw"))
    write("_prep_load_kafka", src_csv(), [], snk_kafka("orders"))
    write("_prep_load_s3", src_csv(), [], snk_s3("lake/orders.parquet"))

    # ---- EL: every source x every sink ----
    write("el01_csv_parquet", src_csv(), [], snk_parquet("./out/el01.parquet"))
    write("el02_csv_postgres", src_csv(), [], snk_pg("orders_sink"))
    write("el03_csv_kafka", src_csv(), [], snk_kafka("el03_out"))
    write("el04_csv_s3", src_csv(), [], snk_s3("el/csv.parquet"))
    write("el05_csv_null", src_csv(), [], snk_null())
    write("el06_parquet_postgres", src_parquet(), [], snk_pg("orders_sink"))
    write("el07_parquet_kafka", src_parquet(), [], snk_kafka("el07_out"))
    write("el08_parquet_s3", src_parquet(), [], snk_s3("el/pq.parquet"))
    write("el09_jsonl_parquet", src_jsonl(), [], snk_parquet("./out/el09.parquet"))
    write("el10_jsonl_postgres", src_jsonl(), [], snk_pg("orders_sink"))
    write("el11_postgres_parquet", src_pg(), [], snk_parquet("./out/el11.parquet"))
    write("el12_postgres_kafka", src_pg(), [], snk_kafka("el12_out"))
    write("el13_postgres_s3", src_pg(), [], snk_s3("el/pg.parquet"))
    write("el14_kafka_parquet", src_kafka("g_el14"), [], snk_parquet("./out/el14.parquet"))
    write("el15_kafka_postgres", src_kafka("g_el15"), [], snk_pg("orders_sink"))
    write("el16_s3_parquet", src_s3(), [], snk_parquet("./out/el16.parquet"))
    write("el17_s3_postgres", src_s3(), [], snk_pg("orders_sink"))

    # ---- ETL: all seven transforms in streaming single-pass ----
    write("etl01_csv_filter_parquet", src_csv(), [FILTER], snk_parquet("./out/etl01.parquet"), "etl")
    write("etl02_csv_filtermap_parquet", src_csv(), [FILTER, MAP_CLEAN], snk_parquet("./out/etl02.parquet"), "etl")
    write("etl03_csv_clean_postgres", src_csv(), [VALIDATE, MAP_CLEAN], snk_pg("orders_clean"), "etl")
    write("etl04_csv_full_s3", src_csv(), [FILTER, VALIDATE, MAP_CLEAN], snk_s3("etl/clean.parquet"), "etl")
    write("etl05_postgres_map_kafka", src_pg(), [MAP_CLEAN], snk_kafka("etl05_out"), "etl")
    write("etl06_kafka_filter_parquet", src_kafka("g_etl06"), [FILTER], snk_parquet("./out/etl06.parquet"), "etl")
    write("etl07_parquet_full_postgres", src_parquet(), [FILTER, VALIDATE, MAP_CLEAN], snk_pg("orders_clean"), "etl")
    write("etl08_csv_schemadrift_parquet", src_csv(), [SCHEMADRIFT], snk_parquet("./out/etl08.parquet"), "etl")
    write("etl09_csv_dq_parquet", src_csv(), [DQ], snk_parquet("./out/etl09.parquet"), "etl")
    write("etl10_csv_hwm_parquet", src_csv(), [HWM], snk_parquet("./out/etl10.parquet"), "etl")
    write("etl11_csv_lookup_parquet", src_csv(), [LOOKUP, MAP_CLEAN], snk_parquet("./out/etl11.parquet"), "etl")

    # ---- Enterprise reliability ----
    # ent01: bad rows routed to a queryable reject table; good rows still load.
    ent01_validate = dict(VALIDATE)
    ent01_validate["errorSinkType"] = "jdbc"
    ent01_validate["errorSinkConfig"] = {"driver": "postgres", "dsn": PG_DSN,
                                         "table": "orders_rejects", "batchSize": 5000}
    write("ent01_dlq_isolation", src_csv(), [ent01_validate], snk_parquet("./out/ent01_good.parquet"), "etl")

    # ent02: row-interval checkpointing so a killed run resumes instead of restarting.
    write("ent02_checkpoint_resume", src_csv(), [FILTER], snk_null(), "etl",
          extra={"checkpoint": {"type": "file", "path": "./checkpoints/ent02",
                                "intervalRows": 1000000, "intervalSeconds": 10,
                                "deleteOnSuccess": False}})

    n = len([f for f in os.listdir(PIPES) if f.endswith(".datapipeline")])
    with open(os.path.join(HERE, "cases.json"), "w") as f:
        json.dump(CASE_META, f, indent=1)
    print("wrote %d pipeline definitions to %s" % (n, PIPES))
    print("wrote cases.json (%d benchmark cases)" % len(CASE_META))


if __name__ == "__main__":
    main()
