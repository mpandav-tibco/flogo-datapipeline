# Example 07 — JSON Lines / NDJSON Source

**Mode:** ETL (Extract, Transform, Load)
**Source:** JSON Lines file (`.jsonl`)
**Transforms:** filter, map
**Sink:** Null (discard)

This example demonstrates reading a newline-delimited JSON (NDJSON) file,
applying schema-on-read column extraction with nested-field flattening, and
then filtering the resulting rows to only those with a specific `event_type`.

The JSON Lines format is common in event streaming, log aggregation, and data
export pipelines. Every line is a self-contained, complete JSON object. This
example mimics an analytics event export from a web storefront.

---

## Directory Layout

```
07-jsonlines-source/
├── events-jsonlines-source.datapipeline   # Pipeline definition (JSON)
├── data/
│   └── events.jsonl                       # 20 NDJSON event records
├── run.sh                                 # Convenience run script
└── README.md                              # This file
```

---

## What is JSON Lines / NDJSON?

JSON Lines (also called NDJSON — Newline Delimited JSON) is a text format
where each line is an independent, valid JSON object terminated by a newline
character (`\n`). The file as a whole is **not** a JSON array; it is a
sequence of JSON objects.

```
{"event_id":"evt-001","user_id":1001,"event_type":"purchase",...}
{"event_id":"evt-002","user_id":1042,"event_type":"view",...}
{"event_id":"evt-003","user_id":1007,"event_type":"click",...}
```

Key properties:

| Property | Detail |
|----------|--------|
| One record per line | Each line is parsed independently; a corrupt line does not affect others |
| Streamable | The engine never loads the full file into memory — it reads one line at a time |
| Appendable | Producers can `echo '{"k":"v"}' >> file.jsonl` without touching existing data |
| 4 MB line buffer | The engine supports individual JSON records up to 4 MB (handles deeply nested or wide objects) |
| Encoding | UTF-8; BOM is stripped automatically |

Common file extensions: `.jsonl`, `.ndjson`, `.jl`.

---

## Schema-on-Read Inference

Unlike CSV, a JSONL file carries no separate header row. The schema is applied
at read time by declaring a `columns` array in the source configuration. Two
strategies are available:

### Strategy 1 — Explicit columns (this example)

Declare exactly which top-level keys you want, and what Go type to cast them to.
Only the listed keys are extracted; everything else is silently discarded.

```json
"columns": [
  { "name": "event_id",          "type": "string" },
  { "name": "user_id",           "type": "int"    },
  { "name": "event_type",        "type": "string" },
  { "name": "properties_amount", "type": "float"  },
  { "name": "timestamp",         "type": "string" }
]
```

### Strategy 2 — Schema inference (omit columns)

Omit the `columns` array entirely. The engine samples the first 100 lines,
discovers all keys present, infers types, and builds the schema automatically.
Nested objects are flattened using underscore-joined key paths (see below).

```json
"source": {
  "type": "jsonlines",
  "path": "./data/events.jsonl"
}
```

Inferred schema for `events.jsonl`:

| Inferred column name     | Source JSON path            | Inferred type |
|--------------------------|-----------------------------|---------------|
| `event_id`               | `.event_id`                 | `string`      |
| `user_id`                | `.user_id`                  | `int`         |
| `event_type`             | `.event_type`               | `string`      |
| `properties_amount`      | `.properties.amount`        | `float`       |
| `properties_currency`    | `.properties.currency`      | `string`      |
| `properties_page`        | `.properties.page`          | `string`      |
| `properties_sku`         | `.properties.sku`           | `string`      |
| `properties_method`      | `.properties.method`        | `string`      |
| `properties_element`     | `.properties.element`       | `string`      |
| `timestamp`              | `.timestamp`                | `string`      |
| `session_id`             | `.session_id`               | `string`      |
| `platform`               | `.platform`                 | `string`      |

> **Note:** When you only need a subset of columns — especially from wide
> objects with dozens of keys — using the explicit `columns` array is more
> efficient: the engine skips JSON fields that are not in the list.

---

## Nested Field Flattening

Each JSON event in `events.jsonl` contains a nested `properties` object:

```json
{
  "event_id": "evt-001",
  "user_id": 1001,
  "event_type": "purchase",
  "properties": {
    "amount": 149.99,
    "currency": "USD",
    "sku": "HDPH-BT-001"
  },
  "timestamp": "2024-03-01T08:12:44Z",
  "session_id": "sess-a1b2c3",
  "platform": "web"
}
```

The engine flattens nested objects into top-level columns by joining key names
with an underscore (`_`). The mapping rule is:

```
<parent_key>_<child_key>  →  column name
```

So `properties.amount` becomes the column `properties_amount`. This is true
whether you declare the column explicitly or let the engine infer it.

For deeply nested structures the same rule applies recursively:

| JSON path                    | Flattened column name          |
|------------------------------|--------------------------------|
| `.properties.amount`         | `properties_amount`            |
| `.properties.currency`       | `properties_currency`          |
| `.meta.geo.country`          | `meta_geo_country`             |
| `.audit.created.by.user_id`  | `audit_created_by_user_id`     |

> Arrays inside JSON objects are **not** flattened. If a value is a JSON array,
> the engine stores it as a raw JSON string in a `string`-typed column.

---

## Pipeline Definition Explained

```json
{
  "name": "events-jsonlines-source",
  "mode": "etl",
  "source": {
    "type": "jsonlines",
    "path": "./data/events.jsonl",
    "columns": [
      { "name": "event_id",          "type": "string" },
      { "name": "user_id",           "type": "int"    },
      { "name": "event_type",        "type": "string" },
      { "name": "properties_amount", "type": "float"  },
      { "name": "timestamp",         "type": "string" }
    ]
  },
  "transforms": [
    {
      "type": "filter",
      "expr": "event_type = 'purchase' OR event_type = 'refund'"
    },
    {
      "type": "map",
      "fields": {
        "id":     "=$event_id",
        "user":   "=$user_id",
        "type":   "=$event_type",
        "amount": "=$properties_amount",
        "ts":     "=$timestamp"
      }
    }
  ],
  "sink": { "type": "null" }
}
```

### Source fields

| Field | Value | Purpose |
|-------|-------|---------|
| `type` | `"jsonlines"` | Activates the NDJSON reader |
| `path` | `"./data/events.jsonl"` | Relative to the working directory at runtime |
| `columns` | array | Explicit schema; nested paths flatten automatically |
| `columns[].name` | string | Column name; use `parent_child` for nested fields |
| `columns[].type` | string | Cast target: `string`, `int`, `float`, `bool` |

### Transform 1 — filter

```json
{ "type": "filter", "expr": "event_type = 'purchase' OR event_type = 'refund'" }
```

Of the 20 input records, 6 are `purchase` events and 3 are `refund` events.
The filter passes 9 records and drops the remaining 11 (`view`, `click`,
`login`).

### Transform 2 — map

```json
{
  "type": "map",
  "fields": {
    "id":     "=$event_id",
    "user":   "=$user_id",
    "type":   "=$event_type",
    "amount": "=$properties_amount",
    "ts":     "=$timestamp"
  }
}
```

The map transform renames columns. The `=$fieldName` syntax copies the value
of an upstream column into the new output column. After the map, the row has
five columns with the shorter names: `id`, `user`, `type`, `amount`, `ts`.

### Sink — null

The null sink discards every row it receives. It is the correct sink to use
during development when the goal is to test the source or transforms without
writing data anywhere.

---

## Sample Data Breakdown

`data/events.jsonl` contains 20 records spread across five event types:

| event_type | Count | Passes filter? | properties.amount |
|------------|-------|----------------|-------------------|
| `purchase` | 6     | Yes            | 49.95 – 299.00    |
| `refund`   | 3     | Yes            | 49.95 – 299.00    |
| `view`     | 4     | No             | 0 (not monetary)  |
| `click`    | 4     | No             | 0 (not monetary)  |
| `login`    | 3     | No             | 0 (not monetary)  |

The `view`, `click`, and `login` records all have `"amount": 0` in their
`properties` object, which is correct — these events have no monetary value.
The filter intentionally excludes them.

Records that pass the filter:

| event_id  | user_id | event_type | properties_amount | timestamp            |
|-----------|---------|------------|-------------------|----------------------|
| evt-001   | 1001    | purchase   | 149.99            | 2024-03-01T08:12:44Z |
| evt-004   | 1055    | purchase   | 299.00            | 2024-03-01T08:17:11Z |
| evt-006   | 1001    | refund     | 149.99            | 2024-03-01T08:20:07Z |
| evt-008   | 1088    | purchase   | 79.50             | 2024-03-01T08:23:19Z |
| evt-011   | 1055    | refund     | 299.00            | 2024-03-01T08:26:47Z |
| evt-012   | 1014    | purchase   | 49.95             | 2024-03-01T08:28:03Z |
| evt-015   | 1088    | purchase   | 199.00            | 2024-03-01T08:32:08Z |
| evt-017   | 1014    | refund     | 49.95             | 2024-03-01T08:34:50Z |
| evt-019   | 1042    | purchase   | 129.99            | 2024-03-01T08:37:29Z |

---

## How to Run

### Prerequisites

- `flogo-datapipeline` binary on your `PATH`.
- Run from the `07-jsonlines-source/` directory (so relative paths resolve).

### Option A — run script

```bash
chmod +x run.sh
./run.sh
```

### Option B — direct invocation

```bash
cd 07-jsonlines-source/
flogo-datapipeline -f events-jsonlines-source.datapipeline
```

### Option C — verbose mode

```bash
flogo-datapipeline -f events-jsonlines-source.datapipeline --log-level debug
```

Debug mode logs each record as it is parsed, shows the flattened column values,
and reports filter decisions per row. Useful for verifying that nested field
extraction is working as expected.

---

## Expected Output

```
INFO  pipeline starting               name=events-jsonlines-source mode=etl
INFO  jsonlines source opened         path=./data/events.jsonl
INFO  schema applied                  columns=5 inferred=false
INFO  batch processed                 rows_read=20 rows_after_filter=9 batch=1
INFO  pipeline complete               rows_read=20 rows_written=0 duration=18ms
```

Key observations:

- `rows_read=20` — all 20 NDJSON lines were parsed without error.
- `rows_after_filter=9` — exactly the 6 purchase and 3 refund records survive.
- `rows_written=0` — the null sink consumed but discarded all 9 filtered rows.
- Exit code `0` — clean termination.

---

## How to Verify

After the run completes:

1. **Exit code:** `echo $?` should print `0`.
2. **No output files created** — the null sink writes nothing.
3. **Log line `rows_read=20`** — confirms all 20 NDJSON lines were parsed.
4. **Log line `rows_after_filter=9`** — confirms filter passed exactly 9 rows.
5. **Log line `rows_written=0`** — confirms null sink discarded all output.

To inspect what reaches the sink, temporarily change the sink to a parquet or
CSV file:

```json
"sink": {
  "type": "parquet",
  "path": "./output/filtered_events.parquet"
}
```

Then open `output/filtered_events.parquet` with a Parquet viewer to confirm the
8 expected rows and 5 renamed columns (`id`, `user`, `type`, `amount`, `ts`).

---

## Common Errors and Fixes

| Error message | Likely cause | Fix |
|---------------|--------------|-----|
| `failed to open source file` | Path is wrong or file missing | Check that `data/events.jsonl` exists relative to your working directory |
| `invalid JSON on line N` | Truncated or corrupt line | Inspect line N of the `.jsonl` file; it must be a complete JSON object |
| `type cast error: cannot convert ... to float` | A non-numeric value in `properties_amount` | Check the offending record; ensure all `properties.amount` values are numbers |
| `unknown column: properties_amount` | Nested path not flattened correctly | Use `properties_amount` (underscore) not `properties.amount` (dot) in the column name |
| `schema mismatch` | Column declared but key absent in some lines | JSONL records do not need to be uniform; missing keys resolve to the zero value for the declared type |

---

## Configuration Reference

### JSON Lines Source

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `path` | string | Yes | — | Absolute or relative to CWD at runtime |
| `columns` | array | No | inferred | If omitted, schema is inferred from first 100 lines |
| `columns[].name` | string | Yes | — | Use `parent_child` notation for nested fields |
| `columns[].type` | string | Yes | — | `string`, `int`, `float`, or `bool` |

### Filter Transform

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `expr` | string | Yes | Boolean expression; column names reference post-source columns |

Supported operators in `expr`: `=`, `!=`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `IN`, `LIKE`.

### Map Transform

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `fields` | object | Yes | Keys are output column names; values are `=$sourceColumn` expressions |

### Null Sink

The null sink accepts no configuration fields. An empty object `{}` is valid.

### Performance

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `batchSize` | int | `5000` | Rows buffered per internal batch before flush |
| `maxMemoryMB` | int | `512` | Soft heap limit; ingestion pauses if exceeded |

---

## Schema Inference vs Explicit Columns

| Consideration | Infer (omit columns) | Explicit columns |
|---------------|----------------------|------------------|
| Setup effort | None — just point at the file | Must list every column you need |
| Performance | Slower start (samples 100 lines) | Immediate; no sampling needed |
| Output width | All discovered keys included | Only the columns you declare |
| Nested fields | All levels flattened automatically | Must use `parent_child` naming |
| Sparse records | All keys union-merged; missing → zero value | Same |
| Schema changes | New keys appear automatically | New keys silently ignored |

For production pipelines, explicit columns are recommended: they make the
schema contract visible in the pipeline file and guard against unexpected new
fields polluting downstream systems.

---

## Next Steps

| Example | What it adds |
|---------|-------------|
| [08-kafka-source](../08-kafka-source/) | Read NDJSON records from a Kafka topic instead of a flat file |
| [09-objstore-source](../09-objstore-source/) | Read `.jsonl` files from S3/GCS/ADLS with a glob pattern |
| [03-validate-and-dlq](../03-validate-and-dlq/) | Add a validate transform to enforce not-null and range rules on the amount field |
| [04-http-lookup-enrich](../04-http-lookup-enrich/) | Enrich filtered events with a user profile fetched from a REST API |
