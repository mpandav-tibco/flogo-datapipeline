# Example 11 — Schema Evolution & Drift Handling

Demonstrates the `schemadrift` transform and its four policies: **additive**, **strict**, **drop**, and **project**. The example uses two CSV files — a baseline v1 schema and an evolved v2 schema with two extra columns — to show how each policy reacts when the incoming data does not match the declared pipeline schema.

---

## The Problem: Upstream Schema Changes Break Pipelines

In enterprise data integration, the most common source of silent failures is not bad data values — it is a schema change from an upstream system that the pipeline was not told about.

Consider a nightly vendor file drop:

- **Week 1** the vendor sends `id,name,status`. Your pipeline is configured for exactly those three columns.
- **Week 4** the vendor adds `region` and `score` without warning. Your pipeline breaks, silently drops columns, or — worst of all — maps the wrong values into the wrong fields.

The `schemadrift` transform gives you explicit, auditable control over how the pipeline reacts when the actual schema deviates from the declared one.

---

## Data Files

```
data/
  v1_records.csv   — 10 rows, original schema (id, name, status)
  v2_records.csv   — 10 rows, evolved schema (id, name, status, region, score)
```

### v1_records.csv — baseline schema

```
id,name,status
1001,Alice Monroe,ACTIVE
1002,Brian Taft,INACTIVE
...
```

Three columns. This is what the pipeline was originally designed for.

### v2_records.csv — evolved schema

```
id,name,status,region,score
1001,Alice Monroe,ACTIVE,EMEA,92.4
1002,Brian Taft,INACTIVE,AMER,61.0
...
```

Two new columns added by the upstream vendor: `region` (string) and `score` (float). The `schemadrift` transform detects this mismatch when it compares the file's actual header against the `columns` array declared in the source block.

---

## Pipeline Files

| File | Policy | Input | Expected outcome |
|------|--------|-------|-----------------|
| `additive.datapipeline` | `additive` | `v2_records.csv` | Succeeds; extra columns forwarded |
| `strict.datapipeline` | `strict` | `v2_records.csv` | Fails with schema mismatch error |

Both pipelines declare the same 3-column source schema and use `sink: null` so all attention stays on drift detection.

---

## The Four Policies

### `additive` — accept growth, preserve history

```json
{ "type": "schemadrift", "policy": "additive" }
```

**What it does.** Columns that appear in the declared schema are always passed through. Columns that appear in the *file* but not in the declared schema are appended at the end of each row. Downstream sinks that are schema-aware (JDBC, Parquet) will either auto-extend or need a column-add migration; schema-agnostic sinks (null, DLQ CSV) forward them verbatim.

**When to use it.**
- Append-only data lakes where new columns are always additive.
- Vendor integrations where the supplier adds instrumentation columns over time.
- Early-stage pipelines where you want to capture everything and curate later.

**What it does NOT do.** It does not detect column *renames* (a renamed column looks like a delete + add). It does not fail loudly — new columns arrive silently, which may surprise downstream consumers.

---

### `strict` — enforce the contract, fail fast

```json
{ "type": "schemadrift", "policy": "strict" }
```

**What it does.** If the incoming file contains any column not listed in the source `columns` array, the pipeline aborts immediately with a schema mismatch error before processing any rows. Equally, if a declared column is missing from the file, the pipeline also fails.

**When to use it.**
- Regulated or financial pipelines where schema changes must go through a change-management process.
- Downstream systems that are strictly typed (e.g. a dimensional model with a fixed star schema).
- Situations where a silent schema change would propagate bad data into production reports.

**What it does NOT do.** It does not route bad rows to a DLQ — the entire pipeline halts. Pair with an alerting hook to page the on-call engineer.

---

### `drop` — ignore the extra, keep the known

```json
{ "type": "schemadrift", "policy": "drop" }
```

**What it does.** Extra columns found in the file are silently discarded. Only the columns listed in the declared schema are passed downstream. The pipeline continues without error.

**When to use it.**
- You own the sink schema and it is fixed. You do not want new vendor columns polluting your target table.
- You need maximum resilience — any file the vendor sends will be processed, stripped to the known columns.

**What it does NOT do.** New columns are permanently discarded. If a column you later need was silently dropped for weeks, recovery requires a reload. Use `additive` if retention matters.

---

### `project` — alias for `drop` with semantic clarity

```json
{ "type": "schemadrift", "policy": "project" }
```

**What it does.** Functionally identical to `drop`: the pipeline projects only the declared columns from the input, ignoring any extras. The name `project` is borrowed from relational algebra and SQL (`SELECT` without `*`), making the intent clearer in code review — you are *projecting* a known column set, not *dropping* unexpected garbage.

**When to use it.**
- Anywhere `drop` applies, but when you want the intent to be self-documenting.
- Shared pipeline configurations reviewed by data engineers who prefer relational terminology.

---

## Policy Comparison Table

| Policy | Extra columns | Missing columns | Pipeline continues? | Recommended for |
|--------|--------------|-----------------|--------------------|----|
| `additive` | Forwarded | Null-padded | Yes | Data lakes, exploratory pipelines |
| `strict` | Error — abort | Error — abort | No | Regulated, contract-critical pipelines |
| `drop` | Discarded | Error — abort | Yes (extra) / No (missing) | Fixed-schema sinks, resilience-first |
| `project` | Discarded | Error — abort | Yes (extra) / No (missing) | Same as drop, clearer semantics |

---

## Pipeline Anatomy

Both `.datapipeline` files follow the same structure. Here is `additive.datapipeline` annotated:

```json
{
  "name": "schema-drift-additive",
  "mode": "etl",
  "source": {
    "type": "csv",
    "path": "./data/v2_records.csv",
    "delimiter": ",",
    "hasHeader": true,
    "columns": [           // ← declared schema: only original 3 columns
      { "name": "id",     "type": "string" },
      { "name": "name",   "type": "string" },
      { "name": "status", "type": "string" }
    ]
  },
  "transforms": [
    {
      "type": "schemadrift",
      "policy": "additive"  // ← drift policy
    }
  ],
  "sink": { "type": "null" }
}
```

The only difference in `strict.datapipeline` is `"policy": "strict"`.

---

## How to Run

### Prerequisites

Build the engine binary (one-time):

```bash
cd ../../
go build -o flogo-datapipeline ./cmd/...
```

### Run both pipelines

```bash
chmod +x run.sh
./run.sh
```

### Run a single pipeline

```bash
./run.sh additive   # only the additive run
./run.sh strict     # only the strict run
```

### Run directly

```bash
# additive — should succeed
../../flogo-datapipeline run additive.datapipeline

# strict — should fail with schema mismatch
../../flogo-datapipeline run strict.datapipeline
```

---

## Expected Output

### Additive pipeline (`additive.datapipeline`)

```
Running: ../../flogo-datapipeline run additive.datapipeline
------------------------------------------------------------------------
[INFO]  source: opened ./data/v2_records.csv (5 columns detected, 3 declared)
[INFO]  schemadrift: policy=additive; extra columns=[region score]; appending
[INFO]  sink: null — 10 rows discarded
------------------------------------------------------------------------
Result: SUCCESS — all 10 rows processed; extra columns forwarded.
```

Key log lines to look for:
- `extra columns=[region score]` — confirms the engine detected the two new columns.
- `policy=additive; ...appending` — confirms the policy action taken.
- `10 rows` — all rows were processed successfully.

### Strict pipeline (`strict.datapipeline`)

```
Running: ../../flogo-datapipeline run strict.datapipeline
------------------------------------------------------------------------
[ERROR] schemadrift: policy=strict; undeclared columns=[region score]; aborting
------------------------------------------------------------------------
Result: FAILED as expected — schema mismatch detected and pipeline aborted.
```

Key log lines to look for:
- `policy=strict; undeclared columns=[region score]` — identifies the exact offending columns.
- `aborting` — confirms zero rows were written before the abort.
- Non-zero exit code — the script captures this and prints the "FAILED as expected" message.

---

## Verifying Results

Because both sinks are `null`, there is no output file to inspect. Verification is done via:

1. **Exit code** — the additive pipeline exits `0`; the strict pipeline exits non-zero.
2. **Log output** — look for `extra columns`, `policy=`, and `aborting` in the engine logs.
3. **Row count in logs** — additive should log `10 rows`; strict should log `0 rows` (or no row-count log at all, since it aborts before processing).

To test other policies, copy either pipeline file and change the `policy` field:

```bash
cp additive.datapipeline drop.datapipeline
# edit drop.datapipeline: change "policy": "additive" → "policy": "drop"
../../flogo-datapipeline run drop.datapipeline
```

---

## Real-World Scenario: Nightly Vendor File Drop

A financial services team receives a nightly transaction file from a payment processor. For two years, the file has had five columns. Then one quarter the processor adds three instrumentation columns for their own internal tracking.

**Without schema drift handling:** the pipeline silently maps wrong values to wrong columns (or crashes on type mismatches), and bad data enters the ledger.

**With `strict` policy:** the pipeline alerts on-call at 2 AM the first night the new file arrives. The team reviews the change, updates the declared schema, and re-runs. Zero bad data enters the ledger.

**With `additive` policy:** the pipeline continues processing. The new columns are captured in the raw landing zone automatically. An analyst notices them in the data catalog the next morning and decides whether to promote them to the curated layer.

Which policy is correct depends on your data governance model. The `schemadrift` transform makes that choice explicit, auditable, and enforced — rather than leaving it to chance.

---

## Key Configuration Reference

```json
{
  "type": "schemadrift",
  "policy": "additive | strict | drop | project"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Must be `"schemadrift"` |
| `policy` | string | Yes | One of: `additive`, `strict`, `drop`, `project` |

The transform has no other configuration fields. All behaviour is determined by the policy name and the `columns` array declared in the source block.

---

## Related Examples

| Example | What it shows |
|---------|---------------|
| 03 — Validate + DLQ | Field-level validation; route bad rows to a dead-letter queue |
| 10 — CSV to SQLite | Full ETL with validate + map + JDBC upsert |
| 08 — Parquet Columnar | Parquet source where column projection is built into the file format |
