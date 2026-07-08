# Example 08 — Parquet Columnar Source

**Mode:** ETL (Extract, Transform, Load)
**Source:** Parquet file with column pruning
**Transforms:** Filter
**Sink:** Null (discard)

This example demonstrates the Parquet source with two performance-critical
features: **column pruning** and **row-group parallelism**. The source file
contains 7 columns, but the pipeline declares only 4. The engine passes this
narrowed column list to the Parquet reader, so only the byte ranges for the
declared columns are read from disk. The remaining 3 columns are never fetched.
Row groups are decoded by 2 concurrent goroutines, halving decode time on
multi-row-group files.

A filter transform then retains only `COMPLETED` transactions with
`amount > 50.0` before the null sink discards the survivors.

---

## Directory Layout

```
08-parquet-columnar/
├── transactions-parquet-columnar.datapipeline   # Pipeline definition (JSON)
├── data/
│   ├── transactions.parquet                     # Binary Parquet file (generate first — see below)
│   ├── generate_transactions.py                 # Python script to create the Parquet file
│   └── transactions.parquet.README.md           # Parquet generation guide (Python + DuckDB)
├── run.sh                                       # Convenience run script
└── README.md                                    # This file
```

---

## Prerequisites

1. `flogo-datapipeline` binary on your `PATH`.
2. `data/transactions.parquet` must be generated before the first run (see
   [Generating the Parquet File](#generating-the-parquet-file)).
3. Run from the `08-parquet-columnar/` directory so relative paths resolve.

---

## Generating the Parquet File

Parquet is a binary columnar format and cannot be committed as plain text. Use
one of the two methods below to create `data/transactions.parquet` locally.

### Option A — Python / pandas (deterministic, recommended)

```bash
pip install pandas pyarrow
python data/generate_transactions.py
```

The script writes 50 rows with 7 columns to `data/transactions.parquet` and
prints a brief summary:

```
Written 50 rows to .../data/transactions.parquet

Column schema:
  txn_id       object
  amount       float64
  currency     object
  status       object
  merchant     object
  region       object
  card_last4   object

Rows that will pass the pipeline filter (status=COMPLETED AND amount>50.0): 28
Rows that will be dropped by the filter: 22
```

### Option B — DuckDB CLI

```bash
duckdb -c "
COPY (
  SELECT
    'TXN-' || printf('%04d', n)                                       AS txn_id,
    round(10 + random() * 990, 2)                                      AS amount,
    ['USD','EUR','GBP','CAD','AUD','JPY'][1 + (n % 6)]                AS currency,
    ['COMPLETED','PENDING','FAILED','CANCELLED'][1 + (n % 4)]          AS status,
    'Merchant-' || n                                                   AS merchant,
    'REGION-' || (n % 5)                                               AS region,
    printf('%04d', (n * 7919) % 10000)                                 AS card_last4
  FROM generate_series(1, 50) t(n)
) TO 'data/transactions.parquet' (FORMAT PARQUET);
"
```

> The DuckDB snippet uses `random()` so exact filter-pass counts will differ
> from the Python version. Use Option A for reproducible expected output.

See `data/transactions.parquet.README.md` for a full verification snippet.

---

## Pipeline Definition Explained

```json
{
  "name": "transactions-parquet-columnar",
  "mode": "etl",
  "source": {
    "type": "parquet",
    "path": "./data/transactions.parquet",
    "columns": [
      { "name": "txn_id",   "type": "string" },
      { "name": "amount",   "type": "float"  },
      { "name": "currency", "type": "string" },
      { "name": "status",   "type": "string" }
    ],
    "rowGroupParallelism": 2
  },
  "transforms": [
    {
      "type": "filter",
      "expr": "status = 'COMPLETED' AND amount > 50.0"
    }
  ],
  "sink": { "type": "null" },
  "performance": { "batchSize": 5000, "maxMemoryMB": 512 },
  "scaling":     { "mode": "single" },
  "checkpoint":  { "type": "none" }
}
```

### Key fields

| Field | Value | Purpose |
|-------|-------|---------|
| `mode` | `"etl"` | Extract, Transform, Load — transforms are active |
| `source.type` | `"parquet"` | Read a columnar Parquet file |
| `source.path` | `"./data/transactions.parquet"` | Relative to CWD when the engine is invoked |
| `source.columns` | 4-element array | **Column pruning list** — only these 4 columns are read from disk; the other 3 in the file (`merchant`, `region`, `card_last4`) are ignored |
| `source.rowGroupParallelism` | `2` | Decode up to 2 row groups concurrently; set to 1 for single-threaded reads |
| `transforms[0].type` | `"filter"` | Drop rows that do not match the expression |
| `transforms[0].expr` | `"status = 'COMPLETED' AND amount > 50.0"` | Retain high-value completed transactions |
| `sink.type` | `"null"` | Discard all rows; writes nothing to disk |
| `performance.batchSize` | `5000` | Rows per internal buffer flush |
| `scaling.mode` | `"single"` | Single-threaded pipeline; row-group parallelism is intra-source, not cross-pipeline |

---

## What is Column Pruning?

Parquet stores each column in its own contiguous byte range inside the file
(a *column chunk*). When you declare fewer columns than the file contains, the
engine passes the pruned column list to the Parquet reader. The reader computes
exactly which byte offsets to `seek()` to and skips everything else.

```
File on disk (7 columns):
┌──────────┬────────┬──────────┬────────┬──────────┬────────┬────────────┐
│  txn_id  │ amount │ currency │ status │ merchant │ region │ card_last4 │
│   READ   │  READ  │   READ   │  READ  │  SKIP    │  SKIP  │   SKIP     │
└──────────┴────────┴──────────┴────────┴──────────┴────────┴────────────┘
                                                      ^^^^^^^^^^^^^^^^^^^^^^^^
                                                      Bytes never fetched
```

For a wide table — say 100 columns, of which you need 5 — column pruning
typically reduces I/O by 90-95% and decoding CPU by a proportional amount.
The effect is most pronounced on cloud object storage (S3, GCS, ADLS) where
random seeks are cheap but byte transfer is metered.

### Predicate Pushdown

Many Parquet writers record column statistics (min/max values, null counts)
per row group in the file footer. The engine reads these statistics before
accessing row-group data. If a row-group's min/max range guarantees that no
row in that group can satisfy the filter expression, the entire row group is
skipped without decoding. This is called *predicate pushdown* or *row-group
pruning*.

For example, if a row group's `amount` column statistics show `max=45.0` and
your filter requires `amount > 50.0`, the engine skips that row group entirely.
Predicate pushdown is automatic when column statistics are present; it requires
no pipeline configuration.

---

## What is Row-Group Parallelism?

A Parquet file is divided into horizontal slices called *row groups*. A file
produced with PyArrow's default `row_group_size=128 MB` might contain several
groups for a large dataset. The `rowGroupParallelism` field controls how many
groups the engine decodes simultaneously.

```
File with 4 row groups, rowGroupParallelism=2:

  Time →
  Worker 1: [Row Group 0] ────── [Row Group 2] ──────
  Worker 2:       [Row Group 1] ────── [Row Group 3] ──────
```

Increasing `rowGroupParallelism` beyond the number of row groups in your file
has no effect — the engine caps concurrency at `min(parallelism, num_row_groups)`.

Recommended values:

| File size | Row groups (approx.) | Recommended parallelism |
|-----------|----------------------|-------------------------|
| < 128 MB  | 1                    | 1 (no benefit above 1)  |
| 128–512 MB| 2–4                  | 2                       |
| 512 MB–2 GB| 5–16               | 4                       |
| > 2 GB    | 16+                  | 8 (maximum)             |

The small test file used in this example (`transactions.parquet`, ~50 rows)
likely contains a single row group. `rowGroupParallelism: 2` is set here to
illustrate the configuration; on a production-scale file it would yield a
meaningful speedup.

---

## Parquet vs CSV — When to Use Which

| Concern | CSV | Parquet |
|---------|-----|---------|
| Human-readable | Yes — open in any text editor | No — binary format |
| Column pruning | No — full row must be parsed | Yes — only declared columns read |
| Compression | None (or external gzip) | Built-in Snappy/ZSTD/Gzip per column |
| Schema | Inferred or declared at read time | Embedded in file footer |
| Row filtering (predicate pushdown) | No | Yes — skip row groups via statistics |
| Write speed | Fast | Slower (encode + compress) |
| Read speed (full table) | Comparable | Faster for large files due to compression |
| Read speed (few columns) | Same as full table | Much faster — skip unneeded columns |
| Streaming / append | Easy | Hard — Parquet files are sealed at write time |
| Tooling | Universal | Requires Parquet-aware reader (pandas, DuckDB, Spark, …) |
| Best for | Small files, human-edited data, interchange | Analytical workloads, wide tables, data lakes |

**Rule of thumb:** Use CSV when humans need to read or edit the file, or when
the toolchain is simple and column count is small (< 20). Use Parquet when the
file is consumed by a pipeline or analytics engine, the table is wide, or the
dataset is large enough that I/O savings matter.

---

## How to Run

### Option A — run script (recommended)

```bash
cd 08-parquet-columnar/
chmod +x run.sh
./run.sh
```

The script performs a preflight check for `data/transactions.parquet` and
prints instructions if the file is missing.

### Option B — direct invocation

```bash
cd 08-parquet-columnar/
flogo-datapipeline -f transactions-parquet-columnar.datapipeline
```

### Option C — verbose mode

```bash
flogo-datapipeline -f transactions-parquet-columnar.datapipeline --log-level debug
```

Debug mode prints per-row-group I/O stats, column pruning decisions, and
predicate-pushdown skip counts.

---

## Expected Output

```
INFO  pipeline starting               name=transactions-parquet-columnar mode=etl
INFO  parquet source opened           path=./data/transactions.parquet columns=4 file_columns=7 pruned=3
INFO  row group scan                  groups=1 parallelism=2 rows_per_group=50
INFO  batch processed                 rows=50 batch=1
INFO  filter applied                  expr="status = 'COMPLETED' AND amount > 50.0" passed=28 dropped=22
INFO  pipeline complete               rows_read=50 rows_written=0 duration=8ms
```

Key observations:

- `columns=4 file_columns=7 pruned=3` — the engine confirms that 3 columns
  (`merchant`, `region`, `card_last4`) were pruned and never read.
- `rows_read=50` — all 50 rows were decoded from the declared 4 columns.
- `passed=28 dropped=22` — 28 of 50 transactions are `COMPLETED` with
  `amount > 50.0`; the remaining 22 are filtered out.
- `rows_written=0` — the null sink discards the 28 survivors.
- Exit code `0` — clean termination.

---

## How to Verify

After the run:

1. **Exit code:** `echo $?` should print `0`.
2. **No output files:** the null sink writes nothing to disk. Confirm no new
   files appear in the working directory.
3. **Log line `pruned=3`:** confirms merchant, region, and card_last4 were
   never read.
4. **Log line `rows_read=50`:** confirms all 50 rows were decoded.
5. **Log line `passed=28`:** confirms the filter is working correctly.

To cross-check the filter count independently:

```python
import pandas as pd
df = pd.read_parquet("data/transactions.parquet")
passed = df[(df["status"] == "COMPLETED") & (df["amount"] > 50.0)]
print(len(passed))   # expected: 28
```

---

## Data Layout

`transactions.parquet` has 7 columns but the pipeline reads only 4:

| Column | Type | Declared in pipeline | Notes |
|--------|------|----------------------|-------|
| `txn_id` | string | Yes | Unique transaction identifier |
| `amount` | float | Yes | Transaction value; filter applies here |
| `currency` | string | Yes | ISO 4217 currency code |
| `status` | string | Yes | `COMPLETED`, `PENDING`, `FAILED`, `CANCELLED`; filter applies here |
| `merchant` | string | **No** | Pruned — byte range never read |
| `region` | string | **No** | Pruned — byte range never read |
| `card_last4` | string | **No** | Pruned — byte range never read |

Filter breakdown across the 50 rows:

| Status | Count | Amount <= 50 | Amount > 50 | Passes filter |
|--------|-------|--------------|-------------|---------------|
| COMPLETED | 31 | 3 | 28 | 28 |
| PENDING | 8 | 8 | 0 | 0 (wrong status) |
| FAILED | 6 | 3 | 3 | 0 (wrong status) |
| CANCELLED | 5 | 4 | 1 | 0 (wrong status) |
| **Total** | **50** | — | — | **28** |

---

## Configuration Reference

### Parquet Source

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `path` | string | Yes | — | Absolute or relative to CWD |
| `columns` | array | Yes | — | `[{name, type}]` — column pruning list; only these columns are read |
| `rowGroupParallelism` | int | No | `1` | Concurrent row-group decoders; range 1–8 |

Supported column types:

| Type | Parquet physical type | Notes |
|------|-----------------------|-------|
| `string` | `BYTE_ARRAY` (UTF-8) | Also maps to `FIXED_LEN_BYTE_ARRAY` |
| `int` | `INT32` / `INT64` | Engine selects width based on Parquet schema |
| `float` | `FLOAT` / `DOUBLE` | Engine selects precision based on Parquet schema |
| `bool` | `BOOLEAN` | — |

### Filter Transform

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `expr` | string | Yes | Boolean SQL-like expression; column names match `source.columns[].name` |

Supported operators in `expr`:

| Operator | Example |
|----------|---------|
| Comparison | `amount > 50.0`, `status = 'COMPLETED'` |
| Logical AND | `status = 'COMPLETED' AND amount > 50.0` |
| Logical OR | `currency = 'USD' OR currency = 'EUR'` |
| Negation | `status != 'CANCELLED'` |
| NULL check | `txn_id IS NOT NULL` |

### Null Sink

The null sink accepts no configuration fields. Use `"sink": { "type": "null" }`.

### Performance

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `batchSize` | int | `5000` | Rows per internal buffer flush |
| `maxMemoryMB` | int | `512` | Soft heap limit; engine pauses ingestion if exceeded |

### Scaling

| Field | Options | Notes |
|-------|---------|-------|
| `mode` | `"single"` | Single pipeline thread; `rowGroupParallelism` provides intra-source concurrency independent of this setting |

### Checkpoint

| Field | Options | Notes |
|-------|---------|-------|
| `type` | `"none"` | No state persisted; pipeline always re-reads the entire file on restart |

---

## Variations to Try

### 1 — Read all columns (disable pruning)

Declare all 7 columns to observe the I/O difference:

```json
"columns": [
  { "name": "txn_id",     "type": "string" },
  { "name": "amount",     "type": "float"  },
  { "name": "currency",   "type": "string" },
  { "name": "status",     "type": "string" },
  { "name": "merchant",   "type": "string" },
  { "name": "region",     "type": "string" },
  { "name": "card_last4", "type": "string" }
]
```

The log should now show `pruned=0` and slightly higher memory usage.

### 2 — Single-threaded row-group decode

```json
"rowGroupParallelism": 1
```

For a single-row-group file this makes no measurable difference. On a large
multi-row-group file, compare wall-clock times between `1` and `4`.

### 3 — Currency filter

Change the filter to retain only USD transactions:

```json
{ "type": "filter", "expr": "status = 'COMPLETED' AND amount > 50.0 AND currency = 'USD'" }
```

Expected survivors: rows that are COMPLETED, amount > 50, and currency = USD.
Cross-check with `python -c "import pandas as pd; df=pd.read_parquet('data/transactions.parquet'); print(len(df[(df.status=='COMPLETED') & (df.amount>50) & (df.currency=='USD')]))"`.

### 4 — Write survivors to a new Parquet file

Replace the null sink with a Parquet sink to produce a filtered output file:

```json
"sink": {
  "type": "parquet",
  "path": "./data/completed_high_value.parquet",
  "rowGroupSize": 1000
}
```

---

## Next Steps

| Example | What it adds |
|---------|-------------|
| [01-csv-to-null](../01-csv-to-null/) | Baseline CSV read — compare I/O semantics with Parquet |
| [02-filter-and-map](../02-filter-and-map/) | Chain filter + map transforms on the pruned columns |
| [03-validate-and-dlq](../03-validate-and-dlq/) | Add validation rules and route bad rows to a dead-letter queue |
| [09-parquet-sink](../09-parquet-sink/) | Write filtered rows to a Parquet file instead of discarding them |
