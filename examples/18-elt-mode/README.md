# Example 18 — ELT Mode (Extract → Load → Transform)

This example demonstrates the **ELT pattern**: raw data is extracted from a CSV file and loaded directly into a SQLite staging table — with **no transformations in the pipeline**. All normalisation, currency conversion, and aggregation happen inside the database via `transform.sql`, which runs as a post-load step.

This mirrors how modern cloud data warehouses (BigQuery, Snowflake, Redshift) and tools like **dbt** work: move data first, shape it later.

## What This Example Demonstrates

| Feature | Detail |
|---|---|
| `mode: elt` | Pipeline performs Extract and Load only; the `transforms` array is empty |
| Raw staging load | All 50 CSV rows are loaded as-is — messy region codes, mixed-case categories, multiple currencies |
| `onConflict: insert` | No deduplication at load time; the staging table is a raw append zone |
| Post-load SQL transform | `transform.sql` runs inside SQLite after the pipeline; builds `clean_sales` and four aggregate tables |
| `postLoadHint` | Metadata block in the `.datapipeline` file that documents (and can auto-invoke) the SQL step |
| dbt-style pattern | Same separation of concerns as a dbt project: pipeline = data movement; SQL = business logic |

---

## ELT vs ETL — When to Use Each

### ETL (Extract → Transform → Load)
The classic pattern. Transformations happen **inside the pipeline**, before data reaches the destination.

```
CSV ──► [filter] ──► [map] ──► [validate] ──► SQLite (clean table)
```

**Use ETL when:**
- The target database is small or resource-constrained (SQLite, MySQL on shared hosting)
- Transforms are simple (rename a column, drop bad rows)
- You want a single artefact (the pipeline definition) to capture all business rules
- You need to reduce the volume of data before it hits the network or target storage

### ELT (Extract → Load → Transform)
The modern cloud warehouse pattern. The pipeline only moves data. Transformations happen **inside the target** after load.

```
CSV ──► SQLite staging (raw) ──► [SQL / dbt] ──► SQLite analytics tables
         (pipeline)                (post-load)
```

**Use ELT when:**
- The target is a powerful analytic database — BigQuery, Snowflake, Redshift, DuckDB — that executes SQL transforms more efficiently than an application-layer pipeline
- Transforms are complex: multi-table joins, window functions, GROUP BY aggregations, CASE normalisation across millions of rows
- You want SQL (or dbt models) as the single source of truth for business logic, not a pipeline definition
- You need the raw data retained in the staging table for lineage, audit, or replay
- Data engineers who own the transforms prefer SQL, not pipeline DSL

### EL (Extract → Load only)
A subset of ELT where no post-load transform is defined at all. Used for pure data archiving or when the transform layer is managed completely outside the pipeline tool (e.g., a separate dbt Cloud job).

| Mode | Pipeline does | Transform where | Raw data preserved |
|---|---|---|---|
| `etl` | Extract + Transform + Load | In the pipeline | No (clean data only) |
| `elt` | Extract + Load | In the database (SQL/dbt) | Yes (staging table) |
| `el` | Extract + Load | Nowhere (handled externally) | Yes |

---

## The dbt Pattern

[dbt (data build tool)](https://www.getdbt.com/) is the dominant framework for the Transform step in ELT. A dbt project contains SQL `models` — SELECT statements that define how to build each analytics table from the staging data.

```
flogo-datapipeline run pipeline.datapipeline   # Extract + Load (EL)
dbt run                                         # Transform (T)
```

`transform.sql` in this example is a simplified, single-file version of what a dbt project would produce. In production you would replace `transform.sql` with:

```
dbt run --select clean_sales sales_by_region sales_by_category
```

The pipeline definition would stay identical — `mode: elt`, empty `transforms`, same `sink`.

---

## Data: Why the Staging Table Is Intentionally Messy

`data/raw_sales.csv` contains 50 rows with several data quality issues that are **deliberately left unfixed by the pipeline** and instead corrected by `transform.sql` inside the database:

| Issue | Examples in CSV | Fixed in |
|---|---|---|
| Mixed-case region codes | `emea`, `amer`, `apac` (should be `EMEA`, `AMER`, `APAC`) | `clean_sales` via `UPPER(region)` |
| Inconsistent category spelling | `'Electronics'`, `'ELECTRONICS'`, `'electronics'`, `'Food & Bev'`, `'food and bev'`, `'food and beverage'` | `clean_sales` via `CASE LOWER(raw_category)` |
| Multiple currencies | USD, EUR, GBP, JPY, INR, AUD | `clean_sales` via FX rate CASE expression |
| No aggregation | Individual transactions, not summaries | `sales_by_region`, `sales_by_category`, etc. |

In ETL mode you would add `map` and `filter` transforms to fix these inside the pipeline. In ELT mode you accept the raw data and fix it in SQL — because the target database is where the compute should run.

---

## Directory Layout

```
18-elt-mode/
├── 18-elt-mode.datapipeline    # Pipeline definition (mode: elt, transforms: [])
├── transform.sql               # Post-load SQL: normalise + aggregate inside SQLite
├── post-transform.sh           # Runs transform.sql and prints result tables
├── setup.sh                    # Creates analytics.db with raw_sales_staging table
├── verify.sh                   # Spot-checks the output after the full ELT run
├── data/
│   └── raw_sales.csv           # 50 rows of raw sales data (messy, multi-currency)
└── README.md
```

---

## Pipeline Definition Walkthrough

### Mode: elt

```json
"mode": "elt"
```

The `elt` mode flag tells the runtime that this pipeline intentionally performs no mid-pipeline transforms. The `transforms` array is empty. The runtime does not warn or fail on an empty transform chain in `elt` mode — it is expected.

### Source — CSV

```json
"source": {
  "type": "csv",
  "path": "./data/raw_sales.csv",
  "delimiter": ",",
  "hasHeader": true,
  "columns": [
    { "name": "id",           "type": "string" },
    { "name": "region",       "type": "string" },
    { "name": "amount",       "type": "float"  },
    { "name": "currency",     "type": "string" },
    { "name": "sale_date",    "type": "string" },
    { "name": "raw_category", "type": "string" }
  ]
}
```

The column is named `raw_category` (not `category`) to signal in the schema itself that this field is unprocessed source data. The final `category` column only exists in `clean_sales` after the SQL transform.

### Transforms — empty

```json
"transforms": []
```

This is the defining characteristic of ELT. No filter, no map, no validate — the pipeline is a pure data mover.

### Sink — JDBC staging table

```json
"sink": {
  "type": "jdbc",
  "driver": "sqlite",
  "dsn": "./data/analytics.db",
  "table": "raw_sales_staging",
  "batchSize": 1000,
  "onConflict": "insert"
}
```

`onConflict: insert` (plain INSERT, no upsert) is intentional. The staging table is a raw append zone. If you re-run the pipeline, new rows are appended — `transform.sql` uses `DROP TABLE IF EXISTS ... CREATE TABLE AS SELECT` which is idempotent regardless.

### postLoadHint — SQL transform metadata

```json
"postLoadHint": {
  "type": "sql",
  "script": "./transform.sql",
  "runner": "sqlite3",
  "runnerArgs": ["./data/analytics.db"]
}
```

`postLoadHint` is a metadata block — it documents the post-load step and can be used by orchestration tooling to automatically invoke `transform.sql` after the pipeline finishes. It does not change pipeline execution behaviour; the pipeline still stops at the sink. The actual execution of `transform.sql` is handled by `post-transform.sh`.

---

## The SQL Transform Layer (transform.sql)

`transform.sql` implements eight steps that run entirely inside SQLite:

| Step | Action | Output table |
|---|---|---|
| 1 | `UPPER(region)` — normalise region codes | `clean_sales` |
| 2 | `CASE LOWER(raw_category)` — collapse 6 spelling variants to 3 canonical names | `clean_sales` |
| 3 | Amount × FX rate — convert all currencies to USD | `clean_sales` |
| 4 | `CREATE TABLE clean_sales AS SELECT ...` — build the trusted fact table | `clean_sales` |
| 5 | `GROUP BY region` — revenue per region | `sales_by_region` |
| 6 | `GROUP BY category` — revenue per category | `sales_by_category` |
| 7 | `GROUP BY region, category` — cross-tab | `sales_by_region_category` |
| 8 | `GROUP BY sale_date` — daily time series | `daily_revenue` |

FX rates used (approximate 2024-Q1):

| Currency | Rate to USD |
|---|---|
| USD | 1.0000 |
| EUR | 1.0900 |
| GBP | 1.2700 |
| JPY | 0.0067 |
| INR | 0.0120 |
| AUD | 0.6500 |

---

## How to Run

### Prerequisites

- `sqlite3` CLI available on `PATH` (`brew install sqlite`, `apt install sqlite3`, or included in Windows Git Bash distributions)
- `flogo-datapipeline` CLI installed and on `PATH`

### Step 1 — Create the staging database

```bash
chmod +x setup.sh
./setup.sh
```

Creates `data/analytics.db` with the `raw_sales_staging` table. No analytics tables yet — those are built by `transform.sql` after the load.

### Step 2 — Run the pipeline (Extract + Load)

```bash
flogo-datapipeline run 18-elt-mode.datapipeline
```

Expected console output:

```
[source]  csv  ./data/raw_sales.csv  rows_read=50
[sink]    jdbc sqlite ./data/analytics.db  rows_written=50
```

Note: there is no `[transform]` line — the pipeline has no transform steps. All 50 rows are loaded verbatim.

### Step 3 — Run the SQL transform (Transform)

```bash
chmod +x post-transform.sh
./post-transform.sh
```

This runs `transform.sql` inside `analytics.db` and prints each output table.

### Step 4 — Verify the results

```bash
chmod +x verify.sh
./verify.sh
```

Or run all four steps in sequence:

```bash
./setup.sh && \
flogo-datapipeline run 18-elt-mode.datapipeline && \
./post-transform.sh && \
./verify.sh
```

---

## Expected Output

### raw_sales_staging (after pipeline)

All 50 rows loaded exactly as they appear in the CSV — messy case, mixed currencies, inconsistent categories:

```
id    region  amount     currency  sale_date   raw_category
----  ------  ---------  --------  ----------  ----------------
S001  AMER    1250.0     USD       2024-01-03  Electronics
S002  emea    875.5      EUR       2024-01-03  electronics
S003  APAC    42000.0    JPY       2024-01-04  ELECTRONICS
S004  AMER    3400.0     USD       2024-01-04  Clothing
...
```

### clean_sales (after transform.sql)

Normalised, USD-converted, all 50 rows retained:

```
id    region  category       raw_currency  raw_amount  amount_usd  sale_date
----  ------  -------------  ------------  ----------  ----------  ----------
S001  AMER    Electronics    USD           1250.0      1250.0      2024-01-03
S002  EMEA    Electronics    EUR           875.5       954.3       2024-01-03
S003  APAC    Electronics    JPY           42000.0     281.4       2024-01-04
S004  AMER    Clothing       USD           3400.0      3400.0      2024-01-04
S005  EMEA    Clothing       GBP           210.75      267.65      2024-01-05
```

### sales_by_region

```
region  deal_count  total_revenue_usd  avg_deal_usd  min_deal_usd  max_deal_usd
------  ----------  -----------------  ------------  ------------  ------------
AMER    17          ...                ...           63.65         8100.0
APAC    17          ...                ...           ...           ...
EMEA    16          ...                ...           ...           ...
```

### sales_by_category

```
category        deal_count  total_revenue_usd  avg_deal_usd
--------------  ----------  -----------------  ------------
Electronics     17          ...                ...
Clothing        17          ...                ...
Food & Beverage 16          ...                ...
```

### Category normalisation verification

The 6 raw spellings from the CSV collapse to exactly 3 canonical values:

| Raw values in staging | Canonical value in clean_sales |
|---|---|
| `Electronics`, `ELECTRONICS`, `electronics` | `Electronics` |
| `Clothing`, `CLOTHING`, `clothing` | `Clothing` |
| `Food & Bev`, `food and bev`, `food and beverage`, `Food & Beverage` | `Food & Beverage` |

---

## Re-running (Idempotency)

Because `transform.sql` uses `DROP TABLE IF EXISTS ... CREATE TABLE AS SELECT`, the SQL transform step is fully idempotent — run it 10 times and `clean_sales` always has exactly 50 rows.

The pipeline itself uses `onConflict: insert`, so re-running the pipeline **appends** 50 more rows to `raw_sales_staging`. To reset to a clean state, run `setup.sh` again before re-running the pipeline.

For a fully idempotent ELT pipeline, use one of:
- `onConflict: replace` with a `conflictKey` on `id`
- Truncate the staging table before each pipeline run (add a pre-load SQL step)
- Partition the staging table by load date and overwrite only the current partition

---

## mode: elt vs mode: el — Key Distinction

| Mode | Transforms array | postLoadHint | Use case |
|---|---|---|---|
| `elt` | Must be empty; runtime enforces this | Recommended — documents the SQL step | You own both load and transform; transform.sql is part of this repo |
| `el` | Must be empty | Absent or omitted | Transform is handled by a completely separate team / tool (e.g. dbt Cloud CI job) |

In `el` mode the pipeline is purely a data movement primitive. The transform layer is invisible to the pipeline definition.

---

## Key Configuration Reference

### Top-level mode field

| Value | Meaning |
|---|---|
| `"etl"` | Full ETL: transforms[] is executed between source and sink |
| `"elt"` | ELT: transforms[] must be empty; data is loaded raw; transform happens post-load |
| `"el"` | Extract and Load only; no transforms at any stage |

### postLoadHint object

| Field | Type | Description |
|---|---|---|
| `type` | string | `"sql"` — indicates a SQL script post-load transform |
| `script` | string | Relative path to the SQL file (e.g. `"./transform.sql"`) |
| `runner` | string | CLI used to execute the script (`"sqlite3"`, `"psql"`, `"bq"`, `"snowsql"`) |
| `runnerArgs` | array | Arguments passed to the runner before the script (e.g. DSN, database name) |

`postLoadHint` is informational metadata. It does not cause the runtime to execute the script automatically — use `post-transform.sh` or an orchestrator (Airflow DAG, dbt job) to run it.

### jdbc sink — ELT-specific options

| Option | Recommended for ELT | Reason |
|---|---|---|
| `onConflict: "insert"` | Yes, for staging tables | Raw append zone; duplicates resolved in SQL layer |
| `onConflict: "replace"` | Yes, for idempotent staging | Replaces rows by key; safe for pipeline reruns |
| `onConflict: "upsert"` | Only if staging has a primary key | Staging tables rarely have primary key constraints |
| `batchSize` | 1000–10000 | Larger batches = fewer round-trips; beneficial for high-volume ELT loads |

---

## Common Modifications

**Switch to PostgreSQL staging (same pipeline, different sink DSN):**

```json
"sink": {
  "type": "jdbc",
  "driver": "postgres",
  "dsn": "host=dwh.example.com port=5432 user=loader password=secret dbname=raw sslmode=require",
  "table": "raw_sales_staging",
  "batchSize": 5000,
  "onConflict": "insert"
}
```

Then run `transform.sql` with `psql`:

```bash
psql "$DSN" < transform.sql
```

**Replace transform.sql with a dbt project:**

```bash
# After the pipeline loads raw_sales_staging:
cd dbt_project/
dbt run --select clean_sales sales_by_region sales_by_category
```

**Add a high-water mark to avoid re-loading rows (incremental ELT):**

Add a `hwm` transform to the pipeline (even in `elt` mode, hwm is a load-control mechanism, not a data transform):

```json
"transforms": [
  {
    "type": "hwm",
    "column": "sale_date",
    "backend": "file",
    "backendConfig": { "path": "./data/.hwm_sales" }
  }
]
```

Then in SQL, partition the staging table by load date and rebuild only the affected partitions.

**Add a DQ check on the staging table after load (SQL-side):**

```sql
-- In transform.sql, before building clean_sales:
SELECT CASE WHEN COUNT(*) = 0
       THEN RAISE(ABORT, 'DQ FAIL: raw_sales_staging is empty')
       END
FROM raw_sales_staging;
```

This keeps quality assertions in SQL alongside the business logic, consistent with the dbt `tests:` pattern.
