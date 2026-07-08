# Example 09 — JDBC Database Source (SQLite)

**Mode:** EL (Extract-Load, no transforms)
**Source:** JDBC — SQLite database table
**Sink:** Null (discard)
**Features demonstrated:** JDBC source, keyset pagination, partition-aware reads, SQLite as a zero-dependency test database

---

## Overview

This example shows how to read rows from a relational database table using the JDBC source. Two core capabilities are demonstrated:

**Keyset pagination** — instead of loading the entire table into memory with a single `SELECT *`, the engine issues a sequence of bounded queries, each starting where the previous one left off:

```sql
-- Page 1 (partition 0, first batch)
SELECT * FROM products WHERE product_id > 0   ORDER BY product_id LIMIT 100;

-- Page 2 (partition 0, next batch)
SELECT * FROM products WHERE product_id > 100  ORDER BY product_id LIMIT 100;
```

Memory usage is bounded by `batchSize`, not by the size of the table. A 10-row table and a 10-billion-row table consume the same working set.

**Partition-aware reads** — the key range is split across `N` independent workers, each responsible for a non-overlapping slice of rows. With `partitions: 2` and 50 rows whose `product_id` runs from 1–50, the engine assigns:

- Partition 0: `product_id` in [1, 25]
- Partition 1: `product_id` in [26, 50]

Each partition runs its own keyset-paginated read loop in parallel, so the effective throughput scales with the number of partitions.

**SQLite** is used here because it requires no server, no credentials, and no installation beyond the `sqlite3` CLI needed to seed the database file. Everything runs from a single `.db` file on disk. The JDBC source configuration is otherwise identical to PostgreSQL or MySQL — only `driver` and `dsn` change.

---

## Directory Layout

```
09-jdbc-source-sqlite/
├── inventory-jdbc-source.datapipeline   # Pipeline definition (JSON)
├── data/
│   └── inventory.db                     # SQLite database (created by setup.sh)
├── setup.sh                             # Creates the database (Linux / macOS)
├── setup.bat                            # Creates the database (Windows)
├── run.sh                               # Runs the pipeline (Linux / macOS)
└── README.md                            # This file
```

> `data/inventory.db` is created by `setup.sh` / `setup.bat` and is not committed to version control.

---

## Sample Data

The `products` table contains 50 rows across five categories:

| Category    | product_id range | Row count |
|-------------|-----------------|-----------|
| Electronics | 1 – 10          | 10        |
| Furniture   | 11 – 20         | 10        |
| Clothing    | 21 – 30         | 10        |
| Sports      | 31 – 40         | 10        |
| Kitchen     | 41 – 50         | 10        |

Table schema:

```sql
CREATE TABLE products (
    product_id   INTEGER PRIMARY KEY,
    name         TEXT    NOT NULL,
    category     TEXT    NOT NULL,
    unit_price   REAL    NOT NULL,
    stock_qty    INTEGER NOT NULL,
    last_updated TEXT    NOT NULL
);
```

Sample rows:

```
product_id  name                              category     unit_price  stock_qty  last_updated
----------  --------------------------------  -----------  ----------  ---------  ------------
1           Wireless Bluetooth Headphones     Electronics  89.99       142        2024-11-15
12          Ergonomic Mesh Office Chair       Furniture    289.99      42         2024-10-20
32          Yoga Mat Non-Slip 6mm             Sports       29.99       618        2024-09-05
43          Stainless Chef Knife 8 in         Kitchen      54.99       222        2024-08-19
```

---

## Pipeline Configuration

```json
{
  "name": "inventory-jdbc-source",
  "description": "Read all rows from a SQLite products table using keyset pagination and partition-aware reads.",
  "version": "1.0.0",
  "mode": "el",
  "source": {
    "type": "jdbc",
    "driver": "sqlite",
    "dsn": "./data/inventory.db",
    "table": "products",
    "keyColumn": "product_id",
    "batchSize": 100,
    "partitions": 2
  },
  "transforms": [],
  "sink": {
    "type": "null"
  },
  "performance": {
    "batchSize": 100,
    "maxMemoryMB": 256
  },
  "scaling": { "mode": "single" },
  "checkpoint": { "type": "none" }
}
```

---

## How Keyset Pagination Works

Traditional offset pagination (`LIMIT N OFFSET K`) degrades as the offset grows: the database must count and discard K rows on every page. Keyset pagination avoids this entirely by anchoring each page on the last key seen:

```
┌────────────────────────────────────────────────────────────────────┐
│  Initial state: lastKey = 0 (sentinel, before first row)           │
│                                                                    │
│  Page 1: SELECT * FROM products                                    │
│            WHERE product_id > 0                                    │
│            ORDER BY product_id LIMIT 100;                          │
│          → Returns rows 1–50 (all rows fit in one batch)           │
│          → lastKey becomes 50                                      │
│                                                                    │
│  Page 2: SELECT * FROM products                                    │
│            WHERE product_id > 50                                   │
│            ORDER BY product_id LIMIT 100;                          │
│          → Returns 0 rows → stop                                   │
└────────────────────────────────────────────────────────────────────┘
```

Requirements for the key column:

1. **Unique** — no two rows share the same key value.
2. **Ordered** — the column must be the leading key of an index (a `PRIMARY KEY` always satisfies this).
3. **Monotonically increasing** — new rows must have a higher key than existing rows if you want incremental reads with a high-water mark (see the `hwm` transform, not used here).

`INTEGER PRIMARY KEY` in SQLite is the rowid alias and satisfies all three requirements automatically.

---

## How Partition-Aware Reads Work

Before reading begins, the engine probes the table to find the key range:

```sql
SELECT MIN(product_id), MAX(product_id) FROM products;
-- → 1, 50
```

With `partitions: 2`, the key space [1, 50] is split into two equal slices:

```
Partition 0: WHERE product_id >= 1  AND product_id <= 25
Partition 1: WHERE product_id >= 26 AND product_id <= 50
```

Each partition then runs its own keyset-paginated loop independently. The full query executed by Partition 0, Page 1:

```sql
SELECT * FROM products
WHERE product_id >= 1 AND product_id <= 25
  AND product_id > 0          -- keyset anchor
ORDER BY product_id
LIMIT 100;
```

For SQLite (which is single-file and not designed for concurrent writers) both partitions read the same file, so parallelism mainly benefits multi-core scenarios where data post-processing is the bottleneck. For PostgreSQL or MySQL, partitions map to separate connections and generate genuine concurrent I/O.

---

## How to Run

### Step 1 — Create the database

**Linux / macOS:**

```bash
chmod +x setup.sh
./setup.sh
```

**Windows:**

```bat
setup.bat
```

Expected output:

```
==> Creating SQLite database at ./data/inventory.db ...

==> Database created successfully.

    Table  : products
    Rows   : 50
    File   : ./data/inventory.db

    Category breakdown:
    category     rows  first_id  last_id
    -----------  ----  --------  -------
    Clothing     10    21        30
    Electronics  10    1         10
    Furniture    10    11        20
    Kitchen      10    41        50
    Sports       10    31        40

==> Setup complete. Run ./run.sh to execute the pipeline.
```

### Step 2 — Run the pipeline

**Linux / macOS:**

```bash
chmod +x run.sh
./run.sh
```

**Windows:**

```bat
flogo-datapipeline -f inventory-jdbc-source.datapipeline
```

### Direct invocation (any platform)

```bash
cd 09-jdbc-source-sqlite/
flogo-datapipeline -f inventory-jdbc-source.datapipeline
```

---

## Expected Output

```
INFO  pipeline starting        name=inventory-jdbc-source mode=el
INFO  jdbc source opened       driver=sqlite dsn=./data/inventory.db table=products
INFO  partition scan           partitions=2 key_min=1 key_max=50
INFO  partition 0 started      key_range=[1,25]
INFO  partition 1 started      key_range=[26,50]
INFO  partition 0 batch        page=1 rows=25 last_key=25
INFO  partition 1 batch        page=1 rows=25 last_key=50
INFO  partition 0 complete     rows=25
INFO  partition 1 complete     rows=25
INFO  null sink                rows_discarded=50
INFO  pipeline complete        rows_read=50 rows_written=0 duration=38ms
```

Key observations:

- `rows_read=50` — all 50 product rows were fetched.
- `rows_written=0` — the null sink consumed but did not persist any rows.
- Two partition log lines appear, each covering half the key range.
- Exit code `0` — clean termination.

---

## How to Verify

After the run completes:

1. **Exit code:** `echo $?` (bash) or `echo %ERRORLEVEL%` (cmd) should print `0`.
2. **No output files created** — the null sink writes nothing to disk.
3. **Log line `rows_read=50`** — confirms both partitions read their 25 rows each.
4. **Cross-check with SQLite:**

```bash
sqlite3 ./data/inventory.db "SELECT COUNT(*) FROM products;"
# Expected: 50
```

---

## Swapping to PostgreSQL

Only two fields in the pipeline file change. Everything else — `table`, `keyColumn`, `batchSize`, `partitions` — is identical:

```json
{
  "source": {
    "type": "jdbc",
    "driver": "postgres",
    "dsn": "postgres://user:password@localhost:5432/mydb?sslmode=disable",
    "table": "products",
    "keyColumn": "product_id",
    "batchSize": 5000,
    "partitions": 4
  }
}
```

Considerations when migrating to PostgreSQL:

- Increase `batchSize` to 1000–10000 (network round-trips cost more than local SQLite file reads).
- Increase `partitions` to match the number of available CPU cores or the PostgreSQL `max_connections / active_pipelines` budget.
- The `keyColumn` must be indexed. `SERIAL PRIMARY KEY` or `BIGSERIAL PRIMARY KEY` in PostgreSQL satisfies keyset pagination requirements out of the box.
- Replace the DSN with your actual connection string; environment variable substitution (`${PG_DSN}`) is supported.

---

## Swapping to MySQL

```json
{
  "source": {
    "type": "jdbc",
    "driver": "mysql",
    "dsn": "user:password@tcp(localhost:3306)/mydb",
    "table": "products",
    "keyColumn": "product_id",
    "batchSize": 5000,
    "partitions": 4
  }
}
```

MySQL DSN uses the `user:pass@tcp(host:port)/db` format (the Go MySQL driver convention).

---

## JDBC Source Configuration Reference

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `type` | string | Yes | — | Must be `"jdbc"` |
| `driver` | string | Yes | — | `"sqlite"`, `"postgres"`, or `"mysql"` |
| `dsn` | string | Yes | — | Connection string; format varies by driver (see examples above) |
| `table` | string | Yes* | — | Table to read; used when no custom `query` is provided |
| `query` | string | No | — | Override the generated SELECT with a custom query; mutually exclusive with `table` |
| `keyColumn` | string | Yes | — | Primary key column for keyset pagination; must be unique and indexed |
| `batchSize` | int | No | `5000` | Number of rows fetched per paginated query |
| `partitions` | int | No | `1` | Number of parallel read workers; key range is split evenly across partitions |

*`table` is required when `query` is omitted. When `query` is set, the engine wraps it as a subquery and applies keyset pagination on top of it.

### `batchSize` — tuning guidance

| Table size | Recommended `batchSize` |
|------------|------------------------|
| < 10,000 rows | 500 – 1,000 |
| 10,000 – 1M rows | 1,000 – 10,000 |
| > 1M rows | 5,000 – 50,000 |

Larger values reduce the number of round-trips to the database but increase peak memory per partition. Peak memory per partition is approximately `batchSize × avg_row_bytes`.

### `partitions` — tuning guidance

| Scenario | Recommended `partitions` |
|----------|--------------------------|
| SQLite (single-file, no concurrency) | 1 – 2 |
| PostgreSQL, lightly loaded | 2 – 4 |
| PostgreSQL, dedicated replica | 4 – 16 |
| MySQL, read replica | 4 – 8 |

Setting `partitions` higher than `max_connections` will cause connection errors. As a rule of thumb, `partitions` should not exceed `max_connections / 4` to leave headroom for other traffic.

---

## Performance Section Reference

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `batchSize` | int | `5000` | Rows buffered by the engine's internal ring buffer before flushing to the sink |
| `maxMemoryMB` | int | `512` | Soft heap limit; the engine will pause source reads if exceeded |

Note: `performance.batchSize` is the engine's internal buffer size, which is separate from `source.batchSize` (the SQL `LIMIT` clause). Keep them equal or make `performance.batchSize` a multiple of `source.batchSize` to avoid buffer stalls.

---

## Scaling and Checkpoint Reference

### Scaling

| Field | Value | Notes |
|-------|-------|-------|
| `mode` | `"single"` | Single-threaded engine coordinator; partitions still run in parallel goroutines |

### Checkpoint

| Field | Value | Notes |
|-------|-------|-------|
| `type` | `"none"` | No restart state persisted; the pipeline always reads from the beginning of the key range |

To enable incremental reads (only new rows since the last run), replace the `checkpoint` section with a `hwm` (high-water mark) transform:

```json
{
  "transforms": [
    {
      "type": "hwm",
      "column": "product_id",
      "backend": "file",
      "backendConfig": { "path": "./data/hwm_product_id.json" }
    }
  ]
}
```

The HWM transform records the maximum `product_id` seen in each run and injects a `WHERE product_id > <last_max>` clause into the source query on subsequent runs.

---

## Next Steps

| Example | What it adds |
|---------|--------------|
| [10-jdbc-source-to-parquet](../10-jdbc-source-to-parquet/) | Write JDBC source rows to a Parquet file instead of discarding them |
| [11-jdbc-hwm-incremental](../11-jdbc-hwm-incremental/) | Add a `hwm` transform for incremental loads — only read rows added since the last run |
| [12-jdbc-to-jdbc](../12-jdbc-to-jdbc/) | Full database-to-database pipeline: read from a source table, write to a target table with upsert |
