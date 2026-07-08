# Example 10 — CSV to SQLite (Classic ETL)

This example demonstrates the canonical **Extract → Transform → Load** pattern using Flogo Data Pipeline. A customer CSV file is read, rows with bad data are silently dropped, surviving rows are mapped to a clean target schema, and the results are upserted into a SQLite database.

## What This Example Demonstrates

| Feature | Detail |
|---|---|
| Full ETL mode | `"mode": "etl"` — all three phases run in a single pipeline |
| CSV source with typed columns | `hasHeader: true`, explicit `columns` array with `float` and `string` types |
| Validate transform (`drop`) | Four rules; invalid rows are silently dropped before the map step |
| Map transform | Field renaming, `concat()` for name concatenation, `upper()` for normalisation |
| JDBC sink (SQLite) | `driver: sqlite`, `onConflict: upsert`, `conflictKey: ["id"]` for idempotent loads |

---

## Directory Layout

```
10-csv-to-sqlite/
├── 10-csv-to-sqlite.datapipeline   # Pipeline definition
├── data/
│   ├── customers.csv               # 30-row sample input (4 intentionally invalid)
│   └── output.db                   # SQLite database (created by setup.sh)
├── setup.sh                        # Creates output.db with the target schema
├── verify.sh                       # Runs SQLite queries to confirm the load
└── README.md
```

---

## Sample Data

`data/customers.csv` contains **30 rows** representing customers from around the world. Four rows are deliberately invalid to illustrate the `validate → drop` behaviour:

| Row | customer_id | Problem | Rule violated |
|---|---|---|---|
| 11 | C011 | `not-an-email` | `email` pattern |
| 23 | C023 | `walther.schmidt@@badformat.com` | `email` pattern (double `@`) |
| 27 | C027 | `annual_revenue: -500.00` | `annual_revenue` min: 0 |
| 28 | C028 | `account_tier: DIAMOND` | `account_tier` not in allowed list |

After the validate step, **26 rows** pass through to the map step and are upserted into SQLite.

---

## Pipeline Walkthrough

### 1. Source — CSV file

```json
"source": {
  "type": "csv",
  "path": "./data/customers.csv",
  "delimiter": ",",
  "hasHeader": true,
  "columns": [
    { "name": "customer_id",    "type": "string" },
    { "name": "first_name",     "type": "string" },
    { "name": "last_name",      "type": "string" },
    { "name": "email",          "type": "string" },
    { "name": "country",        "type": "string" },
    { "name": "annual_revenue", "type": "float"  },
    { "name": "account_tier",   "type": "string" }
  ]
}
```

The CSV reader parses the header row to name columns, converts `annual_revenue` to a floating-point value, and streams rows to the transform chain.

### 2. Transform 1 — Validate (drop on violation)

```json
{
  "type": "validate",
  "rules": [
    { "field": "customer_id",    "notNull": true },
    { "field": "email",          "notNull": true, "pattern": "^[^@]+@[^@]+\\.[^@]+$" },
    { "field": "annual_revenue", "min": 0 },
    { "field": "account_tier",   "in": ["BRONZE", "SILVER", "GOLD", "PLATINUM"] }
  ],
  "onViolation": "drop"
}
```

`onViolation: "drop"` means any row that fails **any** rule is silently discarded — no error, no dead-letter queue. This is appropriate when invalid source rows are expected and the business requirement is simply to skip them. Use `onViolation: "route"` (with a `dlqSink`) if you need to capture and inspect rejected rows.

**Rules in this example:**

| Rule | Field | Constraint |
|---|---|---|
| Not null | `customer_id` | Row must have an ID |
| Not null + regex | `email` | Present and matches basic email pattern |
| Minimum value | `annual_revenue` | Must be >= 0 (negative revenue is invalid) |
| Allowed list | `account_tier` | Must be one of BRONZE / SILVER / GOLD / PLATINUM |

### 3. Transform 2 — Map to target schema

```json
{
  "type": "map",
  "fields": {
    "id":          "=$customer_id",
    "full_name":   "=concat($.first_name, ' ', $.last_name)",
    "email":       "=$email",
    "country":     "=upper($.country)",
    "revenue_usd": "=$annual_revenue",
    "tier":        "=$account_tier"
  }
}
```

The map step performs three kinds of transformation:

- **Rename** — `customer_id` becomes `id`; `annual_revenue` becomes `revenue_usd`. This decouples the source schema from the target schema; upstream column renames in the CSV do not require changes to the sink table.
- **Concatenate** — `full_name` is built by calling the built-in `concat()` function. `$.first_name` and `$.last_name` refer to the current row fields using the `$.field` shorthand.
- **Normalise** — `country` is upper-cased with `upper()`. Source rows carry mixed-case country codes (`us`, `gb`, `Vn`); the target always stores them in upper case (`US`, `GB`, `VN`).

### 4. Sink — JDBC (SQLite) with upsert

```json
"sink": {
  "type": "jdbc",
  "driver": "sqlite",
  "dsn": "./data/output.db",
  "table": "customers_enriched",
  "batchSize": 100,
  "onConflict": "upsert",
  "conflictKey": ["id"]
}
```

`onConflict: "upsert"` combined with `conflictKey: ["id"]` means:

- If a row with the same `id` does **not** exist → **INSERT**.
- If a row with the same `id` already exists → **UPDATE** all other columns.

This makes the pipeline **idempotent**: running it twice against the same input produces the same 26 rows in the database, not 52 duplicates. This is the recommended default for any pipeline that may be re-run after a partial failure.

Under the hood the runtime emits:

```sql
INSERT INTO customers_enriched (id, full_name, email, country, revenue_usd, tier)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  full_name   = excluded.full_name,
  email       = excluded.email,
  country     = excluded.country,
  revenue_usd = excluded.revenue_usd,
  tier        = excluded.tier;
```

---

## How to Run

### Prerequisites

- `sqlite3` CLI available on `PATH`
- `flogo-datapipeline` CLI installed

### Step 1 — Create the target database

```bash
chmod +x setup.sh
./setup.sh
```

This drops any existing `output.db`, creates a fresh one, and defines the `customers_enriched` table:

```sql
CREATE TABLE customers_enriched (
  id          TEXT PRIMARY KEY,
  full_name   TEXT NOT NULL,
  email       TEXT NOT NULL,
  country     TEXT NOT NULL,
  revenue_usd REAL NOT NULL,
  tier        TEXT NOT NULL
);
```

### Step 2 — Execute the pipeline

```bash
flogo-datapipeline run 10-csv-to-sqlite.datapipeline
```

Expected console output (abridged):

```
[source]    csv  ./data/customers.csv  rows_read=30
[validate]  dropped=4  passed=26
[map]       rows_mapped=26
[sink]      jdbc sqlite ./data/output.db  rows_written=26  upserted=26
```

### Step 3 — Verify the load

```bash
chmod +x verify.sh
./verify.sh
```

---

## Expected Output

### Row count by tier

```
tier      row_count
--------  ---------
BRONZE    4
GOLD      8
PLATINUM  4
SILVER    10
```

### Revenue stats

```
total_rows  min_revenue  max_revenue  avg_revenue
----------  -----------  -----------  -----------
26          0.0          420000.0     132125.02
```

### PLATINUM customers

```
id    full_name        email                        revenue_usd
----  ---------------  ---------------------------  -----------
C003  Carol Nguyen     carol.nguyen@techcorp.com    310000.0
C008  Hiro Tanaka      hiro.tanaka@jptech.jp        420000.0
C015  Omar Al-Rashid   omar.alrashid@gulftech.ae    260000.0
C021  Ulrich Meier     ulrich.meier@bavarco.de      340000.0
C030  Diana Chen       diana.chen@pacrim.tw         220000.0
```

> Note: C003 maps to PLATINUM — five PLATINUM rows because C030 also qualifies.

### Dropped rows (not in output.db)

| customer_id | Reason |
|---|---|
| C011 | Email `not-an-email` fails the regex pattern |
| C023 | Email `walther.schmidt@@badformat.com` has double `@` |
| C027 | `annual_revenue` is `-500.00` (below min: 0) |
| C028 | `account_tier` is `DIAMOND` (not in allowed list) |

---

## Re-running (Idempotency Test)

Run the pipeline a second time without calling `setup.sh` again:

```bash
flogo-datapipeline run 10-csv-to-sqlite.datapipeline
```

The row count stays at 26 because `onConflict: upsert` updates existing rows in place rather than inserting duplicates. This is the hallmark of a well-designed ETL sink.

---

## Key Configuration Reference

### validate transform

| Field | Type | Description |
|---|---|---|
| `rules` | array | One object per field to validate |
| `rules[].field` | string | Source column name |
| `rules[].notNull` | bool | Reject the row if the field is null or empty |
| `rules[].min` | number | Reject if numeric value is below this threshold |
| `rules[].max` | number | Reject if numeric value is above this threshold |
| `rules[].minLength` | int | Reject if string length is below this value |
| `rules[].maxLength` | int | Reject if string length is above this value |
| `rules[].pattern` | string | Reject if value does not match this regex |
| `rules[].in` | array | Reject if value is not in this list |
| `onViolation` | string | `"drop"` discard silently; `"route"` send to dlqSink; `"fail"` abort pipeline |

### map transform

| Expression | Meaning |
|---|---|
| `"=$fieldName"` | Copy source field value as-is |
| `"=$.fieldName"` | Same — `$.` prefix is equivalent to `=` for field references |
| `"=concat(a, b, c)"` | Concatenate multiple values or literals |
| `"=upper($.field)"` | Convert string to upper case |
| `"=lower($.field)"` | Convert string to lower case |
| `"=trim($.field)"` | Strip leading/trailing whitespace |

### jdbc sink — onConflict options

| Value | Behaviour |
|---|---|
| `"insert"` | Plain INSERT; fails on duplicate primary key |
| `"upsert"` | INSERT … ON CONFLICT DO UPDATE — idempotent, updates all columns |
| `"replace"` | DELETE + INSERT — replaces the entire row |
| `"ignore"` | INSERT … ON CONFLICT DO NOTHING — keeps the original row |

`conflictKey` must list the column(s) that define uniqueness for `upsert`, `replace`, and `ignore` modes. For SQLite this maps to the `ON CONFLICT(col)` clause; for PostgreSQL it maps to `ON CONFLICT (col) DO UPDATE SET`.

---

## Common Modifications

**Change the violation policy to route bad rows to a DLQ file:**

```json
{
  "type": "validate",
  "rules": [ ... ],
  "onViolation": "route",
  "errorSinkType": "csv",
  "errorSinkConfig": { "path": "./data/rejected.csv" }
}
```

**Add a second validation rule for minimum name length:**

```json
{ "field": "first_name", "notNull": true, "minLength": 2 }
```

**Switch to a PostgreSQL sink (no other changes needed):**

```json
"sink": {
  "type": "jdbc",
  "driver": "postgres",
  "dsn": "host=localhost port=5432 user=etl password=secret dbname=customers sslmode=disable",
  "table": "customers_enriched",
  "batchSize": 500,
  "onConflict": "upsert",
  "conflictKey": ["id"]
}
```
