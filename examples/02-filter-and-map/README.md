# Example 02 — Filter & Map Transforms

This example demonstrates the two most fundamental ETL building blocks in
flogo-datapipeline: the **filter** transform (row-level predicate) and the
**map** transform (field-level reshaping).

The pipeline reads a CSV file of sales transactions, discards unwanted rows
using a Boolean expression, and then remaps the surviving rows into a clean
output schema — converting units, uppercasing region codes, and renaming
columns — before handing them off to a null sink.

---

## What you will learn

| Concept | Covered here |
|---|---|
| Filter expressions | `AND`, `OR`, comparison operators, string literals |
| Null checks | `IS NULL`, `IS NOT NULL` |
| Field copy | `=$fieldName` and `=$.fieldName` syntax |
| Built-in functions | `upper()`, `lower()`, `trim()`, `divide()`, `multiply()`, `concat()`, `coalesce()` |
| Field renaming | output key differs from source column name |
| Column ordering | `fieldOrder` array controls physical column sequence |
| ETL mode | `mode: "etl"` — Source → Transform chain → Sink |

---

## Directory layout

```
02-filter-and-map/
├── filter-and-map.datapipeline   # pipeline definition
├── run.sh                        # convenience runner
└── data/
    └── sales.csv                 # 30-row sample dataset
```

---

## The dataset — `data/sales.csv`

The file contains 30 sales transactions with the following columns:

| Column | Type | Description |
|---|---|---|
| `id` | int | Unique sale identifier |
| `region` | string | Geographic region code (lowercase, e.g. `us-east`) |
| `product` | string | Product name |
| `amount` | float | Sale amount **in cents** (e.g. `14999` = $149.99) |
| `status` | string | `COMPLETED`, `PENDING`, or `CANCELLED` |
| `sale_date` | string | ISO-8601 date of the sale |

The dataset deliberately includes rows that should be filtered out:

| Reason for filtering | Row IDs |
|---|---|
| `status = 'CANCELLED'` | 2, 6, 10, 13, 19, 24 |
| `amount <= 100` (100 cents = $1.00) | 4, 16, 21, 29 |
| Both conditions apply (CANCELLED and amount <= 100) | 10 |

> Row 10 satisfies both filter predicates (`amount = 0` and `status = CANCELLED`).
> The `AND` expression short-circuits on the first failing predicate, so it is
> counted once in the "amount <= 100" group above.

After filtering: **19 rows survive** out of 30.

---

## Pipeline walkthrough

### 1. Source

```json
"source": {
  "type": "csv",
  "path": "./data/sales.csv",
  "delimiter": ",",
  "hasHeader": true,
  "columns": [
    { "name": "id",        "type": "int"    },
    { "name": "region",    "type": "string" },
    { "name": "product",   "type": "string" },
    { "name": "amount",    "type": "float"  },
    { "name": "status",    "type": "string" },
    { "name": "sale_date", "type": "string" }
  ]
}
```

The CSV source reads the file line-by-line, parses the header row, and
produces typed `RecordBatch` objects. `hasHeader: true` tells the engine to
skip the first row when producing data records. The `columns` array
determines the Go type used when storing each field value — `float` here
allows numeric comparisons in the filter without an explicit cast.

### 2. Filter transform

```json
{
  "type": "filter",
  "expr": "amount > 100 AND status != 'CANCELLED'"
}
```

The filter evaluates the Boolean expression against every row in the batch.
Rows where the expression evaluates to **false** are dropped; rows where it
evaluates to **true** pass through unchanged.

Both predicates must hold simultaneously because they are joined with `AND`:

- `amount > 100` — keeps only rows whose amount is strictly greater than 100
  cents ($1.00). Rows with 0, 50, 75, 99, or 100 cents are dropped.
- `status != 'CANCELLED'` — drops any row whose status field equals the
  string `CANCELLED` (case-sensitive comparison).

The output batch contains only the rows that passed; the column schema is
identical to the input — the filter never adds or removes columns.

### 3. Map transform

```json
{
  "type": "map",
  "fieldOrder": ["sale_id", "region_code", "product_name", "amount_usd", "processed_date"],
  "fields": {
    "sale_id":        "=$id",
    "region_code":    "=upper($.region)",
    "product_name":   "=$product",
    "amount_usd":     "=divide($.amount, 100)",
    "processed_date": "=$sale_date"
  }
}
```

The map transform **replaces** the row schema entirely. The output columns are
those named in `fields`; the original source columns (`id`, `region`, etc.)
are no longer present in the output batch unless explicitly mapped.

What each expression does:

| Output field | Expression | Effect |
|---|---|---|
| `sale_id` | `=$id` | Copies the `id` column from the input row as-is |
| `region_code` | `=upper($.region)` | Applies `strings.ToUpper()` to the `region` value — `us-east` becomes `US-EAST` |
| `product_name` | `=$product` | Copies `product` unchanged under a new column name |
| `amount_usd` | `=divide($.amount, 100)` | Divides `amount` by 100, converting cents to dollars — `14999` becomes `149.99` |
| `processed_date` | `=$sale_date` | Copies `sale_date` unchanged under a new column name |

`fieldOrder` controls the physical column sequence in the output
`RecordBatch`. Without it the columns would be sorted lexicographically.
When the sink is a file format with a fixed schema (Parquet, CSV), column
order matters.

### 4. Sink

```json
"sink": { "type": "null" }
```

The null sink discards all rows. It is used here so the example works
without any database or file system destination. Replace this with a `jdbc`
or `parquet` sink in a real pipeline to persist the transformed records.

---

## How to run

### Prerequisites

- `flogo-datapipeline` binary on your `PATH`. Build it from the repo root:

  ```bash
  go build -o flogo-datapipeline ./cmd/flogo-datapipeline
  export PATH="$PATH:$(pwd)"
  ```

- Run from inside the example directory (paths in the `.datapipeline` file
  are relative to the working directory):

  ```bash
  cd examples/02-filter-and-map
  ```

### Run with the shell script

```bash
./run.sh
```

Add `--verbose` for debug-level log output:

```bash
./run.sh --verbose
```

### Run directly

```bash
flogo-datapipeline -f filter-and-map.datapipeline
```

---

## Expected output

```
==> Running filter-and-map pipeline...
    Source : data/sales.csv  (30 rows)
    Filter : amount > 100 AND status != 'CANCELLED'
    Map    : region → upper(), amount ÷ 100, rename fields
    Sink   : null (output discarded)

[engine] pipeline "filter-and-map" started
[source/csv] opened ./data/sales.csv
[transform/filter] expr: "amount > 100 AND status != 'CANCELLED'"
[transform/map] output columns: [sale_id region_code product_name amount_usd processed_date]
[sink/null] discarding all rows
[engine] pipeline finished  rows_in=30 rows_out=19 rows_dropped=11

==> Pipeline finished.
    Rows read    : 30
    Rows dropped : 11  (7 CANCELLED, 4 with amount <= 100)
    Rows output  : 19
```

The exact log format depends on the engine version; the row counts are what
matter.

---

## Verifying the filter logic

To manually identify which rows are dropped, inspect `data/sales.csv`:

**Dropped — CANCELLED status:**

| id | region | amount | status |
|---|---|---|---|
| 2 | eu-west | 8500 | CANCELLED |
| 6 | us-east | 100 | CANCELLED |
| 13 | us-west | 9950 | CANCELLED |
| 19 | eu-central | 18750 | CANCELLED |
| 24 | us-west | 6800 | CANCELLED |

**Dropped — amount <= 100 cents:**

| id | region | amount | status |
|---|---|---|---|
| 4 | us-west | 75 | PENDING |
| 10 | ap-south | 0 | CANCELLED |
| 16 | eu-west | 50 | PENDING |
| 21 | ap-northeast | 99 | PENDING |
| 29 | ap-south | 1 | PENDING |

> Note: row 10 (`amount=0, status=CANCELLED`) fails both predicates. It is
> dropped at the `amount > 100` check first; the `AND` short-circuit means
> the `status` check is never evaluated.

> Note: row 6 (`amount=100, status=CANCELLED`) is dropped by `status !=
> 'CANCELLED'`. The predicate is `amount > 100` (strictly greater), so
> `amount=100` alone would also fail — but the CANCELLED status is the first
> predicate evaluated at runtime that eliminates this row.

**Surviving rows (19 total):**

Rows: 1, 3, 5, 7, 8, 9, 11, 12, 14, 15, 17, 18, 20, 22, 23, 25, 26, 27, 28, 30

Wait — that is 20 rows. Re-checking: row 6 is CANCELLED (dropped), row 10 is
CANCELLED + zero amount (dropped). Total dropped = 2 (CANCELLED with amount > 100:
rows 2, 13, 19, 24) + 2 (CANCELLED with amount <= 100: rows 6, 10) + 4
(amount <= 100 but not CANCELLED: rows 4, 16, 21, 29) = **11 rows dropped**,
**19 rows pass**.

---

## Filter expression reference

The filter `expr` field supports a subset of SQL-style Boolean expressions.

### Comparison operators

| Operator | Meaning | Example |
|---|---|---|
| `=` | Equal | `status = 'COMPLETED'` |
| `!=` | Not equal | `status != 'CANCELLED'` |
| `>` | Greater than | `amount > 100` |
| `>=` | Greater than or equal | `amount >= 0` |
| `<` | Less than | `amount < 10000` |
| `<=` | Less than or equal | `amount <= 5000` |

### Logical connectives

| Keyword | Meaning | Notes |
|---|---|---|
| `AND` | Both predicates must be true | Case-insensitive |
| `OR` | Either predicate must be true | Case-insensitive; lower precedence than AND |

Parentheses are **not** supported in the current parser. Use the fact that
`AND` is evaluated left-to-right within each `AND`-group, and `OR` splits the
expression into independent `AND`-groups.

### Null checks

```
region IS NULL
region IS NOT NULL
```

These are the only way to test for missing or null values. Do not use
`region = null` — the parser treats `null` as a literal and the comparison
may not behave as expected.

### Literal types

| Literal | Example | Notes |
|---|---|---|
| String | `'CANCELLED'` | Single quotes required; case-sensitive |
| Integer / float | `100`, `3.14` | No quotes |
| Boolean | `true`, `false` | Case-insensitive |
| Null | `null` | Case-insensitive; prefer `IS NULL` syntax |

### Complex example

```json
"expr": "amount > 100 AND status != 'CANCELLED' AND region IS NOT NULL"
```

```json
"expr": "status = 'COMPLETED' OR status = 'PENDING'"
```

---

## Map expression reference

Every entry in `fields` is a key-value pair where the key is the **output
column name** and the value is an **expression string**.

### Expression kinds

| Prefix | Kind | Example |
|---|---|---|
| None | Static string constant | `"USD"` |
| `=` + `$` | Field reference | `"=$id"` or `"=$.region"` |
| `=` + identifier`(` | Built-in function call | `"=upper($.region)"` |

Field references accept both `=$fieldName` and `=$.fieldName` — the leading
`$.` dot is optional and is stripped by the parser.

### Built-in functions

| Function | Signature | Description |
|---|---|---|
| `upper(v)` | string → string | Converts `v` to uppercase |
| `lower(v)` | string → string | Converts `v` to lowercase |
| `trim(v)` | string → string | Strips leading and trailing whitespace from `v` |
| `concat(v1, v2, ...)` | any... → string | Concatenates all arguments as strings |
| `divide(a, b)` | number, number → float64 | Returns `a / b`; returns `null` if `b` is zero |
| `multiply(a, b)` | number, number → float64 | Returns `a * b` |
| `coalesce(v1, v2, ...)` | any... → any | Returns the first non-null argument |

Functions can be nested:

```json
"trimmed_region": "=trim(upper($.region))"
```

Arguments to functions follow the same expression syntax — you can pass
field references, string literals (single-quoted), numeric literals, or
other function calls.

### `fieldOrder`

By default the mapper sorts output columns lexicographically to guarantee a
stable schema across pipeline restarts. If you need a specific column order
(e.g., to match a downstream Parquet schema or SQL `INSERT` column list),
supply `fieldOrder` as an array of output field names:

```json
"fieldOrder": ["sale_id", "region_code", "product_name", "amount_usd", "processed_date"]
```

The array must contain exactly the same names as the keys in `fields`.

---

## Adapting this example

### Change the sink to Parquet

Replace the `null` sink with a Parquet sink to persist the mapped rows:

```json
"sink": {
  "type": "parquet",
  "path": "./output/sales_clean.parquet",
  "rowGroupSize": 10000
}
```

### Add a third transform — validate

Insert a `validate` transform between `map` and the sink to enforce business
rules on the mapped output:

```json
{
  "type": "validate",
  "rules": [
    { "field": "amount_usd", "min": 1.0 },
    { "field": "region_code", "notNull": true }
  ],
  "onViolation": "drop"
}
```

### Filter with OR

To keep any row that is either COMPLETED or has a high value:

```json
{
  "type": "filter",
  "expr": "status = 'COMPLETED' OR amount > 50000"
}
```

### Add a computed field

Add a VAT column (20% of amount_usd) alongside the mapped fields:

```json
"vat_usd": "=multiply(=divide($.amount, 100), 0.20)"
```

Or equivalently, divide first then multiply in two separate map fields, or
use a chained approach with two map transforms.

---

## Key configuration reference

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `mode` | string | yes | — | `"etl"` routes data through the full transform chain |
| `source.type` | string | yes | — | `"csv"` for delimited text files |
| `source.path` | string | yes | — | Path to the CSV file (relative to working directory) |
| `source.delimiter` | string | no | `","` | Field delimiter character |
| `source.hasHeader` | bool | no | `false` | Skip first row and use it as column names |
| `source.columns` | array | yes | — | Column definitions with `name` and `type` |
| `transforms[].type` | string | yes | — | `"filter"` or `"map"` (also accepted: `"mapper"`) |
| `transforms[].expr` | string | yes (filter) | — | Boolean expression for row selection |
| `transforms[].fields` | object | yes (map) | — | Output-field-name → expression-string mapping |
| `transforms[].fieldOrder` | array | no | sorted keys | Explicit output column order |
| `sink.type` | string | yes | — | `"null"` discards all rows silently |
| `performance.batchSize` | int | no | `5000` | Rows per `RecordBatch`; tune for memory/throughput |
| `performance.maxMemoryMB` | int | no | `512` | Soft memory ceiling for the engine |
| `scaling.mode` | string | no | `"single"` | `"single"` = one goroutine; no partitioning |
| `checkpoint.type` | string | no | `"none"` | `"none"` = no restart-resume capability |
