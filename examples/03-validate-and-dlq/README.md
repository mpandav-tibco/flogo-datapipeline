# Example 03 — Validation + Dead-Letter Queue

Demonstrates the `validate` transform with `onViolation: route`. Rows that satisfy every rule flow to the main sink; rows that break any rule are automatically diverted to a **Dead-Letter Queue (DLQ)** CSV file, tagged with the reason for rejection.

---

## What This Example Does

```
orders.csv (25 rows)
       │
       ▼
 ┌─────────────┐
 │  validate   │  ← five field rules
 └──────┬──────┘
        │
   ┌────┴─────┐
   │          │
PASS (20)   FAIL (5)
   │          │
   ▼          ▼
null sink   orders_dlq.csv   ← DLQ sink (csv + violation_reason column)
```

The pipeline runs in `etl` mode:
- **Source** — `orders.csv`, 25 rows with five columns.
- **Transform** — `validate` checks five rules; any row that fails at least one rule is *routed* to the DLQ instead of the main path.
- **Sink** — `null` (main path discards valid rows — swap for a real sink in production).
- **dlqSink** — `csv` at `./data/orders_dlq.csv`; the engine appends a `violation_reason` column explaining which rule failed.

---

## The Five Validation Rules

| # | Field | Rule | Why |
|---|-------|------|-----|
| 1 | `order_id` | `notNull: true`, `pattern: ^ORD-[0-9]{6}$` | Order IDs must exist and follow the `ORD-` + 6-digit format. |
| 2 | `customer_id` | `notNull: true` | Every order must be linked to a customer. |
| 3 | `amount` | `notNull: true`, `min: 0.01`, `max: 99999.99` | Amount must be a positive number within the allowed billing range. |
| 4 | `status` | `in: [PENDING, SHIPPED, DELIVERED, CANCELLED]` | Only recognised lifecycle values are accepted. |
| 5 | `email` | `pattern: ^[^@]+@[^@]+\.[^@]+$` | A basic structural check: must contain exactly one `@` with a domain. |

### Rule Details

**Rule 1 — order_id format**

```json
{ "field": "order_id", "notNull": true, "pattern": "^ORD-[0-9]{6}$" }
```

Enforces the canonical order identifier format. A null value triggers `notNull`; a present but malformed value (e.g. `ORD-1300`) triggers `pattern`.

**Rule 2 — customer_id presence**

```json
{ "field": "customer_id", "notNull": true }
```

Referential integrity guard. An order without a customer cannot be fulfilled, billed, or tracked.

**Rule 3 — amount range**

```json
{ "field": "amount", "notNull": true, "min": 0.01, "max": 99999.99 }
```

Rejects zero, negative, and astronomically large amounts that typically indicate data entry errors or unit mismatches (e.g. cents instead of dollars).

**Rule 4 — status allowlist**

```json
{ "field": "status", "in": ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED"] }
```

Only values from the agreed order lifecycle are valid. Anything else (e.g. `RETURNED`, `PROCESSING`) indicates a source schema mismatch.

**Rule 5 — email shape**

```json
{ "field": "email", "pattern": "^[^@]+@[^@]+\\.[^@]+$" }
```

A structural (not deliverability) check. Catches obvious problems such as missing `@`, missing domain, or a raw username with no domain suffix.

---

## onViolation Modes

The `validate` transform supports three behaviours when a row breaks a rule:

| Mode | Behaviour | Use When |
|------|-----------|----------|
| `route` | Failing rows are sent to `dlqSink`; passing rows continue to `sink`. Pipeline never stops. | You want zero data loss and need to audit rejects. |
| `drop` | Failing rows are silently discarded. Pipeline never stops. | Reject rate is expected and audit is not required. |
| `fail` | The pipeline aborts immediately on the first violation. | Zero-tolerance — the source must be clean before processing. |

This example uses **`route`**. The DLQ is a safety net that lets you:
1. Finish the run without losing the good data.
2. Inspect or re-process the bad rows later, after fixing the upstream source.

---

## Intentional Violations in the Sample Data

The file `data/orders.csv` contains 25 rows. Five rows have deliberate defects:

| Row | order_id | Violation | Broken Rule |
|-----|----------|-----------|-------------|
| 11 | *(empty)* | `order_id` is null | Rule 1 — notNull |
| 13 | `ORD-1300` | Does not match `^ORD-[0-9]{6}$` (only 4 digits) | Rule 1 — pattern |
| 14 | `ORD-100014` | `amount = -5.00`, below min of 0.01 | Rule 3 — min |
| 15 | `ORD-100015` | `status = RETURNED`, not in allowed set | Rule 4 — in |
| 17 | `ORD-100017` | `email = quinn.harrisonATbadmail`, missing `@` | Rule 5 — pattern |

The remaining 20 rows satisfy all five rules and are forwarded to the main sink.

---

## Directory Layout

```
03-validate-and-dlq/
├── orders-validate-dlq.datapipeline   # Pipeline definition
├── run.sh                             # Helper script
├── README.md                          # This file
└── data/
    ├── orders.csv                     # 25-row input (5 bad rows)
    └── orders_dlq.csv                 # Generated at runtime by the engine
```

---

## How to Run

### Prerequisites

Build the engine binary from the repository root:

```bash
cd ../../
go build -o flogo-datapipeline ./cmd/...
```

### Run the pipeline

```bash
bash run.sh
```

The script:
1. Removes any previous `orders_dlq.csv`.
2. Invokes the engine against `orders-validate-dlq.datapipeline`.
3. Prints a pass/fail summary and dumps the DLQ contents.

You can also invoke the engine directly:

```bash
../../flogo-datapipeline run orders-validate-dlq.datapipeline
```

---

## How to Verify

**1. DLQ file is created**

After the run, `data/orders_dlq.csv` must exist and contain exactly 5 rows plus a header.

```bash
wc -l data/orders_dlq.csv
# Expected: 6 (1 header + 5 violation rows)
```

**2. DLQ contains only the expected rows**

```bash
cut -d',' -f1 data/orders_dlq.csv
# Expected (order may vary by batch):
# order_id
# (empty)
# ORD-1300
# ORD-100014
# ORD-100015
# ORD-100017
```

**3. violation_reason column explains each rejection**

The engine appends a `violation_reason` column as the last field in the DLQ CSV. Example values:

| order_id | violation_reason |
|----------|-----------------|
| *(empty)* | `field 'order_id' is null` |
| `ORD-1300` | `field 'order_id' does not match pattern '^ORD-[0-9]{6}$'` |
| `ORD-100014` | `field 'amount' value -5 is below minimum 0.01` |
| `ORD-100015` | `field 'status' value 'RETURNED' is not in allowed set` |
| `ORD-100017` | `field 'email' does not match pattern '^[^@]+@[^@]+\.[^@]+$'` |

**4. Main sink received 20 rows**

The engine log line `rows written` (or equivalent) must report 20. With the `null` sink, you can confirm by subtracting DLQ rows from source rows:

```
25 source rows - 5 DLQ rows = 20 rows forwarded to main sink
```

---

## Swapping the Main Sink for a Real Target

Replace the `sink` block with any supported sink type. For example, to write valid orders to a Parquet file:

```json
"sink": {
  "type": "parquet",
  "path": "./data/orders_valid.parquet",
  "rowGroupSize": 10000
}
```

Or to a JDBC table:

```json
"sink": {
  "type": "jdbc",
  "driver": "postgres",
  "dsn": "host=localhost port=5432 dbname=orders user=etl password=secret sslmode=disable",
  "table": "orders_clean",
  "batchSize": 1000,
  "onConflict": "upsert",
  "conflictKey": ["order_id"]
}
```

---

## Swapping the DLQ Sink

The `dlqSink` block accepts any sink type. To route violations to a Kafka dead-letter topic instead of a CSV file:

```json
"dlqSink": {
  "type": "kafka",
  "brokers": "kafka:9092",
  "topic": "orders.dlq",
  "batchSize": 100
}
```

Or to a dedicated database table:

```json
"dlqSink": {
  "type": "jdbc",
  "driver": "postgres",
  "dsn": "host=localhost port=5432 dbname=orders user=etl password=secret sslmode=disable",
  "table": "orders_violations",
  "batchSize": 500,
  "onConflict": "insert"
}
```

---

## Key Configuration Reference

| Field | Type | Description |
|-------|------|-------------|
| `transforms[].type` | string | Must be `"validate"` to use this transform. |
| `transforms[].rules` | array | One object per field to validate. |
| `rules[].field` | string | Column name in the source schema. |
| `rules[].notNull` | bool | Reject if the value is null or empty string. |
| `rules[].min` | number | Reject if numeric value is below this threshold (inclusive). |
| `rules[].max` | number | Reject if numeric value is above this threshold (inclusive). |
| `rules[].minLength` | int | Reject if string length is below this value. |
| `rules[].maxLength` | int | Reject if string length is above this value. |
| `rules[].pattern` | string | RE2 regular expression; reject if the value does not match. |
| `rules[].in` | array | Allowlist of accepted string values; reject anything else. |
| `transforms[].onViolation` | string | `"route"`, `"drop"`, or `"fail"`. |
| `transforms[].errorSinkType` | string | Sink type for the internal error channel (set to `"null"` when using `dlqSink`). |
| `transforms[].errorSinkConfig` | object | Config for the internal error sink (empty `{}` when `dlqSink` is used). |
| `dlqSink` | object | Top-level sink definition for rejected rows. Required when `onViolation` is `"route"`. |
| `dlqSink.type` | string | Any supported sink type (`csv`, `jdbc`, `kafka`, `parquet`, `objstore`). |
| `dlqSink.includeViolationReason` | bool | When `true` (csv sink only), appends a `violation_reason` column to the DLQ output. |

---

## Next Steps

- **Example 04** will show the `filter` transform for discarding rows based on a boolean expression before they reach the sink.
- To add a second validation pass after a `map` transform, simply add a second `validate` entry to the `transforms` array — the engine executes transforms in order.
- For high-volume sources, increase `performance.batchSize` and switch `scaling.mode` to `"auto"` to enable parallel validation workers.
