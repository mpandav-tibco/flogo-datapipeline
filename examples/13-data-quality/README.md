# Example 13 — Data Quality Expectations

Demonstrates the `dq` transform: define named expectations against a dataset, score each row fractionally, and gate pipeline behaviour on a configurable quality threshold.

---

## What This Example Does

```
financial_records.csv  (40 rows)
          │
          ▼
  ┌───────────────┐
  │   dq transform │  ← 5 expectations evaluated per row
  └───────┬───────┘
          │
   quality score = rows_passing_all_checks / total_rows
          │
   score (0.825) < threshold (0.95)
          │
          ▼
   onFail = "report"  → quality report emitted to reportSinkType
          │             pipeline continues
          ▼
      null sink  ← all 40 rows forwarded regardless
```

The pipeline runs in `etl` mode:

- **Source** — `financial_records.csv`, 40 rows with six financial columns.
- **Transform** — `dq` evaluates five expectations against every row. Rows that fail one or more checks are counted; the engine computes a fractional score.
- **Sink** — `null` (discards all rows — swap for a real sink in production).
- **DQ gate** — if the quality score is below `threshold: 0.95`, the `onFail` action fires. Here it is `"report"`, so the engine writes a quality report and continues.

---

## The Five Expectations

| # | Column | Check | Rule | Why |
|---|--------|-------|------|-----|
| 1 | `record_id` | `not_null` | Value must be present and non-empty | Every financial record must carry a traceable identifier |
| 2 | `transaction_amount` | `range` | Value must be in `[0.01, 1000000]` | Zero or negative amounts indicate data errors; values above 1M likely indicate unit or currency mismatches |
| 3 | `currency` | `in_set` | Value must be one of `USD`, `EUR`, `GBP`, `JPY`, `CAD` | Only supported settlement currencies are accepted |
| 4 | `account_id` | `not_null` | Value must be present and non-empty | A transaction without an account cannot be posted or reconciled |
| 5 | `txn_date` | `not_null` | Value must be present and non-empty | A date is required for ledger ordering and regulatory reporting |

### Expectation Details

**Expectation 1 — record_id completeness**

```json
{ "column": "record_id", "check": "not_null", "value": null }
```

Completeness check: the primary key of every financial record must be populated. A null `record_id` makes the row untraceable across downstream systems.

**Expectation 2 — transaction_amount range**

```json
{ "column": "transaction_amount", "check": "range", "value": { "min": 0.01, "max": 1000000 } }
```

Range guard: amounts below `0.01` include zero (a likely data entry error or placeholder) and negatives (returns belong in a dedicated credit flow). Amounts above `1,000,000` in this context indicate a likely currency or unit mismatch — an amount denominated in minor units (e.g. cents) that was not converted.

**Expectation 3 — currency membership**

```json
{ "column": "currency", "check": "in_set", "value": ["USD", "EUR", "GBP", "JPY", "CAD"] }
```

Allowlist check: only the five supported settlement currencies are valid. Any other value (e.g. `XBT`, `CHF`, `AUD`) signals an unsupported currency that the downstream ledger cannot process.

**Expectation 4 — account_id completeness**

```json
{ "column": "account_id", "check": "not_null", "value": null }
```

Referential integrity: every transaction must be linked to an account. Without `account_id`, the record cannot be posted, reconciled, or attributed.

**Expectation 5 — txn_date completeness**

```json
{ "column": "txn_date", "check": "not_null", "value": null }
```

Temporal completeness: the transaction date is required for ledger ordering, regulatory timelines, and audit trails.

---

## Fractional Scoring

The `dq` transform scores each row as a **binary pass/fail** against all configured expectations. A row passes only if it satisfies every expectation. The dataset quality score is then:

```
quality_score = count(rows_passing_all_checks) / count(total_rows)
```

This is a fractional value in `[0.0, 1.0]`. The `threshold` field specifies the minimum acceptable score. If `quality_score >= threshold`, the pipeline continues silently. If `quality_score < threshold`, the engine executes the `onFail` action.

### Worked Example

The 40-row `financial_records.csv` file contains seven deliberate defects across five rows:

| Row | Column | Defect | Expectation Broken |
|-----|--------|--------|--------------------|
| 11 | `record_id` | null (empty cell) | not_null |
| 16 | `transaction_amount` | 1,200,000.00 — above max of 1,000,000 | range |
| 21 | `account_id` | null (empty cell) | not_null |
| 26 | `currency` | `XBT` — not in the allowed set | in_set |
| 31 | `record_id` | null (empty cell) | not_null |
| 33 | `transaction_amount` | 0.00 — below min of 0.01 | range |
| 35 | `account_id` | null (empty cell) | not_null |

Seven rows fail at least one expectation. Thirty-three rows pass all five:

```
quality_score = 33 / 40 = 0.825
threshold     = 0.95

0.825 < 0.95  →  threshold breached  →  onFail fires
```

The score is well below the 95% threshold, so the engine emits a quality report.

---

## onFail Modes

The `dq` transform supports three actions when the quality score falls below `threshold`:

| Mode | Behaviour | Pipeline outcome | Use When |
|------|-----------|------------------|----------|
| `abort` | Engine immediately halts with a non-zero exit code. The quality report is written before exit. | Pipeline stops; downstream sink receives no rows. | Zero-tolerance source contracts — data must be clean before any downstream system is touched. |
| `report` | Quality report is emitted to `reportSinkType`. Pipeline continues. | All rows are forwarded to the main sink, quality issues or not. | Observability is required but the pipeline must not block; quality is monitored over time. |
| `pass` | Quality score is logged but no report is emitted and no action is taken. | Pipeline continues silently. | Development or diagnostic mode; the DQ gate is informational only. |

This example uses **`report`**. The pipeline runs to completion and all 40 rows reach the null sink; the engine additionally writes a structured quality report describing which rows failed which expectations.

### Choosing the Right Mode

```
Is data correctness a hard prerequisite?
   YES → onFail: "abort"   (nothing downstream receives bad data)
   NO  → Do you need a written audit trail?
             YES → onFail: "report"  (pipeline continues; report is persisted)
             NO  → onFail: "pass"    (log only; useful during development)
```

---

## DQ Transform vs Validate Transform

Both transforms evaluate data quality, but they operate at different levels:

| Aspect | `validate` transform | `dq` transform |
|--------|----------------------|----------------|
| **Level** | Row-level — each row is individually accepted or rejected | Dataset-level — the entire dataset receives a single quality score |
| **Action on failure** | Routes/drops/fails the individual row | Gates the pipeline on a fractional threshold |
| **Output** | Rows split into main path and DLQ | All rows continue; quality score triggers `onFail` |
| **Best for** | Row routing — separate good from bad records | Quality measurement — is this dataset fit for purpose? |
| **Sinks affected** | Only the DLQ sink receives rejected rows | `reportSinkType` receives the quality report; main sink receives all rows |
| **Threshold concept** | Not applicable — every row is evaluated independently | Central concept — the ratio of passing rows vs total rows |

**Rule of thumb:** use `validate` when you want to route or remove individual bad rows. Use `dq` when you want to measure the overall health of a dataset and either block or alert based on a quality threshold.

These two transforms are complementary and can be chained in the same pipeline:

```json
"transforms": [
  { "type": "dq", "expectations": [...], "threshold": 0.95, "onFail": "abort" },
  { "type": "validate", "rules": [...], "onViolation": "route" }
]
```

In the above sequence, the DQ gate runs first: if the dataset is too dirty, the pipeline aborts before the validate transform (and the main sink) receives any data. If the dataset passes the quality threshold, individual row validation proceeds.

---

## Supported Check Types

The `dq` transform supports five check types on the `check` field of each expectation:

| Check | `value` type | Description |
|-------|-------------|-------------|
| `not_null` | `null` | Column must not be null or empty string |
| `range` | `{ "min": number, "max": number }` | Numeric column must be within `[min, max]` (inclusive) |
| `in_set` | `["val1", "val2", ...]` | String column must match one of the listed values |
| `pattern` | `"regex_string"` | String column must match the RE2 regular expression |
| `unique` | `null` | Column values must be distinct across all rows in the batch |

This example demonstrates `not_null`, `range`, and `in_set`. The `pattern` and `unique` checks follow the same expectation shape:

```json
{ "column": "record_id", "check": "pattern", "value": "^REC-[0-9]{6}$" }
{ "column": "record_id", "check": "unique",   "value": null }
```

---

## Directory Layout

```
13-data-quality/
├── data-quality.datapipeline   # Pipeline definition
├── run.sh                      # Helper script
├── README.md                   # This file
└── data/
    └── financial_records.csv   # 40-row input with 7 deliberate defects
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
1. Locates the engine binary.
2. Prints a pre-run summary of the expected defects and quality score.
3. Invokes the engine against `data-quality.datapipeline`.
4. Prints a post-run explanation of the `onFail: report` behaviour.

You can also invoke the engine directly:

```bash
../../flogo-datapipeline run data-quality.datapipeline
```

---

## How to Verify

**1. Engine log reports a quality score below the threshold**

Look for a log line similar to:

```
[dq] quality score: 0.825 (33/40 rows passed) — below threshold 0.95 — executing onFail: report
```

**2. Quality report is emitted**

With `reportSinkType: "null"` the report is discarded after being written (useful for verifying engine behaviour without writing files). To persist the report, change `reportSinkType` to a real sink such as `"jsonlines"` and add a `reportSinkConfig` block. For example:

```json
{
  "type": "dq",
  "expectations": [...],
  "threshold": 0.95,
  "onFail": "report",
  "reportSinkType": "jsonlines",
  "reportSinkConfig": {
    "path": "./data/quality_report.jsonl"
  }
}
```

After the run, `data/quality_report.jsonl` will contain one JSON object per failing row:

```jsonl
{"row":11,"column":"record_id","check":"not_null","actual":null,"passed":false}
{"row":16,"column":"transaction_amount","check":"range","actual":1200000,"passed":false}
{"row":21,"column":"account_id","check":"not_null","actual":null,"passed":false}
{"row":26,"column":"currency","check":"in_set","actual":"XBT","passed":false}
{"row":31,"column":"record_id","check":"not_null","actual":null,"passed":false}
{"row":33,"column":"transaction_amount","check":"range","actual":0,"passed":false}
{"row":35,"column":"account_id","check":"not_null","actual":null,"passed":false}
```

**3. Pipeline exits 0 with onFail: report**

```bash
echo $?
# Expected: 0  (report mode continues — the pipeline does not abort)
```

**4. Test abort mode**

Change `onFail` to `"abort"` in `data-quality.datapipeline` and re-run:

```bash
../../flogo-datapipeline run data-quality.datapipeline
echo $?
# Expected: non-zero  (engine halted because quality score < threshold)
```

**5. Test pass mode**

Change `onFail` to `"pass"` in `data-quality.datapipeline` and re-run:

```bash
../../flogo-datapipeline run data-quality.datapipeline
echo $?
# Expected: 0  (engine logged the score but took no action)
```

**6. Test with a passing dataset**

Raise `threshold` to `0.50` (below the actual score of 0.825):

```json
"threshold": 0.50
```

Re-run. The engine should log that the quality score exceeds the threshold and not fire the `onFail` action at all.

---

## Tuning the Threshold

The threshold is a domain decision, not a technical one. Some reference points:

| Domain | Typical threshold | Rationale |
|--------|------------------|-----------|
| Financial / regulatory | 0.99 – 1.00 | Near-zero tolerance; regulators require complete and accurate records |
| Customer master data | 0.95 – 0.99 | High confidence required for billing, identity, and communication |
| Operational analytics | 0.90 – 0.95 | Small quality gaps are acceptable when trends matter more than individual rows |
| Exploratory / data science | 0.70 – 0.85 | Models tolerate some missing values; statistical sampling is valid |
| Log / event data | 0.50 – 0.70 | High volume, best-effort ingestion; individual row completeness is secondary |

Start conservative (higher threshold) and relax only when you have evidence that downstream consumers can tolerate the quality gap.

---

## Swapping the Main Sink for a Real Target

Replace the `sink` block with any supported sink type. For example, to write all records to a Parquet file regardless of quality score:

```json
"sink": {
  "type": "parquet",
  "path": "./data/financial_records_raw.parquet",
  "rowGroupSize": 10000
}
```

Or to a JDBC table:

```json
"sink": {
  "type": "jdbc",
  "driver": "postgres",
  "dsn": "host=localhost port=5432 dbname=finance user=etl password=secret sslmode=disable",
  "table": "financial_records",
  "batchSize": 1000,
  "onConflict": "upsert",
  "conflictKey": ["record_id"]
}
```

---

## Chaining DQ with Other Transforms

The `dq` transform is position-sensitive. A common production pattern is:

```json
"transforms": [
  {
    "type": "dq",
    "expectations": [
      { "column": "record_id",          "check": "not_null", "value": null },
      { "column": "transaction_amount", "check": "range",    "value": { "min": 0.01, "max": 1000000 } },
      { "column": "currency",           "check": "in_set",   "value": ["USD", "EUR", "GBP", "JPY", "CAD"] },
      { "column": "account_id",         "check": "not_null", "value": null },
      { "column": "txn_date",           "check": "not_null", "value": null }
    ],
    "threshold":      0.95,
    "onFail":         "abort",
    "reportSinkType": "jsonlines",
    "reportSinkConfig": { "path": "./data/quality_report.jsonl" }
  },
  {
    "type": "filter",
    "expr": "transaction_amount > 0 AND currency != ''"
  },
  {
    "type": "map",
    "fields": {
      "record_id":          "=$record_id",
      "account_id":         "=$account_id",
      "amount_usd":         "=$transaction_amount",
      "currency":           "=$currency",
      "txn_date":           "=$txn_date",
      "category":           "=$category"
    }
  }
]
```

In this sequence:
1. `dq` measures the incoming dataset. If quality is below 0.95, the pipeline aborts before any data reaches downstream systems.
2. `filter` discards zero-amount or currency-missing rows (belt-and-suspenders after the DQ gate passes).
3. `map` renames fields for the output schema.

---

## Key Configuration Reference

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `transforms[].type` | string | yes | Must be `"dq"` to use this transform. |
| `transforms[].expectations` | array | yes | One or more expectation objects. |
| `expectations[].column` | string | yes | Column name as defined in the source schema. |
| `expectations[].check` | string | yes | Check type: `"not_null"`, `"range"`, `"in_set"`, `"pattern"`, or `"unique"`. |
| `expectations[].value` | varies | yes | Check parameter. `null` for `not_null`/`unique`; `{min,max}` object for `range`; string array for `in_set`; RE2 string for `pattern`. |
| `transforms[].threshold` | float | yes | Minimum acceptable quality score in `[0.0, 1.0]`. A score equal to or above this value is considered passing. |
| `transforms[].onFail` | string | yes | Action when `quality_score < threshold`: `"abort"` halts the pipeline, `"report"` emits a report and continues, `"pass"` logs only. |
| `transforms[].reportSinkType` | string | no | Sink type for the quality report. Required when `onFail` is `"report"`. Accepts any supported sink type or `"null"` to discard. |
| `transforms[].reportSinkConfig` | object | no | Sink-specific configuration for the report sink. Omit or use `{}` for `"null"` sink. |

---

## Next Steps

- **Example 14** will show the `map` transform for field renaming, type coercion, and computed columns.
- To combine DQ measurement with row-level DLQ routing, place a `dq` transform (abort or report) before a `validate` transform (route). The DQ gate ensures the dataset is fit enough to process; the validate transform handles individual row exceptions.
- For time-series quality monitoring, route the quality report to a `jdbc` sink and plot `quality_score` over pipeline runs — a declining trend signals upstream data deterioration before it reaches critical thresholds.
