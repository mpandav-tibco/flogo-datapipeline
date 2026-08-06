# Data Quality transform — `dq`

Evaluates data-quality **expectations** over each batch, computes a quality score, and —
when the score falls below a threshold — routes failing rows, fails the run, warns, or
drops the batch. Optionally stamps a per-row score and writes a DQ report. Implemented in
[`transform/dq/transform.go`](../../transform/dq/transform.go); registered as `dq`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `expectations` | `[]object` | — | ✅ | The checks to evaluate (see below). |
| `onFail` | string | `route` | — | `route`, `fail`, `warn`, or `drop` (discard the whole batch). |
| `qualityThreshold` | float | `0.95` | — | Overall score in `[0,1]` below which `onFail` fires. |
| `scoreColumn` | string | — | — | If set, append a per-row quality score column. |
| `errorSinkType` / `errorSinkConfig` | string / object | — | — | Sink for failing rows (when `route`). |
| `reportSinkType` / `reportSinkConfig` | string / object | — | — | Sink for a per-batch DQ report row. |

## Expectation types

| Type | Keys | Score |
|---|---|---|
| `expectColumnNotNull` | `column`, `threshold` (1.0) | non-null ÷ total |
| `expectColumnUnique` | `column`, `threshold` (1.0) | distinct ÷ non-null |
| `expectColumnValuesBetween` | `column`, `min`, `max` | in-range ÷ numeric |
| `expectColumnValuesInSet` | `column`, `values`, `caseSensitive` (false) | in-set ÷ non-null |
| `expectTableRowCountBetween` | `min`, `max` | 1.0 if in range |
| `expectColumnMeanBetween` | `column`, `min`, `max` | 1.0 at midpoint, degrades |
| `expectColumnStddevBelow` | `column`, `threshold` | `max(0, 1 − stddev/threshold)` |
| `expectColumnValuesInReferenceDataset` | `column`, `referenceDataset`, `referenceColumn` (=`column`), `threshold` (1.0), `maxRows` (1,000,000) | matched ÷ non-null |

The overall score is the average of all expectation scores. Row-level expectations
(`…ValuesBetween`, `…ValuesInSet`, `…InReferenceDataset`) also contribute to the per-row
`scoreColumn`; dataset-level expectations do not.

### Referential integrity — `expectColumnValuesInReferenceDataset`

A foreign-key / referential-integrity check: asserts that at least `threshold` fraction of
non-null values in `column` exist in a **bounded reference dataset** (any registered source
type — `csv`, `jdbc`, `parquet`, a batch-drained `kafka` topic, …). It reuses the same
bounded-reference-load pattern as the [`join`](join.md) transform's `right` side: the
reference dataset is read fully into an in-memory set **once**, at pipeline setup (not per
batch), so it must be bounded — guarded by `maxRows` (default 1,000,000; exceeding it is a
setup-time error, same as `join`).

```json
{
  "type": "expectColumnValuesInReferenceDataset",
  "column": "customer_id",
  "referenceDataset": { "type": "jdbc", "driver": "postgres", "dsn": "...", "table": "customers" },
  "referenceColumn": "id",
  "threshold": 1.0
}
```

Unlike `join`, this does **not** add any columns to the batch — it only scores referential
integrity; orphaned rows (a `customer_id` with no matching `customers.id`) are what pull the
score down, exactly like every other row-level expectation.

## Example

```json
{
  "type": "dq",
  "expectations": [
    { "type": "expectColumnNotNull", "column": "customer_id", "threshold": 0.99 },
    { "type": "expectColumnValuesInSet", "column": "status", "values": ["NEW", "ACTIVE", "CLOSED"] },
    { "type": "expectTableRowCountBetween", "min": 1, "max": 1000000 }
  ],
  "onFail": "route",
  "qualityThreshold": 0.95,
  "scoreColumn": "_dq_score",
  "errorSinkType": "dlq"
}
```

## See also

- [Transforms index](../transforms.md) · [Data quality](../data-quality.md) · [Validate](validate.md) · [Join](join.md)
- Demo: [`examples/13-data-quality`](../../examples/13-data-quality)
