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

The overall score is the average of all expectation scores. Row-level expectations
(`…ValuesBetween`, `…ValuesInSet`) also contribute to the per-row `scoreColumn`;
dataset-level expectations do not.

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

- [Transforms index](../transforms.md) · [Data quality](../data-quality.md) · [Validate](validate.md)
- Demo: [`examples/13-data-quality`](../../examples/13-data-quality)
