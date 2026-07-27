# Cast transform — `cast`

Converts one or more columns to a target type in every row and updates the batch
schema to match. A pure **map-side, per-row** transform — no shuffle, no state.
Implemented in [`transform/cast/transform.go`](../../transform/cast/transform.go);
registered as `cast`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `columns` | `[]object` | — | ✅ | Columns to convert. Each has `name`, `type`, optional `format` (date layout) and `scale` (decimal places). |
| `onError` | string | `null` | — | `null` — unconvertible values become null; `fail` — the batch errors on the first bad value. |

Target `type` is one of `string`, `int`, `float`, `decimal`, `bool`, `date`, `any`.

## Behavior

- Each listed column's value is converted per row; other columns pass through
  untouched. The output schema's column `type` (and `format`/`scale`) is updated.
- A configured column that is **absent** from a batch is skipped (non-fatal),
  matching the pipeline's schema-tolerant style.
- `null` values always pass through as null.
- **Decimal casts are exact** (`record.Decimal`); with a `scale` the value is
  normalised to exactly that many fractional digits (padded or rounded
  half-away-from-zero). Casting an out-of-range value is governed by `onError`.
- Conversions: `int`/`float`/`decimal`/`bool` accept numeric, string and each
  other; `string` renders decimals with their exact scale and dates with `format`
  (or RFC3339); `date` accepts `time.Time` or a string parsed with `format` (with
  RFC3339 / `2006-01-02 15:04:05` / `2006-01-02` fallbacks), normalised to UTC.

## Example

```json
{
  "type": "cast",
  "columns": [
    { "name": "amount", "type": "decimal", "scale": 2 },
    { "name": "qty",    "type": "int" },
    { "name": "ts",     "type": "date", "format": "2006-01-02" }
  ],
  "onError": "null"
}
```

## See also

- [Transforms index](../transforms.md)
- [Expression language](expression-language.md) — for deriving/coercing values inside `map`
