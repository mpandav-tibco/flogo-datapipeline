# Validate transform — `validate`

Applies per-field rules and, on violation, **routes** bad rows to an error sink / DLQ,
**fails** the pipeline, or **drops** them. Implemented in
[`transform/validate/transform.go`](../../transform/validate/transform.go); registered as
`validate`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `rules` | `[]object` | — | ✅ | Per-field rules (see below). |
| `onViolation` | string | `route` | — | `route` (to the error sink), `fail` (abort), or `drop` (silently discard). |
| `errorSinkType` | string | — | — | Sink type for routed rows (e.g. `dlq`, `null`). Defaults to a discard sink. |
| `errorSinkConfig` | object | `{}` | — | Configuration for that error sink. |

### Rule object

| Key | Type | Description |
|---|---|---|
| `field` | string | Column to validate (**required**). |
| `notNull` | bool | Field must be non-null (checked first; if null, other checks skip). |
| `min` / `max` | number | Inclusive numeric bounds (non-numeric values skip the check). |
| `minLength` / `maxLength` | number | String length bounds (Unicode rune count). |
| `pattern` | string | Go regex the value must match. |
| `in` | `[]string` | Allowed values (case-insensitive membership). |

## Behavior

- A row passes when **all** rules for **all** their fields pass; otherwise `onViolation`
  fires.
- `route` writes the failing rows to the error sink and lets good rows continue — pair it
  with a [DLQ sink](../sinks/dlq.md) to capture the error envelope.
- This is the primary way to enforce a schema contract before a load. See
  [Data quality](../data-quality.md) for the DQ/scoring counterpart.

## Example

```json
{
  "type": "validate",
  "rules": [
    { "field": "order_id", "notNull": true, "pattern": "^ORD-\\d{6}$" },
    { "field": "amount", "notNull": true, "min": 0, "max": 1000000 },
    { "field": "currency", "in": ["USD", "EUR", "GBP"] }
  ],
  "onViolation": "route",
  "errorSinkType": "dlq",
  "errorSinkConfig": { "innerType": "jsonlines", "innerOptions": { "path": "./out/rejects.ndjson" } }
}
```

## See also

- [Transforms index](../transforms.md) · [Data quality](../data-quality.md) · [DLQ sink](../sinks/dlq.md)
- Demo: [`examples/03-validate-and-dlq`](../../examples/03-validate-and-dlq)
