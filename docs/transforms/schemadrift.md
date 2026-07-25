# Schema Drift transform — `schemadrift`

Baselines the schema from the first batch, then reacts when later batches add, drop, or
retype columns. Implemented in
[`transform/schemadrift/transform.go`](../../transform/schemadrift/transform.go);
registered as `schemadrift`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `policy` | string | `project` | — | `project`, `additive`, `strict`, or `drop`. |
| `onDrift` | string | `log` | — | `log`, `alert`, or `silent`. |

## Policies

| Policy | Behavior |
|---|---|
| `project` (default) | Emit only baseline columns; new columns dropped, missing ones filled with `nil`. Keeps a stable schema for the sink. |
| `additive` | Grow the baseline to include new/retyped columns and reproject; type changes are promoted. Pairs well with an auto-migrating [JDBC sink](../sinks/jdbc.md). |
| `strict` | Abort the pipeline on any schema change. |
| `drop` | Discard any batch whose schema differs from the baseline. |

`onDrift` controls notification only: `log` (WARN per change), `silent` (suppress), or
`alert` (reserved; currently logs).

## Example

```json
{ "type": "schemadrift", "policy": "additive", "onDrift": "log" }
```

## See also

- [Transforms index](../transforms.md) · [Data quality](../data-quality.md) · [JDBC sink](../sinks/jdbc.md)
- Demo: [`examples/11-schema-drift`](../../examples/11-schema-drift)
