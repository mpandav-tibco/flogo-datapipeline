# High-Water Mark transform — `hwm`

A **pass-through observer** that tracks the maximum value of a column and exposes it to the
[watermark store](../reliability.md) for incremental loads. Rows are not modified.
Implemented in [`transform/hwm/transform.go`](../../transform/hwm/transform.go);
registered as `hwm`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `column` | string | — | ✅ | Column to track. |
| `valueType` | string | `string` | — | `datetime`, `int`, or `string` — selects the comparison. |

## Behavior

- Scans every non-null value of `column` per batch and keeps the running maximum
  (`datetime` = latest time, `int` = numeric max, `string` = lexicographic max).
- After each checkpoint interval the engine reads the max and persists it via the
  configured watermark backend, so the next run only reads newer rows.
- Use it with non-JDBC sources (JDBC injects the watermark predicate directly). Column
  absence is non-fatal — the batch passes through untouched.

## Example

```json
{ "type": "hwm", "column": "updated_at", "valueType": "datetime" }
```

## See also

- [Transforms index](../transforms.md) · [Reliability & incremental load](../reliability.md) · [JDBC source](../sources/jdbc.md)
- Demo: [`examples/12-incremental-hwm`](../../examples/12-incremental-hwm)
