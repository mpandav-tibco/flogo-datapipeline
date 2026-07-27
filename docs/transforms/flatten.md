# Flatten transform — `flatten`

Replaces nested-object columns with one **dotted column per leaf**
(`address` → `address.city`, `address.zip`). Nested objects arrive as
`record.TypeAny` map values from JSON/XML sources. Implemented in
[`transform/flatten/transform.go`](../../transform/flatten/transform.go); registered
as `flatten`. Pure map-side, per-row.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `columns` | `[]string` | all map-valued | — | Which columns to flatten. Default: every column whose value is a nested map. |
| `separator` | string | `.` | — | Joins parent and child keys (`address.city`). |
| `depth` | int | `0` | — | Maximum nesting depth to flatten; `0` = unlimited. Deeper maps remain as values. |

## Behavior

- A flattened column is **replaced** by one column per leaf. The output schema is
  the **union of leaf keys across the batch**, in deterministic sorted order; rows
  missing a key get null.
- **Arrays are left as values** — use [`explode`](explode.md) to expand them, then
  `flatten` the resulting objects.
- A row whose selected column is not a map contributes nulls for that column's
  leaves. Non-map and unselected columns pass through unchanged.

## Example

```json
{ "type": "flatten", "columns": ["address"], "separator": "." }
```

Input row `{ "id": 1, "address": { "city": "NYC", "zip": "10001" } }` →
columns `id`, `address.city`, `address.zip`.

## See also

- [Transforms index](../transforms.md)
- [`explode`](explode.md) — expand array columns into rows first
