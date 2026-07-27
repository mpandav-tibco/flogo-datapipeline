# Explode transform — `explode`

Expands an array-valued column into **one output row per element**, duplicating the
other columns. This is the map-side way to normalise arrays-of-objects (from JSON /
XML / API sources) into flat rows. Implemented in
[`transform/explode/transform.go`](../../transform/explode/transform.go); registered
as `explode`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `column` | string | — | ✅ | The array-valued column to expand. |
| `into` | string | `column` | — | Output column name for each element. |
| `keepEmpty` | bool | `false` | — | When true, a null or empty array still emits one row (element = null) instead of dropping it. |

## Behavior

- An array of N elements yields **N rows**; an empty array or null yields **0 rows**
  (or one null row when `keepEmpty`).
- A non-array scalar yields **one row** (element = the value), so "sometimes an
  array, sometimes a scalar" JSON is handled gracefully.
- JSON arrays (`[]interface{}`) and other slice kinds are both accepted; a `[]byte`
  is treated as a scalar. A missing `column` passes the batch through unchanged.
- Pair with [`flatten`](flatten.md) to expand the exploded objects into columns.

## Example

```json
{ "type": "explode", "column": "items", "into": "item" }
```

Input row `{ "orderId": 7, "items": [ {"sku":"A"}, {"sku":"B"} ] }` →
two rows, each with `orderId=7` and `item` set to one element.

## See also

- [Transforms index](../transforms.md)
- [`flatten`](flatten.md) — flatten the resulting nested objects into columns
