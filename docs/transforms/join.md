# Join transform — `join`

Keyed **stream-table join**: enriches each row of the primary (left) stream by
matching it against a second data source (the right side) loaded once into an
in-memory keyed table — the streaming equivalent of a broadcast join. Implemented
in [`transform/join/transform.go`](../../transform/join/transform.go); registered
as `join`.

The right side is a **bounded** reference dataset (CSV, JDBC, Parquet, a Kafka
batch drain, …); the left side is the pipeline's streaming source. Compose it with
[`window`](window.md) for a *windowed keyed join* — join to enrich, then window and
aggregate the joined stream.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `right` | object | — | ✅ | An inline source definition (`type` + its options) for the reference dataset, e.g. `{ "type": "csv", "path": "…", "hasHeader": true }`. |
| `leftKey` | string | — | ✅ | Left-stream column to match on. |
| `rightKey` | string | `leftKey` | — | Right-dataset column to match on. |
| `joinType` | string | `inner` | — | `inner` drops unmatched left rows; `left` keeps them with null right columns. |
| `rightColumns` | `[]string` | all but `rightKey` | — | Right columns to append to each output row. |
| `prefix` | string | — | — | Optional prefix for the appended right columns (e.g. `cust_`). |
| `maxRows` | int | `1000000` | — | Hard cap on right rows loaded into memory. |

## Behavior

- The right dataset is loaded **once** into an in-memory multimap keyed by
  `rightKey`; each left row is then matched by the string form of its key.
- `inner` drops left rows with no match; `left` keeps them with null right columns.
- A left row matching **N** right rows yields **N** output rows (many-to-many).
- The right side is held in memory and **must be bounded** — a Kafka right source
  is auto-drained to its current end, and loading more than `maxRows` rows is a
  hard error. Size the reference dataset accordingly.
- Keys are matched by **string form**, so align key column types across the two
  sides (both int, or both string) to avoid surprises.

## Example

Enrich an order stream with customer attributes from a local CSV:

```json
{
  "type": "join",
  "right": { "type": "csv", "path": "customers.csv", "hasHeader": true },
  "leftKey": "customerId",
  "rightKey": "id",
  "joinType": "left",
  "rightColumns": ["name", "tier"],
  "prefix": "cust_"
}
```

## See also

- [Transforms index](../transforms.md)
- [`window`](window.md) — compose after `join` for a windowed keyed join
- [Stream processing](../stream-processing.md)
