# Union transform — `union`

Appends one or more **bounded secondary datasets** to the primary stream,
producing a single same-schema stream (N→1) — the map-side equivalent of SQL
`UNION ALL`. Implemented in
[`transform/union/transform.go`](../../transform/union/transform.go); registered
as `union`.

The primary stream flows through unchanged; each `with` source (CSV, JDBC,
Parquet, a Kafka batch drain, …) is read once into memory and emitted with its
columns aligned to the primary schema by name.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `with` | `[]object` | — | ✅ | A non-empty array of inline source definitions (`type` + its options) for the datasets to append, e.g. `{ "type": "csv", "path": "…", "hasHeader": true }`. |
| `maxRows` | int | `1000000` | — | Hard cap on the **total** secondary rows buffered across all `with` sources. |

## Behavior

- **`UNION ALL`** — no de-duplication; every secondary row is appended.
- Columns are aligned to the **primary schema by name**: a column present in the
  primary but absent from a secondary row is filled with null; a secondary column
  not in the primary schema is dropped (the output keeps the primary schema).
- The secondary rows ride along with the **first** primary batch (union order is
  not significant). If the primary source produces **no** rows at all, the
  secondaries are not emitted — size the primary as the driving stream.
- Each secondary source is buffered in memory and **must be bounded** — a Kafka
  `with` source is auto-drained to its current end, and buffering more than
  `maxRows` rows in total is a hard error. Raise `maxRows` or use bounded sources.

> For **unbounded, streaming** multi-source consolidation, prefer the graph
> runner's native **fan-in** (several source nodes merging into one downstream
> node) instead of the `union` transform — see [Graph (DAG) execution](../graph.md).
> `union` is the right tool when the extra feeds are small, bounded reference sets.

## Example

Append an archive export and a late-arriving batch to the live order stream:

```json
{
  "type": "union",
  "with": [
    { "type": "csv",  "path": "more-orders.csv", "hasHeader": true },
    { "type": "jdbc", "dsn": "…", "query": "SELECT … FROM archive_orders" }
  ],
  "maxRows": 2000000
}
```

## See also

- [Transforms index](../transforms.md)
- [`join`](join.md) — enrich (rather than append) from a bounded reference dataset
- [Graph (DAG) execution](../graph.md) — streaming multi-source fan-in
