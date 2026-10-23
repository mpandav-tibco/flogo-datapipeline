# JDBC sink — `jdbc`

Bulk-inserts or upserts rows into a relational table (PostgreSQL, MySQL, SQL Server, Oracle), with
optional automatic table creation and column migration. Implemented in
[`sink/jdbc/sink.go`](../../sink/jdbc/sink.go); registered as `jdbc`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `driver` | string | `postgres` | — | `postgres`, `mysql`, `sqlserver`, `oracle`. |
| `dsn` | string | — | ✅ | Connection string. Prefer a [connection](../connections.md). |
| `table` | string | — | ✅ | Target table. |
| `batchSize` | int | `1000` | — | Rows buffered before a flush. |
| `onConflict` | string | `insert` | — | `insert`, `upsert`, or `ignore`. |
| `conflictKey` | `[]string` | — | upsert | Key columns for the upsert match. |
| `autoMigrate` | bool | `false` | — | `CREATE TABLE IF NOT EXISTS` on first batch and `ALTER TABLE … ADD COLUMN` for new columns. When `conflictKey` is set, the created table also declares it as the `PRIMARY KEY` (required for Postgres's upsert; harmless elsewhere). |
| `opColumn` | string | — | — | Turns on delete-capable mode (see below). Requires a non-empty `conflictKey`. |
| `deleteValues` | `[]string` | `["d", "delete"]` | — | `opColumn` values (case-insensitive) that mean "delete this row" instead of upsert. |
| `bulkMode` | string | `auto` | — | `auto` loads plain inserts (`driver: postgres`, `onConflict: insert`, no `opColumn`) with the PostgreSQL `COPY` protocol, typically several times faster than multi-row `INSERT`; every other combination uses multi-row `INSERT`. `insert` always uses multi-row `INSERT` (e.g. behind proxies or Postgres-compatible engines without `COPY FROM STDIN`). |

## Delete-capable mode (`opColumn`)

Set `opColumn` to a metadata column present in every batch — but **not** itself a
target-table column — whose value classifies each row. Rows whose `opColumn` value
matches one of `deleteValues` are `DELETE`d by `conflictKey` instead of written; every
other row is inserted/upserted as usual. A batch that mixes upserts and deletes
commits both in **one transaction**, so a partial failure rolls back atomically.

This turns a periodic full/incremental refresh into a true mirror of the source table
— rows removed upstream are removed here too — **without requiring a full CDC
pipeline**. Pair it with the [kafka source](../sources/kafka.md)'s Debezium-envelope
deserializer to consume real CDC change streams, or with any source that emits an
op/status column of its own.

**Observability:** delete-capable mode reports how many rows were actually deleted
(via `sql.Result.RowsAffected()`, not just how many delete rows were buffered — a
delete targeting a key that no longer exists doesn't inflate the count) through
`Output.RowsDeleted` (also logged as `deleted=N` in the pipeline-run log line and
`pipeline.run.finished` audit events), and as a custom OpenLineage output-dataset
facet (`datapipeline_cdcStats`, key `rowsDeleted`) when [lineage](../observability.md#lineage-openlineage) is
enabled. This distinguishes a CDC run's inserts from its deletes — without it, a run
of "900 updates + 100 deletes" and one of "1000 plain inserts" both show as
`rowsWritten=1000`, indistinguishable from each other.

```json
{
  "type": "jdbc",
  "driver": "postgres",
  "dsn": "host=db dbname=app user=etl sslmode=disable",
  "table": "customers",
  "onConflict": "upsert",
  "conflictKey": ["customer_id"],
  "opColumn": "__op",
  "deleteValues": ["d"]
}
```

## Behavior

- **Upsert** is dialect-aware: Postgres `ON CONFLICT (key) DO UPDATE`; MySQL
  `ON DUPLICATE KEY UPDATE` (only non-key columns updated). `ignore` maps to
  `ON CONFLICT DO NOTHING` / `INSERT IGNORE`.
- **Schema evolution:** with `autoMigrate`, new columns are added on the fly; without it,
  extra columns are trimmed (safe when an upstream [schemadrift](../transforms/schemadrift.md)
  `project` fills dropped columns with nil).
- Rows are copied into the sink immediately, so the caller can recycle the batch. Pool:
  `MaxOpenConns=4`, `MaxIdleConns=2`.

## Example

```json
{
  "type": "jdbc",
  "driver": "postgres",
  "dsn": "host=db dbname=app user=etl sslmode=disable",
  "table": "orders",
  "batchSize": 1000,
  "onConflict": "upsert",
  "conflictKey": ["order_id"],
  "autoMigrate": false
}
```

## See also

- [Sinks index](../sinks.md) · [JDBC source](../sources/jdbc.md) · [Schema drift](../transforms/schemadrift.md)
- Demo: [`examples/17-kafka-streaming`](../../examples/17-kafka-streaming) (Kafka → JDBC upsert sink)
