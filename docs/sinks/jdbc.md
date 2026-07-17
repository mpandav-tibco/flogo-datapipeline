# JDBC sink — `jdbc`

Bulk-inserts or upserts rows into a relational table (PostgreSQL, MySQL, SQLite), with
optional automatic table creation and column migration. Implemented in
[`sink/jdbc/sink.go`](../../sink/jdbc/sink.go); registered as `jdbc`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `driver` | string | `postgres` | — | `postgres`, `mysql`, `sqlite3`. |
| `dsn` | string | — | ✅ | Connection string. Prefer a [connection](../connections.md). |
| `table` | string | — | ✅ | Target table. |
| `batchSize` | int | `1000` | — | Rows buffered before a flush. |
| `onConflict` | string | `insert` | — | `insert`, `upsert`, or `ignore`. |
| `conflictKey` | `[]string` | — | upsert | Key columns for the upsert match. |
| `autoMigrate` | bool | `false` | — | `CREATE TABLE IF NOT EXISTS` on first batch and `ALTER TABLE … ADD COLUMN` for new columns. |

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
- Demo: [`examples/10-csv-to-sqlite`](../../examples/10-csv-to-sqlite)
