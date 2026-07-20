# JDBC source — `jdbc` · *partitionable*

Reads rows from a relational database (PostgreSQL, MySQL, SQLite; Oracle via ODBC) in
**table mode** or with a raw **query**, using keyset pagination for scalable, resumable
reads. Implemented in [`source/jdbc/source.go`](../../source/jdbc/source.go); registered
as `jdbc`.

- **Reads:** SQL result sets
- **Partitionable:** ✅ — keyset or LIMIT/OFFSET bands across [scaling](../scaling.md) instances
- **Streaming:** bounded — consumes the result set, batched
- **Checkpoint / resume:** O(1) cursor (`lastKeyVal` for keyset, `offset` otherwise)

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `driver` | string | `postgres` | — | `postgres`, `mysql`, `sqlite3`. |
| `dsn` | string | — | ✅ | Driver connection string. Prefer a [connection](../connections.md) so secrets stay off disk. |
| `table` | string | — | ✅ (or `query`) | Table to read; combine with `columns`/`where`/`orderKey`. |
| `query` | string | — | ✅ (or `table`) | Raw `SELECT` (overrides table mode). |
| `columns` | `[]string` | all | — | Column subset. |
| `where` | string | — | — | Extra predicate (AND-ed with the injected watermark filter). |
| `orderKey` | string | — | — | Unique/stable key enabling keyset pagination. |
| `batchSize` | int | `5000` | — | Rows per fetch. |

> `hwmColumn` / `hwmValue` / `hwmValueType` are **injected by the engine** during
> [incremental load](../reliability.md); you do not set them by hand.

## Behavior

- **Keyset pagination** (`WHERE orderKey > $last LIMIT N`) is O(log N) per page and safe
  under concurrent inserts. Without `orderKey` the source falls back to LIMIT/OFFSET
  (avoid for very large tables).
- In **table mode**, an incremental watermark predicate (`hwmColumn > value`) is appended
  automatically. In **raw query mode** you must add the predicate yourself.
- Schema is discovered with a `LIMIT 0` probe. Connection pooling is
  `MaxOpenConns=4`, `MaxIdleConns=2` per instance.

## Example — table mode

```json
{
  "type": "jdbc",
  "driver": "postgres",
  "dsn": "host=db port=5432 dbname=app sslmode=disable",
  "table": "transactions",
  "columns": ["id", "amount", "created_at"],
  "where": "status = 'ACTIVE'",
  "orderKey": "id",
  "batchSize": 1000
}
```

## Example — query mode

```json
{
  "type": "jdbc",
  "driver": "postgres",
  "dsn": "host=db port=5432 dbname=app sslmode=disable",
  "query": "SELECT id, amount, ts FROM transactions WHERE amount > 0 ORDER BY ts",
  "batchSize": 5000
}
```

## See also

- [Sources index](../sources.md) · [JDBC sink](../sinks/jdbc.md) · [Reliability & incremental load](../reliability.md)
- Demo: [`examples/09-jdbc-source-sqlite`](../../examples/09-jdbc-source-sqlite), [`examples/12-incremental-hwm`](../../examples/12-incremental-hwm)
