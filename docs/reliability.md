# Reliability & incremental load

Two independent mechanisms keep long runs correct across interruptions and across
schedules:

- **Checkpointing** — resume an *interrupted* run from where it stopped.
- **Incremental load (HWM)** — on each *scheduled* run, read only rows newer than the
  last run.

They compose: a large incremental load can also checkpoint within a run.

## Checkpointing

The engine periodically saves the source position so a crashed or cancelled run
resumes instead of restarting. The token is `{format, payload, rowsProcessed}`;
sources that implement `CursorProvider` seek to it in O(1) (JDBC keyset, Parquet
row-group), while others replay-and-skip.

```json
"checkpoint": {
  "type": "redis",
  "intervalRows": 10000,
  "intervalSeconds": 30,
  "deleteOnSuccess": true
}
```

| Key | Default | Notes |
|---|---|---|
| `type` | `none` | `file` \| `redis` \| `none` |
| `intervalRows` | `10000` | save every N rows |
| `intervalSeconds` | `30` | …or every N seconds (whichever first) |
| `deleteOnSuccess` | — | drop the checkpoint after a clean finish |

### Backends

| Backend | Config | Scope |
|---|---|---|
| `file` | `path` (temp dir default) | single instance; atomic temp-then-rename, one JSON file per partition |
| `redis` | `addr`, `keyPrefix` (`flogo:pipeline:ckpt`), `passwordEnv`, `ttlSeconds` (`604800`) | multi-instance safe |

See the runnable [`examples/15-checkpoint-resume`](../examples/15-checkpoint-resume).

## Incremental load (high-water mark)

For pipelines that run on a schedule against a growing table or feed, set
`incremental.mode: hwm`. The engine injects a predicate so the source reads only rows
whose `column` is greater than the stored mark, then advances the mark to the max seen
this run.

```json
"incremental": {
  "mode": "hwm",
  "column": "updated_at",
  "valueType": "datetime",
  "initialValue": "1970-01-01T00:00:00Z",
  "storeType": "postgres",
  "storeOptions": { "dsn": "host=… dbname=meta" }
}
```

| Key | Notes |
|---|---|
| `mode` | `hwm` \| `none` |
| `column` | monotonic column tracked as the mark |
| `valueType` | `datetime` \| `int` \| `string` |
| `initialValue` | mark used on the very first run |
| `storeType` | `file` \| `redis` \| `postgres` |
| `storeOptions` | backend connection config |

The JDBC source injects the mark directly into its `WHERE`; other sources pair with
the [`hwm` transform](transforms.md) to observe the max. The mark is
`{column, value, valueType, savedAt}`.

### Watermark backends

| Backend | Config | Scope |
|---|---|---|
| `file` | `path` | single instance; atomic writes, one file per pipeline |
| `redis` | `addr`, `keyPrefix` (`flogo:pipeline:hwm`), `passwordEnv` | multi-instance; no TTL |
| `postgres` | `dsn` | multi-instance, auditable; auto-creates `flogo_pipeline_watermarks`, upserts via `ON CONFLICT` |

See [`examples/12-incremental-hwm`](../examples/12-incremental-hwm).

## Choosing backends

| You have | Checkpoint | Watermark |
|---|---|---|
| one instance, local disk | `file` | `file` |
| several replicas | `redis` | `redis` or `postgres` |
| an audit requirement | `redis` | `postgres` (records `saved_at`) |

## See also

- [Scaling](scaling.md) — multi-instance execution (use a shared backend)
- [Configuration](configuration.md) — performance / memory knobs
- [Feature reference](FEATURES.md#checkpoint-backends)
