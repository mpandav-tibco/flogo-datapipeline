# Object Store sink — `s3`, `gcs`, `adls` (+ `-sink`)

Writes batches to cloud storage (Amazon S3 and S3-compatible stores, Google Cloud Storage,
Azure ADLS Gen2). An inner `format` sink encodes each object; a **key template** decides
object naming, **partitioning**, and **rolling**. Implemented in
[`sink/objstore/sink.go`](../../sink/objstore/sink.go) over the
[`objstore/`](../../objstore) provider abstraction; registered as `s3`, `gcs`, `adls`
(and `s3-sink`, `gcs-sink`, `adls-sink`).

- **Writes:** objects encoded by the inner `format` (`csv`, `jsonlines`, `parquet`)
- **Single / partitioned / rolling** — selected by the key template + roll keys (below)

## Common configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `key` | string | — | ✅ | Destination object-key template (macros below). |
| `format` | string | — | ✅ | Inner sink type. Format-specific keys (e.g. `compression`) pass through. |
| `rollMaxBytes` | int | `0` | — | Roll (close + upload) the object at this size. `0` = no size roll. |
| `rollMaxSeconds` | int | `0` | — | Roll the object at this age (seconds). `0` = no time roll. |
| `spoolDir` | string | — | — | Rolling sinks only: durable local spool directory (see [Durable spool](#durable-spool-rolling)). Empty or `none` = no spool: upload synchronously at each flush (use `none` when the value comes from a property, because resolvers treat an empty value as unset). |
| `spoolMaxBytes` | int | `5368709120` | — | Spool capacity (bytes of sealed, not-yet-uploaded objects). When full, flushes fail (back-pressure) until uploads catch up. |
| `spoolCloseWaitSeconds` | int | `10` | — | On sink close, how long to wait for the spool to finish uploading. Whatever remains stays on disk and is uploaded by the next process. |

### S3 keys

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `bucket` | string | — | ✅ | S3 bucket. |
| `region` | string | `us-east-1` | — | AWS region. |
| `endpoint` | string | — | — | Custom endpoint (MinIO, LocalStack). |
| `forcePathStyle` | bool | `false` | — | Path-style addressing (**required for MinIO**). |
| `accessKeyIdEnv` / `secretAccessKeyEnv` / `sessionTokenEnv` | string | — | — | **Env var names** holding credentials. |

### GCS keys

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `bucket` | string | — | ✅ | GCS bucket. |
| `credentialsFileEnv` | string | — | — | **Env var name** holding the path to the service-account JSON. |
| `endpoint` | string | — | — | Override endpoint (fake-gcs-server). |

### ADLS keys

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `container` | string | — | ✅ | Blob container. |
| `accountName` | string | — | ✅ | Storage account. |
| `sasTokenEnv` / `accountKeyEnv` | string | — | — | **Env var names** holding the SAS token / account key. |
| `serviceURL` | string | derived | — | Custom endpoint (Azurite). |

> Inline credential keys (`accessKeyId`, `secretAccessKey`, `credentialsJson`, `sasToken`,
> `accountKey`) also exist for Studio/dev, but production should use the `*Env` forms or the
> cloud default chain. See [Security](../security.md).

## Key template macros

Expanded once at sink creation (UTC):

| Macro | Expands to | | Macro | Expands to |
|---|---|---|---|---|
| `{YYYY}` | 4-digit year | | `{HH}` | hour `00–23` |
| `{MM}` | month `01–12` | | `{mm}` | minute `00–59` |
| `{DD}` | day `01–31` | | `{ss}` | second `00–59` |
| `{ts}` | Unix seconds | | `{uuid}` | random v4 UUID |

Studio `${date}`, `${year}`, `${month}`, `${day}`, `${ts}`, `${uuid}`, `${runId}` are also
accepted. `{seq}` is the roll counter (auto-inserted before the extension if absent when
rolling).

## Single vs partitioned vs rolling

- **Single object** — key has only date/uuid macros: `data/{YYYY}/{MM}/{DD}/part.parquet`.
- **Partitioned** — key contains a **record-field placeholder** (any column name, e.g.
  `{namespace}`): a child object is written per distinct value. Empty values → `_unknown`;
  values are sanitized (`/`, `\`, `..` → `_`).
- **Rolling** — `rollMaxBytes > 0` or `rollMaxSeconds > 0`: objects close and upload when
  they hit the size or age limit; a `{seq}` counter increments per roll (when the key has no
  `{uuid}`, `{seq}` also carries a short per-sink token so restarts and recreated partitions
  never overwrite earlier objects, e.g. `part-1a2b3c4d-0000.ndjson`). Each rolled object
  gets a `<key>.manifest.json` sidecar (`record_count`, `bytes`, `opened_at`, `closed_at`).

### Durability with checkpoints (rolling)

A rolling sink's `Flush` — which the engine calls at every checkpoint — uploads the open
object(s) with their manifests. A checkpoint is therefore only taken once every row before
it is **in the object store**, never just in the local temp file: a crashed or OOM-killed
collector replays from the last checkpoint (duplicates possible, no loss). Consequences:

- The checkpoint interval is the delivery latency and the object cadence per partition
  (`rollMaxBytes`/`rollMaxSeconds` still cap objects between checkpoints).
- An upload that fails is kept (temp file intact) and retried at the next flush; the
  flush — and so the checkpoint — fails until it succeeds. Rows are never dead-lettered
  because of an upload failure.
- Partitioned children are released after each flush, so per-key state (e.g. one per pod)
  does not accumulate as keys churn.

A non-rolling single object is uploaded only at `Close`; with checkpointing enabled, use a
rolling sink so checkpoints cover uploaded data.

### Durable spool (rolling)

Without a spool, an object-store outage stops checkpoints, which back-pressures the source.
For a node-local log collector that is not enough: kubelet keeps deleting rotated files and
removed pods' logs, so a long outage would still lose data at the source. With `spoolDir`
set, `Flush` makes data durable **on local disk** instead of remotely:

1. The rolling object is written directly into `spoolDir` (`*.part`).
2. At a flush or roll it is fsynced, renamed to `*.data`, and an atomic `*.meta` sidecar
   (key, manifest) is written. Then the flush returns, so the checkpoint and source positions
   advance.
3. Background workers (4 per spool directory) upload sealed objects plus their
   manifests. Failed uploads are retried with backoff (1 s → 30 s) for as long as needed.
   An object is deleted from the spool only after its upload has succeeded.
4. On start, sealed objects left by a previous process are uploaded. `*.part` files are
   discarded because their rows are past the last checkpoint and will be replayed.

When `spoolMaxBytes` is reached, flushes fail, so checkpoints stop and the pipeline
back-pressures exactly as it does without a spool. Size the spool for
`ingest rate × longest outage to ride out`. Use a node path that survives pod restarts
(a `hostPath`), and give the dead-letter sink its own spool directory.

Gauges (label `spool`): `flogo_objstore_spool_bytes`, `flogo_objstore_spool_objects`,
`flogo_objstore_spool_upload_failures`, `flogo_objstore_spool_uploaded_objects`. Alert on
`spool_bytes` approaching `spoolMaxBytes`.

> The spool lives on the node. Losing the node's disk loses what has not been uploaded yet.
> Without a spool, the same data would be lost anyway, because it would still be only in the
> pod logs on that node.

## Credential resolution (S3)

1. `accessKeyIdEnv` **and** `secretAccessKeyEnv` set → read both env vars.
2. Else inline `accessKeyId`/`secretAccessKey` (Studio/dev).
3. Else the AWS default chain (env → `~/.aws` → IRSA / instance profile).

GCS and ADLS follow the same env-first → inline → cloud-default order.

## Example — partitioned, rolling NDJSON to MinIO

```json
{
  "type": "s3",
  "bucket": "adidas-logs",
  "key": "{namespace}/{pod}/{YYYY}/{MM}/{DD}/{node}-{HH}{mm}-{uuid}.ndjson",
  "format": "jsonlines",
  "region": "us-east-1",
  "endpoint": "http://minio:9000",
  "forcePathStyle": true,
  "accessKeyIdEnv": "MINIO_ACCESS_KEY",
  "secretAccessKeyEnv": "MINIO_SECRET_KEY",
  "rollMaxBytes": 134217728,
  "rollMaxSeconds": 300
}
```

## See also

- [Sinks index](../sinks.md) · [Object Store source](../sources/objstore.md) · [Security](../security.md) · [Scaling](../scaling.md)
- Demo: [`examples/adidas-log-collection-usecase`](../../examples/adidas-log-collection-usecase)
