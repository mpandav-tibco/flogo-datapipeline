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
  they hit the size or age limit; a `{seq}` counter increments per roll. Each rolled object
  gets a `<key>.manifest.json` sidecar (`record_count`, `bytes`, `opened_at`, `closed_at`).

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
