# Object Store source — `s3`, `gcs`, `adls` · *partitionable*

Lists and reads objects from cloud storage (Amazon S3 and S3-compatible stores, Google
Cloud Storage, Azure ADLS Gen2), delegating parsing to an inner `format` source. Each
matched object becomes one partition. Implemented in
[`source/objstore/source.go`](../../source/objstore/source.go) over the
[`objstore/`](../../objstore) provider abstraction; registered as `s3`, `gcs`, `adls`.

- **Reads:** files matched by a prefix glob, parsed with the inner `format`
- **Partitionable:** ✅ — one partition per object (see [Scaling](../scaling.md))
- **Streaming:** bounded — consumes all matched objects
- **Checkpoint / resume:** per-object; already-processed objects are skipped

## Common configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `prefix` | string | — | ✅ | Object glob, e.g. `data/2024/*.parquet`. |
| `format` | string | — | ✅ | Inner source type: `csv`, `parquet`, `jsonlines`, … Format-specific keys (e.g. `hasHeader`, `delimiter`) are passed through to it. |
| `download` | bool | format-dependent | — | Spool each object to a temp file before parsing. Implied for `parquet`; but when the store supports ranged reads (S3/GCS/ADLS) and the object size is known, `parquet` instead uses range GETs — fetching only the footer and the selected column chunks, with no whole-object download or temp file. Without `download`, `csv` and `jsonlines` objects are streamed straight into the parser (no temp file); set `download: true` to spool them instead (e.g. when parsing is much slower than the network and long-lived object streams would time out). Other formats spool each object, and its temp file is deleted as soon as that object has been read. |

### S3 keys

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `bucket` | string | — | ✅ | S3 bucket. |
| `region` | string | env/default | — | AWS region. |
| `endpoint` | string | — | — | Custom endpoint (MinIO, LocalStack). |
| `forcePathStyle` | bool | `false` | — | Path-style addressing (required for MinIO). |
| `accessKeyIdEnv` / `secretAccessKeyEnv` / `sessionTokenEnv` | string | — | — | **Env var names** holding the credentials. |

### GCS keys

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `bucket` | string | — | ✅ | GCS bucket. |
| `project` | string | from creds | — | GCP project ID. |
| `credentialsFileEnv` | string | — | — | **Env var name** holding the path to the service-account JSON. |

### ADLS keys

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `container` | string | — | ✅ | ADLS Gen2 container. |
| `accountName` | string | env | — | Storage account name. |
| `accountKeyEnv` / `sasTokenEnv` | string | — | — | **Env var names** holding the account key / SAS token. |
| `serviceURL` | string | derived | — | Custom Azure service URL (Azurite emulator). |

## Credential resolution

1. If the provider's `*Env` keys are set, credentials are read from those environment
   variables.
2. Otherwise the cloud SDK's **default chain** is used (AWS: env → `~/.aws` → IMDS/IRSA;
   GCP: `GOOGLE_APPLICATION_CREDENTIALS` / ADC / Workload Identity; Azure: env → managed
   identity). See [Security](../security.md) — no secret is written to the pipeline file.

## Example — S3 (MinIO) reading Parquet

```json
{
  "type": "s3",
  "bucket": "data-lake",
  "prefix": "archive/2024-*.parquet",
  "format": "parquet",
  "region": "us-east-1",
  "endpoint": "http://minio:9000",
  "forcePathStyle": true
}
```

## See also

- [Sources index](../sources.md) · [Object Store sink](../sinks/objstore.md) · [Scaling](../scaling.md) · [Security](../security.md)
