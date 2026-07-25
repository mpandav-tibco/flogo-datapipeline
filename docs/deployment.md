# Deployment

A pipeline deploys as a **single self-contained binary** — no JVM, no cluster, no
runtime to install. There are two ways to produce it, plus Studio one-click targets.

## All-in-one engine

The [`cmd/flogo-datapipeline`](../cmd/flogo-datapipeline) binary has **every**
component compiled in (~58 MB). It runs any `.datapipeline` and is what the examples
and the Studio use by default.

```bash
go build -o flogo-datapipeline ./cmd/flogo-datapipeline
./flogo-datapipeline -f my.datapipeline
```

## Lean per-app binary (`dp-build`)

For production you ship a binary that links **only** the components a specific pipeline
uses — the datapipeline analogue of `flogo build`. [`cmd/dp-build`](../cmd/dp-build)
inspects the `.datapipeline`, derives the exact import set
([`build/imports.go`](../build/imports.go) — sources, sinks, transforms *and* a
transform's own error/report sink, JDBC drivers, checkpoint, watermark, metrics, and
objstore providers), renders a minimal `main.go`, and compiles it.

```bash
go run ./cmd/dp-build -f my.datapipeline -o bin/my-pipeline
./bin/my-pipeline -f my.datapipeline
```

Typical footprint vs the all-in-one engine:

| Deployed pipeline | Binary |
|---|---:|
| `csv → null` | ~7.6 MB |
| `csv → postgres` | ~8.2 MB |
| `csv → s3 (+parquet)` | ~14.0 MB |
| `kafka → parquet` | ~13.6 MB |
| _all-in-one engine_ | ~58 MB |

Binaries are CGO-free (statically linked) **unless** the app uses the SQLite driver,
which requires CGO.

## Container

A distroless image over the prebuilt Linux binary is enough:

```dockerfile
FROM gcr.io/distroless/static
COPY my-pipeline /my-pipeline
COPY my.datapipeline /my.datapipeline
EXPOSE 9080
ENTRYPOINT ["/my-pipeline","-f","/my.datapipeline","-port","9080"]
```

Cross-compile with `GOOS=linux GOARCH=amd64` (or `arm64`). Publish the API port only
if you need the [REST API](rest-api.md).

## Kubernetes

- **Batch / scheduled** — a `Job` or `CronJob` running the binary once; use
  [checkpointing](reliability.md) so a restarted pod resumes.
- **Sharded** — a `StatefulSet` of N replicas; the pod ordinal supplies
  `INSTANCE_INDEX` and the replica count `INSTANCE_COUNT` for
  [sharded scaling](scaling.md). Point checkpoint/watermark at `redis` / `postgres`.
- **Streaming** — a `Deployment` driven by a Kafka trigger or the startup hook, with a
  `prometheus` metrics endpoint scraped by a `ServiceMonitor`.

## Secrets

Never bake secrets into the image or the `.datapipeline`. Provide them as environment
variables named by the config (`passwordEnv`, `saslPasswordEnv`, `apiKeyEnv`, …) and
sourced from a `Secret`. See [Security](security.md).

## Studio build targets

The Studio's **Build** command produces, from the current pipeline, any of:

- a native **binary**,
- a **Dockerfile**,
- a container **image**, or
- a **TIBCO Platform bundle** (Dockerfile + sharded StatefulSet + README).

See [Studio](studio.md).

## See also

- [Scaling](scaling.md) · [Reliability](reliability.md) · [Observability](observability.md)
- [`examples/19-full-production`](../examples/19-full-production)
