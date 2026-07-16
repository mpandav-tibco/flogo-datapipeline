# Flogo Data Pipeline — Documentation

Reference for the **flogo-datapipeline** engine, the `.datapipeline` file format,
and the VS Code Studio. New here? Start with [Getting started](getting-started.md),
then read the [Architecture](ARCHITECTURE.md) and the [Pipeline file](pipeline-file.md).

Flogo Data Pipeline is a lightweight, embeddable **ETL / ELT / EL** engine built on
the [Project Flogo](https://github.com/project-flogo/core) runtime. A pipeline is a
single Flogo app JSON that streams `Source → [transforms] → Sink` (with an optional
dead-letter branch) as one self-contained binary — no JVM, no cluster, no SaaS.

## Start here

- [Getting started](getting-started.md) — install, build, and run your first pipeline
- [Architecture](ARCHITECTURE.md) — the engine model, registries, and streaming execution
- [Pipeline file](pipeline-file.md) — the `.datapipeline` schema, section by section
- [Configuration](configuration.md) — CLI flags, environment variables, performance tuning

## Build pipelines

- [Sources](sources.md) — 10 source families (CSV, Parquet, JDBC, Kafka, XML, object store, …)
- [Sinks](sinks.md) — 8 sink families plus the Dead-Letter Queue
- [Transforms](transforms.md) — filter, map, validate, lookup, parse, DQ, schema drift, HWM — and the expression language
- [Connections](connections.md) — reusable `*.fgconn` credentials with secrets in Secret Storage
- [Go SDK](sdk.md) — pipeline-as-code with the fluent builder

## Reliability & scale

- [Reliability & incremental load](reliability.md) — checkpoint/resume and high-water-mark watermarks
- [Scaling & coordination](scaling.md) — single / sharded / distributed execution
- [Data quality](data-quality.md) — validation rules, DQ expectations, schema drift, and DLQ routing

## Operate

- [REST API](rest-api.md) — `/v1` management and introspection endpoints
- [Observability](observability.md) — Prometheus and OpenTelemetry metrics
- [Security](security.md) — secrets, TLS, SSRF protection, and no-secrets-on-disk
- [Deployment](deployment.md) — lean per-app binaries (`dp-build`), Docker, and Kubernetes
- [Studio (VS Code)](studio.md) — the visual canvas, run/preview, and build & deploy

## Reference

- [Feature reference](FEATURES.md) — the exhaustive, code-level catalog of every registered
  source, sink, transform, backend, and configuration key

Every feature has a runnable demo under [`examples/`](../examples/).
