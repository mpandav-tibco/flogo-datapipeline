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
- [Graph (DAG) execution](graph.md) — conditional routing and multi-source / multi-sink topologies
- [Configuration](configuration.md) — CLI flags, environment variables, performance tuning

## Build pipelines

- [Sources](sources.md) — 12 source families (CSV, Parquet, Avro, JDBC, Kafka, XML, object store, REST/API, …)
- [Sinks](sinks.md) — 9 sink families plus the Dead-Letter Queue
- [Transforms](transforms.md) — filter, map, validate, lookup, parse, DQ, schema drift, HWM, cast, explode, flatten, router/split, join, union, and event-time windowing — plus the expression language
- [Connections](connections.md) — reusable `*.fgconn` credentials with secrets in Secret Storage
- [Go SDK](sdk.md) — pipeline-as-code with the fluent builder

## Reliability & scale

- [Reliability & incremental load](reliability.md) — checkpoint/resume and high-water-mark watermarks
- [Scaling & coordination](scaling.md) — single / sharded / distributed execution
- [Data quality](data-quality.md) — validation rules, DQ expectations, schema drift, and DLQ routing

## Operate

- [REST API](rest-api.md) — `/v1` management and introspection endpoints
- [Observability](observability.md) — Prometheus / OpenTelemetry metrics, the governance audit log, and OpenLineage lineage
- [Security](security.md) — secrets, TLS, SSRF protection, and no-secrets-on-disk
- [Deployment](deployment.md) — lean per-app binaries (`dp-build`), Docker, and Kubernetes
- [Studio (VS Code)](studio.md) — the visual canvas, run/preview, and build & deploy

## Reference

- Per-activity pages — one page per source, transform, and sink, with full configuration
  keys, behavior, and runnable examples:
  [`docs/sources/`](sources/) · [`docs/transforms/`](transforms/) · [`docs/sinks/`](sinks/)
- [Feature matrix](FEATURES.md) — the one-screen catalog linking every source, sink,
  transform, and engine subsystem to its detailed page

Every feature has a runnable demo under [`examples/`](../examples/).
