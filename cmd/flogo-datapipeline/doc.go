// Package main is the flogo-datapipeline reference binary.
//
// # Registered Type Inventory
//
// Every type listed here is activated by a blank import in main.go.
// Each sub-package's init() function calls the relevant registry's Register()
// so the type is available to any pipeline config that references it by name.
//
// ## Sources
//
//   - csv        — delimited text, optional header, configurable separator
//   - jdbc       — SQL query with keyset-pagination chunking (PostgreSQL, MySQL, SQL Server, Oracle)
//   - parquet    — Parquet file with row-group parallelism and column/predicate pushdown
//   - fixedwidth — positional flat files (mainframe / EBCDIC / ASCII, implied decimal)
//   - xml        — SAX-style streaming XML; one record element per row
//   - jsonlines  — NDJSON / JSON-lines; one JSON object per line
//   - s3         — AWS S3 (and S3-compatible) object-store source (via source/objstore)
//   - gcs        — Google Cloud Storage source (via source/objstore)
//   - adls       — Azure Data Lake Storage Gen2 source (via source/objstore)
//   - kafka      — Kafka consumer with consumer-group offset checkpointing
//
// ## Sinks
//
//   - jdbc      — bulk JDBC insert / upsert with configurable conflict resolution
//   - null      — discard all rows (benchmarking / dry-run)
//   - parquet   — row-group-aligned Parquet file; each Flush() closes the current row group
//   - s3-sink   — AWS S3 sink (via sink/objstore); key supports date macros {YYYY}/{MM}/{DD}
//   - gcs-sink  — Google Cloud Storage sink (via sink/objstore)
//   - adls-sink — Azure Data Lake Storage Gen2 sink (via sink/objstore)
//   - kafka     — Kafka producer with exactly-once semantics via transactions
//   - dlq       — dead-letter queue sink; routes bad rows from the validate transform
//
// ## Transforms
//
//   - filter     — row-level boolean expression; non-matching rows are dropped
//   - map        — field projection, rename, type-cast, and expression-derived columns
//   - lookup     — enrichment via LRU-cached Flogo flow or HTTP call; adds columns to each row
//   - validate   — data-quality rule engine; violations are routed to a DLQ sink (requires sink/dlq)
//   - schemadrift — detects added/removed/type-changed columns at runtime; configurable remediation
//   - hwm        — high-water-mark observer; tracks max value of a column across batches (passthrough)
//
// ## Checkpoints
//
//   - file  — local-filesystem checkpoint (default; zero external dependencies)
//   - redis — Redis hash-backed checkpoint; safe for multi-instance / distributed pipelines
//
// ## Watermarks
//
//   - file     — local-filesystem watermark (built-in default)
//   - redis    — Redis string-backed watermark; survives process restarts
//   - postgres — PostgreSQL upsert-backed watermark (flogo_pipeline_watermarks table, auto-created)
//
// ## Metrics Backends
//
//   - prometheus — exposes a /metrics HTTP scrape endpoint (Prometheus pull model)
//   - otel       — pushes metrics via OpenTelemetry OTLP gRPC to a configured collector
//
// ## ObjStore Providers (shared by source/objstore and sink/objstore)
//
//   - s3   — AWS S3 via aws-sdk-go-v2; supports IAM roles, static credentials, endpoint override
//   - gcs  — Google Cloud Storage via cloud.google.com/go/storage; ADC or service-account JSON
//   - adls — Azure Data Lake Storage Gen2 via azure-sdk-for-go azblob; MSI, SAS, or account key
package main
