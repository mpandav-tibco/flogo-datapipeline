// types.d.ts — Ambient type definitions for the Flogo Data Pipeline Studio extension.
// These mirror the .datapipeline JSON schema (schemas/datapipeline.schema.json) and
// the webview message protocol between pipelineEditorProvider.js and pipelineCanvas.js.

// ── Pipeline document ───────────────────────────────────────────────────────────

/**
 * Top-level .datapipeline document object.
 * @typedef {Object} PipelineDoc
 * @property {string} name - Unique pipeline name (used as binary/process name).
 * @property {string} [description] - Human-readable description.
 * @property {string} [version] - Config version, e.g. "1.0.0".
 * @property {'etl'|'elt'|'el'} [mode] - Pipeline processing mode (default: 'etl').
 * @property {SourceDef} source - Where the pipeline reads data from.
 * @property {TransformDef[]} [transforms] - Ordered transform stages.
 * @property {SinkDef} sink - Where processed records are written.
 * @property {SinkDef} [dlqSink] - Optional dead-letter queue sink.
 * @property {PerformanceDef} [performance] - Throughput / resource tuning.
 * @property {ScalingDef} [scaling] - Horizontal scaling / sharding config.
 * @property {CheckpointDef} [checkpoint] - Progress checkpointing config.
 */

/**
 * Source connector configuration.
 * @typedef {Object} SourceDef
 * @property {'csv'|'tsv'|'jdbc'|'parquet'|'fixedwidth'|'xml'|'jsonlines'|'kafka'|'objstore'} type
 * @property {string} [path] - File-system path or glob (file-based sources).
 * @property {string} [dsn] - JDBC connection string.
 * @property {string} [query] - SQL SELECT query (jdbc source).
 * @property {string} [table] - Table name when query is not set (jdbc source).
 * @property {string} [delimiter] - Field delimiter for CSV (default: ',').
 * @property {boolean} [hasHeader] - Whether source has a header row.
 * @property {string} [encoding] - Character encoding (default: 'utf-8').
 * @property {string[]} [brokers] - Kafka broker addresses.
 * @property {string} [topic] - Kafka topic name.
 * @property {string} [groupId] - Kafka consumer group ID.
 * @property {string} [bucket] - Object store bucket name.
 * @property {string} [prefix] - Object store key prefix.
 * @property {string} [region] - Cloud region for object store.
 * @property {string} [endpoint] - Custom endpoint URL for S3-compatible stores.
 * @property {Record<string,string>} [schema] - Explicit column schema (name -> type string).
 */

/**
 * Transform stage configuration.
 * @typedef {Object} TransformDef
 * @property {'filter'|'map'|'mapper'|'validate'|'lookup'|'hwm'|'schemadrift'|'dq'} type
 * @property {string} [id] - Stable identifier for metrics/logs.
 * @property {string} [description] - Human-readable description.
 * @property {string} [expr] - Filter/map expression (filter type).
 * @property {Record<string,string>} [fields] - Field mappings: output field -> expression (map/mapper type).
 * @property {Array<{field:string,check:string,onFail?:string}>} [rules] - Validation rules.
 * @property {string} [url] - Lookup service URL (lookup type).
 * @property {string} [keyField] - Lookup join key field (lookup type).
 * @property {string[]} [outputFields] - Fields to copy from lookup row (lookup type).
 * @property {number} [concurrency] - Concurrent lookup goroutines (lookup type).
 * @property {number} [cacheSize] - LRU cache size for lookup results (lookup type).
 * @property {number} [timeoutMs] - Lookup request timeout in ms (lookup type).
 * @property {string} [onError] - Error handling for lookup: 'null'|'skip'|'fail'.
 * @property {string} [column] - High-water mark column name (hwm type).
 * @property {string} [backend] - HWM persistence backend: 'file'|'redis'|'postgres'.
 * @property {Record<string,unknown>} [backendConfig] - HWM backend config (hwm type).
 * @property {'additive'|'strict'|'drop'|'project'} [policy] - Schema drift policy.
 * @property {Array<Record<string,unknown>>} [expectations] - DQ expectations (dq type).
 * @property {number} [threshold] - DQ pass threshold 0–1 (dq type).
 * @property {'abort'|'report'|'pass'} [onFail] - DQ failure action.
 */

/**
 * Sink connector configuration. Also used for DLQ sink.
 * @typedef {Object} SinkDef
 * @property {'jdbc'|'parquet'|'objstore'|'kafka'|'dlq'|'null'|'file'} type
 * @property {string} [dsn] - JDBC connection string.
 * @property {string} [table] - Target table name (jdbc sink).
 * @property {'insert'|'upsert'|'merge'|'replace'} [writeMode] - Write mode (jdbc sink).
 * @property {string[]} [upsertKeys] - Unique key fields for upsert/merge.
 * @property {string} [path] - Output path (parquet/file sink).
 * @property {string[]} [partitionBy] - Partition fields (parquet sink).
 * @property {'none'|'snappy'|'gzip'|'zstd'} [compression] - Compression codec.
 * @property {string} [bucket] - Object store bucket name.
 * @property {string} [prefix] - Object store key prefix.
 * @property {string} [region] - Cloud region.
 * @property {string} [endpoint] - Custom endpoint URL.
 * @property {string[]} [brokers] - Kafka broker addresses.
 * @property {string} [topic] - Kafka topic.
 * @property {string} [keyField] - Kafka message key field.
 * @property {'json'|'avro'|'protobuf'} [format] - Kafka serialization format.
 * @property {number} [batchSize] - Write batch size.
 * @property {'insert'|'upsert'|'replace'|'ignore'} [onConflict] - Conflict handling.
 * @property {string[]|string} [conflictKey] - Conflict key fields.
 */

/**
 * Throughput and resource-tuning parameters.
 * @typedef {Object} PerformanceDef
 * @property {number} [batchSize] - Records per processing batch (default: 1000).
 * @property {number} [maxMemoryMB] - Soft RSS ceiling in MiB (default: 512).
 * @property {number} [transformWorkers] - Parallel transform goroutines (default: 4).
 * @property {number} [sourceBufferBatches] - Source→transform channel depth (default: 8).
 * @property {number} [sinkBufferBatches] - Transform→sink channel depth (default: 8).
 */

/**
 * Horizontal scaling / sharding configuration.
 * @typedef {Object} ScalingDef
 * @property {'single'|'sharded'|'distributed'} [mode] - Scaling strategy (default: 'single').
 * @property {number} [instanceIndex] - Zero-based shard index (sharded mode).
 * @property {number} [instanceCount] - Total parallel instances (sharded mode).
 * @property {CoordinatorDef} [coordinator] - Coordinator config (distributed mode).
 */

/**
 * Distributed-mode coordinator configuration.
 * @typedef {Object} CoordinatorDef
 * @property {'memory'|'postgres'} [type] - Coordinator backend (default: 'memory').
 * @property {string} [dsn] - Connection string for postgres coordinator.
 * @property {number} [leaseSeconds] - Worker lease TTL in seconds (default: 30).
 * @property {number} [heartbeatSeconds] - Heartbeat interval in seconds.
 * @property {number} [maxRetries] - Maximum retry attempts.
 */

/**
 * Progress checkpointing configuration.
 * @typedef {Object} CheckpointDef
 * @property {'none'|'file'|'redis'|'postgres'} [type] - Storage backend (default: 'none').
 * @property {string} [path] - File path for 'file' backend.
 * @property {string} [addr] - Redis server address, e.g. 'localhost:6379'.
 * @property {string} [keyPrefix] - Redis key namespace prefix.
 * @property {string} [dsn] - DSN for 'postgres' backend.
 * @property {number} [intervalRows] - Checkpoint flush every N rows (default: 10000).
 * @property {number} [intervalSeconds] - Checkpoint flush every N seconds (default: 60).
 * @property {boolean} [deleteOnSuccess] - Remove checkpoint on clean exit (default: false).
 */

// ── Webview message protocol ────────────────────────────────────────────────────

/**
 * Messages sent from pipelineEditorProvider.js → pipelineCanvas.js (via postMessage).
 *
 * @typedef {Object} InitMessage
 * @property {'init'} type
 * @property {PipelineDoc} doc - Parsed pipeline document.
 * @property {string} nonce - CSP nonce for this session.
 *
 * @typedef {Object} UpdateMessage
 * @property {'update'} type
 * @property {string} text - Raw JSON text of the updated document.
 *
 * @typedef {Object} RunStartedMessage
 * @property {'runStarted'} type
 * @property {number} port - API port the engine is listening on.
 *
 * @typedef {Object} RunStartingMessage
 * @property {'runStarting'} type
 *
 * @typedef {Object} StoppedMessage
 * @property {'stopped'} type
 *
 * @typedef {Object} ErrorMessage
 * @property {'error'} type
 * @property {string} message - Error description.
 *
 * @typedef {Object} ValidationResultMessage
 * @property {'validationResult'} type
 * @property {ValidationIssue[]} issues
 *
 * @typedef {Object} MetricsResultMessage
 * @property {'metricsResult'} type
 * @property {PipelineMetrics} [data] - Metrics payload on success.
 * @property {string} [error] - Error string on failure.
 *
 * @typedef {Object} RunLogMessage
 * @property {'runLog'} type
 * @property {string} line - One log line appended to the run log.
 *
 * @typedef {Object} RunErrorMessage
 * @property {'runError'} type
 * @property {string} message
 *
 * @typedef {InitMessage|UpdateMessage|RunStartedMessage|RunStartingMessage|StoppedMessage|ErrorMessage|ValidationResultMessage|MetricsResultMessage|RunLogMessage|RunErrorMessage} ProviderToCanvasMessage
 */

/**
 * Messages sent from pipelineCanvas.js → pipelineEditorProvider.js (via vscode.postMessage).
 *
 * @typedef {Object} ReadyMessage
 * @property {'ready'} type
 *
 * @typedef {Object} CanvasUpdateMessage
 * @property {'update'} type
 * @property {string} text - JSON string of the updated document.
 *
 * @typedef {Object} RunMessage
 * @property {'run'} type
 *
 * @typedef {Object} StopMessage
 * @property {'stop'} type
 *
 * @typedef {Object} ValidateMessage
 * @property {'validate'} type
 *
 * @typedef {Object} MetricsPollMessage
 * @property {'metrics'} type
 *
 * @typedef {Object} ExportMessage
 * @property {'export'} type
 * @property {string} format - 'binary'|'dockerfile'|'image'|'platform'
 *
 * @typedef {Object} OpenRawMessage
 * @property {'openRaw'} type
 *
 * @typedef {Object} ShowErrorMessage
 * @property {'showError'} type
 * @property {string} text
 *
 * @typedef {Object} ValidationResultsMessage
 * @property {'validationResults'} type
 * @property {ValidationIssue[]} problems
 *
 * @typedef {Object} CopyTextMessage
 * @property {'copyText'} type
 * @property {string} text
 *
 * @typedef {ReadyMessage|CanvasUpdateMessage|RunMessage|StopMessage|ValidateMessage|MetricsPollMessage|ExportMessage|OpenRawMessage|ShowErrorMessage|ValidationResultsMessage|CopyTextMessage} CanvasToProviderMessage
 */

/**
 * A single validation issue.
 * @typedef {Object} ValidationIssue
 * @property {'error'|'warn'|'info'} sev - Severity.
 * @property {string} [label] - Short field label, e.g. 'source.dsn'.
 * @property {string} msg - Human-readable message.
 * @property {string|null} [anchor] - JSON key to locate in the document.
 */

/**
 * Live pipeline metrics returned by /api/pipeline/status.
 * @typedef {Object} PipelineMetrics
 * @property {number} [rowsProcessed] - Total rows read from source.
 * @property {number} [rowsWritten] - Total rows written to sink.
 * @property {number} [rowsFailed] - Rows routed to DLQ or dropped.
 * @property {number} [durationMs] - Elapsed wall-clock time in ms.
 * @property {number} [memoryRSS] - Current RSS memory in MiB.
 * @property {string} [status] - Engine status string.
 */

/**
 * Engine lifecycle hook callbacks, passed from extension.js to PipelineEditorProvider.
 * @typedef {Object} EngineHooks
 * @property {() => void} [onStarting] - Called just before the engine starts.
 * @property {(port: number, name: string, tools?: unknown[]) => void} [onRunning] - Called once the engine is healthy.
 * @property {() => void} [onStopped] - Called when the engine stops or crashes.
 * @property {(uri: import('vscode').Uri) => void} [onActivePipeline] - Called when the active canvas changes.
 */

export {}
