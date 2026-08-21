// Config-field metadata (labels, enums, widget-selection heuristics) — pure
// data/functions with zero dependency on the webview's live state, extracted
// from webview/main.ts as part of the same incremental module split that
// produced stageCatalog.ts and webviewState.ts.

export const FIELD_LABELS: Record<string, string> = {
    // common
    path: 'File path', dsn: 'Connection string (DSN)', driver: 'Driver', table: 'Table',
    batchSize: 'Batch size', columns: 'Columns', encoding: 'Encoding',
    // csv / tsv
    hasHeader: 'Has header row', delimiter: 'Delimiter', writeHeader: 'Write header row',
    // jdbc source
    query: 'SQL query', orderKey: 'Order-by key', where: 'WHERE clause',
    // fixedwidth
    skipLines: 'Skip lines', recordLength: 'Record length',
    // xml / jsonlines
    recordElement: 'Record element', fields: 'Fields',
    // parquet
    rowGroupParallelism: 'Row-group parallelism', rowGroupSize: 'Row-group size',
    compression: 'Compression',
    // kafka source
    brokers: 'Brokers', topic: 'Topic', groupId: 'Consumer group', startOffset: 'Start offset',
    pollMs: 'Poll interval (ms)', deserializer: 'Deserializer', mode: 'Read mode',
    stopOffset: 'Stop offset', stopTimestamp: 'Stop timestamp', schemaRegistryURL: 'Schema registry URL',
    stopAtCurrentEnd: 'Drain to current end',
    // kafka sink
    ackMode: 'Ack mode', compressionCodec: 'Compression codec', maxMessageBytes: 'Max message bytes',
    partitionKey: 'Partition key', serializer: 'Serializer',
    // objstore
    bucket: 'Bucket', prefix: 'Prefix', format: 'Inner format', download: 'Download locally',
    region: 'Region', key: 'Key template', endpoint: 'Endpoint URL', forcePathStyle: 'Force path-style',
    rollMaxBytes: 'Roll at size (bytes)', rollMaxSeconds: 'Roll at age (seconds)',
    accessKeyIdEnv: 'Access-key env var', secretAccessKeyEnv: 'Secret-key env var',
    // jdbc sink
    onConflict: 'On conflict', conflictKey: 'Conflict key', autoMigrate: 'Auto-migrate table',
    // dlq destination (top-level dlq block + options)
    envelopeColumn: 'Error envelope column', maxRowsPerBatch: 'Max rows per batch', onFull: 'On full',
    // filter / map
    expr: 'Filter expression', fieldOrder: 'Field order',
    // sectioned (Text Document) source
    headerKVRegex: 'Header key/value pattern', bodyColumnMarker: 'Table header line',
    bodyDelimiter: 'Column separator', broadcastHeader: 'Attach header fields to every row',
    formatPreset: 'Format preset',
    // template (Custom Text) sink
    template: 'Template', templatePath: 'Template file', templateSource: 'Template from',
    // validate
    rules: 'Validation rules', onViolation: 'On violation',
    errorSinkType: 'Error sink type', errorSinkConfig: 'Error sink config',
    // hwm
    column: 'Watermark column', valueType: 'Value type',
    // schemadrift
    policy: 'Drift policy', onDrift: 'On drift',
    // dq
    expectations: 'Expectations', onFail: 'On fail', qualityThreshold: 'Quality threshold',
    scoreColumn: 'Score column', reportSinkType: 'Report sink type', reportSinkConfig: 'Report sink config',
    threshold: 'Threshold',
    // lookup
    keyField: 'Key field', url: 'URL', method: 'HTTP method', concurrency: 'Concurrency',
    cacheSize: 'Cache size', timeoutMs: 'Timeout (ms)', onError: 'On error', outputFields: 'Output fields',
    // parse (mode/prefix labels are shared with kafka/objstore above)
    source: 'Source field', pattern: 'Regex pattern', keepSource: 'Keep source field',
    // checkpoint / watermark shared
    addr: 'Address', keyPrefix: 'Key prefix', backend: 'Backend', nullable: 'Nullable',
};
export const FIELD_ENUMS: Record<string, string[]> = {
    driver: ['postgres', 'mysql', 'sqlite'],
    encoding: ['utf-8', 'ebcdic-037', 'latin-1'],
    compression: ['snappy', 'gzip', 'zstd', 'none'],
    // kafka
    startOffset: ['earliest', 'latest'],
    deserializer: ['json', 'avro', 'string'],
    serializer: ['json', 'avro', 'string'],
    mode: ['streaming', 'batch'],
    ackMode: ['none', 'leader', 'all'],
    compressionCodec: ['none', 'gzip', 'snappy', 'lz4', 'zstd'],
    // objstore
    format: ['csv', 'jsonlines', 'parquet'],
    // jdbc sink
    onConflict: ['insert', 'upsert', 'ignore'],
    // validate / dq
    onViolation: ['route', 'drop', 'fail'],
    onFail: ['route', 'fail', 'warn', 'drop'],
    // schemadrift
    policy: ['project', 'additive', 'strict', 'drop'],
    onDrift: ['log', 'alert', 'silent'],
    // hwm
    valueType: ['datetime', 'int', 'string'],
    // lookup
    method: ['GET', 'POST'],
    onError: ['null', 'skip', 'fail'],
    // dlq
    onFull: ['block', 'drop'],
    // watermark
    backend: ['file', 'redis', 'postgres'],
    // rest source/sink
    authType: ['none', 'bearer', 'basic', 'apikey'],
};
// Per-stage-type enum overrides — used when a field name is shared across stage
// types with different allowed values (e.g. `mode` is streaming/batch for kafka
// but regex/json for parse; `onError` is null/skip/fail for lookup but
// keep/drop/route for parse). Keyed by stage type, then field name.
export const FIELD_ENUMS_BY_TYPE: Record<string, Record<string, string[]>> = {
    parse: {
        mode: ['regex', 'json'],
        onError: ['keep', 'drop', 'route'],
    },
    rest: {
        // sink/rest's `mode` (perRow | batch) is unrelated to kafka's streaming/batch.
        mode: ['perRow', 'batch'],
        method: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'],
    },
};

// Config keys that reference an INPUT field name (not free text). For these we
// render an autocomplete input backed by the upstream schema — so users pick
// from the available fields instead of guessing the exact name — while still
// allowing a custom value (self-describing sources, computed names). Keyed by
// stage type, then the set of field-name config keys.
export const FIELD_NAME_KEYS: Record<string, Set<string>> = {
    parse: new Set(['source']),
    hwm: new Set(['column']),
    lookup: new Set(['keyField']),
};
export const FIELD_HINTS: Record<string, string> = {
    path: 'Local path or glob. Supports ${date}/${year} templating.',
    dsn: 'e.g. host=localhost port=5432 dbname=app user=… sslmode=disable',
    expr: "Boolean expression over row fields, e.g. amount > 0 AND status != 'CANCELLED'",
    conflictKey: 'Comma-separated key columns used for upsert matching.',
    table: 'Destination table. Created automatically if Auto-migrate is on.',
    onConflict: 'insert = append rows · upsert = insert or update on the conflict key · ignore = skip duplicates.',
    autoMigrate: 'Create the table if missing and add new columns as they appear (pairs well with additive schema drift).',
    batchSize: 'Rows per INSERT batch. Higher = faster, more memory. 1000–5000 is typical.',
    fields: 'JSON map of output field → expression.',
    query: 'Full SELECT statement. Overrides table/orderKey when set.',
    brokers: 'Comma-separated host:port list, e.g. broker-1:9092, broker-2:9092',
    recordElement: 'XML element that marks one record, e.g. "order".',
    key: 'Object key template, supports ${date}, ${uuid}, ${runId}.',
    endpoint: 'Custom S3-compatible endpoint (MinIO/Ceph), e.g. http://minio:9000. Leave empty for real AWS S3.',
    forcePathStyle: 'Path-style addressing (bucket in the URL path). Required for MinIO and most S3-compatible stores.',
    rollMaxBytes: 'Close & upload the current object once it reaches this many bytes. 0 = one object per run (no size roll).',
    rollMaxSeconds: 'Close & upload the current object after it has been open this many seconds. 0 = no time-based roll.',
    accessKeyIdEnv: 'Name of the env var holding the access key ID (e.g. AWS_ACCESS_KEY_ID). Keeps the secret out of the pipeline file.',
    secretAccessKeyEnv: 'Name of the env var holding the secret access key. Keeps the secret out of the pipeline file.',
    rules: 'JSON array of field rules: [{ "field": "amount", "notNull": true }]',
    expectations: 'JSON array of DQ expectations (expectColumnNotNull, expectValuesInSet, …).',
    url: 'Enrichment endpoint. Use {keyField} as the placeholder, e.g. https://api/enrich/{id} — substituted per row.',
    dataPath: 'Dot-path into the JSON response locating the row array, e.g. "data.items". Empty = the response body itself is the array.',
    bearerTokenEnv: 'Name of the env var holding the bearer token. Never written inline in the pipeline file.',
    basicUserEnv: 'Name of the env var holding the basic-auth username.',
    basicPasswordEnv: 'Name of the env var holding the basic-auth password. Never written inline.',
    apiKeyEnv: 'Name of the env var holding the API key value. Never written inline.',
    apiKeyHeader: 'Header name the API key is sent in (default "X-Api-Key").',
    successStatusCodes: 'JSON array of HTTP status codes treated as success, e.g. [200, 201, 204].',
    pagination: 'JSON pagination config — { "type": "none" | "page" | "offset" | "cursor", … }. See docs/sources/rest.md.',
    headerKVRegex: 'Regex with two capture groups (key, value) matching each header line. Default handles “Key  <2+ spaces>  Value”.',
    bodyColumnMarker: 'Regex identifying the line that names the table columns, e.g. ^MEASUREMENT\\s',
    bodyDelimiter: 'Regex that splits each body row into fields. Default \\s+ handles whitespace-aligned tables.',
    broadcastHeader: 'Stamp every header attribute onto each body row so downstream transforms and the sink see file-level context.',
    template: 'Go text/template. Available: .Rows (each row as a map), .Count, .Columns, .Attr (header fields). Helpers: rpad, lpad, padc, sci, upper, lower, trim, add, seq.',
    templatePath: 'Path to a .tmpl file. Overrides the inline template when set.',
    source: 'Any text or JSON field to structure into columns — a log line, a delimited blob, an embedded JSON string, a composite key, etc. Not just logs (e.g. message).',
    pattern: 'Regex with named groups (?P<name>…); each group becomes a column. Add (?s) to let . span multiple lines (e.g. stitched stack traces).',
    prefix: 'Optional prefix added to each extracted column name (JSON mode), e.g. log_.',
    keepSource: 'Keep the original source field alongside the extracted columns.',
    // podlog (K8s pod-log collector)
    root: 'Directory to tail CRI pod logs from (default /var/log/pods). Point it at a local folder for testing.',
    node: 'Node name stamped onto every record and metrics labels — usually the K8s node this collector runs on.',
    positionFile: 'Crash-safe offset registry file — lets the collector resume where it left off after a restart. Recommended in production.',
    fromBeginning: 'On: read existing log files from the start. Off: tail only new lines (from end of file).',
    bounded: 'On (test/drain): stop with EOF once all logs are read. Off: run as a continuous live collector.',
    pollMs: 'How often (ms) to poll idle files for new lines. Default 500.',
    includeNamespaces: 'JSON array — if non-empty, ONLY these namespaces are tailed, e.g. ["team-a","team-b"]. Empty = all.',
    excludeNamespaces: 'JSON array of namespaces to skip, e.g. ["kube-system","kube-public"].',
};

// Fixed emitted schemas for sources whose columns are NOT declared in the
// .datapipeline JSON (the engine knows them internally). Surfacing them here
// means the map editor's "Available data" palette is populated immediately —
// users see the $fields to map from without first running a preview. Keyed by
// source type.
export const SOURCE_FIXED_SCHEMA: Record<string, Array<{ name: string; type: string }>> = {
    podlog: [
        { name: 'namespace', type: 'string' },
        { name: 'pod', type: 'string' },
        { name: 'container', type: 'string' },
        { name: 'uid', type: 'string' },
        { name: 'stream', type: 'string' },
        { name: 'log_time', type: 'string' },
        { name: 'message', type: 'string' },
        { name: 'node', type: 'string' },
        { name: 'idempotency_key', type: 'string' },
    ],
};

// Columns a `parse` transform appends: regex named-capture groups, or the JSON
// field allow-list (with optional prefix). Empty when unknown (json without an
// explicit fields list), matching the runtime.
export function parseProducedColumns(t: any): string[] {
    if (!t || t.type !== 'parse') return [];
    const out: string[] = [];
    if (String(t.mode || 'regex') === 'json') {
        const prefix = typeof t.prefix === 'string' ? t.prefix : '';
        if (Array.isArray(t.fields)) for (const f of t.fields) out.push(prefix + String(f));
    } else {
        const pat = String(t.pattern || '');
        const re = /\(\?P<([A-Za-z_][A-Za-z0-9_]*)>/g;
        let m: RegExpExecArray | null;
        while ((m = re.exec(pat)) !== null) out.push(m[1]);
    }
    if (String(t.onError) === 'route') out.push('_parse_error');
    return out;
}

export function humanize(k: string): string {
    return FIELD_LABELS[k] || k.replace(/([A-Z])/g, ' $1').replace(/^./, (c) => c.toUpperCase());
}
export function isBoolField(k: string, v: string): boolean {
    return v === 'true' || v === 'false' ||
        /^(has|is|enable|delete|trim|nullable|repeating|download|autoMigrate|caseSensitive|deleteOnSuccess|forcePathStyle)/i.test(k);
}
export function isFileField(k: string): boolean {
    return /(^path$|^root$|_path$|file$|dir$|directory$)/i.test(k);
}
export function isNumField(k: string, v: string): boolean {
    return /(size|port|workers|batches|interval|memory|parallelism|scale|length|start|retries|threshold|rows|seconds|delay|lines|concurrency|ms$|bytes|offset|count|limit|poll)/i.test(k) &&
        v.trim() !== '' && !isNaN(Number(v));
}
export function isJsonField(v: string): boolean {
    const t = v.trim();
    return t.startsWith('{') || t.startsWith('[');
}
