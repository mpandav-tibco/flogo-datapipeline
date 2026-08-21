// Stage catalog — the single source of truth for every source/sink/transform
// type the Studio knows about (label, icon, default config, and for
// transforms the canvas summary + fan-in/fan-out arity badge), plus the dq
// transform's expectation-type catalog. Mirrors TRIGGER_CATALOG's pattern
// (model/triggers.ts): the palette, type dropdowns, node subtitles and
// default-config population all derive from this ONE array — add a backend
// type by adding one entry here instead of touching five separate lists (the
// recurring drift bug this replaces; see the "PALETTE vs RENDER/BACKEND
// MISMATCH" / "OBJECT-STORE blank-panel" class of bugs webview/main.ts used
// to be prone to). Verified against the engine's actual registrations via
// studio/vscode/scripts/check-stage-parity.mjs, which parses this file.

export interface StageTypeDef {
    kind: 'source' | 'sink' | 'transform';
    type: string; // engine id — must match the backend's registered type string
    label: string; // short display name (breadcrumb / type dropdown / sidebar palette)
    description?: string; // longer palette description (transform insert quick-pick); defaults to label
    icon: string; // single glyph for the canvas/palette
    connectionFamily?: string; // 'db' | 'stream' | 'obj' — omit if not connection-bindable
    arity?: string; // fan-in/fan-out badge, transforms only (omit for the 1→1 majority)
    defaultConfig: Record<string, unknown>; // starter config on add/switch — deep-cloned at each use site
    summary?: (cfg: any) => string; // transform node subtitle; source/sink share a generic one (see metaFor)
}
export function plural(n: number, w: string): string {
    return `${n} ${w}${n === 1 ? '' : 's'}`;
}
export const STAGE_CATALOG: StageTypeDef[] = [
    // ── Sources ────────────────────────────────────────────────────────────
    { kind: 'source', type: 'csv', label: 'csv', icon: 'C', defaultConfig: { path: './data/input.csv', hasHeader: true, delimiter: ',', columns: [] } },
    { kind: 'source', type: 'tsv', label: 'tsv', icon: 'T', defaultConfig: { path: './data/input.tsv', hasHeader: true, columns: [] } },
    { kind: 'source', type: 'fixedwidth', label: 'fixedwidth', icon: '▦', defaultConfig: { path: './data/input.dat', encoding: 'utf-8', recordLength: 0, columns: [] } },
    {
        kind: 'source', type: 'sectioned', label: 'Text Document', icon: '▦', defaultConfig: {
            path: './data/input.txt', headerKVRegex: '^(\\S[\\S ]*?)\\s{2,}(.*?)\\s*$', bodyColumnMarker: '^COLUMNS\\s',
            bodyDelimiter: '\\s+', broadcastHeader: true, columns: [],
        },
    },
    { kind: 'source', type: 'parquet', label: 'parquet', icon: '◱', defaultConfig: { path: './data/input.parquet', rowGroupParallelism: 4 } },
    { kind: 'source', type: 'avro', label: 'avro', icon: '◈', defaultConfig: { path: './data/input.avro' } },
    { kind: 'source', type: 'xml', label: 'xml', icon: '⟨', defaultConfig: { path: './data/input.xml', recordElement: 'record' } },
    { kind: 'source', type: 'jsonlines', label: 'jsonlines', icon: '⌗', defaultConfig: { path: './data/input.jsonl' } },
    { kind: 'source', type: 'jdbc', label: 'jdbc', icon: 'J', connectionFamily: 'db', defaultConfig: { driver: 'postgres', dsn: '', table: '', where: '', orderKey: '', batchSize: 5000 } },
    { kind: 'source', type: 'kafka', label: 'kafka', icon: 'K', connectionFamily: 'stream', defaultConfig: { brokers: '', topic: '', groupId: 'dp-consumer', startOffset: 'earliest', deserializer: 'json', mode: 'batch', stopAtCurrentEnd: true } },
    { kind: 'source', type: 'rest', label: 'rest', icon: '⇄', defaultConfig: { url: 'https://api.example.com/v1/orders', method: 'GET', authType: 'none', headers: {}, queryParams: {}, dataPath: '', pagination: { type: 'none' }, timeoutMs: 30000, maxRetries: 3, retryBackoffMs: 500 } },
    { kind: 'source', type: 's3', label: 's3', icon: '◇', connectionFamily: 'obj', defaultConfig: { bucket: '', prefix: '', region: 'us-east-1', format: 'csv' } },
    { kind: 'source', type: 'gcs', label: 'gcs', icon: '◇', connectionFamily: 'obj', defaultConfig: { bucket: '', prefix: '', format: 'csv' } },
    { kind: 'source', type: 'adls', label: 'adls', icon: '◇', connectionFamily: 'obj', defaultConfig: { bucket: '', prefix: '', format: 'csv' } },
    {
        kind: 'source', type: 'podlog', label: 'Pod Logs (K8s)', icon: '☰', defaultConfig: {
            root: '/var/log/pods', node: '', positionFile: './out/positions.json', fromBeginning: false, bounded: false,
            pollMs: 500, includeNamespaces: [], excludeNamespaces: ['kube-system'],
            multiline: { startPattern: '', flushIdleMs: 2000, maxLines: 500 },
        },
    },

    // ── Sinks ──────────────────────────────────────────────────────────────
    { kind: 'sink', type: 'jdbc', label: 'jdbc', icon: 'J', connectionFamily: 'db', defaultConfig: { driver: 'postgres', dsn: '', table: '', batchSize: 1000, onConflict: 'insert', autoMigrate: true } },
    { kind: 'sink', type: 'parquet', label: 'parquet', icon: '◱', defaultConfig: { path: './out/output.parquet', compression: 'snappy', rowGroupSize: 100000 } },
    { kind: 'sink', type: 'csv', label: 'csv', icon: 'C', defaultConfig: { path: './out/output.csv', delimiter: ',', writeHeader: true } },
    { kind: 'sink', type: 'tsv', label: 'tsv', icon: 'T', defaultConfig: { path: './out/output.tsv', writeHeader: true } },
    { kind: 'sink', type: 'jsonlines', label: 'jsonlines', icon: '⌗', defaultConfig: { path: './out/output.jsonl' } },
    { kind: 'sink', type: 'avro', label: 'avro', icon: '◈', defaultConfig: { path: './out/output.avro', compression: 'snappy', recordName: 'Row' } },
    { kind: 'sink', type: 'template', label: 'Custom Text', icon: '❏', defaultConfig: { path: './out/{{index .Attr "id"}}.txt', template: '{{range .Rows}}{{.}}\n{{end}}' } },
    { kind: 'sink', type: 'kafka', label: 'kafka', icon: 'K', connectionFamily: 'stream', defaultConfig: { brokers: '', topic: '', ackMode: 'all', serializer: 'json' } },
    { kind: 'sink', type: 'rest', label: 'rest', icon: '⇄', defaultConfig: { url: 'https://api.example.com/v1/customers/{id}', method: 'POST', authType: 'none', headers: {}, mode: 'perRow', timeoutMs: 30000, maxRetries: 3, retryBackoffMs: 500, successStatusCodes: [200, 201, 202, 204] } },
    {
        kind: 'sink', type: 's3', label: 's3', icon: '◇', connectionFamily: 'obj', defaultConfig: {
            bucket: '', key: '${date}/part-${uuid}.parquet', format: 'parquet', region: 'us-east-1', endpoint: '',
            forcePathStyle: false, accessKeyIdEnv: '', secretAccessKeyEnv: '', rollMaxBytes: 0, rollMaxSeconds: 0,
        },
    },
    { kind: 'sink', type: 'gcs', label: 'gcs', icon: '◇', connectionFamily: 'obj', defaultConfig: { bucket: '', key: '${date}/part-${uuid}.parquet', format: 'parquet' } },
    { kind: 'sink', type: 'adls', label: 'adls', icon: '◇', connectionFamily: 'obj', defaultConfig: { bucket: '', key: '${date}/part-${uuid}.parquet', format: 'parquet' } },
    { kind: 'sink', type: 'null', label: 'Null (discard)', icon: '∅', defaultConfig: {} },

    // ── Transforms ─────────────────────────────────────────────────────────
    {
        kind: 'transform', type: 'filter', label: 'Filter', icon: '▽', description: 'Filter — drop rows by a boolean expression',
        defaultConfig: { expr: "amount > 0 AND status != 'CANCELLED'" },
        summary: (cfg) => cfg.expr ? String(cfg.expr).slice(0, 30) : 'filter',
    },
    {
        kind: 'transform', type: 'map', label: 'Map', icon: 'ƒ', description: 'Map — project, rename, coerce & derive fields',
        defaultConfig: { fields: { net_amount: '=multiply($amount, 1.1)' } },
        summary: (cfg) => plural(cfg.fields && typeof cfg.fields === 'object' ? Object.keys(cfg.fields).length : 0, 'field'),
    },
    {
        kind: 'transform', type: 'validate', label: 'Validate', icon: '✓', arity: '1→2', description: 'Validate — per-field rules, route to DLQ',
        defaultConfig: { rules: [{ field: 'amount', notNull: true }], onViolation: 'route' },
        summary: (cfg) => plural(Array.isArray(cfg.rules) ? cfg.rules.length : 0, 'rule') + (cfg.onViolation ? ' · ' + cfg.onViolation : ''),
    },
    {
        kind: 'transform', type: 'lookup', label: 'Lookup', icon: '⌕', description: 'Lookup — HTTP enrichment with LRU cache',
        defaultConfig: { keyField: 'id', url: 'https://api.internal/enrich/{id}', method: 'GET', cacheSize: 10000, onError: 'null' },
        summary: (cfg) => {
            const host = cfg.url ? String(cfg.url).replace(/^https?:\/\//, '').split('/')[0] : '';
            return (cfg.keyField || 'key') + (host ? ' → ' + host : '');
        },
    },
    {
        kind: 'transform', type: 'hwm', label: 'High-Water Mark', icon: '⇡', description: 'High-water mark — incremental cursor',
        defaultConfig: { column: 'updated_at', valueType: 'datetime' },
        summary: (cfg) => cfg.column ? 'hwm ' + cfg.column : 'high-water mark',
    },
    {
        kind: 'transform', type: 'dq', label: 'Data Quality', icon: '◉', arity: '1→2', description: 'Data quality — expectations + quality score',
        defaultConfig: { expectations: [{ type: 'expectColumnNotNull', column: 'id' }], onFail: 'warn', qualityThreshold: 0.95 },
        summary: (cfg) => plural(Array.isArray(cfg.expectations) ? cfg.expectations.length : 0, 'expectation') + (cfg.onFail ? ' · ' + cfg.onFail : ''),
    },
    {
        kind: 'transform', type: 'schemadrift', label: 'Schema Drift', icon: '⟳', description: 'Schema drift — detect column add/remove/type',
        defaultConfig: { policy: 'additive', onDrift: 'log' },
        summary: (cfg) => `${cfg.policy || 'additive'}` + (cfg.onDrift ? ' · ' + cfg.onDrift : ''),
    },
    {
        kind: 'transform', type: 'parse', label: 'Parse', icon: '❮', description: 'Parse — structure a text field into columns (regex/JSON)',
        defaultConfig: { source: 'text', mode: 'regex', pattern: '^(?P<key>\\w+)=(?P<value>.*)$', onError: 'keep', keepSource: true },
        summary: (cfg) => `${cfg.mode || 'regex'}` + (cfg.source ? ' · ' + cfg.source : ''),
    },
    {
        kind: 'transform', type: 'window', label: 'Window (aggregate)', icon: '⧖', description: 'Window — event-time aggregation (tumbling/sliding/session)',
        defaultConfig: { window: { kind: 'tumbling', sizeMs: 60000 }, eventTime: { column: 'ts', format: 'RFC3339' }, groupBy: [], aggregations: [{ as: 'n', fn: 'count' }], watermark: { strategy: 'boundedOutOfOrderness', maxLatenessMs: 0 } },
        summary: (cfg) => {
            const wk = (cfg.window && cfg.window.kind) || 'tumbling';
            return `${wk} · ` + plural(Array.isArray(cfg.aggregations) ? cfg.aggregations.length : 0, 'agg');
        },
    },
    {
        kind: 'transform', type: 'join', label: 'Join', icon: '⨝', arity: '2→1', description: 'Join — keyed join against a reference source (stream-table)',
        defaultConfig: { leftKey: 'id', rightKey: 'id', joinType: 'inner', right: { type: 'csv', path: 'reference.csv', hasHeader: true } },
        summary: (cfg) => `${cfg.joinType || 'inner'} · ${cfg.leftKey || 'key'} = ${cfg.rightKey || 'key'}`,
    },
    {
        kind: 'transform', type: 'union', label: 'Union', icon: '⋃', arity: 'N→1', description: 'Union — append bounded same-schema sources (UNION ALL)',
        defaultConfig: { with: [{ type: 'csv', path: 'more-orders.csv', hasHeader: true }] },
        summary: (cfg) => 'union +' + (Array.isArray(cfg.with) ? cfg.with.length : 0),
    },
    {
        kind: 'transform', type: 'cast', label: 'Cast', icon: '⇄', description: 'Cast — convert column types (int / float / decimal / date / …)',
        defaultConfig: { columns: [{ name: 'amount', type: 'decimal' }], onError: 'null' },
        summary: (cfg) => plural(Array.isArray(cfg.columns) ? cfg.columns.length : 0, 'column'),
    },
    {
        kind: 'transform', type: 'explode', label: 'Explode', icon: '✶', arity: '1→N', description: 'Explode — expand an array column into one row per element',
        defaultConfig: { column: 'items' },
        summary: (cfg) => cfg.column ? 'explode ' + cfg.column : 'explode',
    },
    {
        kind: 'transform', type: 'flatten', label: 'Flatten', icon: '≡', description: 'Flatten — nested-object columns into dotted columns',
        defaultConfig: { separator: '.' },
        summary: (cfg) => 'flatten' + (cfg.separator ? ' · "' + cfg.separator + '"' : ''),
    },
    {
        kind: 'transform', type: 'router', label: 'Router / Split', icon: '⋔', arity: '1→N', description: 'Router / Split — route rows to sinks by predicate',
        defaultConfig: { routes: [{ name: 'high', when: 'amount > 1000', sinkType: 'jsonlines', sinkConfig: { path: './out/high.jsonl' } }], onUnmatched: 'pass' },
        summary: (cfg) => plural(Array.isArray(cfg.routes) ? cfg.routes.length : 0, 'route') + ' · first-match',
    },
    {
        kind: 'transform', type: 'broadcast', label: 'Broadcast (fan-out)', icon: '⇉', description: 'Broadcast — fan out every row to N extra sinks (tee)',
        defaultConfig: { sinks: [{ name: 'archive', sinkType: 'jsonlines', sinkConfig: { path: './out/archive.jsonl' } }], passthrough: true },
        summary: (cfg) => {
            const n = Array.isArray(cfg.sinks) ? cfg.sinks.length : 0;
            return plural(n, 'sink') + (cfg.passthrough === false ? ' · terminal' : '');
        },
    },
];
// Legacy/alternate type-string aliases the ENGINE actually registers (verified
// against every source.Register/sink.Register/transform.Register call site —
// see studio/vscode/scripts/check-stage-parity.mjs) but that don't get their
// own STAGE_CATALOG/palette entry, either because they're a pure backward-
// compat synonym (tee/enrich/split/mapper/discard/ndjson/flatfile) or a second
// spelling of the exact same registration (s3-sink/gcs-sink/adls-sink — the
// objstore sink registers both the bare provider name and a "-sink" suffixed
// alias). label/icon/summary lookups resolve through here so a pipeline file
// that happens to use one of these still renders sensibly instead of falling
// back to the raw, unfriendly type string.
export const STAGE_TYPE_ALIAS: Record<'source' | 'sink' | 'transform', Record<string, string>> = {
    source: { flatfile: 'fixedwidth', ndjson: 'jsonlines' },
    sink: { ndjson: 'jsonlines', discard: 'null', 's3-sink': 's3', 'gcs-sink': 'gcs', 'adls-sink': 'adls' },
    transform: { split: 'router', mapper: 'map', tee: 'broadcast', enrich: 'lookup' },
};
export function stageEntry(kind: 'source' | 'sink' | 'transform', type: string): StageTypeDef | undefined {
    const t = STAGE_TYPE_ALIAS[kind][type] || type;
    return STAGE_CATALOG.find((s) => s.kind === kind && s.type === t);
}
export const SOURCE_TYPES = STAGE_CATALOG.filter((s) => s.kind === 'source').map((s) => s.type);
export const SINK_TYPES = STAGE_CATALOG.filter((s) => s.kind === 'sink').map((s) => s.type);
export const TRANSFORMS = STAGE_CATALOG.filter((s) => s.kind === 'transform')
    .map((s) => ({ type: s.type, label: s.description || s.label, cfg: s.defaultConfig }));
export function typeLabel(t: string): string {
    return stageEntry('source', t)?.label || stageEntry('sink', t)?.label || t;
}
export function transformLabel(t: string): string {
    return stageEntry('transform', t)?.label || t;
}
export function nodeIcon(kind: string, type: string): string {
    if (kind === 'source') return stageEntry('source', type)?.icon || '⇥';
    if (kind === 'sink') return stageEntry('sink', type)?.icon || '⇤';
    return stageEntry('transform', type)?.icon || 'ƒ';
}
// Fan-in / fan-out hint shown in the palette (blank for the 1→1 majority).
export const TF_ARITY: Record<string, string> = {};
for (const s of STAGE_CATALOG) if (s.kind === 'transform' && s.arity) TF_ARITY[s.type] = s.arity;
// 'objstore' is a legacy generic type string (pre-dating the s3/gcs/adls split)
// that never got its own STAGE_CATALOG entry — kept here for backward compat.
export const CONN_FAMILY_BY_TYPE: Record<string, string> = { objstore: 'obj' };
for (const s of STAGE_CATALOG) if (s.connectionFamily) CONN_FAMILY_BY_TYPE[s.type] = s.connectionFamily;

// Map a source/sink stage type to a connection family (see CONN_FAMILY_BY_TYPE
// above) so the inspector offers only compatible connections.
export function familyOfType(type: string): string | null {
    // Note: the `lookup` transform reads a full templated `url`, which a base-URL
    // connection can't supply, so it is intentionally not connection-bindable
    // (no STAGE_CATALOG entry sets connectionFamily for it).
    if (type === 'http') return 'http';
    return CONN_FAMILY_BY_TYPE[type] || null;
}
export function familyLabel(fam: string): string {
    return ({ db: 'database', stream: 'Kafka', obj: 'object-store', http: 'HTTP' } as Record<string, string>)[fam] || fam;
}

// Starter config for a freshly chosen source/sink type — matches the keys each
// backend reads, so a newly switched stage is immediately runnable/editable.
// Deep-cloned so multiple stages of the same type never share nested objects.
export function defaultConfigFor(kind: 'source' | 'sink', type: string): Record<string, unknown> {
    const entry = stageEntry(kind, type);
    return entry ? JSON.parse(JSON.stringify(entry.defaultConfig)) : {};
}

// Catalogue of DQ expectation types with their parameter shapes, mirroring the
// engine's expectationFactories (transform/dq/expectation.go). Shared by the
// dq transform's own rule builder and the pipeline-root contract's
// expectations editor so the two can never drift apart from each other (they
// can still each drift from the engine — that's what check-stage-parity.mjs
// is for).
export interface DQExpectationDef {
    type: string;
    label: string;
    params: Array<{ key: string; label: string; kind: 'number' | 'text' | 'list' | 'bool' }>;
}
export const DQ_EXPECTATIONS: DQExpectationDef[] = [
    { type: 'expectColumnNotNull', label: 'Column not null', params: [{ key: 'threshold', label: 'Threshold', kind: 'number' }] },
    { type: 'expectColumnUnique', label: 'Column unique', params: [{ key: 'threshold', label: 'Threshold', kind: 'number' }] },
    { type: 'expectColumnValuesBetween', label: 'Values between', params: [{ key: 'min', label: 'Min', kind: 'number' }, { key: 'max', label: 'Max', kind: 'number' }] },
    { type: 'expectColumnValuesInSet', label: 'Values in set', params: [{ key: 'values', label: 'Allowed', kind: 'list' }, { key: 'caseSensitive', label: 'Case-sensitive', kind: 'bool' }] },
    { type: 'expectTableRowCountBetween', label: 'Row count between', params: [{ key: 'min', label: 'Min', kind: 'number' }, { key: 'max', label: 'Max', kind: 'number' }] },
    { type: 'expectColumnMeanBetween', label: 'Mean between', params: [{ key: 'min', label: 'Min', kind: 'number' }, { key: 'max', label: 'Max', kind: 'number' }] },
    { type: 'expectColumnStddevBelow', label: 'Std-dev below', params: [{ key: 'threshold', label: 'Threshold', kind: 'number' }] },
    { type: 'expectColumnValuesInReferenceDataset', label: 'Referential integrity (foreign key)', params: [{ key: 'referenceColumn', label: 'Reference column', kind: 'text' }, { key: 'threshold', label: 'Threshold', kind: 'number' }] },
];
export const DQ_NEEDS_COLUMN = new Set(['expectColumnNotNull', 'expectColumnUnique', 'expectColumnValuesBetween', 'expectColumnValuesInSet', 'expectColumnMeanBetween', 'expectColumnStddevBelow', 'expectColumnValuesInReferenceDataset']);
