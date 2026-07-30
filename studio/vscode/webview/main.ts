// Flogo Data Pipeline Studio — webview app.
// Renders the active .datapipeline document as an interactive canvas with an
// inspector, bottom panel, run report, DLQ / schema-drift / live-tail views,
// a command palette and right-click menus. All persistence + engine actions
// are delegated to the extension host over postMessage.

import type { PipelineDoc } from '../src/model/pipeline';
import type { HostToWebview, WebviewToHost } from '../src/protocol';
import type { RunResponse, PreviewResult, StatusResponse, StageTestResult } from '../src/engine/apiClient';
import { TRIGGER_CATALOG, triggerDef } from '../src/model/triggers';
import { deriveBranches, branchesByOwner, deriveGraph, type DerivedBranch } from '../src/model/graph';
interface VsCodeApi {
  postMessage(msg: WebviewToHost): void;
  getState(): unknown;
  setState(s: unknown): void;
}
declare function acquireVsCodeApi(): VsCodeApi;
const vscode = acquireVsCodeApi();

// ── tiny DOM helpers ──────────────────────────────────────────────────────
const $ = (s: string, r: ParentNode = document): HTMLElement | null => r.querySelector(s);
function el(tag: string, cls?: string, html?: string): HTMLElement {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (html != null) e.innerHTML = html;
  return e;
}
const fmt = (n: number) => n.toLocaleString('en-US');
const fmtK = (n: number) => (n >= 1e6 ? (n / 1e6).toFixed(2) + 'M' : n >= 1e3 ? Math.round(n / 1e3) + 'k' : String(Math.round(n)));

function post(msg: WebviewToHost) {
  vscode.postMessage(msg);
}

// Make an inspector panel resizable: applies the remembered width and adds a
// left-edge drag grip that updates S.inspWidth live (clamped to a sane range).
function attachResize(insp: HTMLElement): void {
  insp.style.width = (S.inspWidth || 300) + 'px';
  const grip = el('div', 'inspgrip');
  grip.title = 'Drag to resize panel';
  grip.onmousedown = (e: MouseEvent) => {
    e.preventDefault();
    const startX = e.clientX;
    const startW = insp.getBoundingClientRect().width;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    const move = (ev: MouseEvent) => {
      const w = Math.max(260, Math.min(760, startW + (startX - ev.clientX)));
      insp.style.width = w + 'px';
      S.inspWidth = w;
    };
    const up = () => {
      document.removeEventListener('mousemove', move);
      document.removeEventListener('mouseup', up);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };
    document.addEventListener('mousemove', move);
    document.addEventListener('mouseup', up);
  };
  insp.appendChild(grip);
}

// ── node model ────────────────────────────────────────────────────────────
interface Node {
  id: string;
  kind: 'source' | 'transform' | 'sink' | 'dlq';
  type: string;
  nm: string;
  meta: string;
  cfg: Record<string, string>;
  ref: 'source' | 'sink' | 'dlq' | number; // where this maps back into the doc
}

interface Pipe {
  name: string;
  description: string;
  mode: string;
  status: string;
  nodes: Node[];
  branches: DerivedBranch[];
  hasDlq: boolean;
  dlqNode: Node | null;
}

// ── state ─────────────────────────────────────────────────────────────────
const S = {
  fileName: 'pipeline.datapipeline',
  ref: '',
  doc: {} as PipelineDoc,
  pipe: null as Pipe | null,
  selNode: null as string | null,
  inspTab: 'Config',
  inspClosed: false,
  inspWidth: 300,
  showBranches: true,
  canvasMode: 'linear' as 'linear' | 'graph',
  setOpen: null as Record<string, boolean> | null,
  tabs: [{ id: 'canvas', type: 'canvas' as string }],
  active: 'canvas',
  running: false,
  port: null as number | null,
  ran: false,
  lastRun: null as RunResponse | null,
  preview: null as PreviewResult | null,
  previewOn: false,
  issues: [] as { sev: string; label: string; msg: string; anchor: string | null }[],
  tailTimer: 0 as number | 0,
  conns: [] as ConnSummary[],
  stageTest: null as StageTest | null,
  srcSample: null as SrcSample | null,
  sinkSample: null as SrcSample | null,
  db: {} as Record<string, DbIntrospect>,
  srcSchemaLoading: false,
  bindPick: null as { nodeId: string; key: string; suggested: string; cur: string } | null,
};

// Live-DB introspection state, keyed by target ('source' | 'sink' | 'dlq').
interface DbIntrospect {
  tables: string[] | null;   // fetched table list (null = not fetched)
  loadingTables: boolean;
  loadingCols: boolean;
  autoSchema: boolean;       // auto-fetch columns from DB on table select
  error: string | null;
  objects?: string[] | null; // object-store keys (browse)
  loadingObjects?: boolean;
  topics?: string[] | null;  // kafka topic list (browse)
  loadingTopics?: boolean;
}
function dbState(target: string): DbIntrospect {
  if (!S.db[target]) S.db[target] = { tables: null, loadingTables: false, loadingCols: false, autoSchema: true, error: null, objects: null, loadingObjects: false, topics: null, loadingTopics: false };
  return S.db[target];
}
function targetOf(node: Node): 'source' | 'sink' | 'dlq' | null {
  if (node.kind === 'source') return 'source';
  if (node.kind === 'sink') return 'sink';
  if (node.kind === 'dlq') return 'dlq';
  return null;
}

// Read-only source-sample state for the source node's Test tab, so the user can
// SEE the rows the pipeline reads before running anything.
interface SrcSample {
  columns: string[];
  rows: Array<Record<string, unknown>>;
  loading: boolean;
  read: number;      // rowsRead reported by the engine
}

// Atomic stage-tester working state for the currently selected transform.
interface StageTest {
  stage: number;                              // transforms[] index under test
  columns: string[];                          // input column order
  rows: Array<Record<string, unknown>>;       // editable input sample
  result: StageTestResult | null;             // last run outcome
  view: 'grid' | 'json';                      // input editor surface
  seeding: boolean;                           // sample fetch in flight
  running: boolean;                           // test run in flight
  jsonDraft: string | null;                   // unsaved JSON textarea buffer
}

// Connection summary sent by the host (never carries secret values).
interface ConnSummary { id: string; name: string; type: string; family: string; desc: string; }

// Map a source/sink stage type to a connection family so we offer only
// compatible connections in the inspector.
function familyOfType(type: string): string | null {
  if (['jdbc', 'postgres', 'mysql', 'sqlite'].includes(type)) return 'db';
  if (type === 'kafka') return 'stream';
  if (['s3', 'gcs', 'adls', 'objstore'].includes(type)) return 'obj';
  // Note: the `lookup` transform reads a full templated `url`, which a base-URL
  // connection can't supply, so it is intentionally not connection-bindable.
  if (type === 'http') return 'http';
  return null;
}
function familyLabel(fam: string): string {
  return ({ db: 'database', stream: 'Kafka', obj: 'object-store', http: 'HTTP' } as Record<string, string>)[fam] || fam;
}

const statusColor: Record<string, string> = { running: 'b', completed: 'g', partial: 'y', failed: 'r', idle: 'd' };

// ── build a Pipe from the real doc ────────────────────────────────────────
function metaFor(kind: string, type: string, cfg: any): string {
  // Show the friendly display label (e.g. "Text Document" for the sectioned
  // source), falling back to the raw engine id for types without a label.
  const bits: string[] = [typeLabel(type)];
  if (cfg.path) bits.push(String(cfg.path).split(/[\\/]/).pop() || '');
  if (cfg.table) bits.push(cfg.table);
  if (cfg.topic) bits.push(cfg.topic);
  if (cfg.dsn) bits.push('dsn');
  if (cfg.expr) bits.push(String(cfg.expr).slice(0, 24));
  return bits.filter(Boolean).join(' · ');
}
function cfgOf(o: any): Record<string, string> {
  const out: Record<string, string> = {};
  if (!o || typeof o !== 'object') return out;
  for (const [k, v] of Object.entries(o)) {
    if (k === 'type' || k === 'name') continue; // type is the stage kind; name is the editable stage label
    if (v == null) continue;
    if (typeof v === 'object') out[k] = JSON.stringify(v);
    else out[k] = String(v);
  }
  return out;
}
function buildPipe(doc: PipelineDoc): Pipe {
  const src: any = doc.source || {};
  const sink: any = doc.sink || {};
  const transforms = Array.isArray(doc.transforms) ? doc.transforms : [];
  const nodes: Node[] = [];
  nodes.push({
    id: 'source',
    kind: 'source',
    type: src.type || 'source',
    nm: (src.path && String(src.path).split(/[\\/]/).pop()) || typeLabel(src.type) || 'source',
    meta: metaFor('source', src.type || '—', src),
    cfg: cfgOf(src),
    ref: 'source',
  });
  transforms.forEach((t: any, i: number) => {
    nodes.push({
      id: 't' + i,
      kind: 'transform',
      type: t.type || 'transform',
      nm: t.name || transformLabel(t.type) || 'transform',
      meta: metaFor('transform', t.type || '—', t),
      cfg: cfgOf(t),
      ref: i,
    });
  });
  nodes.push({
    id: 'sink',
    kind: 'sink',
    type: sink.type || 'sink',
    nm: sink.table || typeLabel(sink.type) || 'sink',
    meta: metaFor('sink', sink.type || '—', sink),
    cfg: cfgOf(sink),
    ref: 'sink',
  });

  // Dead-Letter Queue is a SEPARATE top-level `dlq` block that routes write-time
  // failures — it sits alongside the main sink, it never replaces it. The DLQ
  // node is built from doc.dlq (destination type + flattened options), not from
  // the transform chain.
  const dlq: any = doc.dlq || {};
  const hasDlq = !!dlq.type;
  let dlqNode: Node | null = null;
  if (hasDlq) {
    const opts = (dlq.options && typeof dlq.options === 'object') ? dlq.options : {};
    // Flatten options + the two top-level tunables for display.
    const cfg = cfgOf({ ...opts, maxRowsPerBatch: dlq.maxRowsPerBatch, onFull: dlq.onFull });
    dlqNode = {
      id: 'dlq',
      kind: 'dlq',
      type: dlq.type,
      nm: opts.table || opts.path || dlq.type,
      meta: metaFor('dlq', dlq.type, opts),
      cfg,
      ref: 'dlq',
    };
  }

  const branches = deriveBranches(doc);
  return {
    name: doc.name || S.fileName.replace(/\.datapipeline$/, ''),
    description: doc.description || '',
    mode: String(doc.mode || 'etl').toUpperCase(),
    status: S.running ? 'running' : S.ran ? 'partial' : 'idle',
    nodes,
    branches,
    hasDlq,
    dlqNode,
  };
}

// Resolve the doc object a stage's config lives in.
// For the DLQ node, the two tunables (maxRowsPerBatch / onFull) live on the
// top-level dlq block; every other field lives in dlq.options (the destination
// sink config). cfgTarget returns dlq.options; setCfgField/Value special-case
// the two tunables.
const DLQ_TOPLEVEL = new Set(['maxRowsPerBatch', 'onFull']);
function cfgTarget(node: Node): any {
  const doc: any = S.doc;
  if (node.ref === 'source') return (doc.source = doc.source || {});
  if (node.ref === 'sink') return (doc.sink = doc.sink || {});
  if (node.ref === 'dlq') {
    doc.dlq = doc.dlq || {};
    doc.dlq.options = doc.dlq.options || {};
    return doc.dlq.options;
  }
  doc.transforms = doc.transforms || [];
  return (doc.transforms[node.ref] = doc.transforms[node.ref] || {});
}

// Write a config field back into the doc and persist (string value, coerced to
// the prior type where possible).
function setCfgField(node: Node, key: string, value: string) {
  const target = node.ref === 'dlq' && DLQ_TOPLEVEL.has(key) ? ((S.doc as any).dlq = (S.doc as any).dlq || {}) : cfgTarget(node);
  const prev = target[key];
  if (typeof prev === 'number' && value.trim() !== '' && !isNaN(Number(value))) target[key] = Number(value);
  else if (typeof prev === 'boolean') target[key] = value === 'true';
  else target[key] = value;
  S.pipe = buildPipe(S.doc);
  post({ type: 'save', doc: S.doc });
}

// Write a raw (non-string) value — used by toggles, number, and structured editors.
function setCfgValue(node: Node, key: string, value: unknown) {
  const target = node.ref === 'dlq' && DLQ_TOPLEVEL.has(key) ? ((S.doc as any).dlq = (S.doc as any).dlq || {}) : cfgTarget(node);
  target[key] = value;
  S.pipe = buildPipe(S.doc);
  post({ type: 'save', doc: S.doc });
}

// ── config-field metadata (labels, enums, widget selection) ─────────────────
const FIELD_LABELS: Record<string, string> = {
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
const FIELD_ENUMS: Record<string, string[]> = {
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
};
// Per-stage-type enum overrides — used when a field name is shared across stage
// types with different allowed values (e.g. `mode` is streaming/batch for kafka
// but regex/json for parse; `onError` is null/skip/fail for lookup but
// keep/drop/route for parse). Keyed by stage type, then field name.
const FIELD_ENUMS_BY_TYPE: Record<string, Record<string, string[]>> = {
  parse: {
    mode: ['regex', 'json'],
    onError: ['keep', 'drop', 'route'],
  },
};

// Config keys that reference an INPUT field name (not free text). For these we
// render an autocomplete input backed by the upstream schema — so users pick
// from the available fields instead of guessing the exact name — while still
// allowing a custom value (self-describing sources, computed names). Keyed by
// stage type, then the set of field-name config keys.
const FIELD_NAME_KEYS: Record<string, Set<string>> = {
  parse: new Set(['source']),
  hwm: new Set(['column']),
  lookup: new Set(['keyField']),
};
const FIELD_HINTS: Record<string, string> = {
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
}; const SOURCE_TYPES = ['csv', 'tsv', 'fixedwidth', 'sectioned', 'parquet', 'xml', 'jsonlines', 'jdbc', 'kafka', 's3', 'gcs', 'adls', 'podlog'];
const SINK_TYPES = ['jdbc', 'parquet', 'csv', 'jsonlines', 'template', 'kafka', 's3', 'gcs', 'adls', 'null'];
// Friendly, generic display labels for stage types whose engine id is jargon.
// The engine id (used in the .datapipeline file) is unchanged; only the picker
// label differs. Types without an entry fall back to their raw id.
const TYPE_LABELS: Record<string, string> = {
  sectioned: 'Text Document',
  template: 'Custom Text',
  null: 'Null (discard)',
  podlog: 'Pod Logs (K8s)',
};
function typeLabel(t: string): string {
  return TYPE_LABELS[t] || t;
}

// Friendly display names for transform stage types, so a node/breadcrumb shows
// "Data Quality" instead of the raw config key "dq". Falls back to the raw type
// for anything not listed.
const TF_LABELS: Record<string, string> = {
  filter: 'Filter',
  map: 'Map',
  mapper: 'Map',
  validate: 'Validate',
  lookup: 'Lookup',
  hwm: 'High-Water Mark',
  dq: 'Data Quality',
  schemadrift: 'Schema Drift',
  parse: 'Parse',
  window: 'Window (aggregate)',
  join: 'Join',
  cast: 'Cast',
  explode: 'Explode',
  flatten: 'Flatten',
  router: 'Router / Split',
  split: 'Router / Split',
};
function transformLabel(t: string): string {
  return TF_LABELS[t] || t;
}

// Fixed emitted schemas for sources whose columns are NOT declared in the
// .datapipeline JSON (the engine knows them internally). Surfacing them here
// means the map editor's "Available data" palette is populated immediately —
// users see the $fields to map from without first running a preview. Keyed by
// source type.
const SOURCE_FIXED_SCHEMA: Record<string, Array<{ name: string; type: string }>> = {
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
function parseProducedColumns(t: any): string[] {
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
// Special selection id for the pipeline-level settings panel (not a real node).
const SETTINGS_NODE = '__pipeline_settings__';
// A DLQ destination is any real sink type (never 'dlq' itself — DLQ is a route,
// not a sink type).
const DLQ_DEST_TYPES = ['jdbc', 'parquet', 'csv', 'jsonlines', 's3', 'gcs', 'adls', 'null'];

// Starter config for a freshly chosen source/sink type — matches the keys each
// backend reads, so a newly switched stage is immediately runnable/editable.
function defaultConfigFor(kind: 'source' | 'sink', type: string): Record<string, unknown> {
  const src: Record<string, Record<string, unknown>> = {
    csv: { path: './data/input.csv', hasHeader: true, delimiter: ',', columns: [] },
    tsv: { path: './data/input.tsv', hasHeader: true, columns: [] },
    fixedwidth: { path: './data/input.dat', encoding: 'utf-8', recordLength: 0, columns: [] },
    sectioned: {
      path: './data/input.txt',
      headerKVRegex: '^(\\S[\\S ]*?)\\s{2,}(.*?)\\s*$',
      bodyColumnMarker: '^COLUMNS\\s',
      bodyDelimiter: '\\s+',
      broadcastHeader: true,
      columns: [],
    },
    parquet: { path: './data/input.parquet', rowGroupParallelism: 4 },
    xml: { path: './data/input.xml', recordElement: 'record' },
    jsonlines: { path: './data/input.jsonl' },
    jdbc: { driver: 'postgres', dsn: '', table: '', where: '', orderKey: '', batchSize: 5000 },
    kafka: { brokers: '', topic: '', groupId: 'dp-consumer', startOffset: 'earliest', deserializer: 'json', mode: 'batch', stopAtCurrentEnd: true },
    s3: { bucket: '', prefix: '', region: 'us-east-1', format: 'csv' },
    gcs: { bucket: '', prefix: '', format: 'csv' },
    adls: { bucket: '', prefix: '', format: 'csv' },
    podlog: {
      root: '/var/log/pods',
      node: '',
      positionFile: './out/positions.json',
      fromBeginning: false,
      bounded: false,
      pollMs: 500,
      includeNamespaces: [],
      excludeNamespaces: ['kube-system'],
      multiline: { startPattern: '', flushIdleMs: 2000, maxLines: 500 },
    },
  };
  const sink: Record<string, Record<string, unknown>> = {
    jdbc: { driver: 'postgres', dsn: '', table: '', batchSize: 1000, onConflict: 'insert', autoMigrate: true },
    parquet: { path: './out/output.parquet', compression: 'snappy', rowGroupSize: 100000 },
    csv: { path: './out/output.csv', delimiter: ',', writeHeader: true },
    jsonlines: { path: './out/output.jsonl' },
    template: { path: './out/{{index .Attr "id"}}.txt', template: '{{range .Rows}}{{.}}\n{{end}}' },
    kafka: { brokers: '', topic: '', ackMode: 'all', serializer: 'json' },
    s3: {
      bucket: '', key: '${date}/part-${uuid}.parquet', format: 'parquet',
      region: 'us-east-1', endpoint: '', forcePathStyle: false,
      accessKeyIdEnv: '', secretAccessKeyEnv: '',
      rollMaxBytes: 0, rollMaxSeconds: 0,
    },
    gcs: { bucket: '', key: '${date}/part-${uuid}.parquet', format: 'parquet' },
    adls: { bucket: '', key: '${date}/part-${uuid}.parquet', format: 'parquet' },
    null: {},
  };
  return (kind === 'source' ? src[type] : sink[type]) || {};
}

// Starter DLQ destination options for a chosen DLQ destination sink type.
function defaultDlqOptions(type: string): Record<string, unknown> {
  const m: Record<string, Record<string, unknown>> = {
    jdbc: { driver: 'postgres', dsn: '', table: 'pipeline_dlq', envelopeColumn: '_error' },
    parquet: { path: './out/dlq.parquet', compression: 'snappy' },
    csv: { path: './out/dlq.csv', writeHeader: true },
    jsonlines: { path: './out/dlq.jsonl' },
    s3: {
      bucket: '', key: 'dlq/${date}.jsonl', format: 'jsonlines',
      region: 'us-east-1', endpoint: '', forcePathStyle: false,
      accessKeyIdEnv: '', secretAccessKeyEnv: '',
    },
    gcs: { bucket: '', key: 'dlq/${date}.jsonl', format: 'jsonlines' },
    adls: { bucket: '', key: 'dlq/${date}.jsonl', format: 'jsonlines' },
    null: {},
  };
  return m[type] || {};
}

function humanize(k: string): string {
  return FIELD_LABELS[k] || k.replace(/([A-Z])/g, ' $1').replace(/^./, (c) => c.toUpperCase());
}
function isBoolField(k: string, v: string): boolean {
  return v === 'true' || v === 'false' ||
    /^(has|is|enable|delete|trim|nullable|repeating|download|autoMigrate|caseSensitive|deleteOnSuccess|forcePathStyle)/i.test(k);
}
function isFileField(k: string): boolean {
  return /(^path$|^root$|_path$|file$|dir$|directory$)/i.test(k);
}
function isNumField(k: string, v: string): boolean {
  return /(size|port|workers|batches|interval|memory|parallelism|scale|length|start|retries|threshold|rows|seconds|delay|lines|concurrency|ms$|bytes|offset|count|limit|poll)/i.test(k) &&
    v.trim() !== '' && !isNaN(Number(v));
}
function isJsonField(v: string): boolean {
  const t = v.trim();
  return t.startsWith('{') || t.startsWith('[');
}

// browse callbacks keyed by request id (resolved via browseResult host message)
let browseSeq = 0;
const pendingBrowse: Record<string, (p: string) => void> = {};
function requestBrowse(mode: 'file' | 'folder', onPick: (p: string) => void) {
  const id = 'b' + ++browseSeq;
  pendingBrowse[id] = onPick;
  post({ type: 'browse', id, mode });
}

// ── app-property binding ($property) ──────────────────────────────────
// A field value of `$property[name]` binds it to an app property; the ⚡ in the
// field label opens a picker to bind. Declare/edit properties in the "App
// Properties" section of Pipeline Settings. The engine resolves $property[...]
// at load with env-override precedence (see internal/appprops).
function isPropRef(v: string): boolean {
  return /^\s*\$property\[/.test(v);
}
function propRefName(v: string): string {
  const m = String(v).match(/\$property\[\s*"?([^\]"]+)"?\s*\]/);
  return m ? m[1] : '';
}
function appProps(): any[] {
  const doc: any = S.doc;
  if (!Array.isArray(doc.__appProperties)) doc.__appProperties = [];
  return doc.__appProperties;
}
const APP_PROP_TYPES: Array<[string, string]> = [
  ['string', 'String'], ['number', 'Number'], ['boolean', 'Boolean'], ['password', 'Password (secret)'],
];
// Rows for the "App Properties" settings section: declare name / type / default.
function appPropertiesGroupFields(): HTMLElement[] {
  const props = appProps();
  const out: HTMLElement[] = [];
  out.push(el('div', 'hint', 'Named values injected into config via <code>$property[name]</code>. Override per environment with an env var of the same name (or a k8s ConfigMap / Secret). Bind a field with the ⚡ in its inspector.'));
  props.forEach((p: any, i: number) => {
    const row = el('div', 'inputrow');
    row.style.marginBottom = '4px';
    const name = el('input', 'v') as HTMLInputElement;
    name.placeholder = 'name'; name.style.flex = '1.2';
    name.value = String(p.name || '');
    name.onchange = () => { p.name = name.value.trim(); saveSettings(); };
    const type = el('select', 'v') as HTMLSelectElement;
    APP_PROP_TYPES.forEach(([v, l]) => { const o = document.createElement('option'); o.value = v; o.textContent = l; if (v === String(p.type || 'string')) o.selected = true; type.appendChild(o); });
    type.onchange = () => { p.type = type.value; saveSettings(); };
    const val = el('input', 'v') as HTMLInputElement;
    val.placeholder = 'default value'; val.style.flex = '1.2';
    val.value = p.value != null ? String(p.value) : '';
    val.onchange = () => { p.value = val.value; saveSettings(); };
    const rm = el('button', 'browse', '✕');
    rm.title = 'Remove property';
    rm.onclick = () => { props.splice(i, 1); saveSettings(); renderEditor(); };
    row.appendChild(name); row.appendChild(type); row.appendChild(val); row.appendChild(rm);
    out.push(row);
  });
  const add = el('button', 'btn sm', '＋ Add property');
  add.style.marginTop = '4px';
  add.onclick = () => { props.push({ name: '', type: 'string', value: '' }); saveSettings(); renderEditor(); };
  out.push(add);
  return out;
}
function openBindPicker(node: Node, key: string): void {
  const cur = String((cfgTarget(node) as any)[key] ?? '');
  S.bindPick = { nodeId: node.id, key, suggested: key.replace(/[^A-Za-z0-9_.]/g, '_'), cur };
  renderEditor();
}
function renderBindPicker(f: HTMLElement, node: Node, key: string): HTMLElement {
  const bp = S.bindPick;
  if (!bp) return f;
  const props = appProps();
  const sel = el('select', 'v') as HTMLSelectElement;
  const optNew = document.createElement('option'); optNew.value = '__new'; optNew.textContent = '＋ New property…'; sel.appendChild(optNew);
  props.forEach((p: any) => { if (p.name) { const o = document.createElement('option'); o.value = String(p.name); o.textContent = String(p.name); sel.appendChild(o); } });
  const nameInp = el('input', 'v') as HTMLInputElement;
  nameInp.placeholder = 'new property name';
  nameInp.value = bp.suggested;
  const refresh = () => { nameInp.style.display = sel.value === '__new' ? '' : 'none'; };
  sel.onchange = refresh;
  const bind = el('button', 'browse', 'Bind ⚡');
  bind.onclick = () => {
    let name = sel.value;
    if (name === '__new') {
      name = nameInp.value.trim();
      if (!name) { toast('err', 'Property name is required'); return; }
      if (!props.find((p: any) => p.name === name)) {
        const isBool = bp.cur === 'true' || bp.cur === 'false';
        const isNum = !isBool && bp.cur.trim() !== '' && !isNaN(Number(bp.cur));
        props.push({ name, type: isBool ? 'boolean' : isNum ? 'number' : 'string', value: bp.cur });
      }
    }
    S.bindPick = null;
    setCfgValue(node, key, '$property[' + name + ']');
    renderEditor();
  };
  const cancel = el('button', 'browse', '✕');
  cancel.title = 'Cancel';
  cancel.onclick = () => { S.bindPick = null; renderEditor(); };
  const row = el('div', 'inputrow');
  row.appendChild(sel); row.appendChild(bind); row.appendChild(cancel);
  f.appendChild(row);
  f.appendChild(nameInp);
  refresh();
  f.appendChild(el('div', 'hint', 'Pick an existing app property or create one — the field will use <code>$property[name]</code>.'));
  return f;
}
function renderBoundField(f: HTMLElement, node: Node, key: string, value: string): HTMLElement {
  const name = propRefName(value);
  const row = el('div', 'inputrow');
  const chip = el('div', 'v');
  chip.style.cssText = 'flex:1;opacity:.9';
  chip.innerHTML = '⚡ <b>' + escapeHtml(name) + '</b>';
  chip.title = 'Bound to app property "' + name + '" (' + escapeHtml(value) + ')';
  const unbind = el('button', 'browse', '✕ Unbind');
  unbind.title = 'Replace with a literal value (uses the property default)';
  unbind.onclick = () => {
    const p = appProps().find((x: any) => x.name === name);
    setCfgField(node, key, p && p.value != null ? String(p.value) : '');
    renderEditor();
  };
  row.appendChild(chip); row.appendChild(unbind);
  f.appendChild(row);
  f.appendChild(el('div', 'hint', 'Uses app property <code>' + escapeHtml(name) + '</code> — set its default in <b>App Properties</b>; override per env with an env var named <code>' + escapeHtml(name) + '</code>.'));
  return f;
}

// Build one config field widget appropriate to its key/value type.
function renderField(node: Node, key: string, value: string): HTMLElement {
  const f = el('div', 'fld');
  const kdiv = el('div', 'k');
  kdiv.textContent = humanize(key);
  f.appendChild(kdiv);

  // App-property binding: a mid-bind picker, an existing $property[...] binding,
  // or a ⚡ affordance on an eligible scalar field.
  if (S.bindPick && S.bindPick.nodeId === node.id && S.bindPick.key === key) {
    return renderBindPicker(f, node, key);
  }
  if (isPropRef(String(value))) {
    return renderBoundField(f, node, key, String(value));
  }
  const enumForBind = (FIELD_ENUMS_BY_TYPE[node.type] && FIELD_ENUMS_BY_TYPE[node.type][key]) || FIELD_ENUMS[key];
  if (key !== 'columns' && !enumForBind && !(isJsonField(value) && key !== 'columns')) {
    const bico = el('span', 'k-bind', '⚡ bind');
    bico.title = 'Bind to an app property ($property) — override per environment';
    bico.style.cssText = 'cursor:pointer;margin-left:8px;font-size:10px;padding:1px 6px;border:1px solid var(--vscode-widget-border,#454545);border-radius:3px;opacity:.85;color:var(--vscode-textLink-foreground,#3794ff);vertical-align:middle;white-space:nowrap';
    bico.onmouseenter = () => { bico.style.opacity = '1'; bico.style.background = 'var(--vscode-toolbar-hoverBackground,rgba(90,93,94,.3))'; };
    bico.onmouseleave = () => { bico.style.opacity = '.85'; bico.style.background = 'transparent'; };
    bico.onclick = (e) => { e.stopPropagation(); openBindPicker(node, key); };
    kdiv.appendChild(bico);
  }

  // Boolean → toggle
  if (isBoolField(key, value)) {
    const on = value === 'true';
    const t = el('div', 'toggle' + (on ? ' on' : ''));
    t.innerHTML = `<span class="sw"></span><span class="lbl">${on ? 'true' : 'false'}</span>`;
    t.onclick = () => {
      const next = !t.classList.contains('on');
      t.classList.toggle('on', next);
      (t.querySelector('.lbl') as HTMLElement).textContent = String(next);
      setCfgValue(node, key, next);
    };
    f.appendChild(t);
    return f;
  }

  // Enum → dropdown. A per-stage-type override wins over the shared FIELD_ENUMS
  // so fields like `mode`/`onError` show the right options for this stage type.
  const enumOpts = (FIELD_ENUMS_BY_TYPE[node.type] && FIELD_ENUMS_BY_TYPE[node.type][key]) || FIELD_ENUMS[key];
  if (enumOpts) {
    const sel = el('select', 'v') as HTMLSelectElement;
    const opts = enumOpts.slice();
    if (!opts.includes(value) && value) opts.unshift(value);
    opts.forEach((o) => {
      const op = document.createElement('option');
      op.value = o;
      op.textContent = o;
      if (o === value) op.selected = true;
      sel.appendChild(op);
    });
    sel.onchange = () => {
      setCfgField(node, key, sel.value);
      // Some enums change which other fields apply (e.g. jdbc onConflict decides
      // whether the conflict-key picker is shown) — refresh the inspector.
      if (key === 'onConflict') renderEditor();
    };
    f.appendChild(sel);
    return f;
  }

  // Field-name reference → autocomplete input backed by the upstream schema.
  // Users pick from the available input fields (like the validate/dq pickers)
  // instead of typing an exact name, but a custom value is still allowed.
  if (FIELD_NAME_KEYS[node.type]?.has(key)) {
    const cols = upstreamSchema(node);
    const inp = el('input', 'v') as HTMLInputElement;
    inp.value = value;
    inp.placeholder = 'input field name';
    if (cols.length) {
      const listId = `fl_${node.type}_${key}`;
      const dl = document.createElement('datalist');
      dl.id = listId;
      cols.forEach((c) => { const o = document.createElement('option'); o.value = c.name; dl.appendChild(o); });
      inp.setAttribute('list', listId);
      f.appendChild(dl);
    }
    inp.onchange = () => setCfgField(node, key, inp.value);
    f.appendChild(inp);
    const hint = FIELD_HINTS[key] || (cols.length ? `Pick from ${cols.length} available field${cols.length === 1 ? '' : 's'}, or type a name.` : '');
    if (hint) f.appendChild(el('div', 'hint', hint));
    return f;
  }

  // File path → text + Browse
  if (isFileField(key)) {
    const row = el('div', 'inputrow');
    const inp = el('input', 'v') as HTMLInputElement;
    inp.value = value;
    inp.onchange = () => setCfgField(node, key, inp.value);
    const btn = el('button', 'browse', '📂 Browse');
    const folder = node.type === 'file' || /checkpoint/i.test(node.type) || key.toLowerCase().includes('dir') || key === 'root';
    btn.onclick = () => requestBrowse(folder ? 'folder' : 'file', (p) => { inp.value = p; setCfgField(node, key, p); });
    row.appendChild(inp);
    row.appendChild(btn);
    f.appendChild(row);
    if (FIELD_HINTS[key]) f.appendChild(el('div', 'hint', FIELD_HINTS[key]));
    return f;
  }

  // JSON object/array (non-columns) → textarea
  if (isJsonField(value) && key !== 'columns') {
    const ta = document.createElement('textarea');
    ta.className = 'v';
    ta.value = value;
    ta.onchange = () => {
      try {
        setCfgValue(node, key, JSON.parse(ta.value));
        ta.style.borderColor = '';
      } catch {
        ta.style.borderColor = 'var(--red)';
        toast('err', `${humanize(key)} is not valid JSON`);
      }
    };
    f.appendChild(ta);
    return f;
  }

  // Number
  if (isNumField(key, value)) {
    const inp = el('input', 'v') as HTMLInputElement;
    inp.type = 'number';
    inp.value = value;
    inp.onchange = () => setCfgValue(node, key, inp.value === '' ? '' : Number(inp.value));
    f.appendChild(inp);
    if (FIELD_HINTS[key]) f.appendChild(el('div', 'hint', FIELD_HINTS[key]));
    return f;
  }

  // Plain text (default)
  const inp = el('input', 'v') as HTMLInputElement;
  inp.value = value;
  inp.onchange = () => setCfgField(node, key, inp.value);
  f.appendChild(inp);
  if (FIELD_HINTS[key]) f.appendChild(el('div', 'hint', FIELD_HINTS[key]));
  return f;
}

// Dedicated editor for the podlog `multiline` sub-object (stack-trace stitching).
// Renders labeled inputs that read/write source.multiline.{startPattern,
// flushIdleMs, maxLines}; the engine reads these as a nested object.
function renderPodlogMultiline(node: Node): HTMLElement {
  const wrap = el('div', 'pp-multiline');
  wrap.appendChild(el('div', 'k', 'Multiline stitching'));
  wrap.appendChild(el('div', 'hint', 'Reassemble stack traces / wrapped lines into ONE record. Leave Start pattern empty to disable (each line = one record).'));
  const tgt = cfgTarget(node);
  const ml: any = (tgt.multiline && typeof tgt.multiline === 'object') ? tgt.multiline : (tgt.multiline = {});
  const save = () => {
    if (Object.keys(ml).length === 0) delete tgt.multiline;
    S.pipe = buildPipe(S.doc);
    post({ type: 'save', doc: S.doc });
  };
  const row = (label: string, val: string, hint: string, num: boolean, apply: (v: string) => void) => {
    const f = el('div', 'fld');
    f.appendChild(el('div', 'k', label));
    const inp = el('input', 'v') as HTMLInputElement;
    if (num) inp.type = 'number';
    inp.value = val;
    inp.onchange = () => { apply(inp.value); save(); };
    f.appendChild(inp);
    f.appendChild(el('div', 'hint', hint));
    wrap.appendChild(f);
  };
  row('Start pattern (regex)', String(ml.startPattern ?? ''),
    'Regex a NEW record starts with, e.g. ^\\d{4}-\\d{2}-\\d{2} (a timestamp). Non-matching lines are appended to the previous record.',
    false, (v) => { if (v === '') delete ml.startPattern; else ml.startPattern = v; });
  row('Flush idle (ms)', ml.flushIdleMs != null ? String(ml.flushIdleMs) : '',
    'Emit a pending stitched record after the file is idle this long (default 2000).',
    true, (v) => { if (v === '') delete ml.flushIdleMs; else ml.flushIdleMs = Number(v); });
  row('Max lines', ml.maxLines != null ? String(ml.maxLines) : '',
    'Safety cap on stitched lines per record (default 500; 0 = unlimited).',
    true, (v) => { if (v === '') delete ml.maxLines; else ml.maxLines = Number(v); });
  return wrap;
}

// Config keys that a bound connection supplies — hidden from the generic field
// list when a connection is selected (the connection owns them, incl. secrets).
const CONN_MANAGED_KEYS: Record<string, string[]> = {
  db: ['driver', 'dsn'],
  stream: ['brokers', 'securityProtocol', 'saslMechanism', 'saslUser', 'saslPassword'],
  obj: ['bucket', 'region', 'endpoint', 'accessKeyId', 'secretAccessKey', 'projectId', 'credentialsJson', 'account', 'container', 'sasToken'],
  http: ['baseUrl', 'authHeader'],
};

// A "Connection" picker for source/sink/dlq stages whose type maps to a
// connection family. Binding stores `connection: "<name>"` on the stage and the
// host injects the real DSN/secret fields at run time — so nothing sensitive is
// ever written to the .datapipeline file. Returns null when the stage type
// doesn't use a connection (e.g. a CSV file source).
function renderConnectionSelector(node: Node): HTMLElement | null {
  const fam = familyOfType(node.type);
  if (!fam) return null;
  const target = cfgTarget(node);
  const bound: string = typeof target.connection === 'string' ? target.connection : '';
  const matches = S.conns.filter((c) => c.family === fam);

  const f = el('div', 'fld conn-fld');
  f.appendChild(el('div', 'k', 'Connection'));

  const sel = el('select', 'v') as HTMLSelectElement;
  const NONE = '__none__';
  const NEW = '__new__';
  const add = (val: string, label: string, selected: boolean) => {
    const op = document.createElement('option');
    op.value = val; op.textContent = label; op.selected = selected;
    sel.appendChild(op);
  };
  add(NONE, matches.length ? '— none (enter fields manually) —' : '— no ' + familyLabel(fam) + ' connections —', !bound);
  matches.forEach((c) => add(c.name, `${c.name}  ·  ${c.desc}`, c.name === bound));
  // A bound connection that no longer exists — keep it visible so it isn't lost.
  if (bound && !matches.some((c) => c.name === bound)) add(bound, `${bound}  (missing)`, true);
  add(NEW, '＋ New connection…', false);

  sel.onchange = () => {
    const v = sel.value;
    if (v === NEW) {
      post({ type: 'newConnection', family: fam });
      renderEditor();
      return;
    }
    if (v === NONE) {
      delete target.connection;
      persistAndRebuild();
      return;
    }
    target.connection = v;
    // Drop any manually-entered managed fields so the connection is the single
    // source of truth (secrets are injected at run time by the host).
    for (const k of CONN_MANAGED_KEYS[fam] || []) delete target[k];
    persistAndRebuild();
    toast('ok', `Bound ${node.kind} → connection "${v}"`);
  };
  f.appendChild(sel);

  if (bound) {
    const row = el('div', 'conn-actions');
    const test = el('button', 'browse', '⟿ Test');
    const conn = S.conns.find((c) => c.name === bound);
    test.onclick = () => conn ? post({ type: 'testConnection', id: conn.id }) : toast('warn', 'Connection not found');
    const edit = el('button', 'browse', '✎ Edit');
    edit.onclick = () => conn ? post({ type: 'editConnection', id: conn.id }) : post({ type: 'newConnection', family: fam });
    row.appendChild(test); row.appendChild(edit);
    f.appendChild(row);
    f.appendChild(el('div', 'hint', 'DSN & secrets come from this connection at run time — never written to the pipeline file.'));
  } else {
    f.appendChild(el('div', 'hint', 'Pick a saved connection to avoid typing DSNs/secrets, or enter fields below.'));
  }
  return f;
}

// Live-DB "pick a table" field for JDBC source/sink: a text input plus a
// dropdown populated from the database (via the connection) and a "Fetch schema"
// action that auto-populates the columns list. Falls back to plain text entry
// when the DB can't be reached.
function renderTableField(node: Node, value: string): HTMLElement {
  const target = targetOf(node);
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', 'Table'));
  const st = target ? dbState(target) : null;

  const row = el('div', 'inputrow');
  const inp = el('input', 'v') as HTMLInputElement;
  inp.value = value;
  inp.placeholder = 'schema.table or table';
  inp.onchange = () => { setCfgField(node, 'table', inp.value); if (st?.autoSchema && node.kind === 'source') fetchCols(node, inp.value); };
  row.appendChild(inp);
  const browse = el('button', 'browse', st?.loadingTables ? '…' : '⤓ Tables');
  browse.title = 'List tables from the database';
  browse.onclick = () => { if (target) { dbState(target).loadingTables = true; renderEditor(); post({ type: 'fetchDbTables', target }); } };
  row.appendChild(browse);
  f.appendChild(row);

  // Table dropdown once fetched.
  if (st?.tables && st.tables.length) {
    const sel = el('select', 'v') as HTMLSelectElement;
    const ph = document.createElement('option'); ph.value = ''; ph.textContent = `— ${st.tables.length} tables — pick one —`; sel.appendChild(ph);
    st.tables.forEach((t) => { const o = document.createElement('option'); o.value = t; o.textContent = t; if (t === value) o.selected = true; sel.appendChild(o); });
    sel.style.marginTop = '6px';
    sel.onchange = () => {
      if (!sel.value) return;
      inp.value = sel.value;
      setCfgField(node, 'table', sel.value);
      if (st.autoSchema && node.kind === 'source') fetchCols(node, sel.value);
    };
    f.appendChild(sel);
  } else if (st?.tables && !st.tables.length) {
    f.appendChild(el('div', 'hint', 'No tables found in this database.'));
  }
  if (st?.error) f.appendChild(el('div', 'hint', '⚠ ' + st.error));

  // Auto-fetch-schema toggle (source only — sink schema derives from the chain).
  if (node.kind === 'source' && st) {
    const t = el('div', 'toggle' + (st.autoSchema ? ' on' : ''));
    t.style.marginTop = '8px';
    t.innerHTML = `<span class="sw"></span><span class="lbl">Auto-fetch columns from DB</span>`;
    t.onclick = () => { st.autoSchema = !st.autoSchema; renderEditor(); };
    f.appendChild(t);
    if (value) {
      const fetchBtn = el('button', 'browse', st.loadingCols ? 'Fetching…' : '⭳ Fetch schema now');
      fetchBtn.style.marginTop = '6px';
      fetchBtn.onclick = () => fetchCols(node, value);
      f.appendChild(fetchBtn);
    }
    f.appendChild(el('div', 'hint', 'When on, selecting a table reads its column schema from the database. Turn off to edit columns manually.'));
  }
  return f;
}

// Request the DB column schema for a table into the given node's target.
function fetchCols(node: Node, table: string) {
  const target = targetOf(node);
  if (!target || !table) return;
  dbState(target).loadingCols = true; renderEditor();
  post({ type: 'fetchDbColumns', target, table });
}

// Object-store key/prefix field with a "browse objects" action that lists the
// bucket's objects so users can pick a file/prefix instead of hand-typing.
function renderObjectField(node: Node, key: string, value: string): HTMLElement {
  const target = targetOf(node);
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', humanize(key)));
  const st = target ? dbState(target) : null;
  const row = el('div', 'inputrow');
  const inp = el('input', 'v') as HTMLInputElement;
  inp.value = value;
  inp.placeholder = key === 'key' ? 'path/to/object or ${date}/part.parquet' : 'prefix/';
  inp.onchange = () => setCfgField(node, key, inp.value);
  row.appendChild(inp);
  const browse = el('button', 'browse', st?.loadingObjects ? '…' : '⤓ Browse');
  browse.title = 'List objects in the bucket';
  browse.onclick = () => { if (target) { dbState(target).loadingObjects = true; renderEditor(); post({ type: 'fetchObjects', target }); } };
  row.appendChild(browse);
  f.appendChild(row);
  if (st?.objects && st.objects.length) {
    const sel = el('select', 'v') as HTMLSelectElement;
    const ph = document.createElement('option'); ph.value = ''; ph.textContent = `— ${st.objects.length} objects — pick one —`; sel.appendChild(ph);
    st.objects.forEach((o) => { const op = document.createElement('option'); op.value = o; op.textContent = o; if (o === value) op.selected = true; sel.appendChild(op); });
    sel.style.marginTop = '6px';
    sel.onchange = () => { if (sel.value) { inp.value = sel.value; setCfgField(node, key, sel.value); } };
    f.appendChild(sel);
  } else if (st?.objects && !st.objects.length) {
    f.appendChild(el('div', 'hint', 'No objects found under this prefix.'));
  }
  if (FIELD_HINTS[key]) f.appendChild(el('div', 'hint', FIELD_HINTS[key]));
  return f;
}

// Kafka topic field with a "list topics" action so users can pick a topic from
// the cluster instead of hand-typing it.
function renderKafkaTopicField(node: Node, value: string): HTMLElement {
  const target = targetOf(node);
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', 'Topic'));
  const st = target ? dbState(target) : null;
  const row = el('div', 'inputrow');
  const inp = el('input', 'v') as HTMLInputElement;
  inp.value = value;
  inp.placeholder = 'topic name';
  inp.onchange = () => setCfgField(node, 'topic', inp.value);
  row.appendChild(inp);
  const browse = el('button', 'browse', st?.loadingTopics ? '…' : '⤓ Topics');
  browse.title = 'List topics from the Kafka cluster';
  browse.onclick = () => { if (target) { dbState(target).loadingTopics = true; renderEditor(); post({ type: 'fetchKafkaTopics', target }); } };
  row.appendChild(browse);
  f.appendChild(row);
  if (st?.topics && st.topics.length) {
    const sel = el('select', 'v') as HTMLSelectElement;
    const ph = document.createElement('option'); ph.value = ''; ph.textContent = `— ${st.topics.length} topics — pick one —`; sel.appendChild(ph);
    st.topics.forEach((t) => { const o = document.createElement('option'); o.value = t; o.textContent = t; if (t === value) o.selected = true; sel.appendChild(o); });
    sel.style.marginTop = '6px';
    sel.onchange = () => { if (sel.value) { inp.value = sel.value; setCfgField(node, 'topic', sel.value); } };
    f.appendChild(sel);
  } else if (st?.topics && !st.topics.length) {
    f.appendChild(el('div', 'hint', 'No topics found on this cluster.'));
  }
  if (st?.error) f.appendChild(el('div', 'hint', '⚠ ' + st.error));
  return f;
}

// Schema Registry URL field with a "read Avro schema" action that fetches the
// latest schema for the current topic and populates the source columns.
function renderSchemaRegistryField(node: Node, value: string): HTMLElement {
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', humanize('schemaRegistryURL')));
  const inp = el('input', 'v') as HTMLInputElement;
  inp.value = value;
  inp.placeholder = 'http://localhost:8081';
  inp.onchange = () => setCfgField(node, 'schemaRegistryURL', inp.value);
  f.appendChild(inp);
  if (node.kind === 'source' && value) {
    const readBtn = el('button', 'browse', S.srcSchemaLoading ? 'Reading…' : '⭳ Read schema from registry');
    readBtn.style.marginTop = '6px';
    readBtn.title = 'Fetch the latest Avro/JSON schema for the current topic and set the columns';
    readBtn.onclick = () => { S.srcSchemaLoading = true; renderEditor(); post({ type: 'fetchSchemaRegistry' }); };
    f.appendChild(readBtn);
  }
  f.appendChild(el('div', 'hint', 'Avro deserializer only. Reads <topic>-value from the registry to set columns.'));
  return f;
}

// XML recordElement field with a "detect from file" action that samples the XML
// file to auto-fill the record element, field xpaths and columns.
function renderXmlRecordField(node: Node, value: string): HTMLElement {
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', humanize('recordElement')));
  const inp = el('input', 'v') as HTMLInputElement;
  inp.value = value;
  inp.placeholder = 'e.g. product';
  inp.onchange = () => setCfgField(node, 'recordElement', inp.value);
  f.appendChild(inp);
  if (node.kind === 'source') {
    const btn = el('button', 'browse', S.srcSchemaLoading ? 'Detecting…' : '⤓ Detect structure from file');
    btn.style.marginTop = '6px';
    btn.title = 'Sample the XML file to detect the record element, fields and column types';
    btn.onclick = () => { S.srcSchemaLoading = true; renderEditor(); post({ type: 'fetchXmlSchema' }); };
    f.appendChild(btn);
  }
  f.appendChild(el('div', 'hint', 'The repeating element that becomes one record (e.g. <product> in <catalog>).'));
  return f;
}

// Format presets for the Text Document (sectioned) source: each fills the three
// regexes for a common document shape so users rarely need to write a regex.
const SECTIONED_PRESETS: Record<string, { headerKVRegex: string; bodyColumnMarker: string; bodyDelimiter: string; note: string }> = {
  'Instrument / metrology': {
    headerKVRegex: '^(\\S[\\S ]*?)\\s{2,}(.*?)\\s*$',
    bodyColumnMarker: '^MEASUREMENT\\s',
    bodyDelimiter: '\\s+',
    note: 'Key <2+ spaces> Value header; a MEASUREMENT line names the table columns.',
  },
  'Whitespace table': {
    headerKVRegex: '^(\\S[\\S ]*?)\\s{2,}(.*?)\\s*$',
    bodyColumnMarker: '^COLUMNS\\s',
    bodyDelimiter: '\\s+',
    note: 'Generic whitespace-aligned table; a COLUMNS line names the columns.',
  },
  'Colon key/value + CSV body': {
    headerKVRegex: '^([^:]+):\\s*(.*?)\\s*$',
    bodyColumnMarker: '^DATA,',
    bodyDelimiter: ',',
    note: 'Header lines like "Key: Value"; a DATA, line names comma-separated columns.',
  },
  'Pipe-delimited body': {
    headerKVRegex: '^(\\S[\\S ]*?)\\s{2,}(.*?)\\s*$',
    bodyColumnMarker: '^HEADER\\|',
    bodyDelimiter: '\\|',
    note: 'A HEADER|… line names pipe-separated columns.',
  },
};

// Format-preset selector for the sectioned source. Applying a preset writes the
// three regex fields; "Custom" leaves them as-is for manual editing below.
function renderSectionedPreset(node: Node): HTMLElement {
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', humanize('formatPreset')));
  const sel = el('select', 'v') as HTMLSelectElement;
  const names = Object.keys(SECTIONED_PRESETS);
  const cur = cfgTarget(node);
  // Detect which preset (if any) currently matches the config.
  let match = 'Custom';
  for (const n of names) {
    const p = SECTIONED_PRESETS[n];
    if (cur.headerKVRegex === p.headerKVRegex && cur.bodyColumnMarker === p.bodyColumnMarker && cur.bodyDelimiter === p.bodyDelimiter) { match = n; break; }
  }
  ['Custom', ...names].forEach((n) => {
    const op = document.createElement('option');
    op.value = n; op.textContent = n;
    if (n === match) op.selected = true;
    sel.appendChild(op);
  });
  sel.onchange = () => {
    const p = SECTIONED_PRESETS[sel.value];
    if (p) {
      const t = cfgTarget(node);
      t.headerKVRegex = p.headerKVRegex;
      t.bodyColumnMarker = p.bodyColumnMarker;
      t.bodyDelimiter = p.bodyDelimiter;
      S.pipe = buildPipe(S.doc);
      post({ type: 'save', doc: S.doc });
    }
    renderEditor();
  };
  f.appendChild(sel);
  const noteText = match !== 'Custom' ? SECTIONED_PRESETS[match].note : 'Pick a preset to fill the patterns below, or edit them by hand.';
  f.appendChild(el('div', 'hint', noteText));
  return f;
}

// Combined editor for the Custom Text (template) sink: an Inline/File toggle
// plus a monospace template textarea (or a template-file path picker), and a
// click-to-insert reference strip of the template variables and helper funcs.
const TEMPLATE_VARS = ['{{range .Rows}}…{{end}}', '{{.Count}}', '{{index .Attr "id"}}', '{{.Columns}}'];
const TEMPLATE_HELPERS = ['rpad', 'lpad', 'padc', 'sci', 'upper', 'lower', 'trim', 'add', 'seq'];
function renderTemplateEditor(node: Node): HTMLElement {
  const wrap = el('div', 'fld');
  const cur = cfgTarget(node);
  const usingFile = typeof cur.templatePath === 'string' && cur.templatePath.trim() !== '';

  // Source toggle: Inline template vs Template file.
  wrap.appendChild(el('div', 'k', humanize('templateSource')));
  const seg = el('div', 'inputrow');
  const mkBtn = (label: string, on: boolean, fn: () => void) => {
    const b = el('button', 'browse' + (on ? ' pri' : ''), label);
    b.onclick = fn;
    return b;
  };
  seg.appendChild(mkBtn('Inline template', !usingFile, () => {
    const t = cfgTarget(node);
    delete t.templatePath;
    if (typeof t.template !== 'string') t.template = '';
    S.pipe = buildPipe(S.doc); post({ type: 'save', doc: S.doc }); renderEditor();
  }));
  seg.appendChild(mkBtn('Template file', usingFile, () => {
    const t = cfgTarget(node);
    if (typeof t.templatePath !== 'string') t.templatePath = './layout.tmpl';
    S.pipe = buildPipe(S.doc); post({ type: 'save', doc: S.doc }); renderEditor();
  }));
  wrap.appendChild(seg);

  if (usingFile) {
    const row = el('div', 'inputrow');
    const inp = el('input', 'v') as HTMLInputElement;
    inp.value = String(cur.templatePath || '');
    inp.placeholder = './layout.tmpl';
    inp.onchange = () => setCfgField(node, 'templatePath', inp.value);
    const btn = el('button', 'browse', '📂 Browse');
    btn.onclick = () => requestBrowse('file', (p) => { inp.value = p; setCfgField(node, 'templatePath', p); });
    row.appendChild(inp);
    row.appendChild(btn);
    wrap.appendChild(row);
    wrap.appendChild(el('div', 'hint', FIELD_HINTS['templatePath']));
    return wrap;
  }

  // Inline: monospace textarea for the template body.
  const ta = document.createElement('textarea');
  ta.className = 'v';
  ta.style.fontFamily = 'var(--vscode-editor-font-family, monospace)';
  ta.style.minHeight = '120px';
  ta.style.whiteSpace = 'pre';
  ta.value = String(cur.template || '');
  ta.onchange = () => setCfgValue(node, 'template', ta.value);
  wrap.appendChild(ta);

  const insert = (snippet: string) => {
    const start = ta.selectionStart ?? ta.value.length;
    const end = ta.selectionEnd ?? ta.value.length;
    ta.value = ta.value.slice(0, start) + snippet + ta.value.slice(end);
    const caret = start + snippet.length;
    ta.setSelectionRange(caret, caret);
    ta.focus();
    setCfgValue(node, 'template', ta.value);
  };

  // Reference strip: variables then helper functions, click-to-insert.
  const varsWrap = el('div', 'mhelp');
  varsWrap.appendChild(el('div', 'lbl', 'Variables'));
  const vchips = el('div', 'chips');
  TEMPLATE_VARS.forEach((v) => {
    const c = el('span', 'chip2', escapeHtml(v));
    c.title = 'Insert ' + v;
    c.onclick = () => insert(v);
    vchips.appendChild(c);
  });
  varsWrap.appendChild(vchips);
  varsWrap.appendChild(el('div', 'lbl', 'Helpers'));
  const hchips = el('div', 'chips');
  TEMPLATE_HELPERS.forEach((h) => {
    const c = el('span', 'chip2 fn', `ƒ ${h}`);
    c.title = 'Insert ' + h + ' helper';
    c.onclick = () => insert(`{{${h} }}`);
    hchips.appendChild(c);
  });
  varsWrap.appendChild(hchips);
  varsWrap.appendChild(el('div', 'focushint', 'Click in the template, then a variable or helper to insert at the caret.'));
  wrap.appendChild(varsWrap);
  wrap.appendChild(el('div', 'hint', FIELD_HINTS['template']));
  return wrap;
}

// ── pipeline-level settings panel ────────────────────────────────────────────
// Reliability (checkpoint & incremental HWM), load strategy (performance),
// scaling, observability/metrics, and execution mode all live at the top of the
// pipeline document (not on any stage). renderPipelineSettings builds a single
// inspector with collapsible sections that read/write those top-level objects.
function settingsSection(obj: any, name: string): any {
  if (!obj[name] || typeof obj[name] !== 'object') obj[name] = {};
  return obj[name];
}
// Remove pipeline-setting sections that are empty (or contain only empty nested
// objects) so merely opening the panel never leaves clutter in the file.
function pruneEmptySettings(): void {
  const doc: any = S.doc;
  const isEmpty = (o: any) => o && typeof o === 'object' && Object.keys(o).length === 0;
  // Nested empties first.
  if (doc.scaling && isEmpty(doc.scaling.coordinator)) delete doc.scaling.coordinator;
  if (doc.observability && isEmpty(doc.observability.options)) delete doc.observability.options;
  if (doc.observability && isEmpty(doc.observability.audit)) delete doc.observability.audit;
  if (doc.observability && isEmpty(doc.observability.lineage)) delete doc.observability.lineage;
  if (doc.incremental && isEmpty(doc.incremental.storeOptions)) delete doc.incremental.storeOptions;
  for (const k of ['checkpoint', 'incremental', 'performance', 'scaling', 'observability']) {
    if (isEmpty(doc[k])) delete doc[k];
  }
}
function saveSettings(): void {
  pruneEmptySettings();
  S.pipe = buildPipe(S.doc);
  post({ type: 'save', doc: S.doc });
  post({ type: 'validate' });
}
// Field builders for the settings panel. Each writes into `obj[key]` and saves.
function stSelect(obj: any, key: string, label: string, options: Array<[string, string]>, hint: string, reRender = false): HTMLElement {
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', label));
  const sel = el('select', 'v') as HTMLSelectElement;
  const cur = obj[key] != null ? String(obj[key]) : options[0][0];
  options.forEach(([val, lbl]) => {
    const op = document.createElement('option');
    op.value = val; op.textContent = lbl;
    if (val === cur) op.selected = true;
    sel.appendChild(op);
  });
  sel.onchange = () => { obj[key] = sel.value; saveSettings(); if (reRender) renderEditor(); };
  f.appendChild(sel);
  if (hint) f.appendChild(el('div', 'hint', hint));
  return f;
}
function stNum(obj: any, key: string, label: string, placeholder: string, hint: string): HTMLElement {
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', label));
  const inp = el('input', 'v') as HTMLInputElement;
  inp.type = 'number';
  inp.placeholder = placeholder;
  if (obj[key] != null && obj[key] !== '') inp.value = String(obj[key]);
  inp.onchange = () => {
    if (inp.value.trim() === '') delete obj[key];
    else obj[key] = Number(inp.value);
    saveSettings();
  };
  f.appendChild(inp);
  if (hint) f.appendChild(el('div', 'hint', hint));
  return f;
}
function stText(obj: any, key: string, label: string, placeholder: string, hint: string): HTMLElement {
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', label));
  const inp = el('input', 'v') as HTMLInputElement;
  inp.placeholder = placeholder;
  if (obj[key] != null) inp.value = String(obj[key]);
  inp.onchange = () => {
    if (inp.value.trim() === '') delete obj[key];
    else obj[key] = inp.value;
    saveSettings();
  };
  f.appendChild(inp);
  if (hint) f.appendChild(el('div', 'hint', hint));
  return f;
}
function stBool(obj: any, key: string, label: string, hint: string, reRender = false): HTMLElement {
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', label));
  const on = obj[key] === true;
  const t = el('div', 'toggle' + (on ? ' on' : ''));
  t.innerHTML = `<span class="sw"></span><span class="lbl">${on ? 'true' : 'false'}</span>`;
  t.onclick = () => {
    const next = !t.classList.contains('on');
    t.classList.toggle('on', next);
    (t.querySelector('.lbl') as HTMLElement).textContent = String(next);
    obj[key] = next;
    saveSettings();
    if (reRender) renderEditor();
  };
  f.appendChild(t);
  if (hint) f.appendChild(el('div', 'hint', hint));
  return f;
}
function stGroup(title: string, subtitle: string, ...fields: HTMLElement[]): HTMLElement {
  // Category accent + glyph keyed off the section title, so each settings group
  // reads as part of a colour-coded family (execution / reliability / load /
  // scaling / observability).
  const cat = /mode/i.test(title) ? 'exec'
    : /trigger/i.test(title) ? 'trig'
      : /reliab|checkpoint|incremental/i.test(title) ? 'rel'
        : /load|performance/i.test(title) ? 'perf'
          : /scal/i.test(title) ? 'scale'
            : /observ|metric/i.test(title) ? 'obs' : 'exec';
  const glyph = { exec: '◧', trig: '⇥', rel: '❖', perf: '⚡', scale: '⤢', obs: '📈' }[cat] || '◧';
  const g = el('div', 'setgroup sg-' + cat);
  const open = !!(S.setOpen && S.setOpen[title]);
  g.classList.toggle('collapsed', !open);
  const h = el('div', 'setgroup-h');
  h.innerHTML = `<span class="caret">▸</span><span class="sgico">${glyph}</span><div class="sthead"><span class="stitle">${title}</span>${subtitle ? `<span class="sub">${subtitle}</span>` : ''}</div>`;
  const body = el('div', 'setgroup-b');
  fields.forEach((f) => body.appendChild(f));
  h.onclick = () => {
    const nowCollapsed = g.classList.toggle('collapsed');
    if (!S.setOpen) S.setOpen = {};
    S.setOpen[title] = !nowCollapsed;
  };
  g.appendChild(h);
  g.appendChild(body);
  return g;
}
// Build the fields for the Trigger settings group. The pipeline stores its
// chosen trigger inline as doc.trigger = { kind, settings }. Changing the kind
// resets the settings to that trigger's defaults. Non-public trigger kinds show
// a build-time note. The trigger→pipeline wiring is applied at deploy.
function triggerGroupFields(doc: any): HTMLElement[] {
  if (!doc.trigger || typeof doc.trigger !== 'object') doc.trigger = { kind: 'timer', settings: {} };
  const tr = doc.trigger;
  if (!tr.kind) tr.kind = 'timer';
  if (!tr.settings || typeof tr.settings !== 'object') tr.settings = {};
  const def = triggerDef(tr.kind);

  const fields: HTMLElement[] = [];
  // Trigger kind picker — resets settings to the new kind's defaults.
  const kindFld = el('div', 'fld');
  kindFld.appendChild(el('div', 'k', 'Trigger'));
  const sel = el('select', 'v') as HTMLSelectElement;
  TRIGGER_CATALOG.forEach((t) => {
    const op = document.createElement('option');
    op.value = t.kind; op.textContent = t.label;
    if (t.kind === tr.kind) op.selected = true;
    sel.appendChild(op);
  });
  sel.onchange = () => {
    const nd = triggerDef(sel.value);
    const settings: Record<string, unknown> = {};
    nd.fields.forEach((f) => { if (f.default !== undefined) settings[f.name] = f.default; });
    doc.trigger = { kind: nd.kind, settings };
    saveSettings();
    renderEditor();
  };
  kindFld.appendChild(sel);
  kindFld.appendChild(el('div', 'hint', def.summary));
  fields.push(kindFld);

  if (!def.available) {
    const warn = el('div', 'fld');
    warn.appendChild(el('div', 'hint', `⚠ ${def.note || 'This trigger needs an external module at build time.'} The wiring is generated in the deploy artifacts; local Run uses the timer/REST management path.`));
    fields.push(warn);
  }

  // Dynamic settings fields for the selected trigger.
  def.fields.forEach((f) => {
    const hint = f.hint || '';
    if (f.type === 'boolean') {
      if (tr.settings[f.name] === undefined && f.default !== undefined) tr.settings[f.name] = f.default;
      fields.push(stBool(tr.settings, f.name, f.label, hint));
    } else if (f.type === 'number') {
      fields.push(stNum(tr.settings, f.name, f.label, String(f.placeholder ?? f.default ?? ''), hint));
    } else if (f.type === 'select' && f.options) {
      fields.push(stSelect(tr.settings, f.name, f.label, f.options, hint));
    } else {
      fields.push(stText(tr.settings, f.name, f.label, String(f.placeholder ?? ''), hint));
    }
  });
  return fields;
}

function renderPipelineSettings(): HTMLElement {
  const insp = el('div', 'inspector');
  const ih = el('div', 'ih');
  ih.appendChild(el('span', 'ihname', '⚙ Pipeline Settings'));
  const closeBtn = el('span', 'ihx', '✕');
  closeBtn.title = 'Close';
  closeBtn.onclick = () => { S.selNode = null; renderEditor(); };
  ih.appendChild(closeBtn);
  insp.appendChild(ih);
  const ib = el('div', 'ib');
  const doc: any = S.doc;

  // First open: all sections start collapsed so the panel is a compact index —
  // users scan the group titles and expand only the one they need. Section
  // open/collapse state is remembered in S.setOpen thereafter.
  if (!S.setOpen) {
    S.setOpen = {
      'Execution mode': false,
      'App Properties': true,
      'Trigger · Invocation': false,
      'Reliability · Checkpoint & resume': false,
      'Reliability · Incremental load': false,
      'Load strategy · Performance': false,
      'Scaling': false,
      'Observability · Metrics': false,
    };
  }

  // ── Execution mode ─────────────────────────────────────────────────────────
  ib.appendChild(stGroup('Execution mode', 'How the pipeline moves data',
    stSelect(doc, 'mode', 'Mode', [
      ['etl', 'ETL — transform before load'],
      ['el', 'EL — extract & load (no transforms)'],
      ['elt', 'ELT — load then transform in-target'],
    ], 'ETL runs the transform chain in-flight; EL/ELT skip or defer it.'),
  ));

  // ── App Properties ─────────────────────────────────────────────────────────
  ib.appendChild(stGroup('App Properties', 'Environment-overridable named values ($property)',
    ...appPropertiesGroupFields()));

  // ── Trigger · invocation ───────────────────────────────────────────────────
  ib.appendChild(stGroup('Trigger · Invocation', 'How the pipeline is kicked off',
    ...triggerGroupFields(doc)));

  // ── Reliability: checkpoint & resume ───────────────────────────────────────
  const cp = settingsSection(doc, 'checkpoint');
  const cpType = String(cp.type || 'none');
  const cpFields: HTMLElement[] = [
    stSelect(cp, 'type', 'Checkpoint store', [
      ['none', 'None — no resume'],
      ['file', 'File — local directory'],
      ['redis', 'Redis — shared store'],
    ], 'Persist progress so an interrupted run resumes where it left off.', true),
  ];
  if (cpType === 'file') cpFields.push(stText(cp, 'path', 'Directory', './.checkpoints', 'Folder where checkpoint files are written.'));
  if (cpType === 'redis') {
    cpFields.push(stText(cp, 'addr', 'Redis address', 'localhost:6379', 'host:port of the Redis server.'));
    cpFields.push(stText(cp, 'keyPrefix', 'Key prefix', 'dp:ckpt:', 'Namespace for checkpoint keys.'));
    cpFields.push(stText(cp, 'passwordEnv', 'Password env var', 'REDIS_PASSWORD', 'Env var holding the Redis password.'));
  }
  if (cpType !== 'none') {
    cpFields.push(stNum(cp, 'intervalRows', 'Checkpoint every N rows', '10000', 'Save progress after this many rows (0 = default 10000).'));
    cpFields.push(stNum(cp, 'intervalSeconds', 'Checkpoint every N seconds', '30', 'Also save on this time interval (0 = default 30s).'));
    cpFields.push(stBool(cp, 'deleteOnSuccess', 'Delete checkpoint on success', 'Remove the checkpoint when the run completes cleanly.'));
  }
  ib.appendChild(stGroup('Reliability · Checkpoint & resume', 'Resume an interrupted run', ...cpFields));

  // ── Reliability: incremental (HWM) load ────────────────────────────────────
  const inc = settingsSection(doc, 'incremental');
  const incMode = String(inc.mode || 'none');
  const incFields: HTMLElement[] = [
    stSelect(inc, 'mode', 'Incremental load', [
      ['none', 'None — full load every run'],
      ['hwm', 'High-water mark — read only new rows'],
    ], 'Delta loading: only read rows newer than the last saved mark (needs an hwm transform).', true),
  ];
  if (incMode === 'hwm') {
    incFields.push(stText(inc, 'column', 'Watermark column', 'updated_at', 'Monotonic column tracked between runs.'));
    incFields.push(stSelect(inc, 'valueType', 'Value type', [
      ['datetime', 'datetime'], ['int', 'int'], ['string', 'string'],
    ], 'How the watermark column is ordered.'));
    incFields.push(stText(inc, 'initialValue', 'Initial value', '(empty = full first load)', 'Watermark to use on the very first run.'));
    const incStore = String(inc.storeType || 'file');
    incFields.push(stSelect(inc, 'storeType', 'Watermark store', [
      ['file', 'file'], ['redis', 'redis'], ['postgres', 'postgres'],
    ], 'Backend that persists the last mark between runs.', true));
    // Backend-specific connection settings live under storeOptions so the redis
    // and postgres watermark stores can actually reach their backend.
    const incOpts = settingsSection(inc, 'storeOptions');
    if (incStore === 'file') {
      incFields.push(stText(incOpts, 'path', 'Directory', '(system temp dir)', 'Folder where the .hwm.json watermark file is written.'));
    } else if (incStore === 'redis') {
      incFields.push(stText(incOpts, 'addr', 'Redis address', 'localhost:6379', 'host:port of the Redis server holding the watermark.'));
      incFields.push(stText(incOpts, 'keyPrefix', 'Key prefix', 'flogo:pipeline:hwm', 'Namespace for the watermark key.'));
      incFields.push(stText(incOpts, 'passwordEnv', 'Password env var', 'REDIS_PASSWORD', 'Env var holding the Redis password.'));
    } else if (incStore === 'postgres') {
      incFields.push(stText(incOpts, 'dsn', 'PostgreSQL DSN', 'host=… dbname=… user=…', 'Connection string for the watermark table.'));
    }
  }
  ib.appendChild(stGroup('Reliability · Incremental load', 'Read only new rows each run', ...incFields));

  // ── Performance / load strategy ────────────────────────────────────────────
  const perf = settingsSection(doc, 'performance');
  ib.appendChild(stGroup('Load strategy · Performance', 'Throughput & memory tuning',
    stNum(perf, 'batchSize', 'Batch size (rows)', '5000', 'Rows per in-flight batch. Higher = faster, more memory.'),
    stNum(perf, 'sourceBufferBatches', 'Read-ahead buffer (batches)', '4', 'Depth of the read-ahead buffer — the source reads ahead this many batches so reads overlap transform+write.'),
    stNum(perf, 'maxMemoryMB', 'Max in-flight memory (MB)', '512', 'Backpressure: the reader blocks when buffered in-flight bytes exceed this, bounding memory.'),
    stNum(perf, 'transformWorkers', 'Transform workers', '1', 'Parallel transform threads. When >1 and there is no HWM/DQ/schema-drift transform and checkpointing is off, transforms run across N workers with order-preserving output; otherwise processing stays ordered/single-threaded.'),
    stNum(perf, 'sinkBufferBatches', 'Sink buffer (batches)', '4', 'Reserved. Sink writes stay ordered so checkpoints remain consistent.'),
    stNum(perf, 'rowGroupParallelism', 'Parquet row-group parallelism', '4', 'Concurrent row-group readers — used by the Parquet source.'),
  ));

  // ── Scaling ────────────────────────────────────────────────────────────────
  const sc = settingsSection(doc, 'scaling');
  const scMode = String(sc.mode || 'single');
  const scFields: HTMLElement[] = [
    stSelect(sc, 'mode', 'Scaling mode', [
      ['single', 'Single — one instance'],
      ['sharded', 'Sharded — fixed instance set'],
      ['distributed', 'Distributed — work-queue coordinator'],
    ], 'How work is divided across instances.', true),
  ];
  if (scMode === 'sharded') {
    scFields.push(stNum(sc, 'instanceIndex', 'This instance index', '0', '0-based index of this instance.'));
    scFields.push(stNum(sc, 'instanceCount', 'Total instances', '2', 'Number of instances sharing the load.'));
    scFields.push(stSelect(sc, 'partitionStrategy', 'Partition by', [
      ['rowgroup', 'rowgroup'], ['file', 'file'], ['byterange', 'byterange'],
    ], 'How input is split across shards.'));
  }
  if (scMode === 'distributed') {
    scFields.push(stSelect(sc, 'partitionStrategy', 'Partition by', [
      ['rowgroup', 'rowgroup'], ['file', 'file'], ['byterange', 'byterange'],
    ], 'How input is split into claimable partitions.'));
    scFields.push(stNum(sc, 'partitionsPerClaim', 'Partitions per claim', '1', 'How many partitions an instance claims at once.'));
    const coord = settingsSection(sc, 'coordinator');
    if (!coord.type) coord.type = 'postgres';
    const coordType = String(coord.type);
    scFields.push(stSelect(coord, 'type', 'Coordinator', [
      ['postgres', 'PostgreSQL'],
      ['redis', 'Redis'],
    ], 'Shared work-queue backend that hands out partitions.', true));
    if (coordType === 'redis') {
      scFields.push(stText(coord, 'addr', 'Redis address', 'localhost:6379', 'host:port of the coordinator Redis.'));
      scFields.push(stText(coord, 'keyPrefix', 'Key prefix', 'flogo:pipeline:wq', 'Namespace for the work-queue keys.'));
      scFields.push(stText(coord, 'passwordEnv', 'Password env var', 'REDIS_PASSWORD', 'Env var holding the Redis password.'));
    } else {
      scFields.push(stText(coord, 'dsn', 'PostgreSQL DSN', 'host=… dbname=… user=…', 'Connection string for the coordinator DB.'));
    }
    scFields.push(stNum(coord, 'leaseSeconds', 'Lease TTL (s)', '120', 'How long a claim is held before it can be reclaimed.'));
    scFields.push(stNum(coord, 'heartbeatSeconds', 'Heartbeat (s)', '30', 'Lease-renewal interval.'));
    scFields.push(stNum(coord, 'maxRetries', 'Max retries', '3', 'Partition retry limit before it is marked failed.'));
  }
  ib.appendChild(stGroup('Scaling', 'Run across multiple instances', ...scFields));

  // ── Observability / metrics ────────────────────────────────────────────────
  const obs = settingsSection(doc, 'observability');
  const obsMetrics = String(obs.metrics || 'none');
  const obsFields: HTMLElement[] = [
    stSelect(obs, 'metrics', 'Metrics backend', [
      ['none', 'None'],
      ['prometheus', 'Prometheus — /metrics endpoint'],
      ['otel', 'OpenTelemetry — OTLP export'],
    ], 'Emit throughput, latency and error metrics.', true),
  ];
  const opts = settingsSection(obs, 'options');
  if (obsMetrics === 'prometheus') {
    obsFields.push(stText(opts, 'listenAddr', 'Listen address', ':9090', 'Address the Prometheus scrape endpoint binds to.'));
    obsFields.push(stText(opts, 'path', 'Metrics path', '/metrics', 'HTTP path for the scrape endpoint.'));
  }
  if (obsMetrics === 'otel') {
    obsFields.push(stText(opts, 'endpoint', 'OTLP endpoint', 'localhost:4317', 'gRPC endpoint of the OTel collector.'));
  }
  if (obsMetrics !== 'none') {
    obsFields.push(stNum(obs, 'stallSeconds', 'Stall threshold (s)', '30', 'Emit a stall event when a batch takes longer than this (−1 disables).'));
  }
  ib.appendChild(stGroup('Observability · Metrics', 'Monitor the pipeline', ...obsFields));

  // ── Observability / governance (audit log + OpenLineage) ────────────────────
  const auditObj = settingsSection(obs, 'audit');
  const lineageObj = settingsSection(obs, 'lineage');
  const govFields: HTMLElement[] = [];
  govFields.push(stBool(auditObj, 'enabled', 'Audit log', 'Append-only JSONL trail of run start/finish — component types + counters only, never secrets.', true));
  if (auditObj.enabled) {
    govFields.push(stText(auditObj, 'file', 'Audit file', 'blank = stdout', 'Append-only file path (or set the DP_AUDIT_FILE env var).'));
  }
  govFields.push(stBool(lineageObj, 'enabled', 'Lineage (OpenLineage)', 'Emit OpenLineage run events naming the source & sink datasets.', true));
  if (lineageObj.enabled) {
    govFields.push(stSelect(lineageObj, 'transport', 'Lineage transport', [['console', 'Console — JSON to stdout'], ['file', 'File — append JSONL'], ['http', 'HTTP — POST to a collector']], 'Where run events are sent.', true));
    const lt = String(lineageObj.transport || 'console');
    if (lt === 'http') govFields.push(stText(lineageObj, 'url', 'Collector URL', 'http://marquez:5000/api/v1/lineage', 'OpenLineage endpoint (or set OPENLINEAGE_URL).'));
    if (lt === 'file') govFields.push(stText(lineageObj, 'file', 'Lineage file', 'e.g. ./out/lineage.jsonl', 'Append-only JSONL file path.'));
    govFields.push(stText(lineageObj, 'namespace', 'Namespace', 'flogo-datapipeline', 'Default job / dataset namespace.'));
  }
  ib.appendChild(stGroup('Observability · Governance', 'Audit log & lineage', ...govFields));

  insp.appendChild(ib);
  attachResize(insp);
  return insp;
}

// Structured column editor for the source `columns` array.
// Conflict-key picker for the JDBC sink `conflictKey` — checkboxes over the
// columns visible at the sink, so users pick the upsert key instead of typing a
// JSON array. Only shown when onConflict is 'upsert'.
function renderConflictKeyEditor(node: Node): HTMLElement {
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', 'Conflict key (upsert match)'));
  const target = cfgTarget(node);
  const selected: string[] = Array.isArray(target.conflictKey) ? target.conflictKey.map(String) : [];
  const cols = schemaForNode(node).filter((c) => !c.derived);
  const box = el('div', 'cols');
  if (!cols.length) {
    // No declared columns — fall back to a free-text input.
    const inp = el('input', 'v') as HTMLInputElement;
    inp.value = selected.join(', ');
    inp.placeholder = 'e.g. id, tenant_id';
    inp.onchange = () => setCfgValue(node, 'conflictKey', inp.value.split(',').map((s) => s.trim()).filter(Boolean));
    box.appendChild(inp);
  } else {
    cols.forEach((c) => {
      const row = el('label', 'ckrow');
      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.checked = selected.includes(c.name);
      cb.onchange = () => {
        const next = new Set(selected);
        if (cb.checked) next.add(c.name); else next.delete(c.name);
        setCfgValue(node, 'conflictKey', cols.filter((x) => next.has(x.name)).map((x) => x.name));
      };
      row.appendChild(cb);
      row.appendChild(el('span', 'cklbl', `${escapeHtml(c.name)} <span class="ty">${escapeHtml(c.type)}</span>`));
      box.appendChild(row);
    });
  }
  f.appendChild(box);
  f.appendChild(el('div', 'hint', 'Rows matching these columns are updated instead of duplicated.'));
  return f;
}

// Structured column editor for the source `columns` array.
function renderColumnsEditor(node: Node): HTMLElement {
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', 'Columns'));
  const target = cfgTarget(node);
  const cols: Array<{ name?: string; type?: string; start?: number; length?: number }> =
    Array.isArray(target.columns) ? target.columns : [];
  const box = el('div', 'cols');
  const types = ['string', 'int', 'float', 'bool', 'date', 'decimal'];
  // Fixed-width columns carry byte start (1-based) + length; expose inputs for them.
  const isFixed = node.type === 'fixedwidth' || node.type === 'flatfile';
  const save = () => setCfgValue(node, 'columns', cols);
  cols.forEach((c, i) => {
    const row = el('div', 'crow');
    const name = document.createElement('input');
    name.value = c.name || '';
    name.placeholder = 'name';
    name.onchange = () => { cols[i].name = name.value; save(); };
    const sel = document.createElement('select');
    types.forEach((t) => {
      const op = document.createElement('option');
      op.value = t; op.textContent = t;
      if (t === c.type) op.selected = true;
      sel.appendChild(op);
    });
    sel.onchange = () => { cols[i].type = sel.value; save(); };
    const del = el('span', 'del', '✕');
    del.onclick = () => { cols.splice(i, 1); save(); renderEditor(); };
    row.appendChild(name);
    if (isFixed) {
      const start = document.createElement('input');
      start.type = 'number';
      start.className = 'fw-pos';
      start.title = 'Start (1-based byte position)';
      start.placeholder = 'start';
      start.style.width = '58px';
      if (typeof c.start === 'number') start.value = String(c.start);
      start.onchange = () => { cols[i].start = start.value === '' ? undefined : Number(start.value); save(); };
      const len = document.createElement('input');
      len.type = 'number';
      len.className = 'fw-pos';
      len.title = 'Length (byte count)';
      len.placeholder = 'len';
      len.style.width = '52px';
      if (typeof c.length === 'number') len.value = String(c.length);
      len.onchange = () => { cols[i].length = len.value === '' ? undefined : Number(len.value); save(); };
      row.appendChild(start);
      row.appendChild(len);
    }
    row.appendChild(sel);
    row.appendChild(del);
    box.appendChild(row);
  });
  const add = el('div', 'addcol', '＋ Add column');
  add.onclick = () => {
    const next: { name: string; type: string; start?: number; length?: number } = { name: 'new_column', type: 'string' };
    if (isFixed) {
      // Default the new column's start to just after the previous column.
      const prev = cols[cols.length - 1];
      next.start = prev && typeof prev.start === 'number' && typeof prev.length === 'number'
        ? prev.start + prev.length : 1;
      next.length = 10;
    }
    cols.push(next);
    setCfgValue(node, 'columns', cols);
    renderEditor();
  };
  box.appendChild(add);
  f.appendChild(box);
  if (isFixed) {
    f.appendChild(el('div', 'hint', 'Fixed-width: set each column\u2019s Start (1-based byte position) and Length.'));
  }

  // Auto-generate columns by sampling the source file (CSV/TSV/JSON-lines).
  // XML is excluded: it has its own "Detect structure from file" action on the
  // recordElement field; the generic header sampler would misread the XML
  // prolog (<?xml …?>) as a bogus single column.
  const target2 = cfgTarget(node);
  const filePath = typeof target2.path === 'string' ? target2.path : '';
  if (node.kind === 'source' && filePath && node.type !== 'xml') {
    const infer = el('button', 'browse infer-btn', '✨ Infer columns from file');
    infer.onclick = () => {
      infer.textContent = 'Sampling file…';
      inferTargetNode = node;
      post({
        type: 'inferSchema',
        path: filePath,
        sourceType: node.type,
        delimiter: typeof target2.delimiter === 'string' ? target2.delimiter : undefined,
        hasHeader: target2.hasHeader !== false,
      });
    };
    f.appendChild(infer);
    f.appendChild(el('div', 'hint', 'Reads the header + a sample of rows to guess column names and types.'));
  }

  // Read the schema straight from the source via the engine — works for
  // self-describing formats (Parquet footer) and for object-store sources where
  // the file isn't on the local disk. Available for any source stage.
  // Sources with a FIXED built-in schema (e.g. podlog) don't read a file, so the
  // action is labelled "Load Default Schema" to set the right expectation.
  if (node.kind === 'source') {
    const fixedSchema = !!SOURCE_FIXED_SCHEMA[String(node.type)];
    const readLabel = fixedSchema ? '⤓ Load Default Schema' : '⤓ Read schema from source';
    const loadingLabel = fixedSchema ? 'Loading schema…' : 'Reading schema…';
    const readBtn = el('button', 'browse infer-btn', S.srcSchemaLoading ? loadingLabel : readLabel);
    readBtn.style.marginTop = '6px';
    readBtn.onclick = () => { S.srcSchemaLoading = true; renderEditor(); post({ type: 'fetchSourceSchema' }); };
    f.appendChild(readBtn);
    f.appendChild(el('div', 'hint', fixedSchema
      ? 'Loads the fixed built-in schema this source always emits — no files are read.'
      : 'Opens the source (e.g. reads a Parquet footer) and fills columns from its actual schema.'));
  }
  return f;
}

// The source node awaiting an inferSchema response, so we can write the columns
// back into the right stage when the host replies.
let inferTargetNode: Node | null = null;

// Builtin mapper functions available to the map transform's expressions. These
// mirror the engine's `builtins` map (transform/mapper) — every function offered
// here actually evaluates at runtime. Grouped by category for the palette. Each
// snippet uses $1/$2/$3 caret markers so clicking inserts a ready-to-fill call.
const MAP_FUNCTIONS: Array<{ name: string; snippet: string; hint: string; cat: string }> = [
  // ── String ──
  { cat: 'String', name: 'concat', snippet: 'concat($1, $2)', hint: 'Join values into a string' },
  { cat: 'String', name: 'upper', snippet: 'upper($1)', hint: 'Uppercase a string' },
  { cat: 'String', name: 'lower', snippet: 'lower($1)', hint: 'Lowercase a string' },
  { cat: 'String', name: 'title', snippet: 'string.title($1)', hint: 'Title-case each word' },
  { cat: 'String', name: 'trim', snippet: 'trim($1)', hint: 'Strip surrounding whitespace' },
  { cat: 'String', name: 'length', snippet: 'string.length($1)', hint: 'Character count' },
  { cat: 'String', name: 'substring', snippet: 'string.substring($1, $2, $3)', hint: 'Slice by start/end index' },
  { cat: 'String', name: 'substringBefore', snippet: 'string.substringBefore($1, $2)', hint: 'Text before a separator' },
  { cat: 'String', name: 'substringAfter', snippet: 'string.substringAfter($1, $2)', hint: 'Text after a separator' },
  { cat: 'String', name: 'replace', snippet: 'string.replace($1, $2, $3)', hint: 'Replace all occurrences' },
  { cat: 'String', name: 'split', snippet: 'string.split($1, $2, $3)', hint: 'Split by separator, take Nth token' },
  { cat: 'String', name: 'join', snippet: 'string.join($1, $2, $3)', hint: 'Join values with a separator' },
  { cat: 'String', name: 'indexOf', snippet: 'string.indexOf($1, $2)', hint: 'Position of substring (-1 if absent)' },
  { cat: 'String', name: 'contains', snippet: 'string.contains($1, $2)', hint: 'True if substring present' },
  { cat: 'String', name: 'startsWith', snippet: 'string.startsWith($1, $2)', hint: 'True if prefix matches' },
  { cat: 'String', name: 'endsWith', snippet: 'string.endsWith($1, $2)', hint: 'True if suffix matches' },
  { cat: 'String', name: 'trimPrefix', snippet: 'string.trimPrefix($1, $2)', hint: 'Remove a leading prefix' },
  { cat: 'String', name: 'trimSuffix', snippet: 'string.trimSuffix($1, $2)', hint: 'Remove a trailing suffix' },
  { cat: 'String', name: 'padLeft', snippet: 'string.padLeft($1, $2, $3)', hint: 'Left-pad to width (fill char)' },
  { cat: 'String', name: 'padRight', snippet: 'string.padRight($1, $2, $3)', hint: 'Right-pad to width (fill char)' },
  { cat: 'String', name: 'repeat', snippet: 'string.repeat($1, $2)', hint: 'Repeat a string N times' },
  { cat: 'String', name: 'reverse', snippet: 'string.reverse($1)', hint: 'Reverse the characters' },
  { cat: 'String', name: 'equalsIgnoreCase', snippet: 'string.equalsIgnoreCase($1, $2)', hint: 'Case-insensitive equality' },
  // ── Regex ──
  { cat: 'Regex', name: 'match', snippet: 'regex.match($1, $2)', hint: 'True if pattern matches' },
  { cat: 'Regex', name: 'replace', snippet: 'regex.replace($1, $2, $3)', hint: 'Regex search-and-replace' },
  { cat: 'Regex', name: 'extract', snippet: 'regex.extract($1, $2, $3)', hint: 'Return a capture group' },
  // ── Number / Math ──
  { cat: 'Number', name: 'add', snippet: 'number.add($1, $2)', hint: 'a + b' },
  { cat: 'Number', name: 'subtract', snippet: 'number.subtract($1, $2)', hint: 'a − b' },
  { cat: 'Number', name: 'multiply', snippet: 'multiply($1, $2)', hint: 'a × b' },
  { cat: 'Number', name: 'divide', snippet: 'divide($1, $2)', hint: 'a ÷ b' },
  { cat: 'Number', name: 'mod', snippet: 'math.mod($1, $2)', hint: 'Remainder a mod b' },
  { cat: 'Number', name: 'round', snippet: 'number.round($1, $2)', hint: 'Round to N decimals' },
  { cat: 'Number', name: 'floor', snippet: 'math.floor($1)', hint: 'Round down' },
  { cat: 'Number', name: 'ceil', snippet: 'math.ceil($1)', hint: 'Round up' },
  { cat: 'Number', name: 'trunc', snippet: 'math.trunc($1)', hint: 'Drop the fraction' },
  { cat: 'Number', name: 'abs', snippet: 'math.abs($1)', hint: 'Absolute value' },
  { cat: 'Number', name: 'sign', snippet: 'math.sign($1)', hint: '-1, 0 or 1' },
  { cat: 'Number', name: 'pow', snippet: 'math.pow($1, $2)', hint: 'a to the power b' },
  { cat: 'Number', name: 'sqrt', snippet: 'math.sqrt($1)', hint: 'Square root' },
  { cat: 'Number', name: 'min', snippet: 'math.min($1, $2)', hint: 'Smaller of two' },
  { cat: 'Number', name: 'max', snippet: 'math.max($1, $2)', hint: 'Larger of two' },
  { cat: 'Number', name: 'clamp', snippet: 'clamp($1, $2, $3)', hint: 'Constrain between min and max' },
  { cat: 'Number', name: 'random', snippet: 'number.random($1)', hint: 'Random int below limit' },
  { cat: 'Number', name: 'scientific', snippet: 'scientific($1)', hint: 'Signed scientific notation' },
  // ── Date / Time ──
  { cat: 'Date/Time', name: 'now', snippet: 'datetime.now()', hint: 'Current UTC timestamp (RFC3339)' },
  { cat: 'Date/Time', name: 'today', snippet: 'datetime.today()', hint: "Today's date (YYYY-MM-DD)" },
  { cat: 'Date/Time', name: 'parse', snippet: 'datetime.parse($1, $2)', hint: 'Parse to RFC3339 (optional layout)' },
  { cat: 'Date/Time', name: 'format', snippet: 'datetime.format($1, $2)', hint: 'Format with a Go layout' },
  { cat: 'Date/Time', name: 'year', snippet: 'datetime.year($1)', hint: 'Extract the year' },
  { cat: 'Date/Time', name: 'month', snippet: 'datetime.month($1)', hint: 'Extract the month (1-12)' },
  { cat: 'Date/Time', name: 'day', snippet: 'datetime.day($1)', hint: 'Extract the day of month' },
  { cat: 'Date/Time', name: 'hour', snippet: 'datetime.hour($1)', hint: 'Extract the hour' },
  { cat: 'Date/Time', name: 'dayOfWeek', snippet: 'datetime.dayOfWeek($1)', hint: 'Weekday name' },
  { cat: 'Date/Time', name: 'addDays', snippet: 'datetime.addDays($1, $2)', hint: 'Add N days' },
  { cat: 'Date/Time', name: 'addHours', snippet: 'datetime.addHours($1, $2)', hint: 'Add N hours' },
  { cat: 'Date/Time', name: 'diffDays', snippet: 'datetime.diffDays($1, $2)', hint: 'Whole days between two dates' },
  { cat: 'Date/Time', name: 'epoch', snippet: 'datetime.epoch($1)', hint: 'Unix seconds since epoch' },
  { cat: 'Date/Time', name: 'fromEpoch', snippet: 'datetime.fromEpoch($1)', hint: 'Unix seconds → RFC3339' },
  // ── Convert ──
  { cat: 'Convert', name: 'toInt', snippet: 'coerce.toInt($1)', hint: 'Cast to integer' },
  { cat: 'Convert', name: 'toFloat', snippet: 'coerce.toFloat($1)', hint: 'Cast to float' },
  { cat: 'Convert', name: 'toString', snippet: 'coerce.toString($1)', hint: 'Cast to string' },
  { cat: 'Convert', name: 'toBool', snippet: 'coerce.toBool($1)', hint: 'Cast to boolean' },
  // ── Hash / Encode / Mask ──
  { cat: 'Hash & Encode', name: 'md5', snippet: 'hash.md5($1)', hint: 'MD5 hex digest (surrogate key)' },
  { cat: 'Hash & Encode', name: 'sha256', snippet: 'hash.sha256($1)', hint: 'SHA-256 hex digest' },
  { cat: 'Hash & Encode', name: 'base64Encode', snippet: 'encoding.base64Encode($1)', hint: 'Base64 encode' },
  { cat: 'Hash & Encode', name: 'base64Decode', snippet: 'encoding.base64Decode($1)', hint: 'Base64 decode' },
  { cat: 'Hash & Encode', name: 'uuid', snippet: 'util.uuid()', hint: 'Random UUID v4' },
  { cat: 'Hash & Encode', name: 'mask', snippet: 'util.mask($1, $2)', hint: 'Mask all but last N chars' },
  // ── Logic / Null ──
  { cat: 'Logic', name: 'iif', snippet: 'iif($1, $2, $3)', hint: 'If cond then a else b' },
  { cat: 'Logic', name: 'coalesce', snippet: 'coalesce($1, $2)', hint: 'First non-null value' },
  { cat: 'Logic', name: 'nvl', snippet: 'nvl($1, $2)', hint: 'Value, or fallback when null/empty' },
  { cat: 'Logic', name: 'nullif', snippet: 'nullif($1, $2)', hint: 'Null when the two are equal' },
  { cat: 'Logic', name: 'decode', snippet: 'decode($1, $2, $3)', hint: 'Value map (Oracle DECODE)' },
  { cat: 'Logic', name: 'equals', snippet: 'equals($1, $2)', hint: 'String equality' },
  { cat: 'Logic', name: 'between', snippet: 'between($1, $2, $3)', hint: 'True if lo ≤ v ≤ hi' },
  { cat: 'Logic', name: 'isNull', snippet: 'isNull($1)', hint: 'True when null' },
  { cat: 'Logic', name: 'isEmpty', snippet: 'isEmpty($1)', hint: 'True when empty string' },
];

// The currently focused mapping-expression input, so Available-data / Function
// chips insert at the caret of the row the user is editing.
let mapFocusedInput: HTMLInputElement | null = null;

// The output field currently selected in the mapper's Target pane, whose
// expression is shown/edited in the third (Mapping) pane. Persists across
// re-renders so selection survives a mapping edit.
let mapSelectedTarget: string | null = null;

// Free-text filter for the function palette, so the (large) function library
// stays quick to scan.
let mapFnFilter = '';

// Categories the user has collapsed in the function palette. Persists across
// re-renders. An active filter overrides this and expands matching groups.
const mapFnCollapsed = new Set<string>();

// The map transform stores dynamic expressions with a leading "=" (e.g.
// "=$order_date"), which the engine uses to distinguish an expression from a
// static string literal. For a cleaner experience the "=" is IMPLICIT in the
// UI: the input shows "$order_date" and we add the "=" back on save. Plain text
// that is NOT an expression stays a static literal (no "="), so existing
// literals keep working — matching the OOTB Flogo mapper, where field refs
// ($x), function calls (upper($x)) are expressions and bare text is a constant.
function exprForDisplay(stored: string): string {
  return typeof stored === 'string' && stored.startsWith('=') ? stored.slice(1) : (stored ?? '');
}
function exprForStore(display: string): string {
  const v = (display ?? '').trim();
  if (v === '') return '';
  if (v.startsWith('=')) return v; // already explicit
  // Looks like an expression → make the "=" explicit. Otherwise treat as a
  // static string literal (stored verbatim, no "=").
  const isExpr = v.startsWith('$') || /^[A-Za-z_][\w.]*\s*\(/.test(v);
  return isExpr ? '=' + v : v;
}

// Insert text at the caret of the focused mapping expression input (or append).
function insertIntoMapExpr(text: string) {
  const inp = mapFocusedInput;
  if (!inp) { toast('info', 'Click an expression field first, then pick a field or function'); return; }
  const start = inp.selectionStart ?? inp.value.length;
  const end = inp.selectionEnd ?? inp.value.length;
  inp.value = inp.value.slice(0, start) + text + inp.value.slice(end);
  // Place caret: if the snippet has a $1 marker, select it; else after text.
  const marker = text.indexOf('$1');
  if (marker >= 0) {
    inp.value = inp.value.replace('$1', '').replace('$2', '').replace('$3', '');
    inp.setSelectionRange(start + marker, start + marker);
  } else {
    inp.setSelectionRange(start + text.length, start + text.length);
  }
  inp.focus();
  inp.dispatchEvent(new Event('change'));
}

// Structured field-mapping editor for the `map` transform. Renders each output
// field as an "out ← =expression" row, plus click-to-insert palettes for the
// upstream columns (Available data) and builtin functions — the same building
// blocks as the Flogo mapper, right-sized for the flat map model.
function renderMapEditor(node: Node): HTMLElement {
  const f = el('div', 'fld');
  const target = cfgTarget(node);
  const fields: Record<string, string> = (target.fields && typeof target.fields === 'object') ? target.fields : {};
  let order: string[] = Array.isArray(target.fieldOrder) ? target.fieldOrder.filter((k: string) => k in fields) : [];
  for (const k of Object.keys(fields)) if (!order.includes(k)) order.push(k);

  const persist = () => {
    target.fields = fields;
    target.fieldOrder = order;
    setCfgValue(node, 'fields', fields);
  };
  // Set an output field's expression to a source path (=$path), adding the row
  // if it doesn't exist. Used by both drag-drop and click-to-map.
  const mapInto = (outName: string, path: string) => {
    if (!(outName in fields)) order.push(outName);
    fields[outName] = `=$${path}`;
    persist();
    renderEditor();
  };

  const srcTree = sourceFieldTree(node);
  const hasSchema = srcTree.length > 0;

  // ── Two-pane mapper: Source (left) → Target (right) ──────────────────────
  const mapper = el('div', 'mapper2');

  // LEFT: draggable source-field tree (complex fields expand to nested paths).
  const srcPane = el('div', 'mpane msrc');
  const srcHead = el('div', 'mphead');
  srcHead.innerHTML = `<span>Source</span><span class="cnt">${hasSchema ? srcTree.length : 0}</span>`;
  srcPane.appendChild(srcHead);
  const srcBody = el('div', 'mpbody');
  if (hasSchema) {
    const renderField = (fld: SrcField, depth: number, into: HTMLElement) => {
      const isMapped = Object.values(fields).includes(`=$${fld.path}`);
      const item = el('div', 'mfield' + (isMapped ? ' mapped' : ''));
      item.style.paddingLeft = `${6 + depth * 12}px`;
      const complex = fld.children && fld.children.length > 0;
      const caret = complex ? '<span class="mcaret">▸</span>' : '<span class="mcaret ph"></span>';
      item.innerHTML = `${caret}<span class="mfname">${escapeHtml(fld.name)}</span><span class="mtype">${escapeHtml(fld.type)}</span>`;
      item.setAttribute('draggable', 'true');
      item.title = `Drag onto a target field or the Mapping box, or click to map into the selected target. Path: $${fld.path}`;
      item.ondragstart = (e) => { e.dataTransfer?.setData('text/plain', fld.path); (e.dataTransfer || {} as any).effectAllowed = 'copy'; item.classList.add('dragging'); };
      item.ondragend = () => item.classList.remove('dragging');
      // Click: map into the SELECTED target field if one is selected; otherwise
      // create/fill a same-named output (drag-to-output shortcut).
      item.onclick = (e) => {
        if ((e.target as HTMLElement).classList.contains('mcaret') && complex) {
          item.classList.toggle('open'); kids.classList.toggle('show'); return;
        }
        if (mapSelectedTarget) { fields[mapSelectedTarget] = `=$${fld.path}`; persist(); renderEditor(); }
        else { mapInto(fld.name, fld.path); }
      };
      into.appendChild(item);
      const kids = el('div', 'mkids');
      if (complex) {
        fld.children!.forEach((ch) => renderField(ch, depth + 1, kids));
        into.appendChild(kids);
      }
    };
    srcTree.forEach((fld) => renderField(fld, 0, srcBody));
  } else {
    const empty = el('div', 'mapempty');
    empty.innerHTML = '<div style="font-size:11px;line-height:1.5;color:var(--dim)">No source fields yet. For Parquet/JSON or undeclared sources, load the live schema:</div>';
    const b = el('button', 'btn sm', '⌕ Load fields');
    b.style.marginTop = '6px';
    b.onclick = () => doPreview();
    empty.appendChild(b);
    srcBody.appendChild(empty);
  }
  srcPane.appendChild(srcBody);

  // Ensure a selected target exists so the Mapping pane has something to show.
  if (mapSelectedTarget && !order.includes(mapSelectedTarget)) mapSelectedTarget = null;
  if (!mapSelectedTarget && order.length) mapSelectedTarget = order[0];

  // MIDDLE: target output field names — click to select (its expression shows in
  // the Mapping pane); drop a source here to set/create the mapping directly.
  const tgtPane = el('div', 'mpane mtgt');
  const tgtHead = el('div', 'mphead');
  tgtHead.innerHTML = `<span>Target field</span><span class="cnt">${order.length}</span>`;
  tgtPane.appendChild(tgtHead);
  const tgtBody = el('div', 'mpbody');
  order.forEach((outName, i) => {
    const row = el('div', 'trow' + (outName === mapSelectedTarget ? ' sel' : ''));
    row.ondragover = (e) => { e.preventDefault(); row.classList.add('drop'); };
    row.ondragleave = () => row.classList.remove('drop');
    row.ondrop = (e) => {
      e.preventDefault(); row.classList.remove('drop');
      const p = e.dataTransfer?.getData('text/plain');
      if (p) { fields[outName] = `=$${p}`; mapSelectedTarget = outName; persist(); renderEditor(); }
    };
    const mapped = !!(fields[outName] && String(fields[outName]).trim());
    const out = document.createElement('input');
    out.className = 'tname';
    out.value = outName;
    out.title = 'Output field name';
    out.onfocus = () => { mapSelectedTarget = outName; };
    out.onchange = () => {
      const nn = out.value.trim();
      if (!nn || nn === outName) return;
      const val = fields[outName];
      delete fields[outName]; fields[nn] = val; order[i] = nn;
      if (mapSelectedTarget === outName) mapSelectedTarget = nn;
      persist(); renderEditor();
    };
    const dot = el('span', 'tdot' + (mapped ? ' on' : ''));
    dot.title = mapped ? 'Mapped' : 'Unmapped';
    const del = el('span', 'del', '✕');
    del.title = 'Remove field';
    del.onclick = (e) => { e.stopPropagation(); delete fields[outName]; order = order.filter((k) => k !== outName); if (mapSelectedTarget === outName) mapSelectedTarget = null; persist(); renderEditor(); };
    row.appendChild(dot); row.appendChild(out); row.appendChild(del);
    row.onclick = (e) => { if (e.target === row || (e.target as HTMLElement).classList.contains('tdot')) { mapSelectedTarget = outName; renderEditor(); } };
    tgtBody.appendChild(row);
  });
  const addZone = el('div', 'maddzone');
  addZone.textContent = '＋ Add field (or drop a source)';
  addZone.ondragover = (e) => { e.preventDefault(); addZone.classList.add('drop'); };
  addZone.ondragleave = () => addZone.classList.remove('drop');
  addZone.ondrop = (e) => {
    e.preventDefault(); addZone.classList.remove('drop');
    const p = e.dataTransfer?.getData('text/plain');
    if (p) { const leaf = p.split('.').pop() || p; mapInto(leaf, p); mapSelectedTarget = leaf; }
  };
  addZone.onclick = () => {
    let base = 'new_field', n = base, c = 1;
    while (n in fields) n = base + '_' + c++;
    fields[n] = ''; order.push(n); mapSelectedTarget = n; persist(); renderEditor();
  };
  tgtBody.appendChild(addZone);
  tgtPane.appendChild(tgtBody);

  // RIGHT: the Mapping pane — a large expression editor for the SELECTED target
  // field, plus a function palette. Drop a source field here to insert its path.
  const mapPane = el('div', 'mpane mmap');
  const mapHead = el('div', 'mphead');
  mapHead.innerHTML = `<span>Mapping</span>` + (mapSelectedTarget ? `<span class="cnt">${escapeHtml(mapSelectedTarget)}</span>` : '');
  mapPane.appendChild(mapHead);
  const mapBody = el('div', 'mpbody mmapbody');
  if (mapSelectedTarget) {
    const sel = mapSelectedTarget;
    mapBody.appendChild(el('div', 'mmaplbl', `${escapeHtml(sel)} ←`));
    const ta = document.createElement('textarea');
    ta.className = 'mexpr';
    ta.value = exprForDisplay(fields[sel]);
    ta.placeholder = 'drag a Source field here, or type: $field, upper($field), "constant"';
    ta.rows = 3;
    ta.onfocus = () => { mapFocusedInput = ta as unknown as HTMLInputElement; };
    ta.onchange = () => { fields[sel] = exprForStore(ta.value); persist(); renderEditor(); };
    ta.ondragover = (e) => { e.preventDefault(); ta.classList.add('drop'); };
    ta.ondragleave = () => ta.classList.remove('drop');
    ta.ondrop = (e) => {
      e.preventDefault(); ta.classList.remove('drop');
      const p = e.dataTransfer?.getData('text/plain');
      if (!p) return;
      // Insert at caret if focused/has selection, else replace.
      const cur = ta.value;
      const at = ta.selectionStart ?? cur.length;
      const token = `$${p}`;
      ta.value = cur.slice(0, at) + token + cur.slice(ta.selectionEnd ?? at);
      fields[sel] = exprForStore(ta.value); persist(); renderEditor();
    };
    mapBody.appendChild(ta);
    // Function palette — grouped by category, with a filter box. Clicking a chip
    // inserts a ready-to-fill call into the expression above.
    const fnwrap = el('div', 'mmapfns');
    const fnhdr = el('div', 'mmapfns-hdr');
    fnhdr.appendChild(el('span', 'lbl', 'Functions'));
    const filter = document.createElement('input');
    filter.className = 'mfnfilter';
    filter.placeholder = 'filter…';
    filter.value = mapFnFilter;
    filter.oninput = () => { mapFnFilter = filter.value; renderGroups(); filter.focus(); };
    fnhdr.appendChild(filter);
    fnwrap.appendChild(fnhdr);
    const groupsHost = el('div', 'mfngroups');
    fnwrap.appendChild(groupsHost);
    const renderGroups = () => {
      groupsHost.innerHTML = '';
      const q = mapFnFilter.trim().toLowerCase();
      const cats: string[] = [];
      for (const fn of MAP_FUNCTIONS) if (!cats.includes(fn.cat)) cats.push(fn.cat);
      cats.forEach((cat) => {
        const items = MAP_FUNCTIONS.filter((fn) => fn.cat === cat &&
          (!q || fn.name.toLowerCase().includes(q) || fn.hint.toLowerCase().includes(q) || cat.toLowerCase().includes(q)));
        if (!items.length) return;
        // A group is collapsed when the user has toggled it shut — but an active
        // filter force-expands every matching group so results are always visible.
        const collapsed = !q && mapFnCollapsed.has(cat);
        const grp = el('div', 'mfngroup' + (collapsed ? ' collapsed' : ''));
        const head = el('div', 'mfncat');
        head.innerHTML = `<span class="mfncaret">▾</span><span class="mfncatname">${escapeHtml(cat)}</span><span class="mfncount">${items.length}</span>`;
        head.onclick = () => {
          if (mapFnCollapsed.has(cat)) mapFnCollapsed.delete(cat); else mapFnCollapsed.add(cat);
          renderGroups();
        };
        grp.appendChild(head);
        const chips = el('div', 'chips');
        items.forEach((fn) => {
          const chip = el('span', 'chip2 fn', `ƒ ${fn.name}`);
          chip.title = fn.hint;
          chip.onmousedown = (e) => e.preventDefault();
          chip.onclick = () => { mapFocusedInput = ta as unknown as HTMLInputElement; insertIntoMapExpr(fn.snippet); ta.dispatchEvent(new Event('change')); };
          chips.appendChild(chip);
        });
        grp.appendChild(chips);
        groupsHost.appendChild(grp);
      });
      if (!groupsHost.childElementCount) groupsHost.appendChild(el('div', 'focushint', 'No functions match.'));
    };
    renderGroups();
    mapBody.appendChild(fnwrap);
    mapBody.appendChild(el('div', 'focushint', 'Drag a Source field here or type it. = is implicit — $field, upper($field); plain text is a constant.'));
  } else {
    mapBody.appendChild(el('div', 'muted', '<div style="font-size:11px;padding:8px;color:var(--dim)">Select a Target field (or add one) to edit its mapping here.</div>'));
  }
  mapPane.appendChild(mapBody);

  mapper.appendChild(srcPane);
  mapper.appendChild(tgtPane);
  mapper.appendChild(mapPane);

  f.appendChild(el('div', 'k', 'Field mapping'));
  if (hasSchema) {
    const allBtn = el('button', 'btn sm mapallbtn', '⧉ Map all fields (passthrough)');
    allBtn.onclick = () => {
      srcTree.forEach((c) => { if (!(c.name in fields)) { fields[c.name] = `=$${c.name}`; order.push(c.name); } });
      persist(); renderEditor();
    };
    f.appendChild(allBtn);
  }
  f.appendChild(mapper);
  return f;
}



// Structured rule editor for the `validate` transform. Each rule is a card with
// a field picker (from upstream columns) and plain-language constraints — not
// null, allowed values, min/max, length, pattern — instead of hand-written JSON.
function renderRulesEditor(node: Node): HTMLElement {
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', 'Validation rules'));
  const target = cfgTarget(node);
  const rules: any[] = Array.isArray(target.rules) ? target.rules : [];
  const cols = upstreamSchema(node);
  const save = () => setCfgValue(node, 'rules', rules);

  const box = el('div', 'rcards');
  rules.forEach((r, i) => {
    const card = el('div', 'rcard');
    // header: field picker + delete
    const head = el('div', 'rhead');
    const fld = document.createElement(cols.length ? 'select' : 'input') as HTMLSelectElement | HTMLInputElement;
    if (cols.length) {
      const opts = cols.map((c) => c.name);
      if (r.field && !opts.includes(r.field)) opts.unshift(r.field);
      opts.forEach((o) => {
        const op = document.createElement('option');
        op.value = o; op.textContent = o;
        if (o === r.field) (op as HTMLOptionElement).selected = true;
        fld.appendChild(op);
      });
    } else {
      (fld as HTMLInputElement).placeholder = 'field name';
      (fld as HTMLInputElement).value = r.field || '';
    }
    fld.onchange = () => { r.field = (fld as HTMLInputElement).value; save(); };
    const del = el('span', 'del', '✕');
    del.title = 'Remove rule';
    del.onclick = () => { rules.splice(i, 1); save(); renderEditor(); };
    head.appendChild(el('span', 'muted', '<span style="font-size:11px">Field</span>'));
    head.appendChild(fld);
    head.appendChild(del);
    card.appendChild(head);

    // required toggle
    const req = el('div', 'rtoggle' + (r.notNull ? ' on' : ''));
    req.innerHTML = '<span class="sw"></span><span class="lbl">Required (not null)</span>';
    req.onclick = () => { r.notNull = !r.notNull; if (!r.notNull) delete r.notNull; save(); renderEditor(); };
    card.appendChild(req);

    // helper to render an optional constraint row
    const constraint = (label: string, key: string, ph: string, transform?: (v: string) => any) => {
      const row = el('div', 'rrow');
      row.appendChild(el('label', undefined, label));
      const inp = document.createElement('input');
      inp.placeholder = ph;
      inp.value = r[key] != null ? (Array.isArray(r[key]) ? r[key].join(', ') : String(r[key])) : '';
      inp.onchange = () => {
        const v = inp.value.trim();
        if (v === '') { delete r[key]; } else { r[key] = transform ? transform(v) : v; }
        save();
      };
      row.appendChild(inp);
      card.appendChild(row);
    };
    constraint('In set', 'in', 'emea, amer, apac', (v) => v.split(',').map((s) => s.trim()).filter(Boolean));
    constraint('Min', 'min', 'e.g. 0', (v) => Number(v));
    constraint('Max', 'max', 'e.g. 1000', (v) => Number(v));
    constraint('Min len', 'minLength', 'e.g. 3', (v) => Number(v));
    constraint('Max len', 'maxLength', 'e.g. 50', (v) => Number(v));
    constraint('Pattern', 'pattern', 'regex, e.g. ^CUST-\\d{6}$');
    box.appendChild(card);
  });
  const add = el('button', 'addcard', '＋ Add rule');
  add.onclick = () => {
    rules.push({ field: cols[0]?.name || '', notNull: true });
    setCfgValue(node, 'rules', rules);
    renderEditor();
  };
  box.appendChild(add);
  f.appendChild(box);
  return f;
}

// Catalogue of DQ expectation types with their parameter shapes, mirroring the
// engine's expectationFactories. Each entry declares which fields to render.
const DQ_EXPECTATIONS: Array<{ type: string; label: string; params: Array<{ key: string; label: string; kind: 'number' | 'text' | 'list' | 'bool' }> }> = [
  { type: 'expectColumnNotNull', label: 'Column not null', params: [{ key: 'threshold', label: 'Threshold', kind: 'number' }] },
  { type: 'expectColumnUnique', label: 'Column unique', params: [{ key: 'threshold', label: 'Threshold', kind: 'number' }] },
  { type: 'expectColumnValuesBetween', label: 'Values between', params: [{ key: 'min', label: 'Min', kind: 'number' }, { key: 'max', label: 'Max', kind: 'number' }] },
  { type: 'expectColumnValuesInSet', label: 'Values in set', params: [{ key: 'values', label: 'Allowed', kind: 'list' }, { key: 'caseSensitive', label: 'Case-sensitive', kind: 'bool' }] },
  { type: 'expectTableRowCountBetween', label: 'Row count between', params: [{ key: 'min', label: 'Min', kind: 'number' }, { key: 'max', label: 'Max', kind: 'number' }] },
  { type: 'expectColumnMeanBetween', label: 'Mean between', params: [{ key: 'min', label: 'Min', kind: 'number' }, { key: 'max', label: 'Max', kind: 'number' }] },
  { type: 'expectColumnStddevBelow', label: 'Std-dev below', params: [{ key: 'threshold', label: 'Threshold', kind: 'number' }] },
];
const DQ_NEEDS_COLUMN = new Set(['expectColumnNotNull', 'expectColumnUnique', 'expectColumnValuesBetween', 'expectColumnValuesInSet', 'expectColumnMeanBetween', 'expectColumnStddevBelow']);

// Structured expectation editor for the `dq` transform. Each expectation is a
// card: a type picker (plain-language label), a column picker, and only the
// parameters that type needs.
function renderExpectationsEditor(node: Node): HTMLElement {
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', 'Expectations'));
  const target = cfgTarget(node);
  const exps: any[] = Array.isArray(target.expectations) ? target.expectations : [];
  const cols = upstreamSchema(node);
  const save = () => setCfgValue(node, 'expectations', exps);

  const box = el('div', 'rcards');
  exps.forEach((ex, i) => {
    const spec = DQ_EXPECTATIONS.find((d) => d.type === ex.type) || DQ_EXPECTATIONS[0];
    const card = el('div', 'rcard');
    const head = el('div', 'rhead');
    const typeSel = document.createElement('select');
    DQ_EXPECTATIONS.forEach((d) => {
      const op = document.createElement('option');
      op.value = d.type; op.textContent = d.label;
      if (d.type === ex.type) op.selected = true;
      typeSel.appendChild(op);
    });
    typeSel.onchange = () => { ex.type = typeSel.value; save(); renderEditor(); };
    const del = el('span', 'del', '✕');
    del.onclick = () => { exps.splice(i, 1); save(); renderEditor(); };
    head.appendChild(typeSel);
    head.appendChild(del);
    card.appendChild(head);

    if (DQ_NEEDS_COLUMN.has(ex.type)) {
      const row = el('div', 'rrow');
      row.appendChild(el('label', undefined, 'Column'));
      const colSel = document.createElement(cols.length ? 'select' : 'input') as HTMLSelectElement | HTMLInputElement;
      if (cols.length) {
        const opts = cols.map((c) => c.name);
        if (ex.column && !opts.includes(ex.column)) opts.unshift(ex.column);
        opts.forEach((o) => {
          const op = document.createElement('option');
          op.value = o; op.textContent = o;
          if (o === ex.column) (op as HTMLOptionElement).selected = true;
          colSel.appendChild(op);
        });
      } else {
        (colSel as HTMLInputElement).placeholder = 'column name';
        (colSel as HTMLInputElement).value = ex.column || '';
      }
      colSel.onchange = () => { ex.column = (colSel as HTMLInputElement).value; save(); };
      row.appendChild(colSel);
      card.appendChild(row);
    }

    spec.params.forEach((p) => {
      if (p.kind === 'bool') {
        const tg = el('div', 'rtoggle' + (ex[p.key] ? ' on' : ''));
        tg.innerHTML = `<span class="sw"></span><span class="lbl">${p.label}</span>`;
        tg.onclick = () => { ex[p.key] = !ex[p.key]; if (!ex[p.key]) delete ex[p.key]; save(); renderEditor(); };
        card.appendChild(tg);
        return;
      }
      const row = el('div', 'rrow');
      row.appendChild(el('label', undefined, p.label));
      const inp = document.createElement('input');
      if (p.kind === 'number') inp.type = 'number';
      inp.placeholder = p.kind === 'list' ? 'comma-separated' : '';
      inp.value = ex[p.key] != null ? (Array.isArray(ex[p.key]) ? ex[p.key].join(', ') : String(ex[p.key])) : '';
      inp.onchange = () => {
        const v = inp.value.trim();
        if (v === '') { delete ex[p.key]; }
        else if (p.kind === 'number') ex[p.key] = Number(v);
        else if (p.kind === 'list') ex[p.key] = v.split(',').map((s) => s.trim()).filter(Boolean);
        else ex[p.key] = v;
        save();
      };
      row.appendChild(inp);
      card.appendChild(row);
    });
    box.appendChild(card);
  });
  const add = el('button', 'addcard', '＋ Add expectation');
  add.onclick = () => {
    exps.push({ type: 'expectColumnNotNull', column: cols[0]?.name || '' });
    setCfgValue(node, 'expectations', exps);
    renderEditor();
  };
  box.appendChild(add);
  f.appendChild(box);
  return f;
}

// Filter expression field with a click-to-insert Available-data palette and an
// operators cheat-sheet, so users build boolean conditions without memorising
// syntax.
function renderFilterEditor(node: Node): HTMLElement {
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', 'Keep rows where'));
  const target = cfgTarget(node);
  const inp = document.createElement('input');
  inp.className = 'v';
  inp.value = target.expr || '';
  inp.placeholder = "amount > 0 AND status != 'CANCELLED'";
  inp.onfocus = () => { mapFocusedInput = inp; };
  inp.onchange = () => setCfgField(node, 'expr', inp.value);
  f.appendChild(inp);

  const help = el('div', 'mhelp');
  const cols = upstreamSchema(node);
  if (cols.length) {
    help.appendChild(el('div', 'lbl', 'Available data'));
    const chips = el('div', 'chips');
    cols.forEach((c) => {
      const chip = el('span', 'chip2', c.name);
      chip.title = `Insert ${c.name}`;
      chip.onclick = () => insertIntoMapExpr(c.name);
      chips.appendChild(chip);
    });
    help.appendChild(chips);
  }
  help.appendChild(el('div', 'lbl', 'Operators'));
  const ops = ['=', '!=', '>', '>=', '<', '<=', 'AND', 'OR'];
  const ochips = el('div', 'chips');
  ops.forEach((o) => {
    const chip = el('span', 'chip2 fn', o);
    chip.onclick = () => insertIntoMapExpr(` ${o} `);
    ochips.appendChild(chip);
  });
  help.appendChild(ochips);
  help.appendChild(el('div', 'focushint', "Rows matching this condition are kept; the rest are dropped. Quote text literals, e.g. status != 'CANCELLED'."));
  f.appendChild(help);
  return f;
}

// ── render root ───────────────────────────────────────────────────────────
const WIN_KINDS = ['tumbling', 'sliding', 'session', 'global'];
const AGG_FNS = ['sum', 'avg', 'min', 'max', 'count', 'first', 'last'];
const TIME_FORMATS = ['RFC3339', 'RFC3339Nano', 'DateTime', 'DateOnly', 'epochMillis'];

// Shared structured-editor field builders (used by the window and join editors).
function fieldHint(h: string): HTMLElement {
  return el('div', 'muted', `<div style="font-size:11px;padding:2px 0 6px">${h}</div>`);
}
function fieldInput(label: string, value: any, ph: string, onChange: (v: string) => void, hint?: string): HTMLElement {
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', label));
  const i = el('input', 'v') as HTMLInputElement;
  i.placeholder = ph;
  i.value = value != null ? String(value) : '';
  i.onchange = () => onChange(i.value.trim());
  f.appendChild(i);
  if (hint) f.appendChild(fieldHint(hint));
  return f;
}
function fieldSelect(label: string, value: string, opts: string[], onChange: (v: string) => void, hint?: string, emptyLabel = '(select)'): HTMLElement {
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', label));
  const s = el('select', 'v') as HTMLSelectElement;
  const list = (opts.includes(value) || !value) ? opts : [value, ...opts];
  list.forEach((o) => {
    const op = document.createElement('option');
    op.value = o;
    op.textContent = o || emptyLabel;
    if (o === value) op.selected = true;
    s.appendChild(op);
  });
  s.onchange = () => onChange(s.value);
  f.appendChild(s);
  if (hint) f.appendChild(fieldHint(hint));
  return f;
}

// Structured editor for the `window` transform: window kind + size, event-time
// column/format, group-by keys, an aggregations builder, and watermark / lateness
// / TTL controls. Reads and writes the transform object (nested config) directly.
function renderWindowEditor(node: Node): HTMLElement {
  const wrap = el('div');
  const t: any = cfgTarget(node);
  t.window = t.window || {};
  t.eventTime = t.eventTime || {};
  t.watermark = t.watermark || {};
  if (!Array.isArray(t.groupBy)) t.groupBy = [];
  if (!Array.isArray(t.aggregations)) t.aggregations = [];
  const colNames = upstreamSchema(node).map((c) => c.name);
  const persist = () => persistAndRebuild();
  const selF = fieldSelect;
  const inpF = fieldInput;

  const kind = t.window.kind || 'tumbling';
  wrap.appendChild(selF('Window type', kind, WIN_KINDS, (v) => { t.window.kind = v; persist(); renderEditor(); }));
  if (kind === 'tumbling' || kind === 'sliding') wrap.appendChild(inpF('Window size (ms)', t.window.sizeMs, 'e.g. 60000', (v) => { t.window.sizeMs = Number(v) || 0; persist(); }, '60000 = 1 minute'));
  if (kind === 'sliding') wrap.appendChild(inpF('Slide (ms)', t.window.slideMs, 'e.g. 30000', (v) => { t.window.slideMs = Number(v) || 0; persist(); }, 'How often a new window starts'));
  if (kind === 'session') wrap.appendChild(inpF('Inactivity gap (ms)', t.window.gapMs, 'e.g. 300000', (v) => { t.window.gapMs = Number(v) || 0; persist(); }, 'Close a session after this idle gap'));

  wrap.appendChild(fieldSelect('Event-time column', t.eventTime.column || '', colNames.length ? ['', ...colNames] : [''], (v) => { if (v) t.eventTime.column = v; else delete t.eventTime.column; persist(); renderEditor(); }, 'Blank = processing time (wall clock)', '(processing time)'));
  if (t.eventTime.column) wrap.appendChild(selF('Time format', t.eventTime.format || 'RFC3339', TIME_FORMATS, (v) => { t.eventTime.format = v; persist(); }, 'Numeric columns are treated as epoch millis'));

  wrap.appendChild(inpF('Allowed lateness (ms)', t.watermark.maxLatenessMs, '0', (v) => { t.watermark.strategy = 'boundedOutOfOrderness'; t.watermark.maxLatenessMs = Number(v) || 0; persist(); }, 'Out-of-order tolerance before a window fires'));
  wrap.appendChild(inpF('State TTL (ms)', t.stateTtlMs, 'optional', (v) => { if (v === '') delete t.stateTtlMs; else t.stateTtlMs = Number(v) || 0; persist(); }, 'Drop abandoned window state older than this (bounds memory)'));

  // Spillable state — bound memory for high-cardinality key spaces by paging
  // colder keys to disk (state.maxResidentKeys / state.spillDir).
  const stateCfg = t.state || {};
  wrap.appendChild(inpF('Max resident keys (spill)', stateCfg.maxResidentKeys, 'optional', (v) => {
    if (v === '') { if (t.state) { delete t.state.maxResidentKeys; if (!t.state.spillDir) delete t.state; } }
    else { t.state = t.state || {}; t.state.maxResidentKeys = Number(v) || 0; }
    persist(); renderEditor();
  }, 'Keep at most N keys in memory; page the rest to disk. Blank = all in memory'));
  if (stateCfg.maxResidentKeys) wrap.appendChild(inpF('Spill directory', stateCfg.spillDir, 'optional (OS temp)', (v) => {
    if (v === '') { if (t.state) delete t.state.spillDir; }
    else { t.state = t.state || {}; t.state.spillDir = v; }
    persist();
  }, 'Directory for paged-out state files'));

  // Trigger (early firing) + accumulation mode.
  const trig = t.trigger || {};
  wrap.appendChild(inpF('Early firing every (ms)', trig.earlyEveryMs, 'optional', (v) => {
    if (v === '') { if (t.trigger) { delete t.trigger.earlyEveryMs; if (!Object.keys(t.trigger).length) delete t.trigger; } }
    else { t.trigger = t.trigger || {}; t.trigger.earlyEveryMs = Number(v) || 0; }
    persist(); renderEditor();
  }, 'Emit partial results before a window closes (blank = only on close)'));
  if (trig.earlyEveryMs) wrap.appendChild(selF('Accumulation', t.accumulation || 'discarding', ['discarding', 'accumulating'], (v) => { t.accumulation = v; persist(); }, 'accumulating = running totals; discarding = deltas since last firing'));

  // Group by
  const gbF = el('div', 'fld');
  gbF.appendChild(el('div', 'k', 'Group by (aggregation keys)'));
  if (colNames.length) {
    const box = el('div');
    box.style.cssText = 'display:flex;flex-wrap:wrap;gap:8px;padding:4px 0';
    colNames.forEach((c) => {
      const lab = document.createElement('label'); lab.style.cssText = 'display:inline-flex;align-items:center;gap:4px;font-size:12px';
      const cb = document.createElement('input'); cb.type = 'checkbox'; cb.checked = t.groupBy.includes(c);
      cb.onchange = () => { if (cb.checked) { if (!t.groupBy.includes(c)) t.groupBy.push(c); } else t.groupBy = t.groupBy.filter((x: string) => x !== c); persist(); };
      lab.appendChild(cb); lab.appendChild(document.createTextNode(c));
      box.appendChild(lab);
    });
    gbF.appendChild(box);
  } else {
    const i = el('input', 'v') as HTMLInputElement; i.placeholder = 'comma-separated keys'; i.value = t.groupBy.join(', ');
    i.onchange = () => { t.groupBy = i.value.split(',').map((s) => s.trim()).filter(Boolean); persist(); };
    gbF.appendChild(i);
  }
  wrap.appendChild(gbF);

  // Aggregations
  const aggF = el('div', 'fld');
  aggF.appendChild(el('div', 'k', 'Aggregations'));
  const cards = el('div', 'rcards');
  (t.aggregations as any[]).forEach((a, i) => {
    const card = el('div', 'rcard');
    const head = el('div', 'rhead');
    const asI = el('input') as HTMLInputElement; asI.placeholder = 'output name'; asI.value = a.as || ''; asI.style.flex = '1'; asI.onchange = () => { a.as = asI.value.trim(); persist(); };
    const fnS = el('select') as HTMLSelectElement; AGG_FNS.forEach((o) => { const op = document.createElement('option'); op.value = o; op.textContent = o; if (o === a.fn) op.selected = true; fnS.appendChild(op); }); fnS.onchange = () => { a.fn = fnS.value; persist(); renderEditor(); };
    const del = el('span', 'del', '✕'); del.title = 'Remove'; del.onclick = () => { (t.aggregations as any[]).splice(i, 1); persist(); renderEditor(); };
    head.appendChild(asI); head.appendChild(fnS); head.appendChild(del);
    card.appendChild(head);
    if (a.fn !== 'count') {
      const row = el('div', 'rrow');
      row.appendChild(el('label', undefined, 'Column'));
      let col: HTMLSelectElement | HTMLInputElement;
      if (colNames.length) {
        const s = el('select') as HTMLSelectElement;
        const opts = a.column && !colNames.includes(a.column) ? [a.column, ...colNames] : colNames;
        opts.forEach((o) => { const op = document.createElement('option'); op.value = o; op.textContent = o; if (o === a.column) op.selected = true; s.appendChild(op); });
        col = s;
      } else {
        const inp2 = el('input') as HTMLInputElement; inp2.placeholder = 'column'; inp2.value = a.column || '';
        col = inp2;
      }
      col.onchange = () => { a.column = (col as HTMLInputElement).value; persist(); };
      row.appendChild(col);
      card.appendChild(row);
    }
    cards.appendChild(card);
  });
  const add = el('button', 'addcard', '＋ Add aggregation'); add.onclick = () => { (t.aggregations as any[]).push({ as: 'value', fn: 'count' }); persist(); renderEditor(); };
  cards.appendChild(add);
  aggF.appendChild(cards);
  wrap.appendChild(aggF);

  wrap.appendChild(selF('On late record', t.onLate || 'drop', ['drop', 'route'], (v) => { t.onLate = v; persist(); renderEditor(); }, 'route → error sink (drop discards late rows)'));
  if (t.onLate === 'route') {
    wrap.appendChild(selF('Late sink type', t.errorSinkType || 'jsonlines', ['jsonlines', 'csv', 'kafka', 'null'], (v) => { t.errorSinkType = v; persist(); }, 'Where late rows are routed'));
    const esc = t.errorSinkConfig || {};
    wrap.appendChild(inpF('Late sink path', esc.path, 'e.g. ./out/late.jsonl', (v) => { t.errorSinkConfig = t.errorSinkConfig || {}; if (v === '') delete t.errorSinkConfig.path; else t.errorSinkConfig.path = v; persist(); }, 'File path (file sinks) — or broker/topic for kafka'));
  }

  return wrap;
}

// Structured editor for the `join` transform: match keys, join type, and the
// right-side reference source (loaded once into a keyed table).
function renderJoinEditor(node: Node): HTMLElement {
  const wrap = el('div');
  const t: any = cfgTarget(node);
  t.right = t.right || { type: 'csv' };
  const colNames = upstreamSchema(node).map((c) => c.name);
  const persist = () => persistAndRebuild();
  const inpF = fieldInput;
  const selF = fieldSelect;

  if (colNames.length) {
    wrap.appendChild(selF('Left key (stream column)', t.leftKey || '', ['', ...colNames], (v) => { if (v) t.leftKey = v; else delete t.leftKey; persist(); }, 'Column in the incoming stream to match on'));
  } else {
    wrap.appendChild(inpF('Left key (stream column)', t.leftKey, 'e.g. customerId', (v) => { if (v) t.leftKey = v; else delete t.leftKey; persist(); }, 'Column in the incoming stream to match on'));
  }
  wrap.appendChild(inpF('Right key (reference column)', t.rightKey, 'default: same as left key', (v) => { if (v === '') delete t.rightKey; else t.rightKey = v; persist(); }, 'Join column in the reference source'));
  wrap.appendChild(selF('Join type', t.joinType || 'inner', ['inner', 'left'], (v) => { t.joinType = v; persist(); }, 'inner drops unmatched rows; left keeps them with null right columns'));
  wrap.appendChild(inpF('Right column prefix', t.prefix, 'optional, e.g. cust_', (v) => { if (v === '') delete t.prefix; else t.prefix = v; persist(); }, 'Prefix added to imported right columns (avoid name clashes)'));
  wrap.appendChild(inpF('Right columns', Array.isArray(t.rightColumns) ? t.rightColumns.join(', ') : '', 'default: all but the key', (v) => { const a = v.split(',').map((s) => s.trim()).filter(Boolean); if (a.length) t.rightColumns = a; else delete t.rightColumns; persist(); }, 'Comma-separated right columns to add'));

  wrap.appendChild(el('div', 'k', 'Reference source (right side)'));
  wrap.appendChild(selF('Type', t.right.type || 'csv', ['csv', 'tsv', 'jsonlines', 'parquet', 'jdbc'], (v) => { t.right.type = v; persist(); }, 'A bounded source loaded once into a keyed table'));
  wrap.appendChild(inpF('Path / location', t.right.path, 'e.g. customers.csv', (v) => { if (v === '') delete t.right.path; else t.right.path = v; persist(); }, 'File path (file sources) — or use a connection for jdbc'));

  return wrap;
}

// Structured editor for the `router` (split) transform: an ordered list of
// routes (predicate → sink, first match wins), an optional default sink, and the
// policy for rows that match no route.
function renderRouterEditor(node: Node): HTMLElement {
  const wrap = el('div');
  const t: any = cfgTarget(node);
  if (!Array.isArray(t.routes)) t.routes = [];
  const persist = () => persistAndRebuild();
  const inpF = fieldInput;
  const selF = fieldSelect;
  const SINK_TYPES = ['jsonlines', 'csv', 'parquet', 'jdbc', 'kafka', 'null'];

  // Routes — ordered, first match wins.
  const routesF = el('div', 'fld');
  routesF.appendChild(el('div', 'k', 'Routes (first match wins)'));
  const cards = el('div', 'rcards');
  (t.routes as any[]).forEach((r, i) => {
    r.sinkConfig = r.sinkConfig || {};
    const card = el('div', 'rcard');
    const head = el('div', 'rhead');
    const nameI = el('input') as HTMLInputElement; nameI.placeholder = 'route name'; nameI.value = r.name || ''; nameI.style.flex = '1';
    nameI.onchange = () => { if (nameI.value.trim()) r.name = nameI.value.trim(); else delete r.name; persist(); };
    const del = el('span', 'del', '✕'); del.title = 'Remove route'; del.onclick = () => { (t.routes as any[]).splice(i, 1); persist(); renderEditor(); };
    head.appendChild(nameI); head.appendChild(del);
    card.appendChild(head);

    const wRow = el('div', 'rrow'); wRow.appendChild(el('label', undefined, 'When'));
    const wi = el('input') as HTMLInputElement; wi.placeholder = "e.g. amount > 1000 AND region = 'EU'"; wi.value = r.when || '';
    wi.onchange = () => { r.when = wi.value; persist(); };
    wRow.appendChild(wi); card.appendChild(wRow);

    const sRow = el('div', 'rrow'); sRow.appendChild(el('label', undefined, 'Sink'));
    const ss = el('select') as HTMLSelectElement;
    (SINK_TYPES.includes(r.sinkType) ? SINK_TYPES : [r.sinkType || '', ...SINK_TYPES]).forEach((o) => { const op = document.createElement('option'); op.value = o; op.textContent = o || '(select)'; if (o === r.sinkType) op.selected = true; ss.appendChild(op); });
    ss.onchange = () => { r.sinkType = ss.value; persist(); };
    sRow.appendChild(ss); card.appendChild(sRow);

    const pRow = el('div', 'rrow'); pRow.appendChild(el('label', undefined, 'Path'));
    const pi = el('input') as HTMLInputElement; pi.placeholder = 'e.g. ./out/high.jsonl'; pi.value = r.sinkConfig.path || '';
    pi.onchange = () => { if (pi.value.trim()) r.sinkConfig.path = pi.value.trim(); else delete r.sinkConfig.path; persist(); };
    pRow.appendChild(pi); card.appendChild(pRow);

    cards.appendChild(card);
  });
  const add = el('button', 'addcard', '＋ Add route'); add.onclick = () => { (t.routes as any[]).push({ when: '', sinkType: 'jsonlines', sinkConfig: {} }); persist(); renderEditor(); };
  cards.appendChild(add);
  routesF.appendChild(cards);
  wrap.appendChild(routesF);

  // Optional default sink for unmatched rows.
  wrap.appendChild(selF('Default sink (unmatched rows)', t.defaultSinkType || '', ['', 'jsonlines', 'csv', 'parquet', 'jdbc', 'kafka', 'null'], (v) => { if (v) { t.defaultSinkType = v; } else { delete t.defaultSinkType; delete t.defaultSinkConfig; } persist(); renderEditor(); }, 'Where rows matching no route go — blank uses the policy below', '(none)'));
  if (t.defaultSinkType) {
    const dsc = t.defaultSinkConfig || {};
    wrap.appendChild(inpF('Default sink path', dsc.path, 'e.g. ./out/other.jsonl', (v) => { t.defaultSinkConfig = t.defaultSinkConfig || {}; if (v === '') delete t.defaultSinkConfig.path; else t.defaultSinkConfig.path = v; persist(); }, 'File path (file sinks) — or broker/topic for kafka'));
  } else {
    wrap.appendChild(selF('Unmatched rows', t.onUnmatched || 'pass', ['pass', 'drop'], (v) => { t.onUnmatched = v; persist(); }, 'pass → continue downstream to the main sink; drop → discard'));
  }

  return wrap;
}

function renderAll() {
  renderTabs();
  renderEditor();
  renderStatus();
}

// ── tabs ──────────────────────────────────────────────────────────────────
function tabMeta(t: { type: string }) {
  switch (t.type) {
    case 'canvas': return { ic: '◈', label: S.fileName };
    case 'report': return { ic: '⧉', label: 'Run Report' };
    case 'dlq': return { ic: '⚠', label: 'DLQ' };
    case 'drift': return { ic: '≠', label: 'Schema Drift' };
    case 'tail': return { ic: '≋', label: 'Live Tail' };
    case 'fleet': return { ic: '▤', label: 'Fleet' };
    default: return { ic: '◇', label: t.type };
  }
}
function cur() { return S.tabs.find((t) => t.id === S.active); }
function openTab(id: string, type: string) {
  if (!S.tabs.find((t) => t.id === id)) S.tabs.push({ id, type });
  S.active = id;
  renderAll();
}
function closeTab(id: string) {
  if (id === 'canvas') return;
  const i = S.tabs.findIndex((t) => t.id === id);
  if (i < 0) return;
  S.tabs.splice(i, 1);
  if (S.active === id) S.active = 'canvas';
  renderAll();
}
function renderTabs() {
  const bar = $('#tabsbar')!;
  bar.innerHTML = '';
  S.tabs.forEach((t) => {
    const m = tabMeta(t);
    const d = el('div', 'tab' + (S.active === t.id ? ' on' : ''));
    d.innerHTML = `<span class="ic">${m.ic}</span>${m.label}` + (t.id !== 'canvas' ? ' <span class="x" title="Close">✕</span>' : '');
    d.onclick = (e) => {
      if ((e.target as HTMLElement).classList.contains('x')) { closeTab(t.id); return; }
      S.active = t.id;
      renderAll();
    };
    bar.appendChild(d);
  });
  const acts = el('div', 'egacts');
  if (cur()?.type === 'canvas') {
    const kafka = S.pipe?.nodes[0].type === 'kafka';
    acts.innerHTML =
      '<button class="btn sm" data-a="help" title="Show all features (F1)">? Features</button>' +
      '<button class="btn sm" data-a="settings" title="Reliability, load strategy, scaling, observability & mode">⚙ Pipeline Settings</button>' +
      '<button class="btn sm" data-a="preview">⌕ Dry-run Preview</button>' +
      (kafka ? '<button class="btn sm" data-a="tail">≋ Live Tail</button>' : '') +
      '<span class="sep"></span>' +
      (S.running
        ? '<button class="btn sm" data-a="stop" title="Stop the running engine">■ Stop</button>'
        : '<button class="btn sm pri" data-a="run" title="Run on the warm engine (fast re-test — keeps the engine hot)">▷ Run</button>');
    (acts.querySelector('[data-a="help"]') as HTMLElement).onclick = () => openFeatureGuide();
    (acts.querySelector('[data-a="settings"]') as HTMLElement).onclick = () => { S.selNode = SETTINGS_NODE; S.inspClosed = false; renderEditor(); };
    (acts.querySelector('[data-a="preview"]') as HTMLElement).onclick = () => doPreview();
    const tl = acts.querySelector('[data-a="tail"]') as HTMLElement | null;
    if (tl) tl.onclick = () => openTab('tail', 'tail');
    const rb = acts.querySelector('[data-a="run"]') as HTMLElement | null;
    if (rb) rb.onclick = () => doRun();
    const sb = acts.querySelector('[data-a="stop"]') as HTMLElement | null;
    if (sb) sb.onclick = () => doStop();
  }
  bar.appendChild(acts);
}

// ── editor / canvas ─────────────────────────────────────────────────────────
// Remembered scroll offset of the inspector body (.ib), restored after each
// renderEditor() so field edits don't jump the config panel back to the top.
let inspScrollTop = 0;

function renderEditor() {
  const m = $('#edmain')!;
  const bc = $('#breadcrumb')!;
  // Preserve the inspector body scroll position across re-renders. Editing any
  // field triggers a full renderEditor(), which would otherwise reset the config
  // panel to the top — painful when entering many columns (e.g. fixed-width).
  const prevIb = m.querySelector('.ib') as HTMLElement | null;
  if (prevIb) inspScrollTop = prevIb.scrollTop;
  m.innerHTML = '';
  if (S.tailTimer) { clearInterval(S.tailTimer); S.tailTimer = 0; }
  const t = cur();
  if (!t || !S.pipe) {
    bc.innerHTML = '';
    m.appendChild(el('div', 'canvas', '<div class="muted" style="margin:40px auto;text-align:center">Loading pipeline…</div>'));
    return;
  }
  if (t.type === 'canvas') {
    const sn = (S.selNode === 'dlq' && S.pipe.dlqNode) ? S.pipe.dlqNode : (S.pipe.nodes.find((n) => n.id === S.selNode) || S.pipe.nodes[0]);
    const snLabel = sn ? (sn.nm && sn.nm !== typeLabel(sn.type) && sn.nm !== sn.type ? `${typeLabel(sn.type)}: ${sn.nm}` : typeLabel(sn.type)) : '';
    bc.innerHTML = `◈ ${escapeHtml(S.pipe.name)} <span class="sep">›</span> ${escapeHtml(snLabel)} <span class="sep">›</span> <span class="chip plain" style="padding:1px 7px">${escapeHtml(S.pipe.mode)}</span>`;
    renderCanvas(m);
  } else {
    bc.innerHTML = tabMeta(t).label;
    const c = el('div', 'edcontent');
    m.appendChild(c);
    c.appendChild(el('div', 'wvnote', '<span class="d"></span>Webview view · backed by the running engine when available'));
    const wv = el('div', 'wv');
    c.appendChild(wv);
    if (t.type === 'report') renderReport(wv);
    else if (t.type === 'dlq') renderDLQ(wv);
    else if (t.type === 'drift') renderDrift(wv);
    else if (t.type === 'tail') renderTail(wv);
    else if (t.type === 'fleet') renderFleet(wv);
  }
}

// Render one derived branch (router route-sink, join reference source, validate
// reject sink, source parse-error sink) as a read-only satellite that hangs
// beneath its owner. Clicking it opens the owning stage's inspector — the branch
// is configured there, so the canvas node is a view, not a separate editor.
function renderBranch(b: DerivedBranch): HTMLElement {
  const item = el('div', 'nbranch nbranch-' + b.kind);
  const up = b.flow === 'in';
  const elbow = el('div', 'belbow');
  elbow.innerHTML = `<span class="bline"></span><span class="blabel" title="${escapeHtml(b.label)}">${up ? '▲ ' : ''}${escapeHtml(b.label)}</span>`;
  item.appendChild(elbow);
  const icon = b.nodeKind === 'source' ? '⇥' : '⇤';
  const kindText =
    b.kind === 'reference' ? 'reference input'
      : b.kind === 'route' ? 'route → sink'
        : b.kind === 'default-route' ? 'default → sink'
          : b.kind === 'reject' ? 'rejects → sink'
            : 'parse errors → sink';
  const meta = metaFor(b.nodeKind, b.type, b.cfg);
  const label = typeLabel(b.type);
  const showMeta = meta && meta !== b.title && meta !== label;
  const nd = el('div', 'node satellite s-' + b.nodeKind);
  nd.innerHTML = `<div class="kind"><span class="kico">${icon}</span>${kindText}</div>`
    + `<div class="nm">${escapeHtml(b.title)}</div>`
    + (showMeta ? `<div class="meta">${escapeHtml(meta)}</div>` : '');
  nd.tabIndex = 0;
  nd.title = 'Configured on the ' + (b.ownerIndex === 'source' ? 'source' : 'owning transform') + ' — click to edit.';
  nd.onclick = () => { S.selNode = b.ownerId; S.inspTab = 'Config'; S.inspClosed = false; renderEditor(); };
  item.appendChild(nd);
  return item;
}

// The top-level DLQ ("catch") rendered as a satellite under the sink column, so
// it stays glued to the sink even when other nodes carry branches of their own.
function renderDlqBranch(dn: Node): HTMLElement {
  const item = el('div', 'nbranch nbranch-dlq');
  const elbow = el('div', 'belbow');
  elbow.innerHTML = `<span class="bline"></span><span class="blabel" title="on write failure">⚠ on failure → catch</span>`;
  item.appendChild(elbow);
  const dnode = el('div', 'node catch satellite' + (S.selNode === 'dlq' ? ' sel' : ''));
  dnode.innerHTML = `<div class="kind">dead-letter (catch)</div><div class="nm">${escapeHtml(dn.nm)}</div><div class="meta">${escapeHtml(dn.type)} · ${escapeHtml(dn.meta)}</div>`;
  dnode.tabIndex = 0;
  dnode.title = 'Rows rejected by the sink are captured here — the main pipeline keeps running.';
  dnode.onclick = () => { S.selNode = 'dlq'; S.inspTab = 'Config'; S.inspClosed = false; renderEditor(); };
  dnode.oncontextmenu = (e) => dlqMenu(e);
  item.appendChild(dnode);
  return item;
}

// ── graph (node + wire) canvas ─────────────────────────────────────────────
// A read-only DAG view of the same pipeline: the main chain laid out left-to-
// right with the implicit branches fanning below, drawn with real ports and SVG
// wires. It reuses the node cards + inspector wiring (clicking a node selects it
// exactly as on the linear canvas) and is the substrate the wire-authoring steps
// build on. Toggle it with the "◇ Graph view" pill.
const GSVG_NS = 'http://www.w3.org/2000/svg' as const;
// Set while a node is being dragged so the trailing mouseup click doesn't also
// select it (and open the inspector).
let _gDragged = false;

function gKindCls(kind: string): string {
  return kind === 'source' ? 's-source' : kind === 'sink' ? 's-sink' : 'tf';
}

function buildGraphNode(n: Node, i: number, x: number, y: number, w: number): HTMLElement {
  const nd = el('div', 'gnode node ' + (n.kind === 'source' ? 'src' : n.kind === 'sink' ? 'sink' : 'tf') + (S.selNode === n.id ? ' sel' : ''));
  nd.style.left = x + 'px'; nd.style.top = y + 'px'; nd.style.width = w + 'px';
  const icon = n.kind === 'source' ? '⇥' : n.kind === 'sink' ? '⇤' : 'ƒ';
  const label = typeLabel(n.type);
  const hasCustomName = n.nm && n.nm !== label && n.nm !== n.type;
  const title = hasCustomName ? n.nm : label;
  const showMeta = n.meta && n.meta !== title && n.meta !== label;
  nd.innerHTML = `<div class='kind'><span class='kico'>${icon}</span>${n.kind}</div>`
    + `<div class='nm'>${escapeHtml(title)}</div>`
    + (showMeta ? `<div class='meta'>${escapeHtml(n.meta)}</div>` : '');
  nd.tabIndex = 0;
  nd.onclick = () => { if (_gDragged) { _gDragged = false; return; } S.selNode = n.id; S.inspTab = 'Config'; S.inspClosed = false; renderEditor(); };
  nd.oncontextmenu = (e) => nodeMenu(n, i, e);
  return nd;
}

function buildGraphBranchCard(b: DerivedBranch): HTMLElement {
  const nd = el('div', 'gnode node satellite s-' + b.nodeKind);
  const icon = b.nodeKind === 'source' ? '⇥' : '⇤';
  const kindText =
    b.kind === 'reference' ? 'reference input'
      : b.kind === 'route' ? 'route → sink'
        : b.kind === 'default-route' ? 'default → sink'
          : b.kind === 'reject' ? 'rejects → sink'
            : 'parse errors → sink';
  const meta = metaFor(b.nodeKind, b.type, b.cfg);
  const label = typeLabel(b.type);
  const showMeta = meta && meta !== b.title && meta !== label;
  nd.innerHTML = `<div class='kind'><span class='kico'>${icon}</span>${kindText}</div>`
    + `<div class='nm'>${escapeHtml(b.title)}</div>`
    + (showMeta ? `<div class='meta'>${escapeHtml(meta)}</div>` : '');
  nd.tabIndex = 0;
  nd.title = 'Configured on the ' + (b.ownerIndex === 'source' ? 'source' : 'owning transform') + ' — click to edit.';
  nd.onclick = () => { if (_gDragged) { _gDragged = false; return; } S.selNode = b.ownerId; S.inspTab = 'Config'; S.inspClosed = false; renderEditor(); };
  return nd;
}

function buildGraphDlqCard(dn: Node): HTMLElement {
  const nd = el('div', 'gnode node catch satellite' + (S.selNode === 'dlq' ? ' sel' : ''));
  nd.innerHTML = `<div class='kind'>dead-letter (catch)</div><div class='nm'>${escapeHtml(dn.nm)}</div><div class='meta'>${escapeHtml(dn.type)} · ${escapeHtml(dn.meta)}</div>`;
  nd.tabIndex = 0;
  nd.title = 'Rows rejected by the sink are captured here — the main pipeline keeps running.';
  nd.onclick = () => { if (_gDragged) { _gDragged = false; return; } S.selNode = 'dlq'; S.inspTab = 'Config'; S.inspClosed = false; renderEditor(); };
  nd.oncontextmenu = (e) => dlqMenu(e);
  return nd;
}

function graphPort(wrap: HTMLElement, x: number, y: number, cls: string): void {
  const port = el('span', 'gport gport-' + cls);
  port.style.left = (x - 5) + 'px';
  port.style.top = (y - 5) + 'px';
  wrap.appendChild(port);
}

function graphEdge(svg: SVGElement, x1: number, y1: number, x2: number, y2: number, cls: string): void {
  const path = document.createElementNS(GSVG_NS, 'path');
  const dx = Math.abs(x2 - x1), dy = Math.abs(y2 - y1);
  const d = dx >= dy
    ? `M ${x1} ${y1} C ${(x1 + x2) / 2} ${y1}, ${(x1 + x2) / 2} ${y2}, ${x2} ${y2}`
    : `M ${x1} ${y1} C ${x1} ${(y1 + y2) / 2}, ${x2} ${(y1 + y2) / 2}, ${x2} ${y2}`;
  path.setAttribute('d', d);
  path.setAttribute('class', 'gedge ' + cls);
  path.setAttribute('marker-end', 'url(#gah)');
  svg.appendChild(path);
}

// Lay out the pipeline as a DAG: main chain across the top row, implicit branches
// stacked beneath each owner, wired with SVG paths and ports. Read-only for now.
function renderGraphCanvas(cv: HTMLElement): void {
  const p = S.pipe!;
  const branchMap = branchesByOwner(p.branches);
  const W = 200, H = 96, GX = 92, TOP = 28, BW = 186, BH = 78, BGY = 30;
  const branchTop = TOP + H + 46;
  // Saved node positions live in a side-car `ui.positions` block so the pipeline
  // (and any promoted graph) JSON stays clean. Absent = auto-laid-out.
  const saved: Record<string, { x: number; y: number }> = ((S.doc as any).ui && (S.doc as any).ui.positions) || {};

  // 1) Default auto-layout: main chain across the top, branches stacked below.
  const pos: Record<string, { x: number; y: number; w: number; h: number }> = {};
  const xs: number[] = [];
  let x = 24;
  for (let i = 0; i < p.nodes.length; i++) { xs.push(x); x += W + GX; }
  p.nodes.forEach((n, i) => { pos[n.id] = { x: xs[i], y: TOP, w: W, h: H }; });

  const branchList: Array<{ id: string; owner: string; cls: string; flow: 'in' | 'out'; card: HTMLElement }> = [];
  p.nodes.forEach((n) => {
    if (!S.showBranches) return;
    let row = 0;
    const add = (id: string, card: HTMLElement, cls: string, flow: 'in' | 'out') => {
      pos[id] = { x: pos[n.id].x + W / 2 - BW / 2, y: branchTop + row * (BH + BGY), w: BW, h: BH };
      branchList.push({ id, owner: n.id, cls, flow, card });
      row++;
    };
    (branchMap[n.id] || []).forEach((b) => add(b.id, buildGraphBranchCard(b), 'br-' + b.kind, b.flow));
    if (n.id === 'sink' && p.hasDlq && p.dlqNode) add('dlq', buildGraphDlqCard(p.dlqNode), 'br-dlq', 'out');
  });

  // Apply saved overrides.
  for (const id in saved) if (pos[id]) { pos[id].x = saved[id].x; pos[id].y = saved[id].y; }

  // 2) Content size from the furthest node.
  let cw = 0, ch = 0;
  for (const id in pos) { cw = Math.max(cw, pos[id].x + pos[id].w); ch = Math.max(ch, pos[id].y + pos[id].h); }
  const contentW = cw + 24, contentH = ch + 20;

  const wrap = el('div', 'gcanvas');
  wrap.style.width = contentW + 'px';
  wrap.style.height = contentH + 'px';

  const svg = document.createElementNS(GSVG_NS, 'svg');
  svg.setAttribute('class', 'gedges');
  svg.setAttribute('width', String(contentW));
  svg.setAttribute('height', String(contentH));
  const defs = document.createElementNS(GSVG_NS, 'defs');
  const marker = document.createElementNS(GSVG_NS, 'marker');
  marker.setAttribute('id', 'gah');
  marker.setAttribute('markerWidth', '9');
  marker.setAttribute('markerHeight', '9');
  marker.setAttribute('refX', '7');
  marker.setAttribute('refY', '3');
  marker.setAttribute('orient', 'auto');
  const mpath = document.createElementNS(GSVG_NS, 'path');
  mpath.setAttribute('d', 'M0,0 L7,3 L0,6 Z');
  mpath.setAttribute('fill', '#8a929c');
  marker.appendChild(mpath);
  defs.appendChild(marker);
  svg.appendChild(defs);
  wrap.appendChild(svg);

  // 3) Connection points from the live position map, and a redraw() that repaints
  // every edge + port — called on each drag frame so wires track the node.
  const cp = (id: string) => {
    const q = pos[id];
    return { inX: q.x, inY: q.y + q.h / 2, outX: q.x + q.w, outY: q.y + q.h / 2, botX: q.x + q.w / 2, botY: q.y + q.h, topX: q.x + q.w / 2, topY: q.y };
  };
  const redraw = () => {
    svg.querySelectorAll('.gedge').forEach((e) => e.remove());
    wrap.querySelectorAll('.gport').forEach((e) => e.remove());
    for (let i = 1; i < p.nodes.length; i++) {
      const a = cp(p.nodes[i - 1].id), b = cp(p.nodes[i].id);
      graphEdge(svg, a.outX, a.outY, b.inX, b.inY, 'main');
    }
    p.nodes.forEach((n) => {
      const q = cp(n.id);
      if (n.kind !== 'source') graphPort(wrap, q.inX, q.inY, gKindCls(n.kind));
      if (n.kind !== 'sink') graphPort(wrap, q.outX, q.outY, gKindCls(n.kind));
    });
    branchList.forEach((b) => {
      const o = cp(b.owner), nn = cp(b.id);
      if (b.flow === 'out') graphEdge(svg, o.botX, o.botY, nn.topX, nn.topY, b.cls);
      else graphEdge(svg, nn.topX, nn.topY, o.botX, o.botY, b.cls);
      graphPort(wrap, o.botX, o.botY, 'branch');
      graphPort(wrap, nn.topX, nn.topY, 'branch');
    });
  };

  // 4) Drag-to-reposition: move the node, repaint wires live, persist on release.
  const attachDrag = (nd: HTMLElement, id: string) => {
    nd.addEventListener('mousedown', (ev: MouseEvent) => {
      if (ev.button !== 0) return;
      const sx = ev.clientX, sy = ev.clientY, ox = pos[id].x, oy = pos[id].y;
      let moved = false;
      const move = (m: MouseEvent) => {
        const dx = m.clientX - sx, dy = m.clientY - sy;
        if (!moved && Math.abs(dx) + Math.abs(dy) > 3) moved = true;
        if (moved) {
          pos[id].x = Math.max(0, ox + dx); pos[id].y = Math.max(0, oy + dy);
          nd.style.left = pos[id].x + 'px'; nd.style.top = pos[id].y + 'px';
          redraw();
        }
      };
      const up = () => {
        document.removeEventListener('mousemove', move);
        document.removeEventListener('mouseup', up);
        document.body.style.userSelect = '';
        if (moved) {
          _gDragged = true;
          const doc = S.doc as any;
          doc.ui = doc.ui || {};
          doc.ui.positions = doc.ui.positions || {};
          doc.ui.positions[id] = { x: Math.round(pos[id].x), y: Math.round(pos[id].y) };
          post({ type: 'save', doc: S.doc });
        }
      };
      document.addEventListener('mousemove', move);
      document.addEventListener('mouseup', up);
      document.body.style.userSelect = 'none';
      ev.preventDefault();
    });
  };

  // 5) Render node cards at their positions, then paint the wires.
  p.nodes.forEach((n, i) => {
    const nd = buildGraphNode(n, i, pos[n.id].x, pos[n.id].y, W);
    attachDrag(nd, n.id);
    wrap.appendChild(nd);
  });
  branchList.forEach((b) => {
    b.card.style.left = pos[b.id].x + 'px'; b.card.style.top = pos[b.id].y + 'px'; b.card.style.width = BW + 'px';
    attachDrag(b.card, b.id);
    wrap.appendChild(b.card);
  });

  redraw();
  cv.appendChild(wrap);
}

function renderCanvas(m: HTMLElement) {
  const p = S.pipe!;
  const c = el('div', 'edcontent');
  const cv = el('div', 'canvas' + (S.running ? ' running' : ''));
  c.appendChild(cv);

  // Pipeline header — editable name + description that live in the pipeline file
  // (doc.name / doc.description). Lets users document the pipeline inline.
  const head = el('div', 'pipehead');
  const nameIn = el('input', 'pipename') as HTMLInputElement;
  nameIn.value = p.name;
  nameIn.placeholder = 'Pipeline name';
  nameIn.title = 'Pipeline name (saved in the .datapipeline file)';
  nameIn.onchange = () => {
    const v = nameIn.value.trim();
    (S.doc as any).name = v || undefined;
    persistAndRebuild();
  };
  head.appendChild(nameIn);
  const descIn = el('textarea', 'pipedesc') as HTMLTextAreaElement;
  descIn.value = p.description;
  descIn.placeholder = 'Add a description… (documents this pipeline, saved in the file)';
  descIn.rows = p.description ? Math.min(4, p.description.split('\n').length) : 1;
  descIn.title = 'Pipeline description (saved in the .datapipeline file)';
  descIn.onchange = () => {
    const v = descIn.value.trim();
    (S.doc as any).description = v || undefined;
    persistAndRebuild();
  };
  // Auto-grow the textarea as the user types.
  descIn.oninput = () => { descIn.style.height = 'auto'; descIn.style.height = descIn.scrollHeight + 'px'; };
  head.appendChild(descIn);
  cv.appendChild(head);

  // Flow area — the graph (node + wire) view, or the classic linear flow. The
  // linear branch below is left at its original indentation to keep the diff
  // minimal; both render into the same canvas so the header/hint/inspector are
  // shared.
  if (S.canvasMode === 'graph') {
    renderGraphCanvas(cv);
  } else {
  const branchMap = branchesByOwner(p.branches);
  const row = el('div', 'flowrow');
  p.nodes.forEach((n, i) => {
    if (i > 0) {
      const cn = el('div', 'conn' + (S.running ? ' flow' : S.ran ? ' live' : ''));
      const ins = el('div', 'ins', '＋');
      ins.title = 'Insert transform here';
      ins.onclick = (e) => { e.stopPropagation(); addTransform(i); };
      cn.appendChild(ins);
      cn.onclick = () => addTransform(i);
      row.appendChild(cn);
    }
    const w = [92, 94, 94, 89, 89][i] || 90;
    const nd = el('div', 'node ' + (n.kind === 'source' ? 'src' : n.kind === 'sink' ? 'sink' : 'tf') + (S.ran ? ' ran' : '') + (S.selNode === n.id ? ' sel' : ''));
    nd.style.setProperty('--w', w + '%');
    const icon = n.kind === 'source' ? '⇥' : n.kind === 'sink' ? '⇤' : 'ƒ';
    // Avoid repeating the same word twice (e.g. a null sink whose name defaults
    // to its type — "null / null"). The bold title is the custom name when set,
    // otherwise the friendly type label; the meta line is only shown when it
    // carries extra detail beyond that title.
    const label = typeLabel(n.type);
    const hasCustomName = n.nm && n.nm !== label && n.nm !== n.type;
    const title = hasCustomName ? n.nm : label;
    const showMeta = n.meta && n.meta !== title && n.meta !== label;
    nd.innerHTML = `<div class="kind"><span class="kico">${icon}</span>${n.kind}</div>`
      + `<div class="nm">${title}</div>`
      + (showMeta ? `<div class="meta">${n.meta}</div>` : '')
      + `<div class="thru"><i></i></div>`;
    nd.tabIndex = 0;
    nd.onclick = () => { S.selNode = n.id; S.inspTab = 'Config'; S.inspClosed = false; renderEditor(); };
    nd.oncontextmenu = (e) => nodeMenu(n, i, e);
    // Wrap each node in a column so its implicit branches hang directly beneath
    // it, aligned under their owner, without pushing the main flow off its line.
    const col = el('div', 'nodecol');
    col.appendChild(nd);
    if (S.showBranches) {
      const owned = branchMap[n.id] || [];
      const withDlq = n.id === 'sink' && p.hasDlq && !!p.dlqNode;
      if (owned.length || withDlq) {
        const stack = el('div', 'nbranches');
        owned.forEach((b) => stack.appendChild(renderBranch(b)));
        if (withDlq) stack.appendChild(renderDlqBranch(p.dlqNode!));
        col.appendChild(stack);
      }
    }
    row.appendChild(col);
  });
  row.appendChild(el('div', 'conn'));
  const add = el('div', 'addnode', '＋');
  add.title = 'Add transform (before the sink)';
  add.onclick = () => addTransform(p.nodes.length - 1);
  row.appendChild(add);
  // Wrap the flow + catch lane so the DLQ aligns under the sink end of the flow.
  const wrap = el('div', 'flowwrap');
  wrap.appendChild(row);
  cv.appendChild(wrap);
  }

  // The DLQ ("catch") now renders as a satellite inside the sink's column (see
  // renderDlqBranch), so it stays aligned under the sink alongside any other
  // implicit branches instead of floating below a branch-tall flow row.

  const hint = el('div', 'canvas-hint pillrow');
  const dlqPill = p.hasDlq ? '' : '<span class="pill" data-h="dlq">＋ DLQ</span>';
  const brPill = p.branches.length ? `<span class="pill toggle${S.showBranches ? ' on' : ''}" data-h="branches" title="Show or hide the implicit router, join, reject and parse-error branches">⎇ Branches</span>` : '';
  const modePill = `<span class="pill toggle${S.canvasMode === 'graph' ? ' on' : ''}" data-h="gmode" title="Switch between the linear flow and the graph (node + wire) view">◇ Graph view</span>`;
  const hasGraph = !!(S.doc as any).graph;
  const promotePill = S.canvasMode === 'graph'
    ? (hasGraph
      ? `<span class="pill toggle on" data-h="unpromote" title="Remove the explicit graph block and return to a purely linear file">graph ✓ · reset</span>`
      : `<span class="pill toggle" data-h="promote" title="Write an explicit graph block (nodes + edges) to the file from this pipeline">⇪ Promote to graph</span>`)
    : '';
  hint.innerHTML = '<span class="pill" data-h="src">＋ Source</span><span class="pill" data-h="tf">＋ Transform</span><span class="pill" data-h="sink">＋ Sink</span>' + dlqPill + brPill + modePill + promotePill + '<span class="pill" data-h="fit">⤢ Fit</span>';
  (hint.querySelector('[data-h="tf"]') as HTMLElement).onclick = () => addTransform(p.nodes.length - 1);
  (hint.querySelector('[data-h="src"]') as HTMLElement).onclick = () =>
    quickPick('Change source type', SOURCE_TYPES, (v) => {
      (S.doc.source as any) = { type: v, ...defaultConfigFor('source', v) };
      persistAndRebuild();
      toast('ok', `Source type → ${typeLabel(v)}`);
    }, typeLabel);
  (hint.querySelector('[data-h="sink"]') as HTMLElement).onclick = () =>
    quickPick('Change sink type', SINK_TYPES, (v) => {
      (S.doc.sink as any) = { type: v, ...defaultConfigFor('sink', v) };
      persistAndRebuild();
      toast('ok', `Sink type → ${typeLabel(v)}`);
    }, typeLabel);
  const dlqBtn = hint.querySelector('[data-h="dlq"]') as HTMLElement | null;
  if (dlqBtn) dlqBtn.onclick = () => addDlq();
  const brBtn = hint.querySelector('[data-h="branches"]') as HTMLElement | null;
  if (brBtn) brBtn.onclick = () => { S.showBranches = !S.showBranches; renderEditor(); };
  const gmBtn = hint.querySelector('[data-h="gmode"]') as HTMLElement | null;
  if (gmBtn) gmBtn.onclick = () => { S.canvasMode = S.canvasMode === 'graph' ? 'linear' : 'graph'; renderEditor(); };
  const prBtn = hint.querySelector('[data-h="promote"]') as HTMLElement | null;
  if (prBtn) prBtn.onclick = () => {
    const g = deriveGraph(S.doc);
    (S.doc as any).graph = g;
    persistAndRebuild();
    toast('ok', `Wrote graph block — ${g.nodes.length} nodes, ${g.edges.length} edges`);
  };
  const unBtn = hint.querySelector('[data-h="unpromote"]') as HTMLElement | null;
  if (unBtn) unBtn.onclick = () => {
    delete (S.doc as any).graph;
    persistAndRebuild();
    toast('ok', 'Removed graph block — back to linear');
  };
  const fitBtn = hint.querySelector('[data-h="fit"]') as HTMLElement | null;
  if (fitBtn) fitBtn.onclick = () => {
    const doc = S.doc as any;
    const hadPos = S.canvasMode === 'graph' && doc.ui && doc.ui.positions;
    if (hadPos) { delete doc.ui.positions; if (!Object.keys(doc.ui).length) delete doc.ui; post({ type: 'save', doc: S.doc }); }
    renderEditor();
    const cvEl = $('.canvas'); if (cvEl) cvEl.scrollTo({ left: 0, top: 0 });
    if (hadPos) toast('ok', 'Reset to auto-layout');
  };
  cv.appendChild(hint);
  m.appendChild(c);

  // Inspector is dismissible; when closed, show a slim reopen tab instead.
  if (S.inspClosed) {
    const reopen = el('div', 'inspreopen');
    reopen.title = 'Show config panel';
    reopen.innerHTML = '<span>⚙</span>';
    reopen.onclick = () => { S.inspClosed = false; renderEditor(); };
    m.appendChild(reopen);
    return;
  }

  // Pipeline-level settings panel (checkpoint, performance, scaling,
  // observability, incremental, mode) — rendered instead of a node inspector
  // when the settings entry point is selected.
  if (S.selNode === SETTINGS_NODE) {
    m.appendChild(renderPipelineSettings());
    return;
  }

  // inspector — the selected node may be the DLQ branch node (not in p.nodes).
  const insp = el('div', 'inspector');
  const sn = (S.selNode === 'dlq' && p.dlqNode) ? p.dlqNode : (p.nodes.find((n) => n.id === S.selNode) || p.nodes[0]);
  const ih = el('div', 'ih');
  if (sn) {
    // A color-coded kind badge (Source / Transform / Sink / DLQ) so it's obvious
    // whether e.g. a "jdbc" panel is the source or the sink.
    const kindCls = sn.kind === 'source' ? 'src' : sn.kind === 'sink' ? 'sink' : sn.kind === 'dlq' ? 'dlq' : 'tf';
    const kindLbl = sn.kind === 'source' ? 'Source' : sn.kind === 'sink' ? 'Sink' : sn.kind === 'dlq' ? 'DLQ' : 'Transform';
    ih.appendChild(el('span', 'ihkind ' + kindCls, kindLbl));
  }
  ih.appendChild(el('span', 'ihname', sn ? (sn.kind === 'transform' ? transformLabel(sn.type) : typeLabel(sn.type)) : ''));
  const closeBtn = el('span', 'ihx', '✕');
  closeBtn.title = 'Close config panel';
  closeBtn.onclick = () => { S.inspClosed = true; renderEditor(); };
  ih.appendChild(closeBtn);
  insp.appendChild(ih);
  const ib = el('div', 'ib');
  const tabs = el('div', 'tabs');
  ['Config', 'Schema', 'Test'].forEach((tn) => {
    const s = el('span', S.inspTab === tn ? 'on' : '', tn);
    s.onclick = () => { S.inspTab = tn; renderEditor(); };
    tabs.appendChild(s);
  });
  ib.appendChild(tabs);

  if (S.inspTab === 'Config') {
    // Stage type: dropdown for source/sink/dlq destination, badge for transforms.
    const typeRow = el('div', 'insp-type');
    if (sn.kind === 'source' || sn.kind === 'sink') {
      typeRow.appendChild(el('span', 'badge', sn.kind + ' type'));
      const sel = el('select', 'v') as HTMLSelectElement;
      const opts = sn.kind === 'source' ? SOURCE_TYPES : SINK_TYPES;
      const list = opts.includes(sn.type) ? opts : [sn.type, ...opts];
      list.forEach((o) => {
        const op = document.createElement('option');
        op.value = o; op.textContent = typeLabel(o);
        if (o === sn.type) op.selected = true;
        sel.appendChild(op);
      });
      sel.onchange = () => {
        const kind = sn.kind as 'source' | 'sink';
        (S.doc[kind] as any) = { type: sel.value, ...defaultConfigFor(kind, sel.value) };
        persistAndRebuild();
      };
      typeRow.appendChild(sel);
    } else if (sn.kind === 'dlq') {
      typeRow.appendChild(el('span', 'badge', 'DLQ destination'));
      const sel = el('select', 'v') as HTMLSelectElement;
      const list = DLQ_DEST_TYPES.includes(sn.type) ? DLQ_DEST_TYPES : [sn.type, ...DLQ_DEST_TYPES];
      list.forEach((o) => {
        const op = document.createElement('option');
        op.value = o; op.textContent = o;
        if (o === sn.type) op.selected = true;
        sel.appendChild(op);
      });
      sel.onchange = () => {
        const cur = (S.doc as any).dlq || {};
        (S.doc as any).dlq = {
          type: sel.value,
          options: defaultDlqOptions(sel.value),
          maxRowsPerBatch: cur.maxRowsPerBatch ?? 0,
          onFull: cur.onFull || 'block',
        };
        persistAndRebuild();
        toast('ok', `DLQ destination → ${sel.value}`);
      };
      typeRow.appendChild(sel);
    } else {
      typeRow.appendChild(el('span', 'badge', 'transform'));
      typeRow.appendChild(el('span', 'chip plain', transformLabel(sn.type)));
    }
    ib.appendChild(typeRow);

    if (sn.kind === 'dlq') {
      ib.appendChild(el('div', 'muted', '<div style="font-size:11.5px;padding:0 0 8px">Rows that still fail after per-row isolation are written here with an error-envelope column — the main sink is unaffected.</div>'));
    }

    // Text Document (sectioned) source: a friendly "Format preset" that fills the
    // three regexes for common document shapes, so most users never touch a regex.
    if (sn.kind === 'source' && sn.type === 'sectioned') {
      ib.appendChild(renderSectionedPreset(sn));
    }

    // Transform stages get an editable name; source/sink names are derived.
    if (sn.kind === 'transform') {
      const nameF = el('div', 'fld');
      nameF.appendChild(el('div', 'k', 'Stage name'));
      const ni = el('input', 'v') as HTMLInputElement;
      ni.value = (S.doc.transforms as any)?.[sn.ref as number]?.name || sn.type;
      ni.onchange = () => { (S.doc.transforms as any)[sn.ref as number].name = ni.value; persistAndRebuild(); };
      nameF.appendChild(ni);
      ib.appendChild(nameF);
    }

    // Config fields — type-aware widgets.
    // The map transform's `fields` object gets a dedicated structured mapping
    // editor (output field ← expression rows + click-to-insert available data
    // and functions), like the Flogo mapper — instead of a raw JSON textarea.
    // A "Connection" picker is shown first for stages whose type uses one, so
    // DSNs/secrets can be bound by name instead of typed into the file.
    if (sn.type === 'window') {
      ib.appendChild(renderWindowEditor(sn));
    } else if (sn.type === 'join') {
      ib.appendChild(renderJoinEditor(sn));
    } else if (sn.type === 'router' || sn.type === 'split') {
      ib.appendChild(renderRouterEditor(sn));
    } else {
      const connSel = renderConnectionSelector(sn);
      if (connSel) ib.appendChild(connSel);
      const fam = familyOfType(sn.type);
      const boundConn = fam && typeof cfgTarget(sn).connection === 'string' && cfgTarget(sn).connection;
      const hiddenKeys = new Set<string>(['connection']);
      if (boundConn && fam) for (const k of CONN_MANAGED_KEYS[fam] || []) hiddenKeys.add(k);
      Object.entries(sn.cfg).forEach(([k, v]) => {
        if (hiddenKeys.has(k)) return;
        // JDBC: the conflict key only applies to upsert — hide it otherwise, and
        // render it as a column picker rather than a JSON blob.
        const isJdbc = sn.type === 'jdbc';
        if (isJdbc && k === 'conflictKey') {
          const oc = String((cfgTarget(sn) as any).onConflict || 'insert');
          if (oc === 'upsert') ib.appendChild(renderConflictKeyEditor(sn));
          return;
        }
        // JDBC table: offer a live "pick from database" dropdown + refresh.
        if (isJdbc && k === 'table') {
          ib.appendChild(renderTableField(sn, v));
          return;
        }
        // Object-store key/prefix: offer a "browse objects" picker.
        const isObjStore = ['s3', 'gcs', 'adls', 's3-sink', 'gcs-sink', 'adls-sink'].includes(sn.type);
        if (isObjStore && (k === 'key' || k === 'prefix')) {
          ib.appendChild(renderObjectField(sn, k, v));
          return;
        }
        // Kafka: topic picker + schema-registry schema reader.
        if (sn.type === 'kafka' && k === 'topic') {
          ib.appendChild(renderKafkaTopicField(sn, v));
          return;
        }
        if (sn.type === 'kafka' && k === 'schemaRegistryURL') {
          ib.appendChild(renderSchemaRegistryField(sn, v));
          return;
        }
        // XML: recordElement gets a "detect structure from file" action.
        if (sn.type === 'xml' && k === 'recordElement') {
          ib.appendChild(renderXmlRecordField(sn, v));
          return;
        }
        // Custom Text (template) sink: fold template + templatePath into one editor
        // with an Inline/File toggle and a variable/helper reference strip.
        if (sn.type === 'template' && k === 'template') {
          ib.appendChild(renderTemplateEditor(sn));
          return;
        }
        if (sn.type === 'template' && k === 'templatePath') {
          return; // handled inside renderTemplateEditor
        }
        if (sn.type === 'podlog' && k === 'multiline') { ib.appendChild(renderPodlogMultiline(sn)); return; }
        if (sn.type === 'podlog' && k === 'columns') return; // podlog: pin columns to the bottom (rendered after config, below)
        if (k === 'columns') ib.appendChild(renderColumnsEditor(sn));
        else if (k === 'fields' && (sn.type === 'map' || sn.type === 'mapper')) ib.appendChild(renderMapEditor(sn));
        else if (k === 'rules' && sn.type === 'validate') ib.appendChild(renderRulesEditor(sn));
        else if (k === 'expectations' && sn.type === 'dq') ib.appendChild(renderExpectationsEditor(sn));
        else if (k === 'expr' && sn.type === 'filter') ib.appendChild(renderFilterEditor(sn));
        else ib.appendChild(renderField(sn, k, v));
      });

      // JDBC upsert selected but no conflictKey field present yet — add the picker.
      if (sn.type === 'jdbc' && String((cfgTarget(sn) as any).onConflict) === 'upsert' && !('conflictKey' in sn.cfg)) {
        ib.appendChild(renderConflictKeyEditor(sn));
      }

      // A source with no explicit `columns` key (e.g. Parquet is self-describing)
      // still gets a columns editor so users can view/declare columns and use the
      // "Read schema from source" / "Infer" actions. podlog also renders it here
      // (its `columns` key is skipped in the loop above) so its many config fields
      // stay at the top and the columns/schema section is always last.
      if (sn.kind === 'source' && (!('columns' in sn.cfg) || sn.type === 'podlog')) {
        ib.appendChild(renderColumnsEditor(sn));
      }

      if (!Object.keys(sn.cfg).length && sn.kind !== 'source') {
        ib.appendChild(el('div', 'muted', '<div style="font-size:11.5px;padding:4px 0">No configuration for this stage yet.</div>'));
      }
    }

    if (sn.kind === 'dlq') {
      const rm = el('button', 'btn sm', '✕ Remove DLQ');
      rm.style.marginTop = '10px';
      rm.onclick = () => removeDlq();
      ib.appendChild(rm);
    }
  } else if (S.inspTab === 'Schema') {
    renderSchemaTab(ib, sn);
  } else {
    renderTestTab(ib, sn);
  }
  insp.appendChild(ib);
  attachResize(insp);
  m.appendChild(insp);
  // Restore the previously-remembered scroll position now that the inspector
  // body is in the DOM (see renderEditor()).
  ib.scrollTop = inspScrollTop;
}

// Infer the column schema visible at a given stage by walking the doc from the
// source forward: source columns → map output fields → passthrough.
function schemaForNode(node: Node): Array<{ name: string; type: string; derived?: boolean }> {
  const src: any = S.doc.source || {};
  let cols: Array<{ name: string; type: string; derived?: boolean }> = Array.isArray(src.columns)
    ? src.columns.map((c: any) => ({ name: c.name, type: c.type || 'string' }))
    : (SOURCE_FIXED_SCHEMA[String(src.type)] || []).map((c) => ({ name: c.name, type: c.type }));
  const transforms = Array.isArray(S.doc.transforms) ? S.doc.transforms : [];
  // Source sees no transforms; sink/dlq see the full chain; a transform sees up
  // to its own index.
  const upto = node.kind === 'source' ? -1 : (node.kind === 'sink' || node.kind === 'dlq') ? transforms.length - 1 : (node.ref as number);
  for (let i = 0; i <= upto; i++) {
    const t: any = transforms[i];
    if (!t) continue;
    if (t.type === 'map' && t.fields && typeof t.fields === 'object') {
      // map replaces the projected set with its output fields
      cols = Object.keys(t.fields).map((k) => ({ name: k, type: 'derived', derived: true }));
    }
    if (t.type === 'window') {
      // window replaces the schema with groupBy keys + window bounds + aggregates
      const gb = Array.isArray(t.groupBy) ? t.groupBy : [];
      const aggs = Array.isArray(t.aggregations) ? t.aggregations : [];
      const em = t.emit || {};
      cols = [
        ...gb.map((g: any) => ({ name: String(g), type: 'string' })),
        { name: String(em.windowStartAs || 'window_start'), type: 'int' },
        { name: String(em.windowEndAs || 'window_end'), type: 'int' },
        ...aggs.map((a: any) => ({ name: String(a.as), type: 'derived', derived: true })),
      ];
    }
    // filter/validate/hwm/dq/schemadrift are passthrough for schema purposes.
    // lookup and parse APPEND columns to the existing set.
    if (t.type === 'lookup' && Array.isArray(t.outputFields)) {
      t.outputFields.forEach((f: any) => cols.push({ name: String(f), type: 'derived', derived: true }));
    }
    if (t.type === 'parse') {
      parseProducedColumns(t).forEach((n) => {
        if (!cols.some((c) => c.name === n)) cols.push({ name: n, type: 'derived', derived: true });
      });
    }
    if (t.type === 'join' && Array.isArray(t.rightColumns)) {
      // join APPENDS the selected right columns (optionally prefixed).
      const pfx = typeof t.prefix === 'string' ? t.prefix : '';
      t.rightColumns.forEach((f: any) => {
        const name = pfx + String(f);
        if (!cols.some((c) => c.name === name)) cols.push({ name, type: 'derived', derived: true });
      });
    }
  }
  return cols;
}

// Columns visible ENTERING a transform (schema produced by every stage before
// it) — used by the map editor's Available-data palette so users insert the
// fields they map FROM, not the map's own outputs.
function upstreamSchema(node: Node): Array<{ name: string; type: string; derived?: boolean }> {
  const src: any = S.doc.source || {};
  let cols: Array<{ name: string; type: string; derived?: boolean }> = Array.isArray(src.columns)
    ? src.columns.map((c: any) => ({ name: c.name, type: c.type || 'string' }))
    : (SOURCE_FIXED_SCHEMA[String(src.type)] || []).map((c) => ({ name: c.name, type: c.type }));
  const transforms = Array.isArray(S.doc.transforms) ? S.doc.transforms : [];
  const stop = typeof node.ref === 'number' ? (node.ref as number) : transforms.length;
  for (let i = 0; i < stop; i++) {
    const t: any = transforms[i];
    if (!t) continue;
    if (t.type === 'map' && t.fields && typeof t.fields === 'object') {
      cols = Object.keys(t.fields).map((k) => ({ name: k, type: 'derived', derived: true }));
    }
    if (t.type === 'lookup' && Array.isArray(t.outputFields)) {
      t.outputFields.forEach((f: any) => cols.push({ name: String(f), type: 'derived', derived: true }));
    }
    if (t.type === 'parse') {
      parseProducedColumns(t).forEach((n) => {
        if (!cols.some((c) => c.name === n)) cols.push({ name: n, type: 'derived', derived: true });
      });
    }
  }
  // If a preview ran, prefer its live columns for the very first map (source
  // columns may be undeclared for self-describing formats).
  if (!cols.length && S.preview && S.preview.columns.length) {
    return S.preview.columns.map((c) => ({ name: c, type: 'any' }));
  }
  // Last-resort fallback: when the source declares no schema and no preview has
  // run, harvest the field names the pipeline ALREADY references ($field tokens
  // in filter/map/parse expressions up to and including this stage). This means
  // the "Available data" palette is populated from the pipeline's own intent —
  // so a Text Document / undeclared-CSV source still shows the fields the user
  // is working with, instead of an empty palette.
  if (!cols.length) {
    const harvested = harvestFieldRefs(stop + 1);
    if (harvested.length) return harvested.map((n) => ({ name: n, type: 'any' }));
  }
  return cols;
}

// Scan every stage up to index `stop` for field references and return the
// distinct field names. Map/parse use $field tokens; filter uses BARE field
// names in a boolean expression (e.g. status != 'CANCELLED'), so both forms are
// harvested. Used as the Available-data fallback when no schema is declared.
function harvestFieldRefs(stop: number): string[] {
  const seen = new Set<string>();
  const addDollar = (s: string) => {
    if (typeof s !== 'string') return;
    const re = /\$([A-Za-z_][A-Za-z0-9_]*)/g;
    let m: RegExpExecArray | null;
    while ((m = re.exec(s)) !== null) seen.add(m[1]);
  };
  // Reserved words in filter expressions that are NOT field names.
  const KW = new Set(['AND', 'OR', 'NOT', 'IN', 'LIKE', 'IS', 'NULL', 'TRUE', 'FALSE']);
  const addBare = (s: string) => {
    if (typeof s !== 'string') return;
    // Strip single/double-quoted string literals so their words aren't harvested.
    const noStr = s.replace(/'[^']*'/g, ' ').replace(/"[^"]*"/g, ' ');
    const re = /[A-Za-z_][A-Za-z0-9_.]*/g;
    let m: RegExpExecArray | null;
    while ((m = re.exec(noStr)) !== null) {
      const w = m[0];
      // Skip keywords and function-call names (identifier immediately followed by "(").
      if (KW.has(w.toUpperCase())) continue;
      const after = noStr.slice(m.index + w.length).trimStart();
      if (after.startsWith('(')) continue; // function name, not a field
      seen.add(w);
    }
  };
  const transforms = Array.isArray(S.doc.transforms) ? S.doc.transforms : [];
  for (let i = 0; i < stop && i < transforms.length; i++) {
    const t: any = transforms[i];
    if (!t) continue;
    if (t.type === 'filter') addBare(String(t.expr || ''));
    if ((t.type === 'map' || t.type === 'mapper') && t.fields && typeof t.fields === 'object') {
      for (const v of Object.values(t.fields)) addDollar(String(v));
    }
    if (t.type === 'parse' && typeof t.source === 'string') seen.add(t.source);
  }
  return Array.from(seen);
}

// A node in the source-field tree shown on the LEFT of the mapper. Complex
// fields (objects) carry children so nested paths ($product.name) can be mapped.
interface SrcField { name: string; path: string; type: string; children?: SrcField[] }

// Build the source-field tree for the mapper's left pane. Flat columns come from
// upstreamSchema; complex/nested structure is discovered from the last preview's
// actual row values (JSON/XML objects), so a field like `product` expands into
// product.name / product.price the user can drag individually.
function sourceFieldTree(node: Node): SrcField[] {
  const cols = upstreamSchema(node);
  // Sample object shapes from the preview rows (first row that has the key).
  const sample: Record<string, unknown> = {};
  if (S.preview && Array.isArray(S.preview.rows)) {
    for (const r of S.preview.rows as Array<Record<string, unknown>>) {
      for (const k of Object.keys(r || {})) {
        if (!(k in sample) && r[k] && typeof r[k] === 'object') sample[k] = r[k];
      }
    }
  }
  const childrenOf = (val: unknown, basePath: string, depth: number): SrcField[] | undefined => {
    if (depth > 4 || !val || typeof val !== 'object' || Array.isArray(val)) return undefined;
    const obj = val as Record<string, unknown>;
    const keys = Object.keys(obj);
    if (!keys.length) return undefined;
    return keys.map((k) => {
      const p = `${basePath}.${k}`;
      const v = obj[k];
      const kids = childrenOf(v, p, depth + 1);
      return { name: k, path: p, type: kids ? 'object' : typeofVal(v), children: kids };
    });
  };
  return cols.map((c) => {
    const kids = childrenOf(sample[c.name], c.name, 1);
    return { name: c.name, path: c.name, type: kids ? 'object' : (c.type || 'any'), children: kids };
  });
}

function typeofVal(v: unknown): string {
  if (v === null || v === undefined) return 'any';
  if (Array.isArray(v)) return 'array';
  switch (typeof v) {
    case 'number': return 'number';
    case 'boolean': return 'bool';
    case 'object': return 'object';
    default: return 'string';
  }
}

function renderSchemaTab(ib: HTMLElement, sn: Node) {
  // The DLQ destination receives rejected rows plus an error-envelope column,
  // not the normal pipeline output — describe that instead of a stage schema.
  if (sn.kind === 'dlq') {
    const env = ((S.doc as any).dlq?.options?.envelopeColumn) || '_error';
    ib.appendChild(el('div', 'muted', `<div style="font-size:12px;padding:2px 0 8px">The DLQ receives <b style="color:var(--text)">rejected rows</b> — the pipeline columns as they were when the write failed, plus an error-envelope column <code style="color:var(--orange)">${escapeHtml(env)}</code> carrying the failure reason, pipeline ref and run id.</div>`));
    const cols = schemaForNode(sn);
    if (cols.length) {
      const box = el('div', 'cols');
      cols.forEach((c) => {
        const row = el('div', 'crow');
        row.style.cursor = 'default';
        row.innerHTML = `<span style="flex:1;font:11.5px var(--mono);color:var(--text)">${escapeHtml(c.name)}</span><span class="pill" style="flex:none">${c.derived ? 'derived' : escapeHtml(c.type)}</span>`;
        box.appendChild(row);
      });
      const erow = el('div', 'crow');
      erow.style.cursor = 'default';
      erow.innerHTML = `<span style="flex:1;font:11.5px var(--mono);color:var(--orange)">${escapeHtml(env)}</span><span class="pill" style="flex:none">envelope</span>`;
      box.appendChild(erow);
      ib.appendChild(box);
    }
    return;
  }
  const cols = schemaForNode(sn);
  ib.appendChild(el('div', 'muted', `<div style="font-size:12px;padding:2px 0 8px">Columns visible <b style="color:var(--text)">after ${escapeHtml(sn.nm)}</b>:</div>`));
  if (!cols.length) {
    ib.appendChild(el('div', 'muted', '<div style="font-size:11.5px">No declared schema. For self-describing sources (Parquet, JSON), run <b style="color:var(--text)">Dry-run Preview</b> to see live columns; for CSV/fixed-width declare columns in the Config tab.</div>'));
    const b = el('button', 'btn sm', '⌕ Preview to infer schema');
    b.style.marginTop = '10px';
    b.onclick = () => doPreview();
    ib.appendChild(b);
    return;
  }
  const box = el('div', 'cols');
  cols.forEach((c) => {
    const row = el('div', 'crow');
    row.style.cursor = 'default';
    row.innerHTML = `<span style="flex:1;font:11.5px var(--mono);color:var(--text)">${escapeHtml(c.name)}</span><span class="pill" style="flex:none">${c.derived ? 'derived' : escapeHtml(c.type)}</span>`;
    box.appendChild(row);
  });
  ib.appendChild(box);
  // If a preview ran, surface live column types too.
  if (S.preview && S.preview.columns.length) {
    ib.appendChild(el('div', 'muted', `<div style="font-size:11px;margin-top:10px;border-top:1px solid var(--border);padding-top:8px">Live columns from last preview: <span style="color:var(--blueHi)">${S.preview.columns.map(escapeHtml).join(', ')}</span></div>`));
  }
}

function renderTestTab(ib: HTMLElement, sn: Node) {
  // The DLQ only receives rows on a real write failure, which a side-effect-free
  // dry-run preview cannot produce — guide the user to a full run instead.
  if (sn.kind === 'dlq') {
    ib.appendChild(el('div', 'muted', '<div style="font-size:11.5px;padding:2px 0 12px">The DLQ is exercised only when a real write to the main sink fails. Dry-run Preview never writes, so it cannot populate the DLQ. Run the pipeline end-to-end, then inspect rejected rows.</div>'));
    const run = el('button', 'btn sm pri', '▷ Run pipeline');
    run.onclick = () => doRun();
    ib.appendChild(run);
    const stats = el('button', 'btn sm', '⧉ Open DLQ run stats');
    stats.style.marginLeft = '8px';
    stats.onclick = () => openTab('dlq', 'dlq');
    ib.appendChild(stats);
    if (S.lastRun) {
      ib.appendChild(el('div', 'muted', `<div style="font-size:11px;margin-top:10px">Last run — <span class="chip ${S.lastRun.rowsFailed ? 'err' : 'idle'}">${fmt(S.lastRun.rowsFailed)} failed</span> of ${fmt(S.lastRun.rowsProcessed)} processed.</div>`));
    }
    return;
  }
  // A transform stage is best tested atomically: the stage tester IS the test,
  // so it's the only thing shown — one clear flow instead of two competing
  // buttons + paragraphs. (The whole-chain dry-run preview stays reachable from
  // the canvas ▷ button and the node context menu.)
  if (sn.kind === 'transform' && typeof sn.ref === 'number') {
    renderStageTester(ib, sn, sn.ref as number);
    return;
  }

  // The SOURCE stage: show the actual rows read from the source inline, so the
  // user can SEE what data the pipeline is testing with (read-only preview).
  if (sn.kind === 'source') {
    renderSourceSample(ib);
    return;
  }

  // The SINK stage: show the rows that WOULD be written — the source run through
  // the full transform chain — inline, so the user sees the final data without
  // opening a separate panel and without writing anything to the sink.
  if (sn.kind === 'sink') {
    renderSinkSample(ib);
    return;
  }
}

// Source Test tab: fetch and show a read-only sample of the rows the source
// actually reads, right in the inspector — answers "what data are we testing
// with?" without opening a separate panel.
function renderSourceSample(ib: HTMLElement) {
  const s = S.srcSample;
  ib.appendChild(el('div', 'muted', '<div style="font-size:11.5px;margin:2px 0 12px">Read a live sample straight from the source so you can see exactly what data flows in.</div>'));

  const card = el('div', 'ttcard');
  const head = el('div', 'ttcard-h');
  head.appendChild(el('span', 'ttcard-t', s && s.rows.length ? `Source sample · ${s.rows.length} rows` : 'Source sample'));
  const acts = el('div', 'ttcard-a');
  const refresh = el('button', 'iconbtn', s && s.loading ? '…' : '↺');
  refresh.title = 'Read a fresh sample from the source';
  refresh.onclick = () => sampleSource();
  acts.appendChild(refresh);
  head.appendChild(acts);
  card.appendChild(head);

  if (!s || (!s.rows.length && !s.loading)) {
    const empty = el('div', 'ttempty');
    empty.innerHTML = '';
    const link = el('a', 'link', 'Read a sample');
    link.onclick = () => sampleSource();
    empty.appendChild(link);
    empty.appendChild(document.createTextNode(' from the source.'));
    card.appendChild(empty);
  } else if (s.loading && !s.rows.length) {
    card.appendChild(el('div', 'ttempty', 'Reading sample…'));
  } else {
    card.appendChild(renderReadonlyGrid(s.columns, s.rows));
  }
  ib.appendChild(card);

  if (s && s.rows.length) {
    ib.appendChild(el('div', 'muted', `<div style="font-size:11px;margin-top:8px">Showing ${s.rows.length} of ${fmt(s.read)} row${s.read === 1 ? '' : 's'} read. Edit the file to change the sample.</div>`));
  }
}

// Sink Test tab: show the rows that would be written (source → all transforms),
// read-only and side-effect-free, so the user sees the final output data inline.
function renderSinkSample(ib: HTMLElement) {
  const s = S.sinkSample;
  const dlqNote = S.pipe?.hasDlq
    ? ' Rows that fail validation are routed to the DLQ and are not shown here.'
    : '';
  ib.appendChild(el('div', 'muted', `<div style="font-size:11.5px;margin:2px 0 12px">Preview exactly what would be written — the source after the full transform chain. <b style="color:var(--text)">Nothing is written to the sink.</b>${dlqNote}</div>`));

  const card = el('div', 'ttcard');
  const head = el('div', 'ttcard-h');
  head.appendChild(el('span', 'ttcard-t', s && s.rows.length ? `Would write · ${s.rows.length} rows` : 'Would write'));
  const acts = el('div', 'ttcard-a');
  const refresh = el('button', 'iconbtn', s && s.loading ? '…' : '↺');
  refresh.title = 'Recompute what would be written';
  refresh.onclick = () => sampleSink();
  acts.appendChild(refresh);
  head.appendChild(acts);
  card.appendChild(head);

  if (!s || (!s.rows.length && !s.loading)) {
    const empty = el('div', 'ttempty');
    const link = el('a', 'link', 'Preview the output');
    link.onclick = () => sampleSink();
    empty.appendChild(link);
    empty.appendChild(document.createTextNode(' of the transform chain.'));
    card.appendChild(empty);
  } else if (s.loading && !s.rows.length) {
    card.appendChild(el('div', 'ttempty', 'Computing output…'));
  } else if (!s.rows.length) {
    card.appendChild(el('div', 'ttempty', 'No rows would be written (all dropped or routed).'));
  } else {
    card.appendChild(renderReadonlyGrid(s.columns, s.rows));
  }
  ib.appendChild(card);

  if (s && s.rows.length) {
    ib.appendChild(el('div', 'muted', `<div style="font-size:11px;margin-top:8px">Showing ${s.rows.length} of ${fmt(s.read)} row${s.read === 1 ? '' : 's'} processed. Side-effect-free — the sink is not touched.</div>`));
    const open = el('button', 'btn sm', '⌕ Open in Data Preview');
    open.style.marginTop = '10px';
    open.onclick = () => doPreview();
    ib.appendChild(open);
  }
}

// A compact read-only table for a set of columns + rows.
function renderReadonlyGrid(columns: string[], rows: Array<Record<string, unknown>>): HTMLElement {
  const wrap = el('div', 'ttgrid');
  const table = document.createElement('table');
  const thead = document.createElement('thead');
  const htr = document.createElement('tr');
  columns.forEach((c) => { const th = document.createElement('th'); th.textContent = c; htr.appendChild(th); });
  thead.appendChild(htr);
  table.appendChild(thead);
  const tbody = document.createElement('tbody');
  rows.forEach((row) => {
    const tr = document.createElement('tr');
    columns.forEach((c) => {
      const td = document.createElement('td');
      const v = row[c];
      td.className = 'rocell';
      td.textContent = v == null ? '' : String(v);
      if (v == null) td.classList.add('nullcell');
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  wrap.appendChild(table);
  return wrap;
}

// Request a read-only sample of raw source rows (rows entering the first stage).
function sampleSource() {
  S.srcSample = { columns: S.srcSample?.columns ?? [], rows: S.srcSample?.rows ?? [], loading: true, read: S.srcSample?.read ?? 0 };
  renderEditor();
  post({ type: 'sampleStageInput', stage: 0, limit: 10 });
}

// Request the rows that would be written to the sink — the source run through
// every transform. stage = transform count runs the whole chain (SampleInput
// applies transforms[:stage]).
function sampleSink() {
  const nTransforms = Array.isArray(S.doc.transforms) ? S.doc.transforms.length : 0;
  S.sinkSample = { columns: S.sinkSample?.columns ?? [], rows: S.sinkSample?.rows ?? [], loading: true, read: S.sinkSample?.read ?? 0 };
  renderEditor();
  post({ type: 'sampleStageInput', stage: nTransforms, limit: 10 });
}


// The atomic stage tester: an editable input sample + "run this stage" that
// exercises one transform in isolation and shows per-row kept/dropped/failed.
function renderStageTester(ib: HTMLElement, sn: Node, stage: number) {
  // (Re)initialise working state when switching to a different stage.
  if (!S.stageTest || S.stageTest.stage !== stage) {
    S.stageTest = { stage, columns: [], rows: [], result: null, view: 'grid', seeding: false, running: false, jsonDraft: null };
  }
  const st = S.stageTest;

  // One short line of intent — no competing headers/paragraphs.
  ib.appendChild(el('div', 'muted', `<div style="font-size:11.5px;margin:2px 0 12px">Run just the <b style="color:var(--text)">${sn.type}</b> activity on a sample and see what it keeps, changes or drops.</div>`));

  // "Input sample" card: a labelled header with compact icon actions on the
  // right, then the grid/JSON body — one tidy block instead of a button row.
  const card = el('div', 'ttcard');
  const head = el('div', 'ttcard-h');
  head.appendChild(el('span', 'ttcard-t', 'Input sample'));
  const acts = el('div', 'ttcard-a');
  const seed = el('button', 'iconbtn', st.seeding ? '…' : '↺');
  seed.title = 'Seed with real rows entering this stage';
  seed.onclick = () => {
    st.seeding = true; st.result = null; renderEditor();
    post({ type: 'sampleStageInput', stage, limit: 10 });
  };
  acts.appendChild(seed);
  const addRow = el('button', 'iconbtn', '＋');
  addRow.title = 'Add a blank row';
  addRow.onclick = () => {
    const cols = st.columns.length ? st.columns : upstreamSchema(sn).map((c) => c.name);
    st.columns = cols;
    const blank: Record<string, unknown> = {};
    cols.forEach((c) => (blank[c] = ''));
    st.rows.push(blank);
    st.jsonDraft = null;
    renderEditor();
  };
  acts.appendChild(addRow);
  const toggle = el('button', 'iconbtn', st.view === 'grid' ? '{ }' : '▦');
  toggle.title = st.view === 'grid' ? 'Edit as JSON' : 'Edit as a table';
  toggle.onclick = () => {
    if (st.view === 'grid') { st.jsonDraft = JSON.stringify(st.rows, null, 2); st.view = 'json'; }
    else { commitJsonDraft(st); st.view = 'grid'; }
    renderEditor();
  };
  acts.appendChild(toggle);
  head.appendChild(acts);
  card.appendChild(head);

  // Input body.
  if (!st.rows.length && st.view === 'grid') {
    const empty = el('div', 'ttempty');
    empty.innerHTML = 'No rows yet.';
    const seedLink = el('a', 'link', 'Seed from source');
    seedLink.onclick = () => { st.seeding = true; renderEditor(); post({ type: 'sampleStageInput', stage, limit: 10 }); };
    empty.appendChild(document.createTextNode(' '));
    empty.appendChild(seedLink);
    empty.appendChild(document.createTextNode(' or add a row.'));
    card.appendChild(empty);
  } else if (st.view === 'json') {
    const ta = document.createElement('textarea');
    ta.className = 'v tt-json';
    ta.value = st.jsonDraft ?? JSON.stringify(st.rows, null, 2);
    ta.oninput = () => { st.jsonDraft = ta.value; };
    ta.onblur = () => { commitJsonDraft(st); renderEditor(); };
    card.appendChild(ta);
  } else {
    card.appendChild(renderStageGrid(st));
  }
  ib.appendChild(card);

  // One primary action, full width.
  const run = el('button', 'btn pri ttrun', st.running ? 'Running…' : '▷ Run this stage');
  run.onclick = () => {
    if (st.view === 'json') commitJsonDraft(st);
    if (!st.rows.length) { toast('info', 'Add or seed at least one input row'); return; }
    st.running = true; renderEditor();
    post({ type: 'testStage', stage, columns: st.columns, rows: st.rows });
  };
  ib.appendChild(run);

  // Output.
  if (st.result) ib.appendChild(renderStageResult(st.result));
}

// Commit the JSON textarea buffer back into st.rows (best-effort).
function commitJsonDraft(st: StageTest) {
  if (st.jsonDraft == null) return;
  try {
    const parsed = JSON.parse(st.jsonDraft);
    if (Array.isArray(parsed)) {
      st.rows = parsed;
      const cols: string[] = [];
      parsed.forEach((r) => Object.keys(r || {}).forEach((k) => { if (!cols.includes(k)) cols.push(k); }));
      if (cols.length) st.columns = cols;
    }
  } catch {
    toast('err', 'Input JSON is invalid — fix it before running');
  }
  st.jsonDraft = null;
}

// Editable table grid for the input sample.
function renderStageGrid(st: StageTest): HTMLElement {
  const wrap = el('div', 'ttgrid');
  const table = document.createElement('table');
  const thead = document.createElement('thead');
  const htr = document.createElement('tr');
  st.columns.forEach((c) => { const th = document.createElement('th'); th.textContent = c; htr.appendChild(th); });
  htr.appendChild(document.createElement('th'));
  thead.appendChild(htr);
  table.appendChild(thead);
  const tbody = document.createElement('tbody');
  st.rows.forEach((row, ri) => {
    const tr = document.createElement('tr');
    st.columns.forEach((c) => {
      const td = document.createElement('td');
      const inp = document.createElement('input');
      inp.value = row[c] == null ? '' : String(row[c]);
      inp.oninput = () => { row[c] = coerceCell(inp.value); };
      td.appendChild(inp);
      tr.appendChild(td);
    });
    const del = document.createElement('td');
    const x = el('span', 'del', '✕');
    x.onclick = () => { st.rows.splice(ri, 1); renderEditor(); };
    del.appendChild(x);
    tr.appendChild(del);
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  wrap.appendChild(table);
  return wrap;
}

// Coerce a cell string into number/bool/null where it clearly is one, so the
// engine receives realistic typed values (matches source read coercion).
function coerceCell(v: string): unknown {
  const t = v.trim();
  if (t === '') return '';
  if (t === 'null') return null;
  if (t === 'true') return true;
  if (t === 'false') return false;
  if (/^-?\d+$/.test(t)) return Number(t);
  if (/^-?\d*\.\d+$/.test(t)) return Number(t);
  return v;
}

// Render the per-row test outcome.
function renderStageResult(r: StageTestResult): HTMLElement {
  const box = el('div', 'ttresult');
  box.appendChild(el('div', 'between', `<span class="ttl2">Result</span><span><span class="chip ok">${r.kept} kept</span> <span class="chip ${r.dropped ? 'warn' : 'idle'}">${r.dropped} dropped</span> <span class="chip ${r.failed ? 'err' : 'idle'}">${r.failed} failed</span></span>`));
  const table = document.createElement('table');
  table.className = 'ttout';
  const thead = document.createElement('thead');
  thead.innerHTML = '<tr><th>status</th><th>output (or reason)</th></tr>';
  table.appendChild(thead);
  const tbody = document.createElement('tbody');
  r.rows.forEach((row) => {
    const tr = document.createElement('tr');
    const stCell = document.createElement('td');
    const cls = row.status === 'kept' ? 'ok' : row.status === 'dropped' ? 'warn' : 'err';
    stCell.innerHTML = `<span class="chip ${cls}">${row.status}</span>`;
    tr.appendChild(stCell);
    const outCell = document.createElement('td');
    if (row.status === 'kept' && row.output) {
      outCell.innerHTML = `<code class="ttcode">${escapeHtml(JSON.stringify(row.output))}</code>`;
    } else if (row.status === 'failed') {
      outCell.innerHTML = `<span class="ttreason">${escapeHtml(row.error || 'failed')}</span>`;
    } else {
      outCell.innerHTML = '<span class="muted" style="font-size:11px">filtered out</span>';
    }
    tr.appendChild(outCell);
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  box.appendChild(table);
  return box;
}

function persistAndRebuild() {
  S.pipe = buildPipe(S.doc);
  post({ type: 'save', doc: S.doc });
  renderEditor();
}

// ── add / remove transforms ─────────────────────────────────────────────────
const TRANSFORMS: Array<{ type: string; label: string; cfg: Record<string, unknown> }> = [
  {
    type: 'filter', label: 'Filter — drop rows by a boolean expression',
    cfg: { expr: "amount > 0 AND status != 'CANCELLED'" }
  },
  {
    type: 'map', label: 'Map — project, rename, coerce & derive fields',
    cfg: { fields: { net_amount: '=multiply($amount, 1.1)' } }
  },
  {
    type: 'validate', label: 'Validate — per-field rules, route to DLQ',
    cfg: { rules: [{ field: 'amount', notNull: true }], onViolation: 'route' }
  },
  {
    type: 'lookup', label: 'Lookup — HTTP enrichment with LRU cache',
    cfg: { keyField: 'id', url: 'https://api.internal/enrich/{id}', method: 'GET', cacheSize: 10000, onError: 'null' }
  },
  {
    type: 'hwm', label: 'High-water mark — incremental cursor',
    cfg: { column: 'updated_at', valueType: 'datetime' }
  },
  {
    type: 'dq', label: 'Data quality — expectations + quality score',
    cfg: { expectations: [{ type: 'expectColumnNotNull', column: 'id' }], onFail: 'warn', qualityThreshold: 0.95 }
  },
  {
    type: 'schemadrift', label: 'Schema drift — detect column add/remove/type',
    cfg: { policy: 'additive', onDrift: 'log' }
  },
  {
    type: 'parse', label: 'Parse — structure a text field into columns (regex/JSON)',
    cfg: { source: 'text', mode: 'regex', pattern: '^(?P<key>\\w+)=(?P<value>.*)$', onError: 'keep', keepSource: true }
  },
  {
    type: 'window', label: 'Window — event-time aggregation (tumbling/sliding/session)',
    cfg: { window: { kind: 'tumbling', sizeMs: 60000 }, eventTime: { column: 'ts', format: 'RFC3339' }, groupBy: [], aggregations: [{ as: 'n', fn: 'count' }], watermark: { strategy: 'boundedOutOfOrderness', maxLatenessMs: 0 } }
  },
  {
    type: 'join', label: 'Join — keyed join against a reference source (stream-table)',
    cfg: { leftKey: 'id', rightKey: 'id', joinType: 'inner', right: { type: 'csv', path: 'reference.csv', hasHeader: true } }
  },
  {
    type: 'cast', label: 'Cast — convert column types (int / float / decimal / date / …)',
    cfg: { columns: [{ name: 'amount', type: 'decimal' }], onError: 'null' }
  },
  {
    type: 'explode', label: 'Explode — expand an array column into one row per element',
    cfg: { column: 'items' }
  },
  {
    type: 'flatten', label: 'Flatten — nested-object columns into dotted columns',
    cfg: { separator: '.' }
  },
  {
    type: 'router', label: 'Router / Split — route rows to sinks by predicate',
    cfg: { routes: [{ name: 'high', when: 'amount > 1000', sinkType: 'jsonlines', sinkConfig: { path: './out/high.jsonl' } }], onUnmatched: 'pass' }
  },
];

// Compute the column names visible at a given transforms[] insert position,
// walking the source columns through any map/lookup stages before it. Used to
// pre-populate a new map with passthrough mappings so users start from a
// complete, valid projection.
function upstreamColumnsAt(insertIndex: number): string[] {
  const src: any = S.doc.source || {};
  let cols: string[] = Array.isArray(src.columns)
    ? src.columns.map((c: any) => String(c.name))
    : (SOURCE_FIXED_SCHEMA[String(src.type)] || []).map((c) => c.name);
  const transforms = Array.isArray(S.doc.transforms) ? S.doc.transforms : [];
  for (let i = 0; i < insertIndex && i < transforms.length; i++) {
    const t: any = transforms[i];
    if (!t) continue;
    if (t.type === 'map' && t.fields && typeof t.fields === 'object') {
      cols = Object.keys(t.fields);
    }
    if (t.type === 'lookup' && Array.isArray(t.outputFields)) {
      t.outputFields.forEach((f: any) => cols.push(String(f)));
    }
    if (t.type === 'parse') {
      parseProducedColumns(t).forEach((n) => { if (!cols.includes(n)) cols.push(n); });
    }
  }
  if (!cols.length && S.preview && S.preview.columns.length) cols = S.preview.columns.slice();
  return cols;
}

function addTransform(index: number) {
  // index is a node index; convert to transforms[] insert position.
  const tfInsert = Math.max(0, index - 1);
  quickPick('Insert transform', TRANSFORMS.map((t) => t.label), (label) => {
    const t = TRANSFORMS.find((x) => x.label === label)!;
    const doc: any = S.doc;
    doc.transforms = Array.isArray(doc.transforms) ? doc.transforms : [];
    const cfg = JSON.parse(JSON.stringify(t.cfg));
    // Map: start from a complete, working projection — pass every upstream
    // column through (=$col) so the user edits from a valid state instead of a
    // single placeholder that references a field their schema may not have.
    if (t.type === 'map') {
      const cols = upstreamColumnsAt(tfInsert);
      if (cols.length) {
        const fields: Record<string, string> = {};
        cols.forEach((c) => { fields[c] = '=$' + c; });
        cfg.fields = fields;
      }
    }
    doc.transforms.splice(tfInsert, 0, { type: t.type, ...cfg });
    S.pipe = buildPipe(S.doc);
    S.selNode = 't' + tfInsert;
    S.inspTab = 'Config';
    post({ type: 'save', doc: S.doc });
    renderEditor();
    toast('ok', `Inserted ${t.type} — configure it in the Inspector`);
  });
}
function deleteTransform(ref: number) {
  const doc: any = S.doc;
  if (!Array.isArray(doc.transforms)) return;
  doc.transforms.splice(ref, 1);
  S.pipe = buildPipe(S.doc);
  S.selNode = 'source';
  post({ type: 'save', doc: S.doc });
  renderEditor();
  toast('warn', 'Removed transform stage');
}

// ── DLQ (separate top-level `dlq` block; never replaces the main sink) ────────
function addDlq() {
  quickPick('Add DLQ — pick a destination', DLQ_DEST_TYPES, (type) => {
    (S.doc as any).dlq = {
      type,
      options: defaultDlqOptions(type),
      maxRowsPerBatch: 0,
      onFull: 'block',
    };
    S.pipe = buildPipe(S.doc);
    S.selNode = 'dlq';
    S.inspTab = 'Config';
    post({ type: 'save', doc: S.doc });
    renderEditor();
    toast('ok', `Added DLQ → ${type}. Configure it in the Inspector →`);
  });
}
function removeDlq() {
  delete (S.doc as any).dlq;
  S.pipe = buildPipe(S.doc);
  S.selNode = 'sink';
  post({ type: 'save', doc: S.doc });
  renderEditor();
  toast('warn', 'Removed DLQ — write failures will only be counted, not routed');
}
function dlqMenu(e: MouseEvent) {
  e.preventDefault();
  e.stopPropagation();
  contextMenu(e.clientX, e.clientY, [
    { ic: '⚙', label: 'Configure DLQ', fn: () => { S.selNode = 'dlq'; S.inspTab = 'Config'; S.inspClosed = false; renderEditor(); } },
    { ic: '⧉', label: 'Open DLQ run stats', fn: () => openTab('dlq', 'dlq') },
    { sep: true },
    { ic: '✕', label: 'Remove DLQ', fn: () => removeDlq() },
  ]);
}

// ── run / preview (native VS Code panels) ────────────────────────────────────
// Warm run (top-right ▷ Run): reuse the already-running engine for a fast
// re-test. The host restarts the engine only if the pipeline text changed.
function doRun() {
  if (S.running) return;
  post({ type: 'showOutput' }); // reveal native Output channel
  post({ type: 'run' });
}
// Fresh run (bottom status-bar ▷ Run): tear down any running engine and start a
// brand-new one, so the run is a clean single app lifecycle with a full
// boot→run→done log. The button then becomes ■ Stop (engine lifecycle).
function doRunFresh() {
  if (S.running) return;
  post({ type: 'showOutput' });
  post({ type: 'run', fresh: true });
}
// Stop the running engine from the canvas toolbar. Optimistically flip the
// local running flag so the button returns to ▷ Run immediately; the host also
// broadcasts a 'stopped' message which reconciles state.
function doStop() {
  if (!S.running) return;
  S.running = false;
  renderStatus();
  post({ type: 'stop' });
}
function doPreview() {
  S.previewOn = true;
  // The host reveals the native Data Preview panel and streams rows there.
  post({ type: 'preview', limit: 100 });
}

// ── report / dlq / drift / tail / fleet (engine-or-demo) ─────────────────────
// effectiveSettings summarises the pipeline-level configuration the way the
// engine's run-config echo does, so the Run Report and the run log agree. Each
// entry is [label, value, muted?, category] — muted marks a default/off value,
// category drives the colour accent (matches the Pipeline Settings sections).
function effectiveSettings(doc: any): Array<[string, string, boolean, string]> {
  const d = doc || {};
  const cp = d.checkpoint || {};
  const perf = d.performance || {};
  const sc = d.scaling || {};
  const obs = d.observability || {};
  const inc = d.incremental || {};
  const mode = String(d.mode || 'etl');
  const scMode = String(sc.mode || 'single');
  const cpType = String(cp.type || 'none');
  const metrics = String(obs.metrics || 'none');
  const incMode = String(inc.mode || 'none');
  const out: Array<[string, string, boolean, string]> = [];
  out.push(['Mode', mode.toUpperCase(), mode === 'etl', 'exec']);
  out.push(['Batch size', fmt(perf.batchSize || 5000) + ' rows', !perf.batchSize, 'perf']);
  out.push(['Scaling', scMode + (scMode === 'sharded' ? ` ${sc.instanceIndex || 0}/${sc.instanceCount || 1}` : ''), scMode === 'single', 'scale']);
  out.push(['Checkpoint', cpType === 'none' ? 'none' : `${cpType} · ${fmt(cp.intervalRows || 10000)}r / ${cp.intervalSeconds || 30}s`, cpType === 'none', 'rel']);
  out.push(['Incremental', incMode === 'none' ? 'none' : `${incMode}${inc.column ? ' · ' + inc.column : ''}`, incMode === 'none', 'rel']);
  // Show the address relevant to the chosen backend: Prometheus scrapes a
  // listenAddr, OTel pushes to an endpoint. Reading the wrong key would surface
  // a stale value left in options after switching backends.
  const metricsAddr = metrics === 'prometheus' ? (obs.options && obs.options.listenAddr)
    : metrics === 'otel' ? (obs.options && obs.options.endpoint) : '';
  out.push(['Metrics', metrics === 'none' ? 'none' : `${metrics}${metricsAddr ? ' · ' + metricsAddr : ''}`, metrics === 'none', 'obs']);
  return out;
}

function renderReport(wv: HTMLElement) {
  wv.innerHTML = '';
  const r = S.lastRun;
  if (!r) {
    wv.appendChild(el('div', 'muted', 'Run the pipeline to generate a report.'));
    const b = el('button', 'btn sm pri', '▷ Run now');
    b.style.marginTop = '10px';
    b.onclick = () => doRun();
    wv.appendChild(b);
    return;
  }
  const processed = r.rowsProcessed || 0;
  const written = r.rowsWritten || 0;
  const dropped = r.rowsDropped || 0;
  const failed = r.rowsFailed || 0;
  const pct = processed ? ((written / processed) * 100).toFixed(1) : '0';
  const rps = r.durationMs ? Math.round(processed / (r.durationMs / 1000)) : 0;
  const cards = el('div', 'cards');
  cards.style.marginBottom = '16px';
  const statusCls = r.status === 'completed' ? 'ok' : r.status === 'failed' ? 'bad' : 'warn';
  cards.innerHTML =
    `<div class="card acc-blue"><div class="cap">Processed</div><div class="kpi">${fmt(processed)}</div><div class="sub">read from source</div></div>` +
    `<div class="card acc-green"><div class="cap">Written</div><div class="kpi" style="color:var(--green)">${fmt(written)}</div><div class="sub">${pct}% to sink</div></div>` +
    `<div class="card acc-purple"><div class="cap">Duration</div><div class="kpi">${(r.durationMs / 1000).toFixed(1)}s</div><div class="sub">~${fmtK(rps)} rows/s</div></div>` +
    `<div class="card acc-${statusCls}"><div class="cap">Status</div><div class="kpi" style="color:${r.status === 'completed' ? 'var(--green)' : r.status === 'failed' ? 'var(--red)' : 'var(--yellow)'}">${r.status}</div><div class="sub">runId ${r.runId.slice(0, 8)}</div></div>`;
  wv.appendChild(cards);

  // Effective settings — mirror the engine's run-config echo so the Run Report
  // and the run log agree on which pipeline-level settings were active. Wrapped
  // defensively so a malformed doc can never blank out the rest of the report.
  try {
    const secHdr = el('div', 'between');
    secHdr.style.margin = '4px 0 10px';
    secHdr.innerHTML = '<h3 class="sec" style="margin:0">Effective configuration</h3>'
      + '<span class="setedit">⚙ Edit settings</span>';
    (secHdr.querySelector('.setedit') as HTMLElement).onclick = () => { S.selNode = SETTINGS_NODE; S.inspClosed = false; S.active = 'canvas'; renderAll(); };
    wv.appendChild(secHdr);
    const grid = el('div', 'setgrid');
    effectiveSettings(S.doc as any).forEach(([label, value, muted, cat]) => {
      const tile = el('div', 'settile cat-' + cat + (muted ? ' off' : ''));
      tile.innerHTML = `<div class="stk">${escapeHtml(label)}</div><div class="stv">${escapeHtml(value)}</div>`;
      grid.appendChild(tile);
    });
    wv.appendChild(grid);
  } catch (e) { /* never let settings summary break the report */ }

  wv.appendChild(el('h3', 'sec', 'Row funnel'));
  const fn = el('div', 'funnel');
  const stages: Array<[string, number, string]> = [
    ['', processed, 'Read'],
    ['loss', dropped, 'Dropped'],
    ['err', failed, 'Failed'],
    ['win', written, 'Written'],
  ];
  const max = Math.max(1, processed);
  stages.forEach((s, i) => {
    const st = el('div', 'fstage ' + s[0]);
    st.innerHTML = `<div class="bar"><i></i></div><div class="n">${fmt(s[1])}</div><div class="l">${s[2]}</div>`;
    st.onclick = () => {
      if (s[0] === 'win') toast('ok', `${fmt(written)} rows written to the sink`);
      else if (s[2] === 'Dropped') toast('info', `${fmt(dropped)} rows dropped by a transform`);
      else if (s[2] === 'Failed') openTab('dlq', 'dlq');
      else toast('info', `${s[2]}: ${fmt(s[1])} rows`);
    };
    fn.appendChild(st);
    const h = s[0] === 'win' ? (s[1] / max) * 100 : Math.max(2, (s[1] / max) * 100);
    setTimeout(() => { (st.querySelector('i') as HTMLElement).style.height = h + '%'; }, 60 + i * 80);
  });
  wv.appendChild(fn);

  if (r.qualityReport) {
    const dq = r.qualityReport;
    const total = dq.passed + dq.failed;
    wv.appendChild(el('h3', 'sec', 'Data quality'));
    wv.appendChild(el('div', 'between', `<span class="muted">${dq.passed}/${total} expectations passed</span><span class="chip ${dq.failed ? 'warn' : 'ok'}">${dq.failed} failed</span>`));
    (dq.violations || []).forEach((v) => wv.appendChild(el('div', 'prob', `<span class="i warn">⚠</span><div class="msg">${escapeHtml(v)}</div>`)));
  }

  const actions = el('div', 'row');
  actions.style.marginTop = '16px';
  const refresh = el('button', 'btn sm', '⟳ Refresh');
  refresh.onclick = () => post({ type: 'metrics' });
  const rerun = el('button', 'btn sm pri', '▷ Re-run');
  rerun.onclick = () => doRun();
  actions.appendChild(refresh);
  actions.appendChild(rerun);
  wv.appendChild(actions);
}
function renderDLQ(wv: HTMLElement) {
  wv.innerHTML = '';
  const failed = S.lastRun?.rowsFailed ?? 0;
  wv.appendChild(el('div', 'between', `<h3 class="sec" style="margin:0">Dead-letter · ${S.pipe?.name || ''}</h3><span class="chip ${failed ? 'err' : 'idle'}">${fmt(failed)} rejected</span>`));
  if (failed > 0 && S.pipe?.hasDlq) {
    wv.appendChild(el('div', 'muted', `<div style="margin-top:8px">${fmt(failed)} rows failed the last run and were routed to the configured DLQ sink. Inspect the DLQ sink target to triage them, then fix upstream data and re-run.</div>`));
  } else if (failed > 0) {
    wv.appendChild(el('div', 'muted', '<div style="margin-top:8px">Rows failed but no DLQ sink is configured — add a <b style="color:var(--text)">validate</b> transform with <code>onViolation: route</code> and a <code>dlq</code> block to capture rejects.</div>'));
  } else {
    wv.appendChild(el('div', 'muted', '<div style="margin-top:8px">No rejected rows. The DLQ inspector lists failed rows after a run routes violations to a DLQ sink.</div>'));
  }
}
function renderDrift(wv: HTMLElement) {
  wv.innerHTML = '';
  wv.appendChild(el('div', 'muted', 'Schema drift review compares the incoming batch against the sink contract. Available while a pipeline is running with a schemadrift stage.'));
}
function renderTail(wv: HTMLElement) {
  wv.innerHTML = '';
  const topic = (S.pipe?.nodes[0].cfg.topic) || 'stream';
  wv.appendChild(el('div', 'between', `<h3 class="sec" style="margin:0"><span class="livedot"></span> Live tail · ${topic}</h3>`));
  const log = el('div', 'taillog');
  log.innerHTML = '<table><thead><tr><th>offset</th><th>part</th><th>key</th><th>value</th></tr></thead><tbody id="tlBody"></tbody></table>';
  wv.appendChild(log);
  wv.appendChild(el('div', 'muted', '<div style="font-size:11.5px;margin-top:8px">Streams from the running engine when a Kafka source is active.</div>'));
}
function renderFleet(wv: HTMLElement) {
  wv.innerHTML = '';
  wv.appendChild(el('div', 'muted', 'The Fleet dashboard aggregates all running pipelines. Open individual .datapipeline files to inspect each one.'));
}

// ── status bar ───────────────────────────────────────────────────────────────
function renderStatus() {
  const sb = $('#statusbar')!;
  sb.innerHTML = '';
  const errs = S.issues.filter((i) => i.sev === 'error').length;
  const warns = S.issues.filter((i) => i.sev === 'warn').length;
  const problems = el('div', 'sbi', `<span style="color:var(--red)">⊗ ${errs}</span> <span style="color:var(--yellow)">⚠ ${warns}</span>`);
  problems.onclick = () => post({ type: 'showProblems' });
  sb.appendChild(problems);
  const eng = el('div', 'sbi', S.port ? `◈ engine :${S.port}` : '◈ engine: stopped');
  eng.onclick = () => post({ type: 'showOutput' });
  sb.appendChild(eng);
  sb.appendChild(el('div', 'grow'));
  // Bottom Run/Stop = engine lifecycle. Runs on a fresh engine, then toggles to
  // Stop; click Stop to tear the engine down. (Top-right ▷ Run does warm re-tests.)
  let runLabel: string;
  let runAction: () => void;
  if (S.running) {
    runLabel = '■ Stop';
    runAction = () => doStop();
  } else if (S.port) {
    runLabel = '■ Stop';
    runAction = () => post({ type: 'stop' });
  } else {
    runLabel = '▷ Run';
    runAction = () => doRunFresh();
  }
  const runItem = el('div', 'sbi run', runLabel);
  runItem.title = S.running
    ? 'Stop the running pipeline (tears the engine down).'
    : S.port
      ? 'Stop the engine (frees the port). Click Run again for a fresh run.'
      : 'Start a fresh engine and run the pipeline (full boot→run→done log)';
  runItem.onclick = runAction;
  sb.appendChild(runItem);
  const pal = el('div', 'sbi', 'Data Pipeline');
  pal.onclick = () => openPalette();
  sb.appendChild(pal);
}

// ── command palette + quick pick ──────────────────────────────────────────────
const COMMANDS = [
  { ic: '?', t: 'Data Pipeline: Show All Features', fn: () => openFeatureGuide() },
  { ic: '▷', t: 'Data Pipeline: Run Pipeline', fn: () => doRun() },
  { ic: '■', t: 'Data Pipeline: Stop Pipeline', fn: () => post({ type: 'stop' }) },
  { ic: '⌕', t: 'Data Pipeline: Dry-run Preview', fn: () => doPreview() },
  { ic: '✔', t: 'Data Pipeline: Validate', fn: () => post({ type: 'validate' }) },
  { ic: '＋', t: 'Data Pipeline: New Pipeline from Template…', fn: () => post({ type: 'newFromTemplate' }) },
  { ic: '⧉', t: 'Data Pipeline: Open Run Report', fn: () => openTab('report', 'report') },
  { ic: '▤', t: 'Data Pipeline: Open Fleet Dashboard', fn: () => openTab('fleet', 'fleet') },
  { ic: '⚠', t: 'Data Pipeline: Open DLQ Inspector', fn: () => openTab('dlq', 'dlq') },
  { ic: '≠', t: 'Data Pipeline: Review Schema Drift', fn: () => openTab('drift', 'drift') },
  { ic: '☁', t: 'Data Pipeline: Deploy to Kubernetes…', fn: () => quickPick('Replicas (sharded StatefulSet)', ['1', '2', '4', '8'], (v) => post({ type: 'deploy', replicas: Number(v) })) },
  { ic: '⛁', t: 'Data Pipeline: New Connection…', fn: () => post({ type: 'newConnection' }) },
  { ic: '{}', t: 'Data Pipeline: Open Raw JSON', fn: () => post({ type: 'openRaw' }) },
];
function closeOverlays() {
  document.querySelectorAll('.scrim,.palette,.ctxmenu,.guide').forEach((x) => x.remove());
}

// ── feature guide ─────────────────────────────────────────────────────────────
// A discoverable, categorised overview of everything the studio can do, so new
// users can find capabilities that otherwise only appear contextually (inside a
// node inspector, a right-click menu, or the command palette).
const FEATURE_GUIDE: Array<{ group: string; items: Array<{ ic: string; t: string; d: string }> }> = [
  {
    group: 'Build the pipeline',
    items: [
      { ic: '＋', t: 'Add / change stages', d: 'Use the ＋Source / ＋Transform / ＋Sink / ＋DLQ pills under the canvas, the ＋ between nodes, or right-click a node.' },
      { ic: '⇄', t: '11 sources · 9 sinks', d: 'csv, tsv, fixed-width, parquet, xml, jsonlines, jdbc, kafka, s3, gcs, adls → and jdbc/parquet/csv/jsonlines/kafka/object-store/null sinks. Pick the type from the node dropdown.' },
      { ic: '⛯', t: '7 transforms', d: 'filter, map, validate, lookup, hwm (high-water mark), dq (data-quality gate), schema-drift — insert any between source and sink.' },
      { ic: '⛁', t: 'Connections', d: 'Save reusable, secret-safe credentials (DB, Kafka, S3/GCS/ADLS, HTTP) once and bind them on any source/sink. Command palette → New Connection.' },
    ],
  },
  {
    group: 'Auto-discover schema (in the node inspector)',
    items: [
      { ic: '⤓', t: 'Browse DB tables', d: 'JDBC source/sink: the Table field lists tables from the live database and auto-fetches column types.' },
      { ic: '⭳', t: 'Read schema from source', d: 'Any file source: read the real column schema — Parquet footer, CSV/TSV header, JSON sample.' },
      { ic: '⤓', t: 'Kafka topics + Schema Registry', d: 'Kafka source: list topics from the cluster, and read Avro/JSON columns from a Schema Registry.' },
      { ic: '⤓', t: 'Detect XML structure', d: 'XML source: sample the file to auto-fill the record element, field xpaths and types.' },
      { ic: '⤓', t: 'Browse object storage', d: 'S3/GCS/ADLS: list bucket objects to pick a key/prefix instead of typing it.' },
    ],
  },
  {
    group: 'Test before you run',
    items: [
      { ic: '⌕', t: 'Dry-run Preview', d: 'Top-right ⌕ Dry-run Preview runs the pipeline in memory and shows sample output without writing to the sink.' },
      { ic: '◲', t: 'Per-stage tester', d: 'Select a transform → Test tab: edit input rows and see exactly which rows are kept / dropped / failed by that one stage.' },
      { ic: '◱', t: 'Source & sink samples', d: 'Select the source or sink → Test tab shows the real rows the stage reads / would write.' },
    ],
  },
  {
    group: 'Run, observe, recover',
    items: [
      { ic: '▷', t: 'Run + Run Report', d: 'Run streams live counts; the Run Report tab shows the read → dropped → failed → written funnel and per-stage metrics.' },
      { ic: '≋', t: 'Live Tail', d: 'For Kafka sources, tail records as they flow through the running engine.' },
      { ic: '⚠', t: 'DLQ (catch)', d: 'Add a dead-letter sink to capture rejected rows; the DLQ tab lists them after a run.' },
      { ic: '≠', t: 'Schema Drift', d: 'The Schema-Drift transform + tab flag and reconcile columns that appear/disappear over time.' },
    ],
  },
  {
    group: 'Ship it',
    items: [
      { ic: '☁', t: 'Deploy to Kubernetes', d: 'Command palette → Deploy generates a Dockerfile + sharded StatefulSet with secrets templated from your connections.' },
      { ic: '＋', t: 'Templates', d: 'Command palette → New Pipeline from Template to start from a working example.' },
      { ic: '{}', t: 'Open Raw JSON', d: 'Edit the underlying .datapipeline JSON directly at any time.' },
    ],
  },
];

function openFeatureGuide() {
  closeOverlays();
  const scrim = el('div', 'scrim');
  scrim.onclick = closeOverlays;
  document.body.appendChild(scrim);
  const g = el('div', 'guide');
  const head = el('div', 'guide-h');
  head.innerHTML = '<div class="guide-title">◈ Data Pipeline Studio — everything you can do</div><span class="guide-x" title="Close (Esc)">✕</span>';
  (head.querySelector('.guide-x') as HTMLElement).onclick = closeOverlays;
  g.appendChild(head);
  const body = el('div', 'guide-body');
  FEATURE_GUIDE.forEach((sec) => {
    body.appendChild(el('div', 'guide-group', sec.group));
    sec.items.forEach((it) => {
      const r = el('div', 'guide-item');
      r.innerHTML = `<span class="gic">${it.ic}</span><div class="gtx"><div class="gt">${it.t}</div><div class="gd">${it.d}</div></div>`;
      body.appendChild(r);
    });
  });
  g.appendChild(body);
  const foot = el('div', 'guide-f', 'Tip: press <b>Ctrl/Cmd+Shift+P</b> for the command palette · hover any ⤓ / ⭳ button in a node inspector to auto-discover schema.');
  g.appendChild(foot);
  document.body.appendChild(g);
}

function openPalette() {
  closeOverlays();
  const scrim = el('div', 'scrim');
  scrim.onclick = closeOverlays;
  document.body.appendChild(scrim);
  const pal = el('div', 'palette');
  const inp = el('input') as HTMLInputElement;
  inp.placeholder = 'Type a command…';
  inp.value = '>';
  pal.appendChild(inp);
  const list = el('div');
  pal.appendChild(list);
  document.body.appendChild(pal);
  let idx = 0;
  let filtered = COMMANDS.slice();
  const draw = () => {
    const q = inp.value.replace(/^>/, '').trim().toLowerCase();
    filtered = COMMANDS.filter((c) => c.t.toLowerCase().includes(q));
    idx = 0;
    list.innerHTML = '';
    if (!filtered.length) { list.appendChild(el('div', 'empty', 'No matching commands')); return; }
    filtered.forEach((c, i) => {
      const parts = c.t.split(':');
      const d = el('div', 'pi' + (i === idx ? ' on' : ''), `<span class="ic">${c.ic}</span><span class="cat">${parts[0]}:</span>${parts.slice(1).join(':')}`);
      d.onclick = () => { closeOverlays(); c.fn(); };
      list.appendChild(d);
    });
  };
  inp.oninput = draw;
  inp.onkeydown = (e) => {
    if (e.key === 'ArrowDown') { idx = Math.min(filtered.length - 1, idx + 1); paint(); e.preventDefault(); }
    else if (e.key === 'ArrowUp') { idx = Math.max(0, idx - 1); paint(); e.preventDefault(); }
    else if (e.key === 'Enter') { if (filtered[idx]) { closeOverlays(); filtered[idx].fn(); } }
    else if (e.key === 'Escape') closeOverlays();
  };
  const paint = () => Array.from(list.children).forEach((c, i) => c.classList.toggle('on', i === idx));
  draw();
  setTimeout(() => { inp.focus(); inp.setSelectionRange(inp.value.length, inp.value.length); }, 10);
}
function quickPick(title: string, items: string[], onPick: (v: string) => void, labelFn?: (v: string) => string) {
  const label = labelFn || ((v: string) => v);
  closeOverlays();
  const scrim = el('div', 'scrim');
  scrim.onclick = closeOverlays;
  document.body.appendChild(scrim);
  const pal = el('div', 'palette');
  const inp = el('input') as HTMLInputElement;
  inp.placeholder = title;
  pal.appendChild(inp);
  const list = el('div');
  pal.appendChild(list);
  document.body.appendChild(pal);

  // Type-to-filter with arrow/Enter selection (matches native quick picks).
  let active = 0;
  const rows: HTMLElement[] = [];
  const pick = (v: string) => { closeOverlays(); onPick(v); };

  const rebuild = () => {
    const q = inp.value.trim().toLowerCase();
    // Filter on both the engine id and the friendly label so "text" finds the
    // Text Document / Custom Text types as well as their raw ids.
    const matches = q ? items.filter((it) => it.toLowerCase().includes(q) || label(it).toLowerCase().includes(q)) : items.slice();
    list.innerHTML = '';
    rows.length = 0;
    if (!matches.length) {
      list.appendChild(el('div', 'pi dis', '<span class="ic">∅</span>No matching items'));
      return;
    }
    if (active >= matches.length) active = matches.length - 1;
    if (active < 0) active = 0;
    matches.forEach((it, idx) => {
      const lbl = label(it) === it ? escapeHtml(it) : `${escapeHtml(label(it))} <span class="muted" style="opacity:.6">${escapeHtml(it)}</span>`;
      const d = el('div', 'pi' + (idx === active ? ' sel' : ''), `<span class="ic">◦</span>${lbl}`);
      d.onmouseenter = () => { active = idx; highlight(); };
      d.onclick = () => pick(it);
      rows.push(d);
      list.appendChild(d);
    });
  };
  const highlight = () => rows.forEach((r, i) => r.classList.toggle('sel', i === active));
  const currentMatches = () => {
    const q = inp.value.trim().toLowerCase();
    return q ? items.filter((it) => it.toLowerCase().includes(q) || label(it).toLowerCase().includes(q)) : items.slice();
  };

  inp.oninput = () => { active = 0; rebuild(); };
  inp.onkeydown = (e) => {
    const m = currentMatches();
    if (e.key === 'ArrowDown') { e.preventDefault(); active = Math.min(active + 1, m.length - 1); highlight(); rows[active]?.scrollIntoView({ block: 'nearest' }); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); active = Math.max(active - 1, 0); highlight(); rows[active]?.scrollIntoView({ block: 'nearest' }); }
    else if (e.key === 'Enter') { e.preventDefault(); if (m[active] != null) pick(m[active]); }
    else if (e.key === 'Escape') { e.preventDefault(); closeOverlays(); }
  };

  rebuild();
  setTimeout(() => inp.focus(), 0);
}

// ── right-click context menu ──────────────────────────────────────────────────
function contextMenu(x: number, y: number, items: Array<{ ic?: string; label?: string; fn?: () => void; sep?: boolean; dis?: boolean }>) {
  closeOverlays();
  const scrim = el('div', 'scrim');
  (scrim.style as any).zIndex = '79';
  scrim.onmousedown = closeOverlays;
  scrim.oncontextmenu = (e) => { e.preventDefault(); closeOverlays(); };
  document.body.appendChild(scrim);
  const m = el('div', 'ctxmenu');
  items.forEach((it) => {
    if (it.sep) { m.appendChild(el('div', 'sep')); return; }
    const d = el('div', 'mi' + (it.dis ? ' dis' : ''), `<span class="ic">${it.ic || ''}</span>${it.label}`);
    if (!it.dis) d.onclick = () => { closeOverlays(); it.fn && it.fn(); };
    m.appendChild(d);
  });
  document.body.appendChild(m);
  const r = m.getBoundingClientRect();
  m.style.left = Math.min(x, innerWidth - r.width - 8) + 'px';
  m.style.top = Math.min(y, innerHeight - r.height - 8) + 'px';
}
function nodeMenu(n: Node, i: number, e: MouseEvent) {
  e.preventDefault();
  e.stopPropagation();
  const isT = n.kind === 'transform';
  const items: Array<{ ic?: string; label?: string; fn?: () => void; sep?: boolean; dis?: boolean }> = [
    { ic: '⚙', label: 'Configure', fn: () => { S.selNode = n.id; S.inspTab = 'Config'; S.inspClosed = false; renderEditor(); } },
    { ic: '▷', label: 'Test this stage on sample', fn: () => doPreview() },
    { sep: true },
    { ic: '＋', label: 'Insert transform before', fn: () => addTransform(i) },
    { ic: '＋', label: 'Insert transform after', fn: () => addTransform(i + 1) },
  ];
  // On the sink node, offer to add/configure the DLQ (a separate destination
  // that routes write-time failures alongside this sink).
  if (n.kind === 'sink') {
    items.push({ sep: true });
    if (S.pipe?.hasDlq) {
      items.push({ ic: '⚠', label: 'Configure DLQ', fn: () => { S.selNode = 'dlq'; S.inspTab = 'Config'; S.inspClosed = false; renderEditor(); } });
    } else {
      items.push({ ic: '⚠', label: 'Add DLQ (route write failures)…', fn: () => addDlq() });
    }
  }
  items.push(
    { sep: true },
    { ic: '✕', label: 'Delete stage', dis: !isT, fn: () => { if (typeof n.ref === 'number') deleteTransform(n.ref); } },
  );
  contextMenu(e.clientX, e.clientY, items);
}

// ── toasts ────────────────────────────────────────────────────────────────────
function toast(kind: 'ok' | 'err' | 'warn' | 'info', msg: string, sticky = false) {
  const box = $('#toasts')!;
  const t = el('div', 'toast');
  const icon = { ok: '<span style="color:var(--green)">✓</span>', err: '<span style="color:var(--red)">✕</span>', warn: '<span style="color:var(--yellow)">⚠</span>', info: '<span style="color:var(--blueHi)">ℹ</span>' }[kind];
  t.innerHTML = `<div class="tt"><span class="i">${icon}</span><div>${msg}</div><span class="close">✕</span></div>`;
  (t.querySelector('.close') as HTMLElement).onclick = () => t.remove();
  box.appendChild(t);
  if (!sticky) setTimeout(() => { t.style.opacity = '0'; t.style.transition = 'opacity .3s'; setTimeout(() => t.remove(), 300); }, 3600);
}

function escapeHtml(s: string) {
  return String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// ── host message handling ─────────────────────────────────────────────────────
// Flatten a podlog source's nested `options` block onto the source object so the
// first-class labeled form renders. The engine reads flat or nested identically
// (SourceDef.UnmarshalJSON), so this is a lossless, engine-equivalent migration.
function migratePodlogSource(doc: any): void {
  const src = doc && doc.source;
  if (!src || src.type !== 'podlog') return;
  const opts = src.options;
  if (!opts || typeof opts !== 'object' || Array.isArray(opts)) return;
  for (const [k, v] of Object.entries(opts)) {
    if (!(k in src)) src[k] = v;
  }
  delete src.options;
}

window.addEventListener('message', (ev: MessageEvent<HostToWebview>) => {
  const msg = ev.data;
  switch (msg.type) {
    case 'init':
      S.fileName = msg.fileName;
      S.ref = msg.ref;
      S.doc = msg.doc || {};
      migratePodlogSource(S.doc);
      S.pipe = buildPipe(S.doc);
      if (!S.selNode) S.selNode = 'source';
      renderAll();
      post({ type: 'validate' });
      break;
    case 'update':
      S.doc = msg.doc || {};
      migratePodlogSource(S.doc);
      S.pipe = buildPipe(S.doc);
      if (cur()?.type === 'canvas') renderEditor();
      post({ type: 'validate' });
      break;
    case 'runStarting':
      S.running = false;
      renderStatus();
      break;
    case 'runStarted':
      S.running = true;
      S.ran = true;
      S.port = msg.port;
      S.pipe = buildPipe(S.doc);
      renderTabs();
      renderStatus();
      renderEditor();
      break;
    case 'runResult':
      S.lastRun = msg.result;
      S.ran = true;
      if (msg.final) {
        S.running = false;
        const r = msg.result;
        toast(
          r.status === 'failed' ? 'err' : r.status === 'partial' ? 'warn' : 'ok',
          `Run ${r.status} — ${fmt(r.rowsWritten)} written · ${fmt(r.rowsFailed)} failed · ${(r.durationMs / 1000).toFixed(1)}s`,
          r.status === 'failed',
        );
        openTab('report', 'report');
      }
      renderTabs();
      renderStatus();
      if (cur()?.type === 'report' || cur()?.type === 'dlq') renderEditor();
      break;
    case 'runError':
      S.running = false;
      renderTabs();
      renderStatus();
      toast('err', msg.message, true);
      break;
    case 'stopped':
      S.running = false;
      S.port = null;
      S.pipe = buildPipe(S.doc);
      renderTabs();
      renderStatus();
      renderEditor();
      break;
    case 'validationResult':
      S.issues = msg.issues || [];
      renderStatus();
      break;
    case 'previewResult':
      S.preview = msg.result;
      S.previewOn = true;
      // Full table renders in the native Data Preview panel; here we only refresh
      // the Inspector Schema/Test tabs which surface a summary.
      if (cur()?.type === 'canvas' && (S.inspTab === 'Schema' || S.inspTab === 'Test')) renderEditor();
      break;
    case 'previewError':
      S.preview = null;
      toast('err', 'Preview failed: ' + msg.message);
      break;
    case 'definition':
      toast('info', `Engine definition: ${msg.def.source.type} → ${msg.def.sink.type} · ${msg.def.transforms.length} transform(s)`);
      break;
    case 'metricsResult': {
      const st = msg.data as StatusResponse | undefined;
      if (!msg.error && st && Array.isArray(st.runs) && st.runs.length) {
        S.lastRun = st.runs[st.runs.length - 1];
        if (cur()?.type === 'report' || cur()?.type === 'dlq') renderEditor();
      }
      break;
    }
    case 'browseResult': {
      const cb = pendingBrowse[msg.id];
      if (cb) { cb(msg.path); delete pendingBrowse[msg.id]; }
      break;
    }
    case 'browseCancelled':
      delete pendingBrowse[msg.id];
      break;
    case 'connections':
      S.conns = (msg.items as ConnSummary[]) || [];
      if (cur()?.type === 'canvas' && S.inspTab === 'Config') renderEditor();
      break;
    case 'schemaInferred': {
      const node = inferTargetNode;
      inferTargetNode = null;
      if (node) {
        setCfgValue(node, 'columns', msg.columns);
        renderEditor();
      }
      toast('ok', `Inferred ${msg.count} column${msg.count === 1 ? '' : 's'} from the sample file`);
      break;
    }
    case 'schemaInferError':
      inferTargetNode = null;
      renderEditor();
      toast('err', 'Could not infer columns: ' + msg.message);
      break;
    case 'stageSample':
      // Route to the source read-only sample when the source node is active,
      // to the sink "would write" sample when the sink is active, otherwise to
      // the transform stage tester's editable input.
      if (S.selNode === 'source') {
        S.srcSample = { columns: msg.columns, rows: msg.rows.map((r) => ({ ...r })), loading: false, read: msg.rows.length };
        renderEditor();
        toast(msg.rows.length ? 'ok' : 'info', `Read ${msg.rows.length} row${msg.rows.length === 1 ? '' : 's'} from the source`);
        break;
      }
      if (S.selNode === 'sink') {
        S.sinkSample = { columns: msg.columns, rows: msg.rows.map((r) => ({ ...r })), loading: false, read: msg.rows.length };
        renderEditor();
        toast(msg.rows.length ? 'ok' : 'info', `${msg.rows.length} row${msg.rows.length === 1 ? '' : 's'} would be written`);
        break;
      }
      if (S.stageTest && S.stageTest.stage === msg.stage) {
        S.stageTest.seeding = false;
        S.stageTest.columns = msg.columns.length ? msg.columns : S.stageTest.columns;
        S.stageTest.rows = msg.rows.map((r) => ({ ...r }));
        S.stageTest.jsonDraft = null;
        S.stageTest.result = null;
        renderEditor();
        toast(msg.rows.length ? 'ok' : 'info', `Seeded ${msg.rows.length} input row${msg.rows.length === 1 ? '' : 's'}`);
      }
      break;
    case 'stageSampleError':
      if (S.selNode === 'source' && S.srcSample) { S.srcSample.loading = false; renderEditor(); }
      if (S.selNode === 'sink' && S.sinkSample) { S.sinkSample.loading = false; renderEditor(); }
      if (S.stageTest && S.stageTest.stage === msg.stage) { S.stageTest.seeding = false; renderEditor(); }
      toast('err', 'Could not read sample: ' + msg.message);
      break;
    case 'stageTestResult':
      if (S.stageTest && S.stageTest.stage === msg.stage) {
        S.stageTest.running = false;
        S.stageTest.result = msg.result;
        renderEditor();
      }
      break;
    case 'stageTestError':
      if (S.stageTest && S.stageTest.stage === msg.stage) { S.stageTest.running = false; renderEditor(); }
      toast('err', 'Stage test failed: ' + msg.message);
      break;
    case 'dbTables': {
      const st = dbState(msg.target);
      st.loadingTables = false; st.error = null; st.tables = msg.tables;
      renderEditor();
      toast('ok', `Found ${msg.tables.length} table${msg.tables.length === 1 ? '' : 's'}`);
      break;
    }
    case 'dbTablesError': {
      const st = dbState(msg.target);
      st.loadingTables = false; st.error = msg.message; st.tables = [];
      renderEditor();
      toast('err', 'Could not list tables: ' + msg.message);
      break;
    }
    case 'dbColumns': {
      const st = dbState(msg.target);
      st.loadingCols = false; st.error = null;
      // Write the fetched columns into the stage's `columns` config.
      const p = S.pipe;
      const node = msg.target === 'source' ? p?.nodes.find((n) => n.kind === 'source')
        : msg.target === 'sink' ? p?.nodes.find((n) => n.kind === 'sink')
          : p?.dlqNode;
      if (node) {
        const cols = msg.columns.map((c) => ({ name: c.name, type: c.type }));
        setCfgValue(node, 'columns', cols);
      }
      renderEditor();
      toast('ok', `Loaded ${msg.columns.length} column${msg.columns.length === 1 ? '' : 's'} from the database`);
      break;
    }
    case 'dbColumnsError': {
      const st = dbState(msg.target);
      st.loadingCols = false; st.error = msg.message;
      renderEditor();
      toast('err', 'Could not fetch schema: ' + msg.message);
      break;
    }
    case 'sourceSchema': {
      S.srcSchemaLoading = false;
      const node = S.pipe?.nodes.find((n) => n.kind === 'source');
      if (node) setCfgValue(node, 'columns', msg.columns.map((c) => ({ name: c.name, type: c.type })));
      renderEditor();
      toast('ok', `Read ${msg.columns.length} column${msg.columns.length === 1 ? '' : 's'} from the source`);
      break;
    }
    case 'sourceSchemaError':
      S.srcSchemaLoading = false;
      renderEditor();
      toast('err', 'Could not read source schema: ' + msg.message);
      break;
    case 'objectList': {
      const st = dbState(msg.target);
      st.objects = msg.objects.map((o) => o.key);
      st.loadingObjects = false;
      renderEditor();
      toast(msg.objects.length ? 'ok' : 'info', `Found ${msg.objects.length} object${msg.objects.length === 1 ? '' : 's'}`);
      break;
    }
    case 'objectListError': {
      const st = dbState(msg.target);
      st.loadingObjects = false; st.error = msg.message;
      renderEditor();
      toast('err', 'Could not list objects: ' + msg.message);
      break;
    }
    case 'kafkaTopics': {
      const st = dbState(msg.target);
      st.topics = msg.topics; st.loadingTopics = false; st.error = null;
      renderEditor();
      toast(msg.topics.length ? 'ok' : 'info', `Found ${msg.topics.length} topic${msg.topics.length === 1 ? '' : 's'}`);
      break;
    }
    case 'kafkaTopicsError': {
      const st = dbState(msg.target);
      st.loadingTopics = false; st.error = msg.message;
      renderEditor();
      toast('err', 'Could not list topics: ' + msg.message);
      break;
    }
    case 'schemaRegistry': {
      S.srcSchemaLoading = false;
      const node = S.pipe?.nodes.find((n) => n.kind === 'source');
      if (node) setCfgValue(node, 'columns', msg.columns.map((c) => ({ name: c.name, type: c.type })));
      renderEditor();
      toast('ok', `Read ${msg.columns.length} column${msg.columns.length === 1 ? '' : 's'} from ${msg.subject}`);
      break;
    }
    case 'schemaRegistryError': {
      S.srcSchemaLoading = false;
      renderEditor();
      toast('err', 'Could not read schema registry: ' + msg.message);
      break;
    }
    case 'xmlSchema': {
      S.srcSchemaLoading = false;
      const doc: any = S.doc;
      doc.source = doc.source || {};
      doc.source.recordElement = msg.recordElement;
      doc.source.fields = msg.fields.map((fld) => ({ name: fld.name, xpath: fld.xpath, type: fld.type }));
      doc.source.columns = msg.columns.map((c) => ({ name: c.name, type: c.type }));
      S.pipe = buildPipe(S.doc);
      post({ type: 'save', doc: S.doc });
      renderEditor();
      toast('ok', `Detected <${msg.recordElement}> with ${msg.fields.length} field${msg.fields.length === 1 ? '' : 's'}`);
      break;
    }
    case 'xmlSchemaError': {
      S.srcSchemaLoading = false;
      renderEditor();
      toast('err', 'Could not detect XML structure: ' + msg.message);
      break;
    }
  }
});

// ── keyboard ──────────────────────────────────────────────────────────────────
document.addEventListener('keydown', (e) => {
  const mod = e.metaKey || e.ctrlKey;
  if (mod && e.shiftKey && e.key.toLowerCase() === 'p') { e.preventDefault(); openPalette(); }
  else if (mod && e.key.toLowerCase() === 'r' && e.shiftKey) { e.preventDefault(); doRun(); }
  else if (e.key === 'Escape') closeOverlays();
});

// ── boot ──────────────────────────────────────────────────────────────────────
post({ type: 'ready' });
