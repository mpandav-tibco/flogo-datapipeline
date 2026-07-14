// Flogo Data Pipeline Studio — webview app.
// Renders the active .datapipeline document as an interactive canvas with an
// inspector, bottom panel, run report, DLQ / schema-drift / live-tail views,
// a command palette and right-click menus. All persistence + engine actions
// are delegated to the extension host over postMessage.

import type { PipelineDoc } from '../src/model/pipeline';
import type { HostToWebview, WebviewToHost } from '../src/protocol';
import type { RunResponse, PreviewResult, StatusResponse, StageTestResult } from '../src/engine/apiClient';
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
  const bits: string[] = [type];
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
    nm: (src.path && String(src.path).split(/[\\/]/).pop()) || src.type || 'source',
    meta: metaFor('source', src.type || '—', src),
    cfg: cfgOf(src),
    ref: 'source',
  });
  transforms.forEach((t: any, i: number) => {
    nodes.push({
      id: 't' + i,
      kind: 'transform',
      type: t.type || 'transform',
      nm: t.name || t.type || 'transform',
      meta: metaFor('transform', t.type || '—', t),
      cfg: cfgOf(t),
      ref: i,
    });
  });
  nodes.push({
    id: 'sink',
    kind: 'sink',
    type: sink.type || 'sink',
    nm: sink.table || sink.type || 'sink',
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

  return {
    name: doc.name || S.fileName.replace(/\.datapipeline$/, ''),
    description: doc.description || '',
    mode: String(doc.mode || 'etl').toUpperCase(),
    status: S.running ? 'running' : S.ran ? 'partial' : 'idle',
    nodes,
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
  region: 'Region', key: 'Key template',
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
  rules: 'JSON array of field rules: [{ "field": "amount", "notNull": true }]',
  expectations: 'JSON array of DQ expectations (expectColumnNotNull, expectValuesInSet, …).',
  url: 'Enrichment endpoint. Use {keyField} as the placeholder, e.g. https://api/enrich/{id} — substituted per row.',
  headerKVRegex: 'Regex with two capture groups (key, value) matching each header line. Default handles “Key  <2+ spaces>  Value”.',
  bodyColumnMarker: 'Regex identifying the line that names the table columns, e.g. ^MEASUREMENT\\s',
  bodyDelimiter: 'Regex that splits each body row into fields. Default \\s+ handles whitespace-aligned tables.',
  broadcastHeader: 'Stamp every header attribute onto each body row so downstream transforms and the sink see file-level context.',
  template: 'Go text/template. Available: .Rows (each row as a map), .Count, .Columns, .Attr (header fields). Helpers: rpad, lpad, padc, sci, upper, lower, trim, add, seq.',
  templatePath: 'Path to a .tmpl file. Overrides the inline template when set.',
};
const SOURCE_TYPES = ['csv', 'tsv', 'fixedwidth', 'sectioned', 'parquet', 'xml', 'jsonlines', 'jdbc', 'kafka', 's3', 'gcs', 'adls'];
const SINK_TYPES = ['jdbc', 'parquet', 'csv', 'jsonlines', 'template', 'kafka', 's3', 'gcs', 'adls', 'null'];
// Friendly, generic display labels for stage types whose engine id is jargon.
// The engine id (used in the .datapipeline file) is unchanged; only the picker
// label differs. Types without an entry fall back to their raw id.
const TYPE_LABELS: Record<string, string> = {
  sectioned: 'Text Document',
  template: 'Custom Text',
};
function typeLabel(t: string): string {
  return TYPE_LABELS[t] || t;
}
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
    jdbc: { driver: 'postgres', dsn: '', table: '', orderKey: '', batchSize: 5000 },
    kafka: { brokers: '', topic: '', groupId: 'dp-consumer', startOffset: 'earliest', deserializer: 'json', mode: 'batch', stopAtCurrentEnd: true },
    s3: { bucket: '', prefix: '', region: 'us-east-1', format: 'csv' },
    gcs: { bucket: '', prefix: '', format: 'csv' },
    adls: { bucket: '', prefix: '', format: 'csv' },
  };
  const sink: Record<string, Record<string, unknown>> = {
    jdbc: { driver: 'postgres', dsn: '', table: '', batchSize: 1000, onConflict: 'insert', autoMigrate: true },
    parquet: { path: './out/output.parquet', compression: 'snappy', rowGroupSize: 100000 },
    csv: { path: './out/output.csv', delimiter: ',', writeHeader: true },
    jsonlines: { path: './out/output.jsonl' },
    template: { path: './out/{{index .Attr "id"}}.txt', template: '{{range .Rows}}{{.}}\n{{end}}' },
    kafka: { brokers: '', topic: '', ackMode: 'all', serializer: 'json' },
    s3: { bucket: '', key: '${date}/part-${uuid}.parquet', format: 'parquet' },
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
    s3: { bucket: '', key: 'dlq/${date}.jsonl', format: 'jsonlines' },
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
    /^(has|is|enable|delete|trim|nullable|repeating|download|autoMigrate|caseSensitive|deleteOnSuccess)/i.test(k);
}
function isFileField(k: string): boolean {
  return /(^path$|_path$|file$|dir$|directory$)/i.test(k);
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

// Build one config field widget appropriate to its key/value type.
function renderField(node: Node, key: string, value: string): HTMLElement {
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', humanize(key)));

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

  // Enum → dropdown
  if (FIELD_ENUMS[key]) {
    const sel = el('select', 'v') as HTMLSelectElement;
    const opts = FIELD_ENUMS[key].slice();
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

  // File path → text + Browse
  if (isFileField(key)) {
    const row = el('div', 'inputrow');
    const inp = el('input', 'v') as HTMLInputElement;
    inp.value = value;
    inp.onchange = () => setCfgField(node, key, inp.value);
    const btn = el('button', 'browse', '📂 Browse');
    const folder = node.type === 'file' || /checkpoint/i.test(node.type) || key.toLowerCase().includes('dir');
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
      row.appendChild(el('span', 'cklbl', `${c.name} <span class="ty">${c.type}</span>`));
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
  if (node.kind === 'source') {
    const readBtn = el('button', 'browse infer-btn', S.srcSchemaLoading ? 'Reading schema…' : '⤓ Read schema from source');
    readBtn.style.marginTop = '6px';
    readBtn.onclick = () => { S.srcSchemaLoading = true; renderEditor(); post({ type: 'fetchSourceSchema' }); };
    f.appendChild(readBtn);
    f.appendChild(el('div', 'hint', 'Opens the source (e.g. reads a Parquet footer) and fills columns from its actual schema.'));
  }
  return f;
}

// The source node awaiting an inferSchema response, so we can write the columns
// back into the right stage when the host replies.
let inferTargetNode: Node | null = null;

// Builtin mapper functions available to the map transform's expressions. These
// mirror the engine's `builtins` map (transform/mapper). Each carries a snippet
// with a ${} cursor marker so clicking inserts a ready-to-fill call.
const MAP_FUNCTIONS: Array<{ name: string; snippet: string; hint: string }> = [
  { name: 'concat', snippet: 'concat($1, $2)', hint: 'Join values into a string' },
  { name: 'upper', snippet: 'upper($1)', hint: 'Uppercase a string' },
  { name: 'lower', snippet: 'lower($1)', hint: 'Lowercase a string' },
  { name: 'trim', snippet: 'trim($1)', hint: 'Strip surrounding whitespace' },
  { name: 'coalesce', snippet: 'coalesce($1, $2)', hint: 'First non-null value' },
  { name: 'multiply', snippet: 'multiply($1, $2)', hint: 'Numeric a × b' },
  { name: 'divide', snippet: 'divide($1, $2)', hint: 'Numeric a ÷ b' },
  { name: 'scientific', snippet: 'scientific($1)', hint: 'Signed scientific notation, e.g. +1.234E-05' },
  { name: 'lpad', snippet: 'lpad($1, $2)', hint: 'Left-pad value to width (right-aligned)' },
  { name: 'rpad', snippet: 'rpad($1, $2)', hint: 'Right-pad value to width (left-aligned)' },
  { name: 'clamp', snippet: 'clamp($1, $2, $3)', hint: 'Constrain a number between min and max' },
];

// The currently focused mapping-expression input, so Available-data / Function
// chips insert at the caret of the row the user is editing.
let mapFocusedInput: HTMLInputElement | null = null;

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
    inp.value = inp.value.replace('$1', '').replace('$2', '');
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
  f.appendChild(el('div', 'k', 'Field mappings'));
  const target = cfgTarget(node);
  const fields: Record<string, string> = (target.fields && typeof target.fields === 'object') ? target.fields : {};
  // Preserve a stable, user-visible order via fieldOrder (backend honours it).
  let order: string[] = Array.isArray(target.fieldOrder) ? target.fieldOrder.filter((k: string) => k in fields) : [];
  for (const k of Object.keys(fields)) if (!order.includes(k)) order.push(k);

  const persist = () => {
    target.fields = fields;
    target.fieldOrder = order;
    setCfgValue(node, 'fields', fields);
  };

  const box = el('div', 'maps');
  order.forEach((outName, i) => {
    const row = el('div', 'mrow');
    const out = document.createElement('input');
    out.className = 'out';
    out.value = outName;
    out.placeholder = 'field';
    out.title = 'Output field name';
    out.onchange = () => {
      const nn = out.value.trim();
      if (!nn || nn === outName) return;
      const val = fields[outName];
      delete fields[outName];
      fields[nn] = val;
      order[i] = nn;
      persist();
      renderEditor();
    };
    const eq = el('span', 'eq', '←');
    const expr = document.createElement('input');
    expr.className = 'expr';
    expr.value = fields[outName] ?? '';
    expr.placeholder = '=$sourceField or =upper($field)';
    expr.title = 'Mapping expression';
    expr.onfocus = () => { mapFocusedInput = expr; };
    expr.onchange = () => { fields[outName] = expr.value; persist(); };
    const del = el('span', 'del', '✕');
    del.title = 'Remove mapping';
    del.onclick = () => { delete fields[outName]; order = order.filter((k) => k !== outName); persist(); renderEditor(); };
    row.appendChild(out);
    row.appendChild(eq);
    row.appendChild(expr);
    row.appendChild(del);
    box.appendChild(row);
  });
  const add = el('button', 'addmap', '＋ Add field mapping');
  add.onclick = () => {
    let base = 'new_field';
    let n = base;
    let c = 1;
    while (n in fields) n = base + '_' + c++;
    fields[n] = '';
    order.push(n);
    persist();
    renderEditor();
  };
  box.appendChild(add);
  f.appendChild(box);

  // ── Available data (upstream columns) — click to insert =$field ──────────
  // Show the schema visible ENTERING this map (i.e. produced by everything
  // before it), so users insert the source fields they map from — not the
  // map's own output fields.
  const help = el('div', 'mhelp');
  const cols = upstreamSchema(node);
  if (cols.length) {
    help.appendChild(el('div', 'lbl', 'Available data'));
    const chips = el('div', 'chips');
    cols.forEach((c) => {
      const chip = el('span', 'chip2', c.name);
      chip.title = `Insert =$${c.name}`;
      chip.onclick = () => insertIntoMapExpr(`=$${c.name}`);
      chips.appendChild(chip);
    });
    help.appendChild(chips);
  }

  // ── Functions — click to insert a call snippet ───────────────────────────
  help.appendChild(el('div', 'lbl', 'Functions'));
  const fchips = el('div', 'chips');
  MAP_FUNCTIONS.forEach((fn) => {
    const chip = el('span', 'chip2 fn', `ƒ ${fn.name}`);
    chip.title = fn.hint;
    chip.onclick = () => insertIntoMapExpr(`=${fn.snippet}`);
    fchips.appendChild(chip);
  });
  help.appendChild(fchips);
  help.appendChild(el('div', 'focushint', 'Click an expression field, then a data column or function to insert at the caret. Prefix literals with =, reference fields with =$name.'));
  f.appendChild(help);
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
      '<button class="btn sm" data-a="preview">⌕ Dry-run Preview</button>' +
      (kafka ? '<button class="btn sm" data-a="tail">≋ Live Tail</button>' : '') +
      (S.running
        ? '<button class="btn sm" data-a="running" disabled>⟳ Running…</button>'
        : '<button class="btn sm pri" data-a="run" title="Run on the warm engine (fast re-test — keeps the engine hot)">▷ Run</button>');
    (acts.querySelector('[data-a="help"]') as HTMLElement).onclick = () => openFeatureGuide();
    (acts.querySelector('[data-a="preview"]') as HTMLElement).onclick = () => doPreview();
    const tl = acts.querySelector('[data-a="tail"]') as HTMLElement | null;
    if (tl) tl.onclick = () => openTab('tail', 'tail');
    const rb = acts.querySelector('[data-a="run"]') as HTMLElement | null;
    if (rb) rb.onclick = () => doRun();
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
    bc.innerHTML = `◈ ${S.pipe.name} <span class="sep">›</span> ${sn ? sn.type + ': ' + sn.nm : ''} <span class="sep">›</span> <span class="chip plain" style="padding:1px 7px">${S.pipe.mode}</span>`;
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

function renderCanvas(m: HTMLElement) {
  const p = S.pipe!;
  const c = el('div', 'edcontent');
  const cv = el('div', 'canvas');
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

  const row = el('div', 'flowrow');
  p.nodes.forEach((n, i) => {
    if (i > 0) {
      const cn = el('div', 'conn' + (S.ran ? ' live' : ''));
      const ins = el('div', 'ins', '＋');
      ins.title = 'Insert transform here';
      ins.onclick = (e) => { e.stopPropagation(); addTransform(i); };
      cn.appendChild(ins);
      cn.onclick = () => addTransform(i);
      row.appendChild(cn);
    }
    const w = [92, 94, 94, 89, 89][i] || 90;
    const nd = el('div', 'node ' + (n.kind === 'source' ? 'src' : n.kind === 'sink' ? 'sink' : '') + (S.ran ? ' ran' : '') + (S.selNode === n.id ? ' sel' : ''));
    nd.style.setProperty('--w', w + '%');
    nd.innerHTML = `<div class="kind">${n.kind}</div><div class="nm">${n.nm}</div><div class="meta">${n.meta}</div><div class="thru"><i></i></div>`;
    nd.tabIndex = 0;
    nd.onclick = () => { S.selNode = n.id; S.inspTab = 'Config'; S.inspClosed = false; renderEditor(); };
    nd.oncontextmenu = (e) => nodeMenu(n, i, e);
    row.appendChild(nd);
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

  // DLQ: the pipeline's "catch" — a dead-letter handler that captures rows the
  // main sink rejects (write failures, post-isolation validate rejects). It is
  // rendered as an error-handler lane beneath the flow, elbow-connected from the
  // sink, so it reads as a try/catch rather than a disconnected node.
  if (p.hasDlq && p.dlqNode) {
    const dn = p.dlqNode;
    const br = el('div', 'catchlane');
    const col = el('div', 'catchcol');
    col.innerHTML = '<span class="elbow-line"></span><span class="elbow-tag">⚠ on failure → catch</span>';
    const dnode = el('div', 'node catch' + (S.selNode === 'dlq' ? ' sel' : ''));
    dnode.innerHTML = `<div class="kind">dead-letter (catch)</div><div class="nm">${dn.nm}</div><div class="meta">${dn.type} · ${dn.meta}</div>`;
    dnode.tabIndex = 0;
    dnode.title = 'Rows rejected by the sink are captured here — the main pipeline keeps running.';
    dnode.onclick = () => { S.selNode = 'dlq'; S.inspTab = 'Config'; S.inspClosed = false; renderEditor(); };
    dnode.oncontextmenu = (e) => dlqMenu(e);
    col.appendChild(dnode);
    br.appendChild(col);
    wrap.appendChild(br);
  }

  const hint = el('div', 'canvas-hint pillrow');
  const dlqPill = p.hasDlq ? '' : '<span class="pill" data-h="dlq">＋ DLQ</span>';
  hint.innerHTML = '<span class="pill" data-h="src">＋ Source</span><span class="pill" data-h="tf">＋ Transform</span><span class="pill" data-h="sink">＋ Sink</span>' + dlqPill + '<span class="pill">⤢ Fit</span>';
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

  // inspector — the selected node may be the DLQ branch node (not in p.nodes).
  const insp = el('div', 'inspector');
  const sn = (S.selNode === 'dlq' && p.dlqNode) ? p.dlqNode : (p.nodes.find((n) => n.id === S.selNode) || p.nodes[0]);
  const ih = el('div', 'ih');
  ih.appendChild(el('span', 'ihname', sn ? sn.type : ''));
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
      typeRow.appendChild(el('span', 'chip plain', sn.type));
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
    // "Read schema from source" / "Infer" actions.
    if (sn.kind === 'source' && !('columns' in sn.cfg)) {
      ib.appendChild(renderColumnsEditor(sn));
    }

    if (!Object.keys(sn.cfg).length && sn.kind !== 'source') {
      ib.appendChild(el('div', 'muted', '<div style="font-size:11.5px;padding:4px 0">No configuration for this stage yet.</div>'));
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
    : [];
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
    // filter/validate/hwm/dq/lookup/schemadrift are passthrough for schema purposes
    // (lookup appends outputFields)
    if (t.type === 'lookup' && Array.isArray(t.outputFields)) {
      t.outputFields.forEach((f: any) => cols.push({ name: String(f), type: 'derived', derived: true }));
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
    : [];
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
  }
  // If a preview ran, prefer its live columns for the very first map (source
  // columns may be undeclared for self-describing formats).
  if (!cols.length && S.preview && S.preview.columns.length) {
    return S.preview.columns.map((c) => ({ name: c, type: 'any' }));
  }
  return cols;
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
  ib.appendChild(el('div', 'muted', `<div style="font-size:12px;padding:2px 0 8px">Columns visible <b style="color:var(--text)">after ${sn.nm}</b>:</div>`));
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
  { type: 'filter', label: 'Filter — drop rows by a boolean expression',
    cfg: { expr: "amount > 0 AND status != 'CANCELLED'" } },
  { type: 'map', label: 'Map — project, rename, coerce & derive fields',
    cfg: { fields: { net_amount: '=multiply($amount, 1.1)' } } },
  { type: 'validate', label: 'Validate — per-field rules, route to DLQ',
    cfg: { rules: [{ field: 'amount', notNull: true }], onViolation: 'route' } },
  { type: 'lookup', label: 'Lookup — HTTP enrichment with LRU cache',
    cfg: { keyField: 'id', url: 'https://api.internal/enrich/{id}', method: 'GET', cacheSize: 10000, onError: 'null' } },
  { type: 'hwm', label: 'High-water mark — incremental cursor',
    cfg: { column: 'updated_at', valueType: 'datetime' } },
  { type: 'dq', label: 'Data quality — expectations + quality score',
    cfg: { expectations: [{ type: 'expectColumnNotNull', column: 'id' }], onFail: 'warn', qualityThreshold: 0.95 } },
  { type: 'schemadrift', label: 'Schema drift — detect column add/remove/type',
    cfg: { policy: 'additive', onDrift: 'log' } },
];

// Compute the column names visible at a given transforms[] insert position,
// walking the source columns through any map/lookup stages before it. Used to
// pre-populate a new map with passthrough mappings so users start from a
// complete, valid projection.
function upstreamColumnsAt(insertIndex: number): string[] {
  const src: any = S.doc.source || {};
  let cols: string[] = Array.isArray(src.columns) ? src.columns.map((c: any) => String(c.name)) : [];
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
function doPreview() {
  S.previewOn = true;
  // The host reveals the native Data Preview panel and streams rows there.
  post({ type: 'preview', limit: 100 });
}

// ── report / dlq / drift / tail / fleet (engine-or-demo) ─────────────────────
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
  cards.innerHTML =
    `<div class="card"><div class="cap">Processed</div><div class="kpi">${fmt(processed)}</div><div class="sub">read from source</div></div>` +
    `<div class="card"><div class="cap">Written</div><div class="kpi" style="color:var(--green)">${fmt(written)}</div><div class="sub">${pct}% to sink</div></div>` +
    `<div class="card"><div class="cap">Duration</div><div class="kpi">${(r.durationMs / 1000).toFixed(1)}s</div><div class="sub">~${fmtK(rps)} rows/s</div></div>` +
    `<div class="card"><div class="cap">Status</div><div class="kpi" style="color:${r.status === 'completed' ? 'var(--green)' : r.status === 'failed' ? 'var(--red)' : 'var(--yellow)'}">${r.status}</div><div class="sub">runId ${r.runId.slice(0, 8)}</div></div>`;
  wv.appendChild(cards);

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
    runLabel = '⟳ running…';
    runAction = () => post({ type: 'showOutput' });
  } else if (S.port) {
    runLabel = '■ Stop';
    runAction = () => post({ type: 'stop' });
  } else {
    runLabel = '▷ Run';
    runAction = () => doRunFresh();
  }
  const runItem = el('div', 'sbi run', runLabel);
  runItem.title = S.port
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
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// ── host message handling ─────────────────────────────────────────────────────
window.addEventListener('message', (ev: MessageEvent<HostToWebview>) => {
  const msg = ev.data;
  switch (msg.type) {
    case 'init':
      S.fileName = msg.fileName;
      S.ref = msg.ref;
      S.doc = msg.doc || {};
      S.pipe = buildPipe(S.doc);
      if (!S.selNode) S.selNode = 'source';
      renderAll();
      post({ type: 'validate' });
      break;
    case 'update':
      S.doc = msg.doc || {};
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
