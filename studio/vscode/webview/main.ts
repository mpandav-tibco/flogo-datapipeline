// Flogo Data Pipeline Studio — webview app.
// Renders the active .datapipeline document as an interactive canvas with an
// inspector, bottom panel, run report, DLQ / schema-drift / live-tail views,
// a command palette and right-click menus. All persistence + engine actions
// are delegated to the extension host over postMessage.

import type { PipelineDoc } from '../src/model/pipeline';
import type { HostToWebview, WebviewToHost } from '../src/protocol';
import type { RunResponse, PreviewResult, StatusResponse } from '../src/engine/apiClient';

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
};

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
  mode: ['continuous', 'bounded'],
  ackMode: ['none', 'leader', 'all'],
  compressionCodec: ['none', 'gzip', 'snappy', 'lz4', 'zstd'],
  // objstore
  format: ['csv', 'jsonlines', 'parquet'],
  // jdbc sink
  onConflict: ['insert', 'upsert', 'ignore'],
  // validate / dq
  onViolation: ['route', 'drop', 'fail'],
  onFail: ['warn', 'fail', 'route'],
  // schemadrift
  policy: ['additive', 'strict', 'ignore'],
  onDrift: ['warn', 'fail', 'route'],
  // hwm
  valueType: ['int', 'timestamp', 'string'],
  // lookup
  method: ['GET', 'POST'],
  onError: ['fail', 'skip', 'default'],
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
  fields: 'JSON map of output field → expression.',
  query: 'Full SELECT statement. Overrides table/orderKey when set.',
  brokers: 'Comma-separated host:port list, e.g. broker-1:9092, broker-2:9092',
  recordElement: 'XML element that marks one record, e.g. "order".',
  key: 'Object key template, supports ${date}, ${uuid}, ${runId}.',
  rules: 'JSON array of field rules: [{ "field": "amount", "notNull": true }]',
  expectations: 'JSON array of DQ expectations (expectColumnNotNull, expectValuesInSet, …).',
  url: 'Enrichment endpoint. ${field} placeholders are substituted per row.',
};
const SOURCE_TYPES = ['csv', 'tsv', 'fixedwidth', 'parquet', 'xml', 'jsonlines', 'jdbc', 'kafka', 's3', 'gcs', 'adls'];
const SINK_TYPES = ['jdbc', 'parquet', 'csv', 'jsonlines', 'kafka', 's3', 'gcs', 'adls', 'null'];
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
    parquet: { path: './data/input.parquet', rowGroupParallelism: 4 },
    xml: { path: './data/input.xml', recordElement: 'record' },
    jsonlines: { path: './data/input.jsonl' },
    jdbc: { driver: 'postgres', dsn: '', table: '', orderKey: '', batchSize: 5000 },
    kafka: { brokers: '', topic: '', groupId: 'dp-consumer', startOffset: 'earliest', deserializer: 'json' },
    s3: { bucket: '', prefix: '', region: 'us-east-1', format: 'csv' },
    gcs: { bucket: '', prefix: '', format: 'csv' },
    adls: { bucket: '', prefix: '', format: 'csv' },
  };
  const sink: Record<string, Record<string, unknown>> = {
    jdbc: { driver: 'postgres', dsn: '', table: '', batchSize: 1000, onConflict: 'insert' },
    parquet: { path: './out/output.parquet', compression: 'snappy', rowGroupSize: 100000 },
    csv: { path: './out/output.csv', delimiter: ',', writeHeader: true },
    jsonlines: { path: './out/output.jsonl' },
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
    sel.onchange = () => setCfgField(node, key, sel.value);
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

// Structured column editor for the source `columns` array.
function renderColumnsEditor(node: Node): HTMLElement {
  const f = el('div', 'fld');
  f.appendChild(el('div', 'k', 'Columns'));
  const target = cfgTarget(node);
  const cols: Array<{ name?: string; type?: string }> = Array.isArray(target.columns) ? target.columns : [];
  const box = el('div', 'cols');
  const types = ['string', 'int', 'float', 'bool', 'date', 'decimal'];
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
    row.appendChild(sel);
    row.appendChild(del);
    box.appendChild(row);
  });
  const add = el('div', 'addcol', '＋ Add column');
  add.onclick = () => { cols.push({ name: 'new_column', type: 'string' }); setCfgValue(node, 'columns', cols); renderEditor(); };
  box.appendChild(add);
  f.appendChild(box);
  return f;
}

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
      '<button class="btn sm" data-a="preview">⌕ Dry-run Preview</button>' +
      (kafka ? '<button class="btn sm" data-a="tail">≋ Live Tail</button>' : '') +
      (S.running
        ? '<button class="btn sm" data-a="stop">■ Stop</button>'
        : '<button class="btn sm pri" data-a="run">▷ Run</button>');
    (acts.querySelector('[data-a="preview"]') as HTMLElement).onclick = () => doPreview();
    const tl = acts.querySelector('[data-a="tail"]') as HTMLElement | null;
    if (tl) tl.onclick = () => openTab('tail', 'tail');
    const rb = acts.querySelector('[data-a="run"]') as HTMLElement | null;
    if (rb) rb.onclick = () => doRun();
    const sb = acts.querySelector('[data-a="stop"]') as HTMLElement | null;
    if (sb) sb.onclick = () => post({ type: 'stop' });
  }
  bar.appendChild(acts);
}

// ── editor / canvas ─────────────────────────────────────────────────────────
function renderEditor() {
  const m = $('#edmain')!;
  const bc = $('#breadcrumb')!;
  m.innerHTML = '';
  if (S.tailTimer) { clearInterval(S.tailTimer); S.tailTimer = 0; }
  const t = cur();
  if (!t || !S.pipe) {
    bc.innerHTML = '';
    m.appendChild(el('div', 'canvas', '<div class="muted" style="margin:40px auto;text-align:center">Loading pipeline…</div>'));
    return;
  }
  if (t.type === 'canvas') {
    const sn = S.pipe.nodes.find((n) => n.id === S.selNode) || S.pipe.nodes[0];
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
    nd.onclick = () => { S.selNode = n.id; S.inspTab = 'Config'; renderEditor(); };
    nd.oncontextmenu = (e) => nodeMenu(n, i, e);
    row.appendChild(nd);
  });
  row.appendChild(el('div', 'conn'));
  const add = el('div', 'addnode', '＋');
  add.title = 'Add transform (before the sink)';
  add.onclick = () => addTransform(p.nodes.length - 1);
  row.appendChild(add);
  cv.appendChild(row);

  // DLQ branch: a separate top-level destination for write-time failures. It
  // hangs off the main sink and is selectable/configurable in the Inspector.
  if (p.hasDlq && p.dlqNode) {
    const dn = p.dlqNode;
    const br = el('div', 'branch');
    br.innerHTML = '<div class="down"></div>';
    const dnode = el('div', 'node dlq' + (S.selNode === 'dlq' ? ' sel' : ''));
    dnode.style.width = '150px';
    dnode.innerHTML = `<div class="kind">DLQ · ${dn.type}</div><div class="nm">${dn.nm}</div><div class="meta">${dn.meta}</div>`;
    dnode.tabIndex = 0;
    dnode.onclick = () => { S.selNode = 'dlq'; S.inspTab = 'Config'; renderEditor(); };
    dnode.oncontextmenu = (e) => dlqMenu(e);
    br.appendChild(dnode);
    cv.appendChild(br);
  }

  const hint = el('div', 'canvas-hint pillrow');
  const dlqPill = p.hasDlq ? '' : '<span class="pill" data-h="dlq">＋ DLQ</span>';
  hint.innerHTML = '<span class="pill" data-h="src">＋ Source</span><span class="pill" data-h="tf">＋ Transform</span><span class="pill" data-h="sink">＋ Sink</span>' + dlqPill + '<span class="pill">⤢ Fit</span>';
  (hint.querySelector('[data-h="tf"]') as HTMLElement).onclick = () => addTransform(p.nodes.length - 1);
  (hint.querySelector('[data-h="src"]') as HTMLElement).onclick = () =>
    quickPick('Change source type', SOURCE_TYPES, (v) => {
      (S.doc.source as any) = { type: v, ...defaultConfigFor('source', v) };
      persistAndRebuild();
      toast('ok', `Source type → ${v}`);
    });
  (hint.querySelector('[data-h="sink"]') as HTMLElement).onclick = () =>
    quickPick('Change sink type', SINK_TYPES, (v) => {
      (S.doc.sink as any) = { type: v, ...defaultConfigFor('sink', v) };
      persistAndRebuild();
      toast('ok', `Sink type → ${v}`);
    });
  const dlqBtn = hint.querySelector('[data-h="dlq"]') as HTMLElement | null;
  if (dlqBtn) dlqBtn.onclick = () => addDlq();
  cv.appendChild(hint);
  m.appendChild(c);

  // inspector
  const insp = el('div', 'inspector');
  const sn = p.nodes.find((n) => n.id === S.selNode) || p.nodes[0];
  insp.appendChild(el('div', 'ih', 'Inspector — ' + (sn ? sn.type : '')));
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
        op.value = o; op.textContent = o;
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
    Object.entries(sn.cfg).forEach(([k, v]) => {
      if (k === 'columns') ib.appendChild(renderColumnsEditor(sn));
      else if (k === 'fields' && (sn.type === 'map' || sn.type === 'mapper')) ib.appendChild(renderMapEditor(sn));
      else ib.appendChild(renderField(sn, k, v));
    });

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
  const b = el('button', 'btn sm pri', '▷ Test on live sample');
  b.onclick = () => doPreview();
  ib.appendChild(b);
  ib.appendChild(el('div', 'muted', '<div style="font-size:11.5px;margin:8px 0 12px">Reads a bounded sample from the source and runs it through the chain up to this stage — no sink writes. Results appear in the <b style="color:var(--text)">Data Preview</b> panel below.</div>'));
  const pr = S.preview;
  if (pr) {
    ib.appendChild(el('div', 'between', `<span class="muted" style="font-size:11px">Last sample</span><span class="chip ${pr.rowsFailed ? 'warn' : 'ok'}">${pr.rows.length} rows</span>`));
    ib.appendChild(el('div', 'muted', `<div style="font-size:11px;margin-top:6px">read ${fmt(pr.rowsRead)} · dropped ${fmt(pr.rowsDropped)} · failed ${fmt(pr.rowsFailed)}${pr.truncated ? ' · truncated' : ''}</div>`));
    const open = el('button', 'btn sm', '⌕ Open Data Preview');
    open.style.marginTop = '10px';
    open.onclick = () => doPreview();
    ib.appendChild(open);
  }
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
    cfg: { keyField: 'id', url: 'https://api.internal/enrich/${id}', method: 'GET', cacheSize: 10000, onError: 'skip' } },
  { type: 'hwm', label: 'High-water mark — incremental cursor',
    cfg: { column: 'updated_at', valueType: 'timestamp' } },
  { type: 'dq', label: 'Data quality — expectations + quality score',
    cfg: { expectations: [{ type: 'expectColumnNotNull', column: 'id' }], onFail: 'warn', qualityThreshold: 0.95 } },
  { type: 'schemadrift', label: 'Schema drift — detect column add/remove/type',
    cfg: { policy: 'additive', onDrift: 'warn' } },
];
function addTransform(index: number) {
  // index is a node index; convert to transforms[] insert position.
  const tfInsert = Math.max(0, index - 1);
  quickPick('Insert transform', TRANSFORMS.map((t) => t.label), (label) => {
    const t = TRANSFORMS.find((x) => x.label === label)!;
    const doc: any = S.doc;
    doc.transforms = Array.isArray(doc.transforms) ? doc.transforms : [];
    doc.transforms.splice(tfInsert, 0, { type: t.type, ...JSON.parse(JSON.stringify(t.cfg)) });
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
    { ic: '⚙', label: 'Configure DLQ', fn: () => { S.selNode = 'dlq'; S.inspTab = 'Config'; renderEditor(); } },
    { ic: '⧉', label: 'Open DLQ run stats', fn: () => openTab('dlq', 'dlq') },
    { sep: true },
    { ic: '✕', label: 'Remove DLQ', fn: () => removeDlq() },
  ]);
}

// ── run / preview (native VS Code panels) ────────────────────────────────────
function doRun() {
  if (S.running) return;
  post({ type: 'showOutput' }); // reveal native Output channel
  post({ type: 'run' });
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
  const eng = el('div', 'sbi', S.running ? `◈ engine :${S.port}` : '◈ engine: stopped');
  eng.onclick = () => post({ type: 'showOutput' });
  sb.appendChild(eng);
  sb.appendChild(el('div', 'grow'));
  const runItem = el('div', 'sbi run', S.running ? '⟳ running…' : '▷ Run');
  runItem.onclick = () => (S.running ? post({ type: 'showOutput' }) : doRun());
  sb.appendChild(runItem);
  const pal = el('div', 'sbi', 'Data Pipeline');
  pal.onclick = () => openPalette();
  sb.appendChild(pal);
}

// ── command palette + quick pick ──────────────────────────────────────────────
const COMMANDS = [
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
  document.querySelectorAll('.scrim,.palette,.ctxmenu').forEach((x) => x.remove());
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
function quickPick(title: string, items: string[], onPick: (v: string) => void) {
  closeOverlays();
  const scrim = el('div', 'scrim');
  scrim.onclick = closeOverlays;
  document.body.appendChild(scrim);
  const pal = el('div', 'palette');
  const inp = el('input') as HTMLInputElement;
  inp.placeholder = title;
  inp.readOnly = true;
  pal.appendChild(inp);
  const list = el('div');
  items.forEach((it) => {
    const d = el('div', 'pi', `<span class="ic">◦</span>${it}`);
    d.onclick = () => { closeOverlays(); onPick(it); };
    list.appendChild(d);
  });
  pal.appendChild(list);
  document.body.appendChild(pal);
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
    { ic: '⚙', label: 'Configure', fn: () => { S.selNode = n.id; S.inspTab = 'Config'; renderEditor(); } },
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
      items.push({ ic: '⚠', label: 'Configure DLQ', fn: () => { S.selNode = 'dlq'; S.inspTab = 'Config'; renderEditor(); } });
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
