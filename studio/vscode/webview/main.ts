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
  kind: 'source' | 'transform' | 'sink';
  type: string;
  nm: string;
  meta: string;
  cfg: Record<string, string>;
  ref: 'source' | 'sink' | number; // where this maps back into the doc
}

interface Pipe {
  name: string;
  mode: string;
  status: string;
  nodes: Node[];
  hasDlq: boolean;
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
  panelTab: 'problems',
  panelCollapsed: false,
  running: false,
  port: null as number | null,
  ran: false,
  lastRun: null as RunResponse | null,
  preview: null as PreviewResult | null,
  outputLines: [] as string[],
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
    if (k === 'type') continue;
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
  const hasDlq = transforms.some((t: any) => t && (t.onViolation === 'route' || t.type === 'validate'));
  return {
    name: doc.name || S.fileName.replace(/\.datapipeline$/, ''),
    mode: String(doc.mode || 'etl').toUpperCase(),
    status: S.running ? 'running' : S.ran ? 'partial' : 'idle',
    nodes,
    hasDlq,
  };
}

// Write a config field back into the doc and persist.
function setCfgField(node: Node, key: string, value: string) {
  const doc: any = S.doc;
  let target: any;
  if (node.ref === 'source') target = (doc.source = doc.source || {});
  else if (node.ref === 'sink') target = (doc.sink = doc.sink || {});
  else {
    doc.transforms = doc.transforms || [];
    target = doc.transforms[node.ref] = doc.transforms[node.ref] || {};
  }
  // Coerce back to number/bool where the original was one.
  const prev = target[key];
  if (typeof prev === 'number' && value.trim() !== '' && !isNaN(Number(value))) target[key] = Number(value);
  else if (typeof prev === 'boolean') target[key] = value === 'true';
  else target[key] = value;
  S.pipe = buildPipe(S.doc);
  post({ type: 'save', doc: S.doc });
}

// ── render root ───────────────────────────────────────────────────────────
function renderAll() {
  renderTabs();
  renderEditor();
  renderPanel();
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

  if (p.hasDlq) {
    const br = el('div', 'branch');
    br.innerHTML = '<div class="down"></div>';
    const dn = el('div', 'node dlq');
    dn.style.width = '150px';
    dn.innerHTML = '<div class="kind">DLQ Sink</div><div class="nm">rejects</div><div class="meta">routed rows</div>';
    dn.onclick = () => openTab('dlq', 'dlq');
    br.appendChild(dn);
    cv.appendChild(br);
  }

  const hint = el('div', 'canvas-hint pillrow');
  hint.innerHTML = '<span class="pill" data-h="src">＋ Source</span><span class="pill" data-h="tf">＋ Transform</span><span class="pill" data-h="sink">＋ Sink</span><span class="pill">⤢ Fit</span>';
  (hint.querySelector('[data-h="tf"]') as HTMLElement).onclick = () => addTransform(p.nodes.length - 1);
  (hint.querySelector('[data-h="src"]') as HTMLElement).onclick = () =>
    quickPick('Change source type', ['csv', 'tsv', 'fixedwidth', 'parquet', 'xml', 'jsonlines', 'jdbc', 'kafka', 'objstore'], (v) => {
      (S.doc.source as any) = { ...(S.doc.source || {}), type: v };
      persistAndRebuild();
    });
  (hint.querySelector('[data-h="sink"]') as HTMLElement).onclick = () =>
    quickPick('Change sink type', ['jdbc', 'parquet', 'kafka', 'objstore', 'null'], (v) => {
      (S.doc.sink as any) = { ...(S.doc.sink || {}), type: v };
      persistAndRebuild();
    });
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
    const nameF = el('div', 'fld');
    nameF.innerHTML = '<div class="k">Stage name</div>';
    const ni = el('input', 'v') as HTMLInputElement;
    ni.value = sn.nm;
    ni.onchange = () => {
      if (sn.ref === 'source') (S.doc.source as any).path = (S.doc.source as any)?.path;
      if (typeof sn.ref === 'number') { (S.doc.transforms as any)[sn.ref].name = ni.value; persistAndRebuild(); }
    };
    ib.appendChild(nameF);
    ib.appendChild(ni);
    Object.entries(sn.cfg).forEach(([k, v]) => {
      const f = el('div', 'fld');
      f.innerHTML = `<div class="k">${k}</div>`;
      const inp = el('input', 'v') as HTMLInputElement;
      inp.value = v;
      inp.onchange = () => setCfgField(sn, k, inp.value);
      f.appendChild(inp);
      ib.appendChild(f);
    });
  } else if (S.inspTab === 'Schema') {
    ib.appendChild(el('div', 'muted', `<div style="font-size:12px;padding:6px 0">Inferred schema for <b style="color:var(--text)">${sn.nm}</b>. Run the pipeline to populate live column types.</div>`));
  } else {
    const b = el('button', 'btn sm pri', '▷ Test this stage on sample');
    b.onclick = () => doPreview();
    ib.appendChild(b);
    ib.appendChild(el('div', 'muted', '<div style="font-size:11.5px;margin-top:10px">Runs the pipeline against a live sample.</div>'));
  }
  insp.appendChild(ib);
  m.appendChild(insp);
}

function persistAndRebuild() {
  S.pipe = buildPipe(S.doc);
  post({ type: 'save', doc: S.doc });
  renderEditor();
}

// ── add / remove transforms ─────────────────────────────────────────────────
const TRANSFORMS = [
  { type: 'filter', cfg: { expr: "amount > 0 AND status != 'CANCELLED'" } },
  { type: 'map', cfg: { fields: '{}' } },
  { type: 'validate', cfg: { onViolation: 'route' } },
  { type: 'lookup', cfg: { url: 'https://api.internal/enrich' } },
  { type: 'hwm', cfg: { column: 'updated_at' } },
  { type: 'dq', cfg: { threshold: '0.05' } },
  { type: 'schemadrift', cfg: { policy: 'additive' } },
];
function addTransform(index: number) {
  // index is a node index; convert to transforms[] insert position.
  const tfInsert = Math.max(0, index - 1);
  quickPick('Insert transform', TRANSFORMS.map((t) => t.type), (type) => {
    const t = TRANSFORMS.find((x) => x.type === type)!;
    const doc: any = S.doc;
    doc.transforms = Array.isArray(doc.transforms) ? doc.transforms : [];
    const parsedCfg: any = {};
    for (const [k, v] of Object.entries(t.cfg)) parsedCfg[k] = v;
    doc.transforms.splice(tfInsert, 0, { type: t.type, ...parsedCfg });
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

// ── run / preview ────────────────────────────────────────────────────────────
function doRun() {
  if (S.running) return;
  S.outputLines = [];
  setPanel('output');
  post({ type: 'run' });
}
function doPreview() {
  S.previewOn = true;
  setPanel('preview');
  const pb = $('#panelbody');
  if (pb) pb.innerHTML = '<div class="muted">Sampling up to 100 rows through the transform chain…</div>';
  post({ type: 'preview', limit: 100 });
}

// ── bottom panel ─────────────────────────────────────────────────────────────
const PANELS = [
  { id: 'problems', t: 'Problems', err: true },
  { id: 'output', t: 'Output' },
  { id: 'preview', t: 'Data Preview' },
  { id: 'terminal', t: 'Terminal' },
];
function setPanel(id: string) {
  S.panelTab = id;
  S.panelCollapsed = false;
  renderPanel();
}
function renderPanel() {
  const tabs = $('#paneltabs')!;
  const body = $('#panelbody')!;
  const panel = $('#panel')!;
  panel.classList.toggle('collapsed', S.panelCollapsed);
  tabs.innerHTML = '';
  PANELS.forEach((p) => {
    const cnt = p.id === 'problems' && S.issues.length ? ` <span class="cnt">${S.issues.length}</span>` : '';
    const s = el('div', 'pt' + (p.err ? ' err' : '') + (S.panelTab === p.id ? ' on' : ''), p.t + cnt);
    s.onclick = () => setPanel(p.id);
    tabs.appendChild(s);
  });
  const acts = el('div', 'pacts');
  acts.innerHTML = `<span title="Clear">⌫</span><span title="${S.panelCollapsed ? 'Expand' : 'Collapse'}">${S.panelCollapsed ? '▴' : '▾'}</span>`;
  (acts.lastChild as HTMLElement).onclick = () => { S.panelCollapsed = !S.panelCollapsed; renderPanel(); };
  (acts.firstChild as HTMLElement).onclick = () => { if (S.panelTab === 'output') S.outputLines = []; renderPanel(); };
  tabs.appendChild(acts);
  if (S.panelCollapsed) { body.innerHTML = ''; return; }
  body.innerHTML = '';
  body.className = 'panelbody';
  if (S.panelTab === 'problems') renderProblems(body);
  else if (S.panelTab === 'output') renderOutput(body);
  else if (S.panelTab === 'preview') renderPreview(body);
  else renderTerminal(body);
}
function renderProblems(b: HTMLElement) {
  b.style.fontFamily = 'var(--sans)';
  if (!S.issues.length) { b.innerHTML = '<div class="muted">No problems detected. Edit config or run validation.</div>'; return; }
  S.issues.forEach((x) => {
    const icon = x.sev === 'error' ? 'err' : x.sev === 'warn' ? 'warn' : 'ok';
    const sym = x.sev === 'error' ? '⊗' : x.sev === 'warn' ? '⚠' : 'ℹ';
    const p = el('div', 'prob', `<span class="i ${icon}">${sym}</span><div><div class="msg">${x.label}: ${x.msg}</div><div class="loc">${x.anchor || ''}</div></div>`);
    b.appendChild(p);
  });
}
function renderOutput(b: HTMLElement) {
  b.className = 'panelbody log';
  if (!S.outputLines.length) { b.innerHTML = '<div class="muted">No run yet. Click ▷ Run.</div>'; return; }
  b.innerHTML = S.outputLines.map((l) => `<div class="ln">${escapeHtml(l)}</div>`).join('');
  b.scrollTop = b.scrollHeight;
}
function renderPreview(b: HTMLElement) {
  b.style.fontFamily = 'var(--sans)';
  if (!S.previewOn) { b.innerHTML = '<div class="muted">Click ⌕ Dry-run Preview to sample rows through the chain.</div>'; return; }
  const pr = S.preview;
  if (!pr) { b.innerHTML = '<div class="muted">Sampling rows from the engine…</div>'; return; }
  if (!pr.rows.length) {
    b.innerHTML = `<div class="muted">No sample rows produced. Read ${fmt(pr.rowsRead)} rows · dropped ${fmt(pr.rowsDropped)} · failed ${fmt(pr.rowsFailed)}.</div>`;
    return;
  }
  const cols = pr.columns.length ? pr.columns : Object.keys(pr.rows[0]);
  const pv = el('div', 'pv');
  const head = `<div class="pvtop"><span><b style="color:var(--text)">Live preview</b> · ${pr.rows.length} sampled${pr.truncated ? ' (truncated)' : ''} · read ${fmt(pr.rowsRead)}</span><span class="muted">after transforms</span></div>`;
  const ths = cols.map((c) => `<th>${escapeHtml(c)}</th>`).join('');
  const trs = pr.rows
    .map((row) => '<tr>' + cols.map((c) => `<td>${escapeHtml(cellText(row[c]))}</td>`).join('') + '</tr>')
    .join('');
  pv.innerHTML = head + `<table><thead><tr>${ths}</tr></thead><tbody>${trs}</tbody></table>`;
  b.innerHTML = '';
  b.appendChild(pv);
}
function cellText(v: unknown): string {
  if (v == null) return 'null';
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}
function renderTerminal(b: HTMLElement) {
  b.className = 'panelbody log';
  b.innerHTML = `<div class="ln"><span class="ok">➜</span> flogo-datapipeline $ <span style="opacity:.6">flogo-datapipeline -f ${escapeHtml(S.fileName)} -v</span></div>`;
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
  problems.onclick = () => setPanel('problems');
  sb.appendChild(problems);
  const eng = el('div', 'sbi', S.running ? `◈ engine :${S.port}` : '◈ engine: stopped');
  eng.onclick = () => setPanel('output');
  sb.appendChild(eng);
  sb.appendChild(el('div', 'grow'));
  const runItem = el('div', 'sbi run', S.running ? '⟳ running…' : '▷ Run');
  runItem.onclick = () => (S.running ? setPanel('output') : doRun());
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
  contextMenu(e.clientX, e.clientY, [
    { ic: '⚙', label: 'Configure', fn: () => { S.selNode = n.id; S.inspTab = 'Config'; renderEditor(); } },
    { ic: '▷', label: 'Test this stage on sample', fn: () => doPreview() },
    { sep: true },
    { ic: '＋', label: 'Insert transform before', fn: () => addTransform(i) },
    { ic: '＋', label: 'Insert transform after', fn: () => addTransform(i + 1) },
    { sep: true },
    { ic: '✕', label: 'Delete stage', dis: !isT, fn: () => { if (typeof n.ref === 'number') deleteTransform(n.ref); } },
  ]);
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
      S.outputLines.push('[engine] starting…');
      renderStatus();
      renderOutputIfActive();
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
    case 'runLog':
      S.outputLines.push(msg.line);
      if (S.outputLines.length > 500) S.outputLines.shift();
      renderOutputIfActive();
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
      renderPanel();
      renderStatus();
      break;
    case 'previewResult':
      S.preview = msg.result;
      S.previewOn = true;
      if (S.panelTab === 'preview' && !S.panelCollapsed) renderPreview($('#panelbody')!);
      break;
    case 'previewError': {
      S.preview = null;
      const pb = $('#panelbody');
      if (pb && S.panelTab === 'preview') pb.innerHTML = `<div class="muted">Preview failed: ${escapeHtml(msg.message)}</div>`;
      toast('err', 'Preview failed: ' + msg.message);
      break;
    }
    case 'definition':
      toast('info', `Engine definition: ${msg.def.source.type} → ${msg.def.sink.type} · ${msg.def.transforms.length} transform(s)`);
      break;
    case 'metricsResult': {
      const st = msg.data as StatusResponse | undefined;
      if (!msg.error && st && Array.isArray(st.runs) && st.runs.length) {
        S.lastRun = st.runs[st.runs.length - 1];
        if (cur()?.type === 'report' || cur()?.type === 'dlq') renderEditor();
      }
      const pb = $('#panelbody');
      if (pb && S.panelTab === 'preview' && msg.error) {
        pb.innerHTML = `<div class="muted">${escapeHtml(msg.error)}</div>`;
      }
      break;
    }
    case 'browseResult':
    case 'browseCancelled':
      break;
  }
});

function renderOutputIfActive() {
  if (S.panelTab === 'output' && !S.panelCollapsed) renderOutput($('#panelbody')!);
}

// ── keyboard ──────────────────────────────────────────────────────────────────
document.addEventListener('keydown', (e) => {
  const mod = e.metaKey || e.ctrlKey;
  if (mod && e.shiftKey && e.key.toLowerCase() === 'p') { e.preventDefault(); openPalette(); }
  else if (mod && e.key.toLowerCase() === 'r' && e.shiftKey) { e.preventDefault(); doRun(); }
  else if (e.key === 'Escape') closeOverlays();
});

// ── boot ──────────────────────────────────────────────────────────────────────
post({ type: 'ready' });
