// Core webview state + pipe-building — the shared foundation every other
// webview module imports. Extracted from webview/main.ts as the first step of
// an incremental module split (main.ts was ~6400 lines; this is the layer
// everything else depends on, so it has to be the first thing pulled out).
//
// `S` is a plain mutable object, not a class — modules that import `{ S }`
// get a live reference (ES module bindings), so `S.pipe = ...` etc. done here
// or in any importer is visible everywhere. Never reassign `S` itself.

import type { PipelineDoc } from './pipeline';
import type { WebviewToHost } from '../protocol';
import type { RunResponse, PreviewResult, StageTestResult } from '../engine/apiClient';
import { deriveBranches, type DerivedBranch } from './graph';
import { stageEntry, typeLabel, transformLabel } from './stageCatalog';

export interface VsCodeApi {
    postMessage(msg: WebviewToHost): void;
    getState(): unknown;
    setState(s: unknown): void;
}
declare function acquireVsCodeApi(): VsCodeApi;
export const vscode = acquireVsCodeApi();

// ── tiny DOM helpers ──────────────────────────────────────────────────────
export const $ = (s: string, r: ParentNode = document): HTMLElement | null => r.querySelector(s);
export function el(tag: string, cls?: string, html?: string): HTMLElement {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (html != null) e.innerHTML = html;
    return e;
}
export const fmt = (n: number) => n.toLocaleString('en-US');
export const fmtK = (n: number) => (n >= 1e6 ? (n / 1e6).toFixed(2) + 'M' : n >= 1e3 ? Math.round(n / 1e3) + 'k' : String(Math.round(n)));

export function post(msg: WebviewToHost) {
    vscode.postMessage(msg);
}

// Make the config dock resizable: applies the remembered width and adds a
// left-edge drag grip that updates S.inspWidth live (clamped to a sane range).
export function attachResize(insp: HTMLElement): void {
    // Mappers need more room than a simple form, so give map/mapper nodes a wider
    // default dock — until the user drags it to a width of their own.
    const sn = S.pipe && S.pipe.nodes ? S.pipe.nodes.find((n) => n.id === S.selNode) : null;
    const tall = !!(sn && (sn.type === 'map' || sn.type === 'mapper'));
    const def = tall ? Math.min(560, Math.round(window.innerWidth * 0.5)) : 360;
    insp.style.width = (S.inspWidthSet ? (S.inspWidth || def) : def) + 'px';
    const grip = el('div', 'inspgrip');
    grip.title = 'Drag to resize the config panel';
    grip.onmousedown = (e: MouseEvent) => {
        e.preventDefault();
        const startX = e.clientX;
        const startW = insp.getBoundingClientRect().width;
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';
        const move = (ev: MouseEvent) => {
            // Grip sits on the LEFT edge of the right-docked panel: dragging left widens.
            const w = Math.max(300, Math.min(Math.round(window.innerWidth * 0.6), startW + (startX - ev.clientX)));
            insp.style.width = w + 'px';
            S.inspWidth = w;
            S.inspWidthSet = true;
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
export interface Node {
    id: string;
    kind: 'source' | 'transform' | 'sink' | 'dlq';
    type: string;
    nm: string;
    meta: string;
    cfg: Record<string, string>;
    ref: 'source' | 'sink' | 'dlq' | number | string; // maps back into the doc; an extra sink uses its graph node id
}

export interface Pipe {
    name: string;
    description: string;
    mode: string;
    status: string;
    nodes: Node[];
    branches: DerivedBranch[];
    extraNodes: Node[];
    extraSources: Node[];
    hasDlq: boolean;
    dlqNode: Node | null;
}

// Live-DB introspection state, keyed by target ('source' | 'sink' | 'dlq').
export interface DbIntrospect {
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

// Read-only source-sample state for the source node's Test tab, so the user can
// SEE the rows the pipeline reads before running anything.
export interface SrcSample {
    columns: string[];
    rows: Array<Record<string, unknown>>;
    loading: boolean;
    read: number;      // rowsRead reported by the engine
}

// Atomic stage-tester working state for the currently selected transform.
export interface StageTest {
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
export interface ConnSummary { id: string; name: string; type: string; family: string; desc: string; }

// ── state ─────────────────────────────────────────────────────────────────
export const S = {
    fileName: 'pipeline.datapipeline',
    ref: '',
    doc: {} as PipelineDoc,
    pipe: null as Pipe | null,
    selNode: null as string | null,
    inspTab: 'Config',
    inspClosed: false,
    inspWidth: 360,
    inspWidthSet: false,
    zoom: 1,
    mapMode: 'visual',
    showBranches: true,
    showCounts: true,
    minimapOpen: false,
    paletteOpen: true,
    paletteQ: '',
    paletteCat: {} as Record<string, boolean>,
    rateRps: 0,
    ratePrev: null as { rows: number; at: number } | null,
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

export function dbState(target: string): DbIntrospect {
    if (!S.db[target]) S.db[target] = { tables: null, loadingTables: false, loadingCols: false, autoSchema: true, error: null, objects: null, loadingObjects: false, topics: null, loadingTopics: false };
    return S.db[target];
}
export function targetOf(node: Node): 'source' | 'sink' | 'dlq' | null {
    if (node.kind === 'source') return 'source';
    if (node.kind === 'sink') return 'sink';
    if (node.kind === 'dlq') return 'dlq';
    return null;
}

export const statusColor: Record<string, string> = { running: 'b', completed: 'g', partial: 'y', failed: 'r', idle: 'd' };

// ── build a Pipe from the real doc ────────────────────────────────────────
export function metaFor(kind: string, type: string, cfg: any): string {
    cfg = cfg || {};
    // Transform stages show a semantic one-liner (like the wireframe node
    // subtitles) so the graph documents itself at a glance — sourced from the
    // matching STAGE_CATALOG entry's summary(), so a new transform's subtitle is
    // declared once, alongside its label/icon/defaults, not in a second switch.
    if (kind === 'transform') {
        const entry = stageEntry('transform', type);
        if (entry?.summary) return entry.summary(cfg);
        return transformLabel(type);
    }
    // Source / sink / dlq: friendly label + the salient destination detail.
    const bits: string[] = [typeLabel(type)];
    if (cfg.path) bits.push(String(cfg.path).split(/[\\/]/).pop() || '');
    if (cfg.table) bits.push(cfg.table);
    if (cfg.topic) bits.push(cfg.topic);
    if (cfg.bucket) bits.push(cfg.bucket);
    if (cfg.index) bits.push(cfg.index);
    if (cfg.dsn) bits.push('dsn');
    if (cfg.expr) bits.push(String(cfg.expr).slice(0, 24));
    return bits.filter(Boolean).join(' · ');
}
export function cfgOf(o: any): Record<string, string> {
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
export function buildPipe(doc: PipelineDoc): Pipe {
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

    // Extra sink/source nodes live ONLY in the graph (self-contained `config`),
    // never in the linear model. Surface them as first-class nodes for the canvas
    // + inspector but keep them OUT of `nodes` so they never join the linear main
    // chain. Extra SOURCES are what lets a join's "right" port be wired to a real
    // second source instead of the join's own inline `right` config.
    const extraNodes: Node[] = [];
    const extraSources: Node[] = [];
    const gg: any = (doc as any).graph;
    if (gg && Array.isArray(gg.nodes)) {
        for (const gn of gg.nodes) {
            if (typeof gn.id !== 'string') continue;
            const c = (gn.config && typeof gn.config === 'object') ? gn.config : {};
            if (gn.id.startsWith('xsink-')) {
                extraNodes.push({
                    id: gn.id, kind: 'sink', type: gn.type || 'null',
                    nm: (c.path && String(c.path).split(/[\\/]/).pop()) || c.table || typeLabel(gn.type) || 'sink',
                    meta: metaFor('sink', gn.type || '—', c),
                    cfg: cfgOf(c), ref: gn.id,
                });
            } else if (gn.id.startsWith('xsource-')) {
                extraSources.push({
                    id: gn.id, kind: 'source', type: gn.type || 'csv',
                    nm: (c.path && String(c.path).split(/[\\/]/).pop()) || c.table || typeLabel(gn.type) || 'source',
                    meta: metaFor('source', gn.type || '—', c),
                    cfg: cfgOf(c), ref: gn.id,
                });
            }
        }
    }

    const branches = deriveBranches(doc);
    return {
        name: doc.name || S.fileName.replace(/\.datapipeline$/, ''),
        description: doc.description || '',
        mode: String(doc.mode || 'etl').toUpperCase(),
        status: S.running ? 'running' : S.ran ? 'partial' : 'idle',
        nodes,
        branches,
        extraNodes,
        extraSources,
        hasDlq,
        dlqNode,
    };
}

// Resolve the doc object a stage's config lives in.
// For the DLQ node, the two tunables (maxRowsPerBatch / onFull) live on the
// top-level dlq block; every other field lives in dlq.options (the destination
// sink config). cfgTarget returns dlq.options; setCfgField/Value special-case
// the two tunables.
export const DLQ_TOPLEVEL = new Set(['maxRowsPerBatch', 'onFull']);
export function cfgTarget(node: Node): any {
    const doc: any = S.doc;
    if (node.ref === 'source') return (doc.source = doc.source || {});
    if (node.ref === 'sink') return (doc.sink = doc.sink || {});
    if (node.ref === 'dlq') {
        doc.dlq = doc.dlq || {};
        doc.dlq.options = doc.dlq.options || {};
        return doc.dlq.options;
    }
    if (typeof node.ref === 'string' && node.ref.startsWith('xsink-')) {
        const gn = graphNodeById(doc, node.ref);
        if (gn) return (gn.config = gn.config || {});
        return {};
    }
    doc.transforms = doc.transforms || [];
    const ti = node.ref as number;
    return (doc.transforms[ti] = doc.transforms[ti] || {});
}

// Find a node in the explicit graph block by id (used for self-contained extra
// sink nodes whose config lives in doc.graph.nodes[i].config).
export function graphNodeById(doc: any, id: string): any {
    const g = doc && doc.graph;
    if (!g || !Array.isArray(g.nodes)) return null;
    return g.nodes.find((n: any) => n && n.id === id) || null;
}

// Resolve the currently-selected node across the main chain, the DLQ branch, and
// any extra sink/source nodes (which live only in the graph).
export function findSelNode(p: Pipe): Node {
    if (S.selNode === 'dlq' && p.dlqNode) return p.dlqNode;
    return p.nodes.find((n) => n.id === S.selNode) || p.extraNodes.find((n) => n.id === S.selNode) || p.extraSources.find((n) => n.id === S.selNode) || p.nodes[0];
}

// Write a config field back into the doc and persist (string value, coerced to
// the prior type where possible).
export function setCfgField(node: Node, key: string, value: string) {
    const target = node.ref === 'dlq' && DLQ_TOPLEVEL.has(key) ? ((S.doc as any).dlq = (S.doc as any).dlq || {}) : cfgTarget(node);
    const prev = target[key];
    if (typeof prev === 'number' && value.trim() !== '' && !isNaN(Number(value))) target[key] = Number(value);
    else if (typeof prev === 'boolean') target[key] = value === 'true';
    else target[key] = value;
    S.pipe = buildPipe(S.doc);
    post({ type: 'save', doc: S.doc });
}

// Write a raw (non-string) value — used by toggles, number, and structured editors.
export function setCfgValue(node: Node, key: string, value: unknown) {
    const target = node.ref === 'dlq' && DLQ_TOPLEVEL.has(key) ? ((S.doc as any).dlq = (S.doc as any).dlq || {}) : cfgTarget(node);
    target[key] = value;
    S.pipe = buildPipe(S.doc);
    post({ type: 'save', doc: S.doc });
}
