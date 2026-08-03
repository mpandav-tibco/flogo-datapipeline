// Derived-graph model — turns the linear pipeline Definition into the implicit
// branch structure the canvas draws (router route-sinks, join reference source,
// validate reject sink, source parse-error sink).
//
// This is a PURE, dependency-free derivation (no `vscode`, no DOM) so it is
// trivially unit-testable and shared by the webview renderer. It reads only the
// on-disk `.datapipeline` shape and never mutates it — the engine stays linear
// (source → transforms[] → sink + top-level dlq); these branches already live
// *inside* the transforms, they were simply never drawn.

import type { PipelineDoc } from './pipeline';

/** The kind of implicit branch, which drives its label + styling on the canvas. */
export type BranchKind =
  | 'reference'     // a join's right/reference dataset — a 2nd input INTO the owner
  | 'route'         // a router route → its own sink (matched rows exit here)
  | 'default-route' // a router's default sink (unmatched rows)
  | 'reject'        // a validate transform's rejected-rows sink (onViolation: route)
  | 'source-error'; // a source's parse/deserialization error sink

/** A single derived branch that hangs off a primary node (source / transform). */
export interface DerivedBranch {
  /** Stable id, e.g. `t2.route0`, `t3.right`, `t1.reject`, `source.err`. */
  id: string;
  /** The primary node this branch hangs off: `source` or `t{index}`. */
  ownerId: string;
  /** Transform index (number) or `source`. */
  ownerIndex: number | 'source';
  /** `in` = feeds the owner (join reference); `out` = the owner emits to it. */
  flow: 'in' | 'out';
  kind: BranchKind;
  /** How the satellite node renders — a source-style or sink-style card. */
  nodeKind: 'source' | 'sink';
  /** Stage type of the satellite (csv, jdbc, null, kafka, …). */
  type: string;
  /** Node title (route name / file basename / table / type). */
  title: string;
  /** Edge label — the route predicate, `unmatched`, `violations`, `reference on key`, `parse errors`. */
  label: string;
  /** The satellite's own config (route sinkConfig / join right / errorSink options). */
  cfg: Record<string, unknown>;
  /** For `route` branches, the index into `router.routes[]` (so the inspector can focus it). */
  routeIndex?: number;
}

function asObj(v: unknown): Record<string, unknown> {
  return v && typeof v === 'object' && !Array.isArray(v) ? (v as Record<string, unknown>) : {};
}
function str(v: unknown): string | undefined {
  return typeof v === 'string' && v ? v : undefined;
}
function basename(p: string): string {
  const parts = String(p).split(/[\\/]/);
  return parts[parts.length - 1] || String(p);
}
function omit(o: Record<string, unknown>, keys: string[]): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(o)) if (!keys.includes(k)) out[k] = v;
  return out;
}

/** Best-effort short title for a satellite node from its config, falling back to the type. */
function titleFor(type: string, cfg: Record<string, unknown>): string {
  const path = str(cfg.path);
  if (path) return basename(path);
  return str(cfg.table) || str(cfg.topic) || str(cfg.key) || str(cfg.prefix) || type;
}

/**
 * Derive every implicit branch from a pipeline document, in canvas order
 * (source branches first, then transform branches left-to-right). The main
 * source → transforms[] → sink chain and the top-level `dlq` are NOT returned
 * here — the renderer already draws those; this fills in only the branches that
 * were previously hidden inside transform config.
 */
export function deriveBranches(doc: PipelineDoc | undefined): DerivedBranch[] {
  if (!doc) return [];
  const out: DerivedBranch[] = [];

  // 1. Source parse/deserialization error sink → an exit branch off the source.
  const src = asObj(doc.source);
  const srcErr = asObj(src.errorSink);
  if (str(srcErr.type) || str(srcErr.innerType)) {
    // errorSink is either a direct sink ({type:'csv', path:…}) or a `dlq`
    // wrapper ({type:'dlq', innerType, innerOptions}).
    const wrapped = str(srcErr.type) === 'dlq';
    const type = (wrapped ? str(srcErr.innerType) : str(srcErr.type)) || 'sink';
    const cfg = wrapped ? asObj(srcErr.innerOptions) : omit(srcErr, ['type']);
    out.push({
      id: 'source.err', ownerId: 'source', ownerIndex: 'source', flow: 'out',
      kind: 'source-error', nodeKind: 'sink', type,
      title: titleFor(type, cfg), label: 'parse errors', cfg,
    });
  }

  // 2. Per-transform branches.
  const transforms = Array.isArray(doc.transforms) ? doc.transforms : [];
  transforms.forEach((traw, i) => {
    const t = asObj(traw);
    const type = str(t.type) || '';
    const ownerId = 't' + i;

    if (type === 'join') {
      // The join's right/reference dataset is a 2nd input, inlined in `right`.
      const right = asObj(t.right);
      if (str(right.type)) {
        const rtype = str(right.type) as string;
        const lk = str(t.leftKey) || str(t.key);
        const rk = str(t.rightKey) || lk;
        const label = lk ? `reference on ${lk}${rk && rk !== lk ? ' = ' + rk : ''}` : 'reference';
        out.push({
          id: ownerId + '.right', ownerId, ownerIndex: i, flow: 'in',
          kind: 'reference', nodeKind: 'source', type: rtype,
          title: titleFor(rtype, right), label, cfg: omit(right, []),
        });
      }
    } else if (type === 'router' || type === 'split') {
      // Each route captures matching rows to its own sink; an optional default
      // sink catches the unmatched.
      const routes = Array.isArray(t.routes) ? t.routes : [];
      routes.forEach((rraw, j) => {
        const r = asObj(rraw);
        const stype = str(r.sinkType) || 'sink';
        const cfg = asObj(r.sinkConfig);
        out.push({
          id: ownerId + '.route' + j, ownerId, ownerIndex: i, flow: 'out',
          kind: 'route', nodeKind: 'sink', type: stype,
          title: str(r.name) || titleFor(stype, cfg), label: str(r.when) || 'when …',
          cfg, routeIndex: j,
        });
      });
      const dtype = str(t.defaultSinkType);
      if (dtype) {
        out.push({
          id: ownerId + '.default', ownerId, ownerIndex: i, flow: 'out',
          kind: 'default-route', nodeKind: 'sink', type: dtype,
          title: titleFor(dtype, asObj(t.defaultSinkConfig)), label: 'unmatched',
          cfg: asObj(t.defaultSinkConfig),
        });
      }
    } else if (type === 'validate') {
      // When onViolation is `route`, rejected rows exit to a dedicated sink.
      const onV = str(t.onViolation) || 'route';
      if (onV === 'route' && (str(t.errorSinkType) || asObj(t.errorSinkConfig))) {
        const stype = str(t.errorSinkType) || 'sink';
        const cfg = asObj(t.errorSinkConfig);
        out.push({
          id: ownerId + '.reject', ownerId, ownerIndex: i, flow: 'out',
          kind: 'reject', nodeKind: 'sink', type: stype,
          title: titleFor(stype, cfg), label: 'violations', cfg,
        });
      }
    } else if (type === 'union') {
      // Each bounded secondary source is a reference input into the union.
      const withArr = Array.isArray(t.with) ? t.with : [];
      withArr.forEach((wraw, j) => {
        const w = asObj(wraw);
        if (str(w.type)) {
          const wtype = str(w.type) as string;
          out.push({
            id: ownerId + '.with' + j, ownerId, ownerIndex: i, flow: 'in',
            kind: 'reference', nodeKind: 'source', type: wtype,
            title: titleFor(wtype, w), label: 'union source', cfg: omit(w, []),
          });
        }
      });
    }
  });

  return out;
}

/** Group derived branches by their owner node id (`source` | `t{index}`). */
export function branchesByOwner(branches: DerivedBranch[]): Record<string, DerivedBranch[]> {
  const map: Record<string, DerivedBranch[]> = {};
  for (const b of branches) (map[b.ownerId] = map[b.ownerId] || []).push(b);
  return map;
}

// ─────────────────────────── explicit graph block ───────────────────────────
// deriveGraph builds the board-08 `graph { nodes[], edges[] }` shape from a
// linear pipeline — the same structure the engine's Definition.Graph parses and
// validates. It is the "promote to graph" bridge: Studio writes this block so
// the topology becomes explicit (and, in a future phase, executable). Node ids
// match the canvas node ids (source | t{i} | sink | branch ids | dlq) so a
// promoted graph round-trips with the derived view.

/** A node in the explicit graph block. */
export interface GraphNodeOut {
  id: string;
  kind: 'source' | 'transform' | 'sink';
  type: string;
  ports?: { in?: string[]; out?: string[] };
}

/** An edge in the explicit graph block: ["fromNode:port", "toNode:port"]. */
export type GraphEdgeOut = [string, string] | { from: string; to: string; type?: string; value?: string };

/** The explicit graph block. */
export interface PipeGraph {
  nodes: GraphNodeOut[];
  edges: GraphEdgeOut[];
}

/** Declared ports for a transform, by type (drives the port dots + edge labels). */
function transformPorts(type: string, t: Record<string, unknown>): { in?: string[]; out?: string[] } {
  if (type === 'join') return { in: ['left', 'right'], out: ['out'] };
  if (type === 'validate') return { in: ['in'], out: ['pass', 'reject'] };
  if (type === 'router' || type === 'split') {
    const routes = Array.isArray(t.routes) ? t.routes : [];
    const out = routes.map((r, j) => str(asObj(r).name) || 'route' + j);
    if (str(t.defaultSinkType)) out.push('default');
    out.push('out'); // the pass-through for unmatched rows (onUnmatched: pass)
    return { in: ['in'], out };
  }
  return { in: ['in'], out: ['out'] };
}

/**
 * Build the explicit `graph { nodes, edges }` block from a linear pipeline
 * document, including the implicit branches (router route-sinks, join reference
 * source, validate reject, source parse-error sink, and the top-level DLQ). The
 * result is a DAG (acyclic), so it passes the engine's graph validator.
 */
export function deriveGraph(doc: PipelineDoc | undefined): PipeGraph {
  const nodes: GraphNodeOut[] = [];
  const edges: GraphEdgeOut[] = [];
  if (!doc) return { nodes, edges };

  const src = asObj(doc.source);
  nodes.push({ id: 'source', kind: 'source', type: str(src.type) || 'source', ports: { out: ['rows'] } });

  const transforms = Array.isArray(doc.transforms) ? doc.transforms : [];
  let prevOut = 'source:rows';
  transforms.forEach((traw, i) => {
    const t = asObj(traw);
    const type = str(t.type) || 'transform';
    const id = 't' + i;
    nodes.push({ id, kind: 'transform', type, ports: transformPorts(type, t) });
    edges.push([prevOut, id + ':in']);
    prevOut = id + ':out'; // main pass-through to the next stage
  });

  nodes.push({ id: 'sink', kind: 'sink', type: str(asObj(doc.sink).type) || 'sink', ports: { in: ['in'] } });
  edges.push([prevOut, 'sink:in']);

  // Implicit branches as first-class nodes + edges.
  const branches = deriveBranches(doc);
  branches.forEach((b) => {
    nodes.push({
      id: b.id,
      kind: b.nodeKind,
      type: b.type,
      ports: b.nodeKind === 'sink' ? { in: ['in'] } : { out: ['rows'] },
    });
    if (b.flow === 'out') {
      const port =
        b.kind === 'route' ? (b.title || 'route')
          : b.kind === 'default-route' ? 'default'
            : b.kind === 'reject' ? 'reject'
              : 'errors';
      edges.push([b.ownerId + ':' + port, b.id + ':in']);
    } else {
      // A join's reference source feeds the join's right input.
      edges.push([b.id + ':rows', b.ownerId + ':right']);
    }
  });

  const dlq = asObj(doc.dlq);
  if (str(dlq.type)) {
    nodes.push({ id: 'dlq', kind: 'sink', type: str(dlq.type) as string, ports: { in: ['in'] } });
    edges.push(['sink:err', 'dlq:in']);
  }

  return { nodes, edges };
}

