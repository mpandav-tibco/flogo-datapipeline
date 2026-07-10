// Copyright (c) 2024-2026. Cloud Software Group, Inc. All Rights Reserved. Confidential & Proprietary
/**
 * Pure helpers for parsing a `.datapipeline` document and building the canvas
 * view-model. No `vscode` dependency so it is trivially unit-testable.
 */
import {
  DataPipelineApp,
  FlogoResource,
  PipelineDefinition,
  PipelineGraphVM,
  PipelineNodeVM,
  SinkDef,
  SourceDef,
  TransformDef,
} from './types';

/** Parse a `.datapipeline` document; returns undefined on invalid JSON. */
export function parseApp(text: string): DataPipelineApp | undefined {
  try {
    return JSON.parse(text) as DataPipelineApp;
  } catch {
    return undefined;
  }
}

/** Find the first resource whose id starts with `pipeline:`. */
export function findPipelineResource(
  app: DataPipelineApp | undefined,
): FlogoResource | undefined {
  return app?.resources?.find(
    (r) => typeof r?.id === 'string' && r.id.startsWith('pipeline:'),
  );
}

/** Extract the pipeline Definition (the resource `data` object). */
export function getPipelineDefinition(
  app: DataPipelineApp | undefined,
): PipelineDefinition | undefined {
  return findPipelineResource(app)?.data;
}

/** Short "source → sink" label for the tree / status surfaces. */
export function describePipeline(def: PipelineDefinition | undefined): {
  source: string;
  sink: string;
} {
  return { source: def?.source?.type ?? '—', sink: def?.sink?.type ?? '—' };
}

/** Build the canvas graph view-model from a pipeline Definition. */
export function buildGraph(
  def: PipelineDefinition | undefined,
  name: string,
): PipelineGraphVM {
  const nodes: PipelineNodeVM[] = [];

  if (def?.source) {
    nodes.push({
      id: 'source',
      kind: 'source',
      type: def.source.type,
      title: def.source.path ? basename(def.source.path) : def.source.type,
      meta: sourceMeta(def.source),
    });
  }

  (def?.transforms ?? []).forEach((t, i) => {
    nodes.push({
      id: `t${i}`,
      kind: 'transform',
      type: t.type,
      title: t.type,
      meta: transformMeta(t),
    });
  });

  if (def?.sink) {
    nodes.push({
      id: 'sink',
      kind: 'sink',
      type: def.sink.type,
      title: def.sink.type,
      meta: sinkMeta(def.sink),
    });
  }

  const hasDlq = !!def?.errorSink?.type;
  return {
    name,
    mode: (def?.transforms?.length ?? 0) > 0 ? 'ETL' : 'EL',
    nodes,
    hasDlq,
    dlq: hasDlq
      ? {
          id: 'dlq',
          kind: 'dlq',
          type: def!.errorSink!.type,
          title: 'rejects',
          meta: def!.errorSink!.type,
        }
      : undefined,
  };
}

// ─────────────────────────── meta formatters ───────────────────────────

function sourceMeta(s: SourceDef): string {
  const cols = s.columns?.length ? `${s.columns.length} cols` : undefined;
  return [s.type, s.encoding, cols].filter(Boolean).join(' · ');
}

function sinkMeta(s: SinkDef): string {
  const compression = typeof s.compression === 'string' ? s.compression : undefined;
  const table = typeof s.table === 'string' ? s.table : undefined;
  return [s.type, table, compression].filter(Boolean).join(' · ');
}

function transformMeta(t: TransformDef): string {
  switch (t.type) {
    case 'filter':
      return typeof t.expr === 'string' ? (t.expr as string) : 'filter';
    case 'map':
      return 'projection / derive';
    case 'validate':
      return Array.isArray(t.rules) ? `${(t.rules as unknown[]).length} rules` : 'field rules';
    case 'dq':
      return Array.isArray(t.expectations)
        ? `${(t.expectations as unknown[]).length} expectations`
        : 'data quality';
    default:
      return t.type;
  }
}

function basename(p: string): string {
  const parts = p.split(/[\\/]/);
  return parts[parts.length - 1] || p;
}
