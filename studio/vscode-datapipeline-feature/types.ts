// Copyright (c) 2024-2026. Cloud Software Group, Inc. All Rights Reserved. Confidential & Proprietary
/**
 * Type definitions for the Flogo Data Pipeline studio feature.
 *
 * These mirror the engine's REST management API
 * (github.com/mpandav-tibco/flogo-datapipeline · api/types.go) and the on-disk
 * `.datapipeline` document shape (a `flogo:app` JSON with a `pipeline:*` resource).
 */

/** Lifecycle status of a pipeline or a single run. */
export type PipelineStatus =
  | 'idle'
  | 'running'
  | 'completed'
  | 'partial'
  | 'failed'
  | 'cancelled';

/** Result counters for a single run (POST /v1/pipelines/{ref}/run). */
export interface RunSummary {
  runId: string;
  status: PipelineStatus;
  rowsProcessed: number;
  rowsWritten: number;
  rowsFailed: number;
  rowsDropped: number;
  rowsRouted: number;
  durationMs: number;
  error?: string;
  qualityReport?: DQReport;
}

/** Data-quality expectation results for a run. */
export interface DQReport {
  passed: number;
  failed: number;
  violations?: string[];
}

/** Summary of a registered pipeline (GET /v1/pipelines). */
export interface PipelineInfo {
  ref: string;
  sourceType: string;
  sinkType: string;
  status: PipelineStatus;
  lastRunAt?: string;
  lastRunDurationMs?: number;
  lastRunStatus?: PipelineStatus;
}

/** GET /v1/pipelines/{ref}/status — info + recent run records (ring buffer). */
export interface PipelineStatusResponse extends PipelineInfo {
  runs: RunSummary[];
}

/** GET /v1/capabilities — component types the running engine supports. */
export interface Capabilities {
  sources: string[];
  sinks: string[];
  transforms: string[];
}

/** GET /v1/health. */
export interface HealthResponse {
  status: string;
  pipelines: number;
}

// ─────────────────────────── on-disk shapes ───────────────────────────

/** A single stage in the transform chain. */
export interface TransformDef {
  /** filter | map | validate | lookup | hwm | schemadrift | dq */
  type: string;
  [key: string]: unknown;
}

/** Source configuration (csv, jdbc, kafka, parquet, objstore, …). */
export interface SourceDef {
  type: string;
  encoding?: string;
  columns?: Array<{ name: string; type: string }>;
  path?: string;
  options?: Record<string, unknown>;
  [key: string]: unknown;
}

/** Sink configuration (jdbc, parquet, kafka, objstore, null, …). */
export interface SinkDef {
  type: string;
  [key: string]: unknown;
}

/** Checkpoint / resume configuration. */
export interface CheckpointDef {
  type?: string;
  path?: string;
  intervalRows?: number;
  intervalSeconds?: number;
  deleteOnSuccess?: boolean;
}

/** The pipeline Definition — the `data` object of a `pipeline:*` resource. */
export interface PipelineDefinition {
  name?: string;
  source?: SourceDef;
  transforms?: TransformDef[];
  sink?: SinkDef;
  /** Dead-letter sink for rejected rows. */
  errorSink?: SinkDef;
  checkpoint?: CheckpointDef;
  [key: string]: unknown;
}

/** A `flogo:app` resource entry. */
export interface FlogoResource {
  /** e.g. "pipeline:daily-tx" */
  id: string;
  data?: PipelineDefinition;
  [key: string]: unknown;
}

/** The top-level `.datapipeline` document (a `flogo:app`). */
export interface DataPipelineApp {
  name?: string;
  type?: string;
  resources?: FlogoResource[];
  [key: string]: unknown;
}

/** Options for a local CLI run. */
export interface RunOptions {
  runId?: string;
  inputPath?: string;
  verbose?: boolean;
}

/** Messages exchanged between the canvas webview and the extension host. */
export type WebviewInbound =
  | { type: 'ready' }
  | { type: 'run' }
  | { type: 'preview' }
  | { type: 'selectNode'; nodeId: string }
  | { type: 'reopenAsText' };

export type WebviewOutbound =
  | { type: 'model'; graph: PipelineGraphVM }
  | { type: 'runState'; state: PipelineStatus; summary?: RunSummary };

/** A canvas node view-model. */
export interface PipelineNodeVM {
  id: string;
  kind: 'source' | 'transform' | 'sink' | 'dlq';
  type: string;
  title: string;
  meta: string;
}

/** The full canvas graph view-model sent to the webview. */
export interface PipelineGraphVM {
  name: string;
  mode: 'EL' | 'ETL';
  nodes: PipelineNodeVM[];
  hasDlq: boolean;
  dlq?: PipelineNodeVM;
}
