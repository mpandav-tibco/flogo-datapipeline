// Pipeline data model + flogo:app wrap/unwrap + validation.
// A .datapipeline file is either a flogo:app JSON (resources[0].data = pipeline)
// or a bare/flat pipeline object (auto-migrated to flogo:app on first save).

export interface PipelineStageConfig {
  [key: string]: unknown;
}

export interface PipelineDoc {
  name?: string;
  description?: string;
  version?: string;
  mode?: string; // etl | el | elt
  source?: PipelineStageConfig & { type?: string };
  transforms?: Array<PipelineStageConfig & { type?: string }>;
  sink?: PipelineStageConfig & { type?: string };
  performance?: PipelineStageConfig;
  scaling?: PipelineStageConfig;
  checkpoint?: PipelineStageConfig;
  // Selected invocation trigger (timer/rest/kafka/filepoller). Stored inline in
  // the pipeline resource data; materialised into app triggers[] at deploy.
  trigger?: { kind?: string; settings?: Record<string, unknown> };
  // App-level Flogo properties, surfaced from the app JSON so the Studio can edit
  // them. Split back out to app.properties on save (NEVER stored in the pipeline
  // resource data). Referenced from config fields as `$property[name]`.
  __appProperties?: AppProperty[];
  [key: string]: unknown;
}

export interface AppProperty {
  name: string;
  type?: string;
  value?: unknown;
  description?: string;
}

export interface ValidationIssue {
  sev: 'error' | 'warn' | 'info';
  label: string;
  msg: string;
  anchor: string | null;
}

const PIPELINE_ACTION_REF = 'github.com/mpandav-tibco/flogo-datapipeline/action/pipeline';
const TIMER_TRIGGER_REF = 'github.com/project-flogo/contrib/trigger/timer';

export function slug(name: string): string {
  return String(name || 'untitled-pipeline')
    .toLowerCase()
    .replace(/\s+/g, '-')
    .replace(/[^a-z0-9-]/g, '');
}

// Extract the pipeline data from a flogo:app JSON; fall back to the whole object
// for flat-format files (auto-migration on next save).
export function extractPipelineData(appDoc: any): PipelineDoc {
  const props: AppProperty[] = appDoc && Array.isArray(appDoc.properties) ? appDoc.properties : [];
  if (appDoc && appDoc.type === 'flogo:app') {
    const res = Array.isArray(appDoc.resources) ? appDoc.resources[0] : null;
    const data = (res && res.data) || {};
    return { ...data, __appProperties: props };
  }
  return { ...(appDoc || {}), __appProperties: props };
}

// Wrap a pipeline data object into a complete flogo:app JSON.
export function wrapInAppJson(pd: PipelineDoc): any {
  const name = pd.name || 'untitled-pipeline';
  const resourceId = 'pipeline:' + slug(name);
  // The runnable file always keeps a boot-safe timer trigger (the bundled Studio
  // engine only registers the timer trigger, and Studio drives runs through the
  // REST management API). When the user's selected trigger is the timer, honour
  // its interval/delay here; other trigger kinds are materialised at deploy.
  const t = pd.trigger;
  const timerSettings =
    t && t.kind === 'timer' && t.settings
      ? {
        repeating: t.settings.repeating != null ? t.settings.repeating : true,
        startDelay: t.settings.startDelay != null ? Number(t.settings.startDelay) : 0,
        interval: t.settings.interval != null ? Number(t.settings.interval) : 86400000,
      }
      : { repeating: true, startDelay: 0, interval: 86400000 };
  return {
    name,
    type: 'flogo:app',
    version: pd.version || '1.0.0',
    description: pd.description || '',
    appModel: '1.1.1',
    imports: [PIPELINE_ACTION_REF, TIMER_TRIGGER_REF],
    triggers: [
      {
        id: 'pipeline-timer',
        ref: TIMER_TRIGGER_REF,
        settings: timerSettings,
        handlers: [
          {
            settings: {},
            action: {
              ref: PIPELINE_ACTION_REF,
              settings: { pipelineRef: 'res://' + resourceId },
            },
          },
        ],
      },
    ],
    resources: [{ id: resourceId, data: pd }],
  };
}

// Merge an edited pipeline data object back into the full document text,
// preserving flogo:app structure and keeping the resource id / trigger ref in sync.
export function mergePipelineIntoDoc(currentText: string, pd: PipelineDoc): string {
  let appDoc: any = {};
  try {
    appDoc = JSON.parse(currentText);
  } catch {
    appDoc = {};
  }
  // App-level properties travel on the pipeline doc as __appProperties; split
  // them back into app.properties (never stored inside the pipeline resource).
  const appProps = Array.isArray((pd as any).__appProperties) ? ((pd as any).__appProperties as AppProperty[]) : null;
  const pdData: PipelineDoc = { ...pd };
  delete (pdData as any).__appProperties;
  const applyProps = (target: any) => {
    if (appProps && appProps.length) target.properties = appProps;
    else delete target.properties;
  };
  if (appDoc.type === 'flogo:app' && Array.isArray(appDoc.resources) && appDoc.resources[0]) {
    appDoc.resources[0].data = pdData;
    applyProps(appDoc);
    if (pdData.name) {
      appDoc.name = pdData.name;
      const newId = 'pipeline:' + slug(pdData.name);
      if (appDoc.resources[0].id !== newId) {
        appDoc.resources[0].id = newId;
        const h =
          appDoc.triggers &&
          appDoc.triggers[0] &&
          appDoc.triggers[0].handlers &&
          appDoc.triggers[0].handlers[0];
        if (h && h.action && h.action.settings) {
          h.action.settings.pipelineRef = 'res://' + newId;
        }
      }
    }
    // Mirror the description to the top-level app metadata too (kept in sync
    // with the resource data so both the app JSON and the pipeline resource
    // document the pipeline).
    if (pdData.description) appDoc.description = pdData.description;
    else delete appDoc.description;
    return JSON.stringify(appDoc, null, 2);
  }
  const wrapped = wrapInAppJson(pdData);
  applyProps(wrapped);
  return JSON.stringify(wrapped, null, 2);
}

// Validate a pipeline document. Returns a list of issues (mirrored to Problems).
export function validate(text: string): ValidationIssue[] {
  const issues: ValidationIssue[] = [];
  let appDoc: any = {};
  try {
    appDoc = JSON.parse(text);
  } catch {
    return [{ sev: 'error', label: 'JSON', msg: 'Pipeline file contains invalid JSON.', anchor: null }];
  }
  const doc = extractPipelineData(appDoc);
  const source: any = doc.source || {};
  const sink: any = doc.sink || {};
  const transforms = Array.isArray(doc.transforms) ? doc.transforms : [];
  const mode = String(doc.mode || 'etl').toLowerCase();

  if (!source.type) {
    issues.push({ sev: 'error', label: 'source', msg: 'Source type is required.', anchor: 'source' });
  } else {
    if (source.type === 'jdbc' && !source.dsn && !source.connection) {
      issues.push({ sev: 'error', label: 'source.dsn', msg: 'JDBC source requires a DSN or a bound connection.', anchor: 'dsn' });
    }
    if (source.type === 'kafka' && !source.brokers && !source.connection) {
      issues.push({ sev: 'error', label: 'source.brokers', msg: 'Kafka source requires brokers or a bound connection.', anchor: 'brokers' });
    }
    if ((source.type === 'csv' || source.type === 'parquet' || source.type === 'xml' || source.type === 'jsonlines' || source.type === 'fixedwidth') && !source.path) {
      issues.push({ sev: 'error', label: 'source.path', msg: `${source.type} source requires a path.`, anchor: 'path' });
    }
  }

  if (!sink.type) {
    issues.push({ sev: 'error', label: 'sink', msg: 'Sink type is required.', anchor: 'sink' });
  } else {
    if (sink.type === 'jdbc' && !sink.dsn && !sink.connection) {
      issues.push({ sev: 'error', label: 'sink.dsn', msg: 'JDBC sink requires a DSN or a bound connection.', anchor: 'dsn' });
    }
    if (sink.type === 'kafka' && !sink.brokers && !sink.connection) {
      issues.push({ sev: 'error', label: 'sink.brokers', msg: 'Kafka sink requires brokers or a bound connection.', anchor: 'brokers' });
    }
  }

  if (mode === 'etl' && transforms.length === 0) {
    issues.push({ sev: 'warn', label: 'mode', msg: 'ETL mode is selected but no transforms are defined. Add a transform or switch to EL mode.', anchor: 'transforms' });
  }

  // Streaming sources (podlog tailer, Kafka) run unbounded. Without a checkpoint
  // their delivery position is never persisted mid-run, so a restart replays from
  // the very beginning (or loses the position entirely) — a silent-loss / full-
  // replay risk. A time-based checkpoint is required for at-least-once delivery.
  const streamingSources = new Set(['podlog', 'kafka']);
  const isStreaming = streamingSources.has(String(source.type));
  const bounded = source.options && source.options.bounded === true;
  const checkpoint: any = doc.checkpoint || {};
  const cpType = String(checkpoint.type || 'none');
  if (isStreaming && !bounded && (cpType === 'none' || cpType === '')) {
    issues.push({
      sev: 'warn',
      label: 'checkpoint',
      msg: `A streaming '${source.type}' source without a checkpoint never persists its delivery position — a restart replays from the beginning (risking loss/duplication). Add a checkpoint (e.g. file, intervalSeconds) for at-least-once delivery.`,
      anchor: 'checkpoint',
    });
  }

  // Data contract (schema + quality gate + SLA + reconciliation) — mirrors the
  // engine's own Definition.validate() rules (action/pipeline/definition.go) so
  // a misconfiguration surfaces here at design time instead of only at run time.
  const contract: any = doc.contract;
  if (contract) {
    const onViolation = String(contract.onViolation || 'route');
    if (!['route', 'fail', 'warn', 'drop'].includes(onViolation)) {
      issues.push({ sev: 'error', label: 'contract.onViolation', msg: `contract.onViolation must be 'route', 'fail', 'warn', or 'drop', got '${onViolation}'.`, anchor: 'contract' });
    }
    const recon: any = contract.reconciliation;
    const reconEnabled = !!(recon && recon.enabled);
    const hasSchema = Array.isArray(contract.schema) && contract.schema.length > 0;
    const hasExpectations = Array.isArray(contract.expectations) && contract.expectations.length > 0;
    const hasMaxDuration = !!contract.maxDurationMs;
    if (!hasSchema && !hasExpectations && !hasMaxDuration && !reconEnabled) {
      issues.push({ sev: 'warn', label: 'contract', msg: 'A data contract is present but declares nothing to enforce (set schema, expectations, maxDurationMs, and/or reconciliation).', anchor: 'contract' });
    }
    if (reconEnabled) {
      const onMismatch = String(recon.onMismatch || 'warn');
      if (!['warn', 'fail'].includes(onMismatch)) {
        issues.push({ sev: 'error', label: 'contract.reconciliation.onMismatch', msg: `contract.reconciliation.onMismatch must be 'warn' or 'fail', got '${onMismatch}'.`, anchor: 'contract' });
      }
      if (!recon.sourceCountQuery || !recon.sinkCountQuery) {
        issues.push({ sev: 'error', label: 'contract.reconciliation', msg: 'Reconciliation is enabled but requires both a source count query and a sink count query.', anchor: 'contract' });
      }
      if (recon.maxDriftPct != null && Number(recon.maxDriftPct) < 0) {
        issues.push({ sev: 'error', label: 'contract.reconciliation.maxDriftPct', msg: 'contract.reconciliation.maxDriftPct must be >= 0.', anchor: 'contract' });
      }
      if ((source.type !== 'jdbc') || (sink.type !== 'jdbc')) {
        issues.push({ sev: 'warn', label: 'contract.reconciliation', msg: 'Reconciliation only runs when both source and sink are jdbc — it will be skipped (with a warning) for this pipeline.', anchor: 'contract' });
      }
    }
  }
  return issues;
}
