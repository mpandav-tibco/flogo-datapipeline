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
  [key: string]: unknown;
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
  if (appDoc && appDoc.type === 'flogo:app') {
    const res = Array.isArray(appDoc.resources) ? appDoc.resources[0] : null;
    return (res && res.data) || {};
  }
  return appDoc || {};
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
  if (appDoc.type === 'flogo:app' && Array.isArray(appDoc.resources) && appDoc.resources[0]) {
    appDoc.resources[0].data = pd;
    if (pd.name) {
      appDoc.name = pd.name;
      const newId = 'pipeline:' + slug(pd.name);
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
    if (pd.description) appDoc.description = pd.description;
    else delete appDoc.description;
    return JSON.stringify(appDoc, null, 2);
  }
  return JSON.stringify(wrapInAppJson(pd), null, 2);
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
  return issues;
}
