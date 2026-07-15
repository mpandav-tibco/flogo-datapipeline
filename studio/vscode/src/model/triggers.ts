// Trigger catalog — the reusable set of Flogo triggers that can invoke a data
// pipeline, plus the deploy-time wiring that turns a pipeline's `trigger` config
// into an app-level `triggers[]` block + `imports`.
//
// Design (see also model/pipeline.ts):
//   • A pipeline stores its chosen trigger inline as `pipeline.trigger`
//     ({ kind, settings }). This rides along inside resources[0].data, so it is
//     saved with the .datapipeline file and never needs a separate file.
//   • The editable/runnable .datapipeline always keeps a boot-safe timer in its
//     runnable triggers[] (the Studio's bundled engine only registers the timer
//     trigger, and Studio runs are driven through the REST management API — so
//     the file must always boot). See wrapInAppJson / mergePipelineIntoDoc.
//   • At DEPLOY time, applyTriggerToApp() rewrites the generated app JSON's
//     triggers[] + imports to the real selected trigger, wiring the trigger
//     output into the action input automatically (no hand-editing JSON).
//
// This keeps the canvas clean (no trigger nodes) and the trigger configuration
// lives in a dedicated Pipeline Settings tab, mirroring how connections work.

const PIPELINE_ACTION_REF = 'github.com/mpandav-tibco/flogo-datapipeline/action/pipeline';

export type TriggerFieldType = 'string' | 'number' | 'boolean' | 'select';
export type TriggerFieldScope = 'trigger' | 'handler';

export interface TriggerFieldDef {
  name: string;
  label: string;
  type: TriggerFieldType;
  scope: TriggerFieldScope; // trigger-level settings vs per-handler settings
  default?: string | number | boolean;
  options?: Array<[string, string]>; // [value, label] for select
  placeholder?: string;
  hint?: string;
}

export interface TriggerTypeDef {
  kind: string; // 'timer' | 'rest' | 'kafka' | 'filepoller'
  label: string;
  ref: string; // Flogo import ref emitted into imports[]
  triggerId: string; // id used in triggers[]
  // available === true means the trigger module is a public project-flogo
  // package that resolves at `go build` with no extra setup. false means the
  // module (e.g. a TIBCO plugin) must be made go-gettable / vendored first.
  available: boolean;
  summary: string;
  note?: string; // shown in the UI (e.g. build-time module requirement)
  fields: TriggerFieldDef[];
  // trigger output → action input mapping (Flogo `=$...` expressions).
  inputMapping: Record<string, string>;
  // optional handler.action.output mapping (e.g. REST response).
  outputMapping?: Record<string, string>;
}

export const TRIGGER_CATALOG: TriggerTypeDef[] = [
  {
    kind: 'timer',
    label: 'Timer — run on a schedule',
    ref: 'github.com/project-flogo/contrib/trigger/timer',
    triggerId: 'pipeline-timer',
    available: true,
    summary: 'Runs the pipeline repeatedly on a fixed interval.',
    fields: [
      { name: 'repeating', label: 'Repeating', type: 'boolean', scope: 'trigger', default: true, hint: 'Fire once (false) or on every interval (true).' },
      { name: 'startDelay', label: 'Start delay (ms)', type: 'number', scope: 'trigger', default: 0, placeholder: '0', hint: 'Delay before the first fire.' },
      { name: 'interval', label: 'Interval (ms)', type: 'number', scope: 'trigger', default: 86400000, placeholder: '86400000', hint: 'Time between runs (86400000 = 24h).' },
    ],
    inputMapping: {},
  },
  {
    kind: 'rest',
    label: 'REST — invoke over HTTP',
    ref: 'github.com/project-flogo/contrib/trigger/rest',
    triggerId: 'pipeline-rest',
    available: true,
    summary: 'Exposes an HTTP endpoint; a POST triggers a run. Body may set path/runId/params.',
    fields: [
      { name: 'port', label: 'Port', type: 'number', scope: 'trigger', default: 9090, placeholder: '9090', hint: 'HTTP port the trigger listens on (avoid the management API port).' },
      { name: 'method', label: 'Method', type: 'select', scope: 'handler', default: 'POST', options: [['POST', 'POST'], ['GET', 'GET']], hint: 'HTTP method for the run endpoint.' },
      { name: 'path', label: 'Path', type: 'string', scope: 'handler', default: '/pipeline/run', placeholder: '/pipeline/run', hint: 'HTTP path that triggers a run.' },
    ],
    inputMapping: {
      inputPath: '=$.content.path',
      runId: '=$.content.runId',
      params: '=$.content.params',
    },
    outputMapping: { code: '=200', data: '=$.result' },
  },
  {
    kind: 'kafka',
    label: 'Kafka — run per consumed message',
    ref: 'github.com/project-flogo/messaging-contrib/kafka/trigger',
    triggerId: 'pipeline-kafka',
    available: false,
    summary: 'Consumes a Kafka topic; each message triggers a pipeline run.',
    note: 'Requires the project-flogo Kafka messaging module (github.com/project-flogo/messaging-contrib/kafka) on the build path.',
    fields: [
      { name: 'brokerUrls', label: 'Broker URLs', type: 'string', scope: 'trigger', default: 'localhost:9092', placeholder: 'host1:9092,host2:9092', hint: 'Comma-separated Kafka bootstrap brokers.' },
      { name: 'topic', label: 'Topic', type: 'string', scope: 'handler', default: '', placeholder: 'orders', hint: 'Topic to consume.' },
      { name: 'consumerGroup', label: 'Consumer group', type: 'string', scope: 'handler', default: 'flogo-datapipeline', placeholder: 'flogo-datapipeline', hint: 'Kafka consumer group id.' },
    ],
    inputMapping: {
      params: '=$.message',
    },
  },
  {
    kind: 'filepoller',
    label: 'File Poller — watch a folder',
    ref: 'github.com/tibco/flogo-files/src/app/File/trigger/poller',
    triggerId: 'pipeline-file-poller',
    available: false,
    summary: 'Watches a directory; each matching file event runs the pipeline against that file.',
    note: 'Requires the TIBCO File plugin module (github.com/tibco/flogo-files) on the build path. The changed file path is wired into the source automatically.',
    fields: [
      { name: 'pollingDir', label: 'Polling directory', type: 'string', scope: 'handler', default: '', placeholder: '/data/incoming', hint: 'Directory to watch for file events.' },
      { name: 'recursive', label: 'Include sub-directories', type: 'boolean', scope: 'handler', default: false, hint: 'Watch nested folders too.' },
      { name: 'fileFilter', label: 'File filter (regex)', type: 'string', scope: 'handler', default: '', placeholder: '^.*\\.csv$', hint: 'Only files matching this regex are processed.' },
      { name: 'pollingInterval', label: 'Polling interval (ms)', type: 'number', scope: 'handler', default: 5000, placeholder: '5000', hint: 'How often to check for changes.' },
      { name: 'mode', label: 'Mode', type: 'select', scope: 'handler', default: 'Only Files', options: [['Only Files', 'Only Files'], ['Only Directories', 'Only Directories'], ['Files and Directories', 'Files and Directories']], hint: 'What to watch for.' },
      { name: 'fileEvents', label: 'File events (JSON array)', type: 'string', scope: 'handler', default: '["Create","Write"]', placeholder: '["Create","Write"]', hint: 'Events that trigger a run: Create, Write, Rename, Remove, Move.' },
    ],
    inputMapping: {
      inputPath: '=$.output.fileMetadata.fullPath',
    },
  },
];

export function triggerDef(kind: string | undefined): TriggerTypeDef {
  return TRIGGER_CATALOG.find((t) => t.kind === kind) || TRIGGER_CATALOG[0];
}

// Coerce a stored settings value to the field's declared type so the emitted
// app JSON carries proper JSON types (numbers/bools, not strings).
function coerce(def: TriggerFieldDef, raw: unknown): unknown {
  const v = raw ?? def.default;
  if (def.type === 'number') return typeof v === 'number' ? v : Number(v);
  if (def.type === 'boolean') return typeof v === 'boolean' ? v : v === 'true';
  return v;
}

// Build the app-level trigger block for the given pipeline trigger config.
// resourceId is the pipeline resource id (e.g. "pipeline:daily-tx").
export function buildTriggerBlock(
  trigger: { kind?: string; settings?: Record<string, unknown> } | undefined,
  resourceId: string,
): { ref: string; block: any } {
  const def = triggerDef(trigger?.kind);
  const settings = (trigger && trigger.settings) || {};
  const triggerSettings: Record<string, unknown> = {};
  const handlerSettings: Record<string, unknown> = {};
  for (const f of def.fields) {
    const target = f.scope === 'handler' ? handlerSettings : triggerSettings;
    target[f.name] = coerce(f, settings[f.name]);
  }

  const action: any = {
    ref: PIPELINE_ACTION_REF,
    settings: { pipelineRef: 'res://' + resourceId },
  };
  if (Object.keys(def.inputMapping).length) action.input = { ...def.inputMapping };
  if (def.outputMapping) action.output = { ...def.outputMapping };

  return {
    ref: def.ref,
    block: {
      id: def.triggerId,
      ref: def.ref,
      settings: triggerSettings,
      handlers: [{ settings: handlerSettings, action }],
    },
  };
}

// Rewrite a full flogo:app object's triggers[] + imports to the pipeline's
// selected trigger. Used by the deploy artifact generator so deployed apps run
// under the real trigger (timer/rest/kafka/filepoller) while the editable file
// keeps its boot-safe timer for Studio runs.
export function applyTriggerToApp(app: any): void {
  if (!app || typeof app !== 'object') return;
  const res = Array.isArray(app.resources) ? app.resources[0] : null;
  const pipe = res && res.data;
  if (!pipe || typeof pipe !== 'object') return;
  const resourceId = String(res.id || 'pipeline:untitled').replace(/^res:\/\//, '');

  const { ref, block } = buildTriggerBlock(pipe.trigger, resourceId);

  app.triggers = [block];

  // Ensure the trigger ref is imported (action ref stays; drop the old timer ref
  // if a different trigger is now selected).
  const imports: string[] = Array.isArray(app.imports) ? app.imports.slice() : [];
  const triggerRefs = TRIGGER_CATALOG.map((t) => t.ref);
  const kept = imports.filter((i) => !triggerRefs.includes(i));
  if (!kept.includes(PIPELINE_ACTION_REF)) kept.unshift(PIPELINE_ACTION_REF);
  if (!kept.includes(ref)) kept.push(ref);
  app.imports = kept;
}
