import * as http from 'http';

// Minimal typed HTTP client for the flogo-datapipeline REST management API.
// All requests target 127.0.0.1 on the engine's bound port.

export interface RunResponse {
  runId: string;
  status: string; // running | completed | partial | failed | cancelled
  rowsProcessed: number;
  rowsWritten: number;
  rowsFailed: number;
  rowsDropped?: number;
  durationMs: number;
  error?: string;
  startedAt: string;
  finishedAt?: string;
  qualityReport?: { passed: number; failed: number; violations?: string[] };
}

export interface PipelineInfo {
  ref: string;
  sourceType: string;
  sinkType: string;
  status: string;
  lastRunAt?: string;
  lastRunDurationMs?: number;
  lastRunStatus?: string;
}

export interface StatusResponse extends PipelineInfo {
  runs: RunResponse[];
}

export interface PreviewResult {
  columns: string[];
  rows: Array<Record<string, unknown>>;
  rowsRead: number;
  rowsDropped: number;
  rowsFailed: number;
  truncated: boolean;
}

export interface DefinitionSummary {
  name: string;
  description: string;
  source: { type: string; path: string; columns: Array<{ name: string; type: string }> };
  transforms: Array<Record<string, unknown>>;
  sink: { type: string };
  schemaDrift: string;
  incremental: string;
  observability: string;
  dlq: string;
}

export interface StageTestRow {
  status: 'kept' | 'dropped' | 'failed';
  input: Record<string, unknown>;
  output?: Record<string, unknown>;
  error?: string;
}

export interface StageTestResult {
  type: string;
  columns: string[];
  rows: StageTestRow[];
  rowsIn: number;
  kept: number;
  dropped: number;
  failed: number;
}

export interface IntrospectedColumn {
  name: string;
  type: string;
  dbType?: string;
  nullable?: boolean;
}

function request<T>(
  port: number,
  method: string,
  path: string,
  body?: unknown,
  timeoutMs = 8000,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const payload = body != null ? Buffer.from(JSON.stringify(body)) : undefined;
    const req = http.request(
      {
        host: '127.0.0.1',
        port,
        method,
        path,
        headers: payload
          ? { 'Content-Type': 'application/json', 'Content-Length': payload.length }
          : {},
      },
      (res) => {
        let data = '';
        res.on('data', (d) => (data += d));
        res.on('end', () => {
          const status = res.statusCode ?? 0;
          if (status === 204) {
            resolve(undefined as unknown as T);
            return;
          }
          let parsed: unknown;
          try {
            parsed = data ? JSON.parse(data) : undefined;
          } catch {
            reject(new Error(`invalid JSON from ${path}: ${data.slice(0, 200)}`));
            return;
          }
          if (status >= 200 && status < 300) {
            resolve(parsed as T);
          } else {
            const msg = (parsed as any)?.message || `HTTP ${status}`;
            reject(new Error(msg));
          }
        });
      },
    );
    req.on('error', reject);
    req.setTimeout(timeoutMs, () => req.destroy(new Error(`request to ${path} timed out`)));
    if (payload) req.write(payload);
    req.end();
  });
}

export class ApiClient {
  constructor(private readonly port: number) {}

  health(): Promise<{ status: string; pipelines: number }> {
    return request(this.port, 'GET', '/v1/health');
  }

  listPipelines(): Promise<{ pipelines: PipelineInfo[] }> {
    return request(this.port, 'GET', '/v1/pipelines');
  }

  runPipeline(ref: string, opts: { async?: boolean; runId?: string; params?: Record<string, unknown> } = {}): Promise<RunResponse> {
    const q = opts.async ? '?async=true' : '';
    const body: Record<string, unknown> = {};
    if (opts.runId) body.runId = opts.runId;
    if (opts.params) body.params = opts.params;
    // Sync runs can take arbitrarily long — use a generous timeout for the async ack.
    return request(this.port, 'POST', `/v1/pipelines/${encodeURIComponent(ref)}/run${q}`, body, opts.async ? 8000 : 3_600_000);
  }

  status(ref: string): Promise<StatusResponse> {
    return request(this.port, 'GET', `/v1/pipelines/${encodeURIComponent(ref)}/status`);
  }

  preview(ref: string, limit = 100): Promise<PreviewResult> {
    return request(this.port, 'POST', `/v1/pipelines/${encodeURIComponent(ref)}/preview?limit=${limit}`, {}, 60_000);
  }

  // Sample the rows that ENTER the transform at `stage` (source run through all
  // preceding transforms) — seeds the atomic stage tester.
  sampleInput(ref: string, stage: number, limit = 20): Promise<PreviewResult> {
    return request(this.port, 'POST', `/v1/pipelines/${encodeURIComponent(ref)}/sampleinput`, { stage, limit }, 60_000);
  }

  // Run a single transform (by index) against supplied input rows in isolation.
  testStage(
    ref: string,
    stage: number,
    columns: string[],
    rows: Array<Record<string, unknown>>,
  ): Promise<StageTestResult> {
    return request(
      this.port,
      'POST',
      `/v1/pipelines/${encodeURIComponent(ref)}/teststage`,
      { stage, columns, rows },
      30_000,
    );
  }

  definition(ref: string): Promise<DefinitionSummary> {
    return request(this.port, 'GET', `/v1/pipelines/${encodeURIComponent(ref)}/definition`);
  }

  // List tables reachable through a driver + DSN (live DB introspection).
  introspectTables(driver: string, dsn: string): Promise<{ tables: string[] }> {
    return request(this.port, 'POST', '/v1/introspect/tables', { driver, dsn }, 15_000);
  }

  // Fetch the column schema of a table (mapped to pipeline column types).
  introspectColumns(driver: string, dsn: string, table: string): Promise<{ columns: IntrospectedColumn[] }> {
    return request(this.port, 'POST', '/v1/introspect/columns', { driver, dsn, table }, 15_000);
  }

  // Read a source's own schema (Parquet footer, CSV header, JSON sample, …).
  introspectSourceSchema(type: string, config: Record<string, unknown>): Promise<{ columns: IntrospectedColumn[] }> {
    return request(this.port, 'POST', '/v1/introspect/source-schema', { type, config }, 20_000);
  }

  // List objects in an object store under an optional prefix.
  introspectObjects(provider: string, config: Record<string, unknown>): Promise<{ objects: Array<{ key: string; size: number }> }> {
    return request(this.port, 'POST', '/v1/introspect/objects', { provider, config }, 20_000);
  }

  // List non-internal topic names from a Kafka cluster.
  introspectKafkaTopics(brokers: string[], config: Record<string, unknown>): Promise<{ topics: string[] }> {
    return request(this.port, 'POST', '/v1/introspect/kafka-topics', { brokers, config }, 20_000);
  }

  // Fetch the latest Avro/JSON schema for a topic/subject from a schema registry.
  introspectSchemaRegistry(url: string, topic: string): Promise<{ columns: IntrospectedColumn[]; subject: string }> {
    return request(this.port, 'POST', '/v1/introspect/schema-registry', { url, topic }, 20_000);
  }

  // Sample an XML file to detect the record element + child fields.
  introspectXML(path: string): Promise<{
    recordElement: string;
    fields: Array<{ name: string; xpath: string; type: string }>;
    columns: IntrospectedColumn[];
  }> {
    return request(this.port, 'POST', '/v1/introspect/xml', { path }, 20_000);
  }

  cancel(ref: string, runId: string): Promise<void> {
    return request(this.port, 'DELETE', `/v1/pipelines/${encodeURIComponent(ref)}/run/${encodeURIComponent(runId)}`);
  }
}
