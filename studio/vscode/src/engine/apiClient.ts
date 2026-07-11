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

  definition(ref: string): Promise<DefinitionSummary> {
    return request(this.port, 'GET', `/v1/pipelines/${encodeURIComponent(ref)}/definition`);
  }

  cancel(ref: string, runId: string): Promise<void> {
    return request(this.port, 'DELETE', `/v1/pipelines/${encodeURIComponent(ref)}/run/${encodeURIComponent(runId)}`);
  }
}
