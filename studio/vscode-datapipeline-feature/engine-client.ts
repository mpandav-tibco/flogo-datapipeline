// Copyright (c) 2024-2026. Cloud Software Group, Inc. All Rights Reserved. Confidential & Proprietary
/**
 * Client for the Flogo Data Pipeline engine.
 *
 * Supports two modes, both backed only by node built-ins (no external deps):
 *   1. CLI mode  — spawn the `flogo-datapipeline` binary on a `.datapipeline`
 *                  file and stream its `-v` output; parse the summary line.
 *   2. API mode  — talk to a running engine's REST server (`/v1/*`) when
 *                  `flogo3.datapipeline.engineAddr` is configured.
 *
 * Mirrors the extension's existing binary-exec pattern (`common/flogo-bin.ts`).
 */
import * as vscode from 'vscode';
import * as http from 'node:http';
import * as path from 'node:path';
import { spawn, ChildProcess } from 'node:child_process';
import {
  Capabilities,
  HealthResponse,
  PipelineInfo,
  PipelineStatus,
  PipelineStatusResponse,
  RunOptions,
  RunSummary,
} from './types';

const CFG = 'flogo3.datapipeline';
const DEFAULT_BINARY = 'flogo-datapipeline';

export class DataPipelineEngineClient {
  constructor(private readonly output: vscode.OutputChannel) {}

  private cfg<T>(key: string, dflt: T): T {
    return vscode.workspace.getConfiguration(CFG).get<T>(key, dflt);
  }

  /** Resolve the engine binary: explicit setting → binary name on PATH. */
  resolveBinary(): string {
    const configured = this.cfg<string>('binaryPath', '').trim();
    return configured || DEFAULT_BINARY;
  }

  /** Address of a running engine API server, or '' when not configured. */
  get engineAddr(): string {
    return this.cfg<string>('engineAddr', '').trim();
  }

  /**
   * Run a `.datapipeline` file locally via the CLI, streaming `-v` output to
   * `onLine`. Resolves with the parsed run summary (or a synthetic one).
   */
  runFile(
    fileFsPath: string,
    opts: RunOptions,
    onLine: (line: string) => void,
    token?: vscode.CancellationToken,
  ): Promise<RunSummary> {
    const bin = this.resolveBinary();
    const args = ['-f', fileFsPath];
    if (opts.verbose !== false) {
      args.push('-v');
    }
    if (opts.runId) {
      args.push('--run-id', opts.runId);
    }
    if (opts.inputPath) {
      args.push('--input-path', opts.inputPath);
    }
    this.output.appendLine(`$ ${bin} ${args.join(' ')}`);

    return new Promise<RunSummary>((resolve, reject) => {
      let child: ChildProcess;
      try {
        child = spawn(bin, args, { cwd: path.dirname(fileFsPath) });
      } catch (err) {
        reject(err);
        return;
      }

      let last: RunSummary | undefined;
      const onData = (buf: Buffer) => {
        for (const line of buf.toString().split(/\r?\n/)) {
          if (!line) {
            continue;
          }
          onLine(line);
          const parsed = parseSummaryLine(line);
          if (parsed) {
            last = { ...parsed, runId: opts.runId ?? parsed.runId };
          }
        }
      };

      child.stdout?.on('data', onData);
      child.stderr?.on('data', onData);
      token?.onCancellationRequested(() => child.kill());

      child.on('error', reject);
      child.on('close', (code) => {
        if (last) {
          resolve(last);
          return;
        }
        resolve({
          runId: opts.runId ?? '',
          status: code === 0 ? 'completed' : 'failed',
          rowsProcessed: 0,
          rowsWritten: 0,
          rowsFailed: 0,
          rowsDropped: 0,
          rowsRouted: 0,
          durationMs: 0,
          error: code === 0 ? undefined : `engine exited with code ${code}`,
        });
      });
    });
  }

  // ─────────────────── API-server mode (optional) ───────────────────

  health(): Promise<HealthResponse> {
    return this.get<HealthResponse>('/v1/health');
  }

  capabilities(): Promise<Capabilities> {
    return this.get<Capabilities>('/v1/capabilities');
  }

  async listPipelines(): Promise<PipelineInfo[]> {
    const r = await this.get<{ pipelines: PipelineInfo[] }>('/v1/pipelines');
    return r.pipelines ?? [];
  }

  pipelineStatus(ref: string): Promise<PipelineStatusResponse> {
    return this.get<PipelineStatusResponse>(
      `/v1/pipelines/${encodeURIComponent(ref)}/status`,
    );
  }

  private get<T>(p: string): Promise<T> {
    return this.request<T>('GET', p);
  }

  private request<T>(method: string, p: string, body?: unknown): Promise<T> {
    const addr = this.engineAddr;
    if (!addr) {
      return Promise.reject(
        new Error('flogo3.datapipeline.engineAddr is not set'),
      );
    }
    const { host, port } = splitAddr(addr);
    const payload = body ? Buffer.from(JSON.stringify(body)) : undefined;

    return new Promise<T>((resolve, reject) => {
      const req = http.request(
        {
          host,
          port,
          path: p,
          method,
          headers: payload
            ? {
                'content-type': 'application/json',
                'content-length': String(payload.length),
              }
            : {},
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on('data', (c: Buffer) => chunks.push(c));
          res.on('end', () => {
            const text = Buffer.concat(chunks).toString();
            if ((res.statusCode ?? 500) >= 400) {
              reject(new Error(`HTTP ${res.statusCode}: ${text}`));
              return;
            }
            try {
              resolve(text ? (JSON.parse(text) as T) : ({} as T));
            } catch (e) {
              reject(e);
            }
          });
        },
      );
      req.on('error', reject);
      if (payload) {
        req.write(payload);
      }
      req.end();
    });
  }
}

/**
 * Parse the engine's `-v` summary line, e.g.
 *   `status=completed processed=2000000 written=1876300 failed=0 dropped=119200 routed=4500 duration=9900ms`
 */
export function parseSummaryLine(line: string): RunSummary | undefined {
  if (!/status=/.test(line) && !/processed=/.test(line)) {
    return undefined;
  }
  const num = (k: string): number => {
    const m = line.match(new RegExp(`${k}=(\\d+)`));
    return m ? parseInt(m[1], 10) : 0;
  };
  const statusMatch = line.match(/status=([a-zA-Z]+)/);
  const durationMatch = line.match(/duration=(\d+)ms/);
  return {
    runId: '',
    status: (statusMatch?.[1] as PipelineStatus) ?? 'completed',
    rowsProcessed: num('processed'),
    rowsWritten: num('written'),
    rowsFailed: num('failed'),
    rowsDropped: num('dropped'),
    rowsRouted: num('routed'),
    durationMs: durationMatch ? parseInt(durationMatch[1], 10) : 0,
  };
}

function splitAddr(addr: string): { host: string; port: number } {
  const bare = addr.replace(/^https?:\/\//, '');
  const [host, port] = bare.split(':');
  return { host: host || '127.0.0.1', port: parseInt(port || '8888', 10) };
}
