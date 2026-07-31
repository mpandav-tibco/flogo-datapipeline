import * as vscode from 'vscode';

// A small rolling history of pipeline runs, persisted in workspace state so the
// Pipelines sidebar can show recent runs per pipeline (the wireframe's RUN
// HISTORY section) without a server round-trip.

export interface RunRecord {
  at: number; // epoch ms when the run finished
  status: string; // completed | partial | failed | cancelled
  rowsWritten: number;
  rowsFailed: number;
  durationMs: number;
  runId: string;
}

const KEY = 'flogoDataPipeline.runHistory';
const MAX_PER_PIPELINE = 15;

export class RunHistoryStore {
  private readonly emitter = new vscode.EventEmitter<void>();
  readonly onDidChange = this.emitter.event;

  constructor(private readonly memento: vscode.Memento) {}

  private all(): Record<string, RunRecord[]> {
    return this.memento.get<Record<string, RunRecord[]>>(KEY, {});
  }

  list(pipelinePath: string): RunRecord[] {
    return this.all()[pipelinePath] || [];
  }

  async record(pipelinePath: string, rec: RunRecord): Promise<void> {
    const all = this.all();
    all[pipelinePath] = [rec, ...(all[pipelinePath] || [])].slice(0, MAX_PER_PIPELINE);
    await this.memento.update(KEY, all);
    this.emitter.fire();
  }

  async clear(pipelinePath: string): Promise<void> {
    const all = this.all();
    delete all[pipelinePath];
    await this.memento.update(KEY, all);
    this.emitter.fire();
  }

  dispose(): void {
    this.emitter.dispose();
  }
}
