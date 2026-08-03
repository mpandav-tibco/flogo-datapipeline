import * as vscode from 'vscode';
import * as path from 'path';
import { extractPipelineData } from './model/pipeline';
import { RunHistoryStore, RunRecord } from './runHistory';

type NodeKind = 'pipeline' | 'running' | 'message' | 'run';

export class PipelineTreeItem extends vscode.TreeItem {
  constructor(
    label: string,
    public readonly kind: NodeKind,
    collapsible: vscode.TreeItemCollapsibleState,
    public readonly pipelineUri?: vscode.Uri,
  ) {
    super(label, collapsible);
  }
}

export class SidebarProvider implements vscode.TreeDataProvider<PipelineTreeItem>, vscode.Disposable {
  private readonly emitter = new vscode.EventEmitter<PipelineTreeItem | undefined | void>();
  readonly onDidChangeTreeData = this.emitter.event;

  private running = false;
  private runningPort: number | null = null;
  private runningName = '';
  private activePipeline: vscode.Uri | null = null;
  private buildTarget: string | null = null;
  private readonly watcher: vscode.FileSystemWatcher;

  constructor(private readonly runHistory: RunHistoryStore) {
    this.watcher = vscode.workspace.createFileSystemWatcher('**/*.datapipeline');
    this.watcher.onDidCreate(() => this.refresh());
    this.watcher.onDidDelete(() => this.refresh());
    runHistory.onDidChange(() => this.refresh());
  }

  dispose(): void {
    this.emitter.dispose();
    this.watcher.dispose();
  }

  refresh(): void {
    this.emitter.fire();
  }

  setRunning(port: number, name: string): void {
    this.running = true;
    this.runningPort = port;
    this.runningName = name;
    this.refresh();
  }

  setStopped(): void {
    this.running = false;
    this.runningPort = null;
    this.runningName = '';
    this.refresh();
  }

  setActivePipeline(uri: vscode.Uri | null): void {
    this.activePipeline = uri;
  }

  setBuildTarget(target: string | null): void {
    this.buildTarget = target;
  }

  getTreeItem(element: PipelineTreeItem): vscode.TreeItem {
    return element;
  }

  async getChildren(element?: PipelineTreeItem): Promise<PipelineTreeItem[]> {
    if (element) {
      // Expanding a pipeline reveals its recent runs (the wireframe's RUN HISTORY).
      if (element.kind === 'pipeline' && element.pipelineUri) {
        return this.runHistory.list(element.pipelineUri.fsPath).map((r) => runItem(r));
      }
      return [];
    }

    const items: PipelineTreeItem[] = [];

    if (this.running && this.runningPort) {
      const run = new PipelineTreeItem(
        `${this.runningName} · running :${this.runningPort}`,
        'running',
        vscode.TreeItemCollapsibleState.None,
      );
      run.iconPath = new vscode.ThemeIcon('zap', new vscode.ThemeColor('charts.green'));
      run.contextValue = 'localPipeline';
      run.tooltip = `Engine on port ${this.runningPort}`;
      items.push(run);
    }

    const files = await vscode.workspace.findFiles('**/*.datapipeline', '**/node_modules/**', 200);
    // Exclude the transient connection-resolved run files (.<name>.resolved.datapipeline).
    const visible = files.filter((u) => !path.basename(u.fsPath).endsWith('.resolved.datapipeline'));
    visible.sort((a, b) => a.fsPath.localeCompare(b.fsPath));

    if (!visible.length) {
      const msg = new PipelineTreeItem('No .datapipeline files found', 'message', vscode.TreeItemCollapsibleState.None);
      msg.description = 'Create one from a template';
      items.push(msg);
      return items;
    }

    for (const uri of visible) {
      const name = path.basename(uri.fsPath);
      const runs = this.runHistory.list(uri.fsPath).length;
      const item = new PipelineTreeItem(
        name,
        'pipeline',
        runs ? vscode.TreeItemCollapsibleState.Collapsed : vscode.TreeItemCollapsibleState.None,
        uri,
      );
      item.resourceUri = uri;
      // Summarise the pipeline as source → sink (like the wireframe's list),
      // falling back to the folder path when the file can't be read/parsed.
      let summary = vscode.workspace.asRelativePath(path.dirname(uri.fsPath));
      try {
        const raw = await vscode.workspace.fs.readFile(uri);
        const pd: any = extractPipelineData(JSON.parse(Buffer.from(raw).toString('utf8')));
        const st = pd && pd.source && pd.source.type;
        const sk = pd && pd.sink && pd.sink.type;
        if (st || sk) summary = `${st || '?'} → ${sk || '?'}`;
      } catch { /* keep the folder path */ }
      item.description = summary;
      item.tooltip = `${name}\n${vscode.workspace.asRelativePath(uri)}`;
      item.iconPath = new vscode.ThemeIcon('circuit-board');
      item.contextValue = 'pipelineAction';
      item.command = {
        command: 'vscode.openWith',
        title: 'Open in Data Pipeline Studio',
        arguments: [uri, 'flogoDataPipelineStudio.pipelineEditor'],
      };
      items.push(item);
    }
    return items;
  }
}

// Build a tree item for a single past run (child of its pipeline).
function runItem(r: RunRecord): PipelineTreeItem {
  const item = new PipelineTreeItem(timeAgo(r.at), 'run', vscode.TreeItemCollapsibleState.None);
  item.description = `${fmtInt(r.rowsWritten)} written · ${(r.durationMs / 1000).toFixed(1)}s` +
    (r.rowsFailed ? ` · ${fmtInt(r.rowsFailed)} failed` : '');
  const ok = r.status === 'completed';
  item.iconPath = new vscode.ThemeIcon(
    ok ? 'pass' : r.status === 'partial' ? 'warning' : r.status === 'cancelled' ? 'circle-slash' : 'error',
    new vscode.ThemeColor(ok ? 'charts.green' : r.status === 'failed' ? 'charts.red' : 'charts.yellow'),
  );
  item.tooltip = `${r.status} · ${new Date(r.at).toLocaleString()}\n${r.rowsWritten} written · ${r.rowsFailed} failed · ${r.durationMs}ms`;
  return item;
}

function timeAgo(at: number): string {
  const s = Math.max(0, Math.round((Date.now() - at) / 1000));
  if (s < 60) return `${s}s ago`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.round(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.round(h / 24)}d ago`;
}

function fmtInt(n: number): string {
  return n >= 1000 ? `${(n / 1000).toFixed(n >= 100000 ? 0 : 1).replace(/\.0$/, '')}k` : String(n);
}
