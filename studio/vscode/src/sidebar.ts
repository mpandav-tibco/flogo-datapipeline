import * as vscode from 'vscode';
import * as path from 'path';

type NodeKind = 'pipeline' | 'running' | 'message';

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

  constructor() {
    this.watcher = vscode.workspace.createFileSystemWatcher('**/*.datapipeline');
    this.watcher.onDidCreate(() => this.refresh());
    this.watcher.onDidDelete(() => this.refresh());
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
    if (element) return [];

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
    files.sort((a, b) => a.fsPath.localeCompare(b.fsPath));

    if (!files.length) {
      const msg = new PipelineTreeItem('No .datapipeline files found', 'message', vscode.TreeItemCollapsibleState.None);
      msg.description = 'Create one from a template';
      items.push(msg);
      return items;
    }

    for (const uri of files) {
      const name = path.basename(uri.fsPath);
      const item = new PipelineTreeItem(name, 'pipeline', vscode.TreeItemCollapsibleState.None, uri);
      item.resourceUri = uri;
      item.description = vscode.workspace.asRelativePath(path.dirname(uri.fsPath));
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
