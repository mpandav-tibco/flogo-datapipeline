// Copyright (c) 2024-2026. Cloud Software Group, Inc. All Rights Reserved. Confidential & Proprietary
/**
 * Tree data provider for the "Data Pipelines" view. It lists every
 * `*.datapipeline` file in the workspace with a `source → sink` description and
 * a status icon reflecting the most recent run. Mirrors the structure of the
 * existing `AppExplorerProvider`.
 */
import * as vscode from 'vscode';
import { RunSummary } from './types';
import { describePipeline, getPipelineDefinition, parseApp } from './model';
import { DataPipelineEditorProvider } from './datapipeline-editor-provider';

export class PipelineItem extends vscode.TreeItem {
  constructor(
    public readonly uri: vscode.Uri,
    label: string,
    description: string,
    public readonly lastRun?: RunSummary,
  ) {
    super(label, vscode.TreeItemCollapsibleState.None);
    this.description = description;
    this.resourceUri = uri;
    this.tooltip = uri.fsPath;
    this.contextValue = 'datapipeline';
    this.iconPath = new vscode.ThemeIcon(iconFor(lastRun), colorFor(lastRun));
    this.command = {
      command: 'vscode.openWith',
      title: 'Open Pipeline',
      arguments: [uri, DataPipelineEditorProvider.viewType],
    };
  }
}

export class PipelinesTreeProvider
  implements vscode.TreeDataProvider<PipelineItem>
{
  static readonly viewId = 'flogo3.datapipeline.pipelines';

  private readonly emitter = new vscode.EventEmitter<
    PipelineItem | undefined | void
  >();
  readonly onDidChangeTreeData = this.emitter.event;

  private readonly runs = new Map<string, RunSummary>();

  refresh(): void {
    this.emitter.fire();
  }

  setLastRun(fsPath: string, run: RunSummary): void {
    this.runs.set(fsPath, run);
    this.refresh();
  }

  getTreeItem(element: PipelineItem): vscode.TreeItem {
    return element;
  }

  async getChildren(): Promise<PipelineItem[]> {
    const files = await vscode.workspace.findFiles(
      '**/*.datapipeline',
      '**/node_modules/**',
    );
    const items = await Promise.all(
      files.map(async (uri) => {
        let description = '';
        try {
          const doc = await vscode.workspace.openTextDocument(uri);
          const def = getPipelineDefinition(parseApp(doc.getText()));
          const d = describePipeline(def);
          description = `${d.source} → ${d.sink}`;
        } catch {
          /* leave description empty on parse failure */
        }
        const label = (uri.path.split('/').pop() ?? uri.path).replace(
          /\.datapipeline$/,
          '',
        );
        return new PipelineItem(uri, label, description, this.runs.get(uri.fsPath));
      }),
    );
    return items.sort((a, b) =>
      String(a.label).localeCompare(String(b.label)),
    );
  }
}

function iconFor(run?: RunSummary): string {
  switch (run?.status) {
    case 'running':
      return 'sync~spin';
    case 'completed':
      return 'pass-filled';
    case 'partial':
      return 'warning';
    case 'failed':
      return 'error';
    default:
      return 'circle-large-outline';
  }
}

function colorFor(run?: RunSummary): vscode.ThemeColor | undefined {
  switch (run?.status) {
    case 'completed':
      return new vscode.ThemeColor('testing.iconPassed');
    case 'partial':
      return new vscode.ThemeColor('testing.iconQueued');
    case 'failed':
      return new vscode.ThemeColor('testing.iconFailed');
    default:
      return undefined;
  }
}
