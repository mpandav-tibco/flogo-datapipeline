// Copyright (c) 2024-2026. Cloud Software Group, Inc. All Rights Reserved. Confidential & Proprietary
/**
 * Data Pipeline feature entry point. Wires the custom editor, the Pipelines
 * tree view, diagnostics, the status bar, an output channel, and the feature's
 * commands — following the same shape as `registerFlowEditorUIFeatures`.
 *
 * Call this once from `extension.ts::activate()` (ideally behind the
 * `FLOGO_FEATURE_DATAPIPELINE` feature flag).
 */
import * as vscode from 'vscode';
import { DataPipelineEngineClient } from './engine-client';
import { DataPipelineDiagnostics, isDataPipeline } from './diagnostics';
import { PipelinesTreeProvider } from './pipelines-tree-provider';
import { DataPipelineStatusBar } from './status-bar';
import { DataPipelineEditorProvider } from './datapipeline-editor-provider';
import { buildGraph, getPipelineDefinition, parseApp } from './model';
import { RunSummary } from './types';

export const DataPipelineCommands = {
  Run: 'flogo3.datapipeline.run',
  Preview: 'flogo3.datapipeline.preview',
  Refresh: 'flogo3.datapipeline.refresh',
  ShowOutput: 'flogo3.datapipeline.showOutput',
  NewFromTemplate: 'flogo3.datapipeline.newFromTemplate',
} as const;

interface FeatureCtx {
  engine: DataPipelineEngineClient;
  output: vscode.OutputChannel;
  tree: PipelinesTreeProvider;
  statusBar: DataPipelineStatusBar;
}

export function registerDataPipelineFeature(
  context: vscode.ExtensionContext,
): vscode.Disposable {
  const disposables: vscode.Disposable[] = [];

  const output = vscode.window.createOutputChannel('Flogo Data Pipeline');
  const engine = new DataPipelineEngineClient(output);
  const diagnostics = new DataPipelineDiagnostics();
  const tree = new PipelinesTreeProvider();
  const statusBar = new DataPipelineStatusBar();
  const ctx: FeatureCtx = { engine, output, tree, statusBar };

  disposables.push(output, diagnostics, statusBar);

  // Custom editor for *.datapipeline.
  disposables.push(DataPipelineEditorProvider.register(context, diagnostics));

  // Pipelines tree view (contributed under the shared flogo3-sidebar-view).
  disposables.push(
    vscode.window.createTreeView(PipelinesTreeProvider.viewId, {
      treeDataProvider: tree,
      showCollapseAll: false,
    }),
  );

  // Diagnostics lifecycle.
  vscode.workspace.textDocuments.forEach((d) => diagnostics.validate(d));
  disposables.push(
    vscode.workspace.onDidOpenTextDocument((d) => diagnostics.validate(d)),
    vscode.workspace.onDidSaveTextDocument((d) => diagnostics.validate(d)),
    vscode.workspace.onDidCloseTextDocument((d) => {
      if (isDataPipeline(d)) {
        diagnostics.clear(d.uri);
      }
    }),
  );

  // Keep the tree in sync with files on disk.
  const watcher = vscode.workspace.createFileSystemWatcher('**/*.datapipeline');
  watcher.onDidCreate(() => tree.refresh());
  watcher.onDidDelete(() => tree.refresh());
  watcher.onDidChange(() => tree.refresh());
  disposables.push(watcher);

  // Commands.
  disposables.push(
    vscode.commands.registerCommand(DataPipelineCommands.Run, (uri?: vscode.Uri) =>
      runPipeline(uri, ctx, false),
    ),
    vscode.commands.registerCommand(
      DataPipelineCommands.Preview,
      (uri?: vscode.Uri) => runPipeline(uri, ctx, true),
    ),
    vscode.commands.registerCommand(DataPipelineCommands.Refresh, () =>
      tree.refresh(),
    ),
    vscode.commands.registerCommand(DataPipelineCommands.ShowOutput, () =>
      output.show(true),
    ),
    vscode.commands.registerCommand(DataPipelineCommands.NewFromTemplate, () =>
      newFromTemplate(),
    ),
  );

  return vscode.Disposable.from(...disposables);
}

// ─────────────────────────── commands ───────────────────────────

async function runPipeline(
  arg: vscode.Uri | undefined,
  ctx: FeatureCtx,
  preview: boolean,
): Promise<void> {
  const uri = await resolvePipelineUri(arg);
  if (!uri) {
    return;
  }
  const name = baseName(uri);
  const { engine, output, tree, statusBar } = ctx;
  output.show(true);

  if (preview) {
    const doc = await vscode.workspace.openTextDocument(uri);
    const def = getPipelineDefinition(parseApp(doc.getText()));
    output.appendLine(`\n─ dry-run preview · ${name} ─`);
    if (!def) {
      output.appendLine('  ✗ no "pipeline:*" resource found in this file');
      return;
    }
    const graph = buildGraph(def, name);
    output.appendLine(
      `  plan (${graph.mode}): ${graph.nodes
        .map((n) => n.type)
        .join(' → ')}${graph.hasDlq ? '   ⎇ DLQ' : ''}`,
    );
    output.appendLine(
      '  validation is surfaced in the Problems panel · a sampled row preview requires engine --preview support',
    );
    return;
  }

  await vscode.window.withProgress(
    {
      location: vscode.ProgressLocation.Notification,
      title: `Running ${name}…`,
      cancellable: true,
    },
    async (_progress, token) => {
      statusBar.setRunning(name);
      output.appendLine(`\n─ run · ${name} · ${new Date().toLocaleTimeString()} ─`);
      try {
        const summary = await engine.runFile(
          uri.fsPath,
          { verbose: true },
          (line) => output.appendLine(line),
          token,
        );
        statusBar.setResult(name, summary);
        tree.setLastRun(uri.fsPath, summary);
        renderFunnel(output, summary);
        const action = await vscode.window.showInformationMessage(
          `${name}: ${summary.status} — ${summary.rowsWritten.toLocaleString()} written`,
          'Show Output',
        );
        if (action === 'Show Output') {
          output.show(true);
        }
      } catch (err) {
        statusBar.setIdle(name);
        const message = err instanceof Error ? err.message : String(err);
        output.appendLine(`  ✗ ${message}`);
        void vscode.window.showErrorMessage(`Failed to run ${name}: ${message}`);
      }
    },
  );
}

async function newFromTemplate(): Promise<void> {
  const name = await vscode.window.showInputBox({
    prompt: 'Pipeline name',
    value: 'my-pipeline',
    validateInput: (v) =>
      /^[a-zA-Z0-9._-]+$/.test(v)
        ? undefined
        : 'Use letters, digits, dot, underscore or hyphen only',
  });
  if (!name) {
    return;
  }
  const folders = vscode.workspace.workspaceFolders;
  if (!folders?.length) {
    void vscode.window.showErrorMessage('Open a folder to create a pipeline.');
    return;
  }
  const target = vscode.Uri.joinPath(folders[0].uri, `${name}.datapipeline`);
  await vscode.workspace.fs.writeFile(
    target,
    Buffer.from(starterTemplate(name), 'utf8'),
  );
  await vscode.commands.executeCommand(
    'vscode.openWith',
    target,
    DataPipelineEditorProvider.viewType,
  );
}

// ─────────────────────────── helpers ───────────────────────────

async function resolvePipelineUri(
  arg?: vscode.Uri,
): Promise<vscode.Uri | undefined> {
  if (arg instanceof vscode.Uri) {
    return arg;
  }
  const active = vscode.window.activeTextEditor?.document;
  if (active && active.fileName.endsWith('.datapipeline')) {
    return active.uri;
  }
  const files = await vscode.workspace.findFiles(
    '**/*.datapipeline',
    '**/node_modules/**',
  );
  if (files.length === 0) {
    void vscode.window.showWarningMessage(
      'No .datapipeline files found in the workspace.',
    );
    return undefined;
  }
  if (files.length === 1) {
    return files[0];
  }
  const pick = await vscode.window.showQuickPick(
    files.map((f) => ({ label: baseName(f), uri: f })),
    { placeHolder: 'Select a pipeline to run' },
  );
  return pick?.uri;
}

function renderFunnel(output: vscode.OutputChannel, s: RunSummary): void {
  const f = (n: number) => n.toLocaleString('en-US');
  output.appendLine(
    `  ✓ ${s.status} · processed=${f(s.rowsProcessed)} written=${f(
      s.rowsWritten,
    )} dropped=${f(s.rowsDropped)} routed=${f(s.rowsRouted)} failed=${f(
      s.rowsFailed,
    )} · ${s.durationMs}ms`,
  );
  if (s.qualityReport) {
    output.appendLine(
      `  DQ: ${s.qualityReport.passed} passed / ${s.qualityReport.failed} failed`,
    );
  }
}

function baseName(uri: vscode.Uri): string {
  return (uri.path.split('/').pop() ?? uri.path).replace(/\.datapipeline$/, '');
}

function starterTemplate(name: string): string {
  const app = {
    name,
    type: 'flogo:app',
    version: '1.0.0',
    appModel: '1.1.1',
    resources: [
      {
        id: `pipeline:${name}`,
        data: {
          name,
          source: {
            type: 'csv',
            path: './input.csv',
            hasHeader: true,
            columns: [
              { name: 'id', type: 'string' },
              { name: 'amount', type: 'float' },
              { name: 'status', type: 'string' },
            ],
          },
          transforms: [
            { type: 'filter', expr: "status != 'CANCELLED'" },
            { type: 'map', fields: { net: '=$amount' } },
          ],
          sink: { type: 'null' },
        },
      },
    ],
  };
  return JSON.stringify(app, null, 2) + '\n';
}
