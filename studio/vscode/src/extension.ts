import * as vscode from 'vscode';
import * as path from 'path';

import { BinaryManager } from './binaryManager';
import { PipelineEditorProvider, EngineHooks } from './editor/pipelineEditorProvider';
import { DataPreviewView } from './panel/dataPreviewView';
import { SidebarProvider } from './sidebar';
import { TEMPLATES } from './templates';
import { ConnectionStore } from './connections/store';
import { ConnectionsSidebarProvider, ConnectionTreeItem } from './connections/sidebar';
import { ConnectionEditorProvider } from './connections/editor';
import {
  createConnectionFlow,
  editConnectionFlow,
  deleteConnectionFlow,
  testConnectionFlow,
  revealConnectionsFile,
} from './connections/commands';

let manager: BinaryManager;
let sidebar: SidebarProvider;
let connectionStore: ConnectionStore;
let connectionsSidebar: ConnectionsSidebarProvider;
let statusBar: vscode.StatusBarItem;
let editor: { provider: PipelineEditorProvider; disposable: vscode.Disposable };
let extContext: vscode.ExtensionContext;

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  extContext = context;
  manager = new BinaryManager(context.extensionPath);
  sidebar = new SidebarProvider();
  connectionStore = new ConnectionStore(context.secrets);
  context.subscriptions.push(connectionStore);
  connectionsSidebar = new ConnectionsSidebarProvider(connectionStore);
  context.subscriptions.push(connectionsSidebar);
  context.subscriptions.push(ConnectionEditorProvider.register(context, connectionStore));

  statusBar = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100);
  statusBar.command = 'flogoDataPipelineStudio.focusSidebar';
  statusStopped();
  statusBar.show();
  context.subscriptions.push(statusBar);

  const hooks: EngineHooks = {
    onStarting: () => statusStarting(),
    onRunning: (port, name) => {
      sidebar.setRunning(port, name);
      statusRunning(port);
    },
    onStopped: () => {
      sidebar.setStopped();
      statusStopped();
    },
    onActivePipeline: (uri) => syncActivePipeline(uri),
  };

  context.subscriptions.push(manager.channel);

  // Native panel view for dry-run data preview (docks next to Problems/Output/Terminal).
  const dataPreview = new DataPreviewView(context.extensionUri);
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider(DataPreviewView.viewType, dataPreview),
  );

  editor = PipelineEditorProvider.register(context, manager, hooks, dataPreview, connectionStore);
  context.subscriptions.push(editor.disposable);

  manager.onCrash(() => {
    hooks.onStopped?.();
    editor.provider.broadcastStopped();
  });

  context.subscriptions.push(
    vscode.commands.registerCommand('flogoDataPipelineStudio.runPipeline', (uri) => runPipelineFile(uri)),
    vscode.commands.registerCommand('flogoDataPipelineStudio.stopPipeline', () => stopPipeline()),
    vscode.commands.registerCommand('flogoDataPipelineStudio.newPipeline', (uri) => newPipelineFile(uri)),
    vscode.commands.registerCommand('flogoDataPipelineStudio.newFromTemplate', (uri) => newFromTemplate(uri)),
    vscode.commands.registerCommand('flogoDataPipelineStudio.build', (uri) => build(uri)),
    vscode.commands.registerCommand('flogoDataPipelineStudio.runWithOptions', (arg) => runWithOptions(arg)),
    vscode.commands.registerCommand('flogoDataPipelineStudio.configure', (arg) => configure(arg)),
    vscode.commands.registerCommand('flogoDataPipelineStudio.openFleet', () =>
      vscode.window.showInformationMessage('Fleet Dashboard opens inside a running pipeline editor.'),
    ),
    vscode.commands.registerCommand('flogoDataPipelineStudio.focusSidebar', () =>
      vscode.commands.executeCommand('flogoDataPipelineStudio.pipelinesExplorer.focus'),
    ),
  );

  context.subscriptions.push(sidebar);
  context.subscriptions.push(
    vscode.window.registerTreeDataProvider('flogoDataPipelineStudio.pipelinesExplorer', sidebar),
    vscode.window.registerTreeDataProvider('flogoDataPipelineStudio.pipelinesExplorerMirror', sidebar),
  );

  context.subscriptions.push(
    vscode.window.registerTreeDataProvider('flogoDataPipelineStudio.connectionsExplorer', connectionsSidebar),
    vscode.window.registerTreeDataProvider('flogoDataPipelineStudio.connectionsExplorerMirror', connectionsSidebar),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('flogoDataPipelineStudio.newConnection', async () => {
      const record = await createConnectionFlow(connectionStore);
      if (record) {
        const p = connectionStore.fsPathFor(record.id);
        if (p) {
          await vscode.commands.executeCommand(
            'vscode.openWith',
            vscode.Uri.file(p),
            ConnectionEditorProvider.viewType,
          );
        }
      }
    }),
    vscode.commands.registerCommand('flogoDataPipelineStudio.refreshConnections', () => connectionsSidebar.refresh()),
    vscode.commands.registerCommand('flogoDataPipelineStudio.revealConnectionsFile', () => revealConnectionsFile()),
    vscode.commands.registerCommand('flogoDataPipelineStudio.editConnection', (item: ConnectionTreeItem) => {
      if (item?.connId) editConnectionFlow(connectionStore, item.connId);
    }),
    vscode.commands.registerCommand('flogoDataPipelineStudio.testConnection', (item: ConnectionTreeItem) => {
      if (item?.connId) testConnectionFlow(connectionStore, item.connId);
    }),
    vscode.commands.registerCommand('flogoDataPipelineStudio.deleteConnection', (item: ConnectionTreeItem) => {
      if (item?.connId) deleteConnectionFlow(connectionStore, item.connId);
    }),
    // Invoked from the webview inspector (by connection id).
    vscode.commands.registerCommand('flogoDataPipelineStudio.editConnectionById', (id: string) => {
      if (id) editConnectionFlow(connectionStore, id);
    }),
    vscode.commands.registerCommand('flogoDataPipelineStudio.testConnectionById', (id: string) => {
      if (id) testConnectionFlow(connectionStore, id);
    }),
    vscode.commands.registerCommand('flogoDataPipelineStudio.deleteConnectionById', (id: string) => {
      if (id) deleteConnectionFlow(connectionStore, id);
    }),
    vscode.commands.registerCommand('flogoDataPipelineStudio.initProject', () => initProject()),
  );

  context.subscriptions.push(
    vscode.window.onDidChangeActiveTextEditor((ed) => {
      const u = ed?.document?.uri;
      if (u && u.fsPath.endsWith('.datapipeline')) syncActivePipeline(u);
    }),
  );
}

export function deactivate(): void {
  manager?.stop();
}

// ── Command implementations ────────────────────────────────────────────────

async function runPipelineFile(uri: vscode.Uri | undefined): Promise<void> {
  const target = resolvePipelineUri(uri);
  if (!target) {
    vscode.window.showWarningMessage('Select a .datapipeline file first.');
    return;
  }
  await vscode.commands.executeCommand('vscode.openWith', target, PipelineEditorProvider.viewType);
}

function resolvePipelineUri(arg: any): vscode.Uri | null {
  const isPipe = (p: unknown) => typeof p === 'string' && p.endsWith('.datapipeline');
  if (arg && isPipe(arg.fsPath)) return arg as vscode.Uri;
  const treeUri = arg && (arg.pipelineUri || arg.resourceUri);
  if (treeUri && isPipe(treeUri.fsPath)) return treeUri;
  const active = editor?.provider?.activePipelineUri;
  if (active) return active;
  const ed = vscode.window.activeTextEditor?.document?.uri;
  if (ed && isPipe(ed.fsPath)) return ed;
  return null;
}

function buildConfigKey(uri: vscode.Uri): string {
  return 'flogoDataPipelineStudio.buildConfig:' + uri.fsPath;
}

function getBuildConfig(uri: vscode.Uri): { target: string; imageTag?: string } | null {
  return extContext.workspaceState.get(buildConfigKey(uri)) ?? null;
}

async function setBuildConfig(uri: vscode.Uri, cfg: { target: string; imageTag?: string }): Promise<void> {
  await extContext.workspaceState.update(buildConfigKey(uri), cfg);
  syncActivePipeline(uri);
}

async function promptBuildConfig(uri: vscode.Uri): Promise<{ target: string } | null> {
  const pick = await vscode.window.showQuickPick(
    [
      { label: '$(package) Executable binary', detail: 'Run builds a native binary from source and launches it', target: 'binary' },
      { label: '$(vm) Container image (Docker)', detail: 'Run builds a Docker image and runs the container', target: 'docker' },
    ],
    { title: `Configure Run — ${path.basename(uri.fsPath)}`, placeHolder: 'How should Run build & launch this pipeline?' },
  );
  if (!pick) return null;
  const cfg = { target: pick.target };
  await setBuildConfig(uri, cfg);
  return cfg;
}

function syncActivePipeline(uri: vscode.Uri | null): void {
  sidebar.setActivePipeline(uri);
  const cfg = uri ? getBuildConfig(uri) : null;
  sidebar.setBuildTarget(cfg ? cfg.target : null);
}

function stopPipeline(): void {
  manager.stop();
  sidebar.setStopped();
  statusStopped();
  editor.provider.broadcastStopped();
}

async function defaultDirUri(uri?: vscode.Uri): Promise<vscode.Uri | undefined> {
  if (uri && uri.scheme === 'file') {
    try {
      const stat = await vscode.workspace.fs.stat(uri);
      if (stat.type & vscode.FileType.Directory) return uri;
      return vscode.Uri.joinPath(uri, '..');
    } catch {
      /* fall through */
    }
  }
  // Default new pipelines into a `pipelines/` folder to keep the project tidy
  // (pipelines/, connections/, data/). Created on demand.
  const root = vscode.workspace.workspaceFolders?.[0]?.uri;
  if (!root) return undefined;
  const pipelinesDir = vscode.Uri.joinPath(root, 'pipelines');
  try {
    await vscode.workspace.fs.createDirectory(pipelinesDir);
    return pipelinesDir;
  } catch {
    return root;
  }
}

// Scaffold a tidy Data Pipeline project layout in the first workspace folder:
//   pipelines/     — *.datapipeline flows
//   connections/   — *.fgconn reusable connections (secrets in Secret Storage)
//   data/          — local input/output sample data
// plus a README describing the layout. Idempotent.
async function initProject(): Promise<void> {
  const root = vscode.workspace.workspaceFolders?.[0]?.uri;
  if (!root) {
    vscode.window.showWarningMessage('Open a folder first to initialize a Data Pipeline project.');
    return;
  }
  for (const f of ['pipelines', 'connections', 'data']) {
    try {
      await vscode.workspace.fs.createDirectory(vscode.Uri.joinPath(root, f));
    } catch {
      /* exists */
    }
  }
  const readme = vscode.Uri.joinPath(root, 'README.md');
  try {
    await vscode.workspace.fs.stat(readme);
  } catch {
    const body = [
      '# Data Pipeline Project',
      '',
      'Structure:',
      '',
      '- `pipelines/` — pipeline flows (`*.datapipeline`), edited in Data Pipeline Studio.',
      '- `connections/` — reusable connections (`*.fgconn`). Secrets live in VS Code Secret Storage, never in these files.',
      '- `data/` — local input/output sample data.',
      '',
      'Create a pipeline: **Data Pipeline: New Pipeline from Template…**',
      'Create a connection: the **＋** button on the Connections view.',
      '',
    ].join('\n');
    await vscode.workspace.fs.writeFile(readme, Buffer.from(body, 'utf8'));
  }
  sidebar.refresh();
  connectionsSidebar.refresh();
  const choice = await vscode.window.showInformationMessage(
    'Data Pipeline project initialized: pipelines/, connections/, data/.',
    'New Pipeline',
  );
  if (choice === 'New Pipeline') {
    await vscode.commands.executeCommand('flogoDataPipelineStudio.newFromTemplate');
  }
}

async function newPipelineFile(uri?: vscode.Uri): Promise<void> {
  const dir = await defaultDirUri(uri);
  const saveUri = await vscode.window.showSaveDialog({
    defaultUri: dir ? vscode.Uri.joinPath(dir, 'my-pipeline.datapipeline') : undefined,
    filters: { 'Flogo Data Pipeline': ['datapipeline'] },
    title: 'Create new Flogo Data Pipeline file',
  });
  if (!saveUri) return;

  const name = path.basename(saveUri.fsPath, '.datapipeline');
  const template = TEMPLATES[0].make(name);
  await vscode.workspace.fs.writeFile(saveUri, Buffer.from(JSON.stringify(template, null, 2), 'utf8'));
  await vscode.commands.executeCommand('vscode.openWith', saveUri, PipelineEditorProvider.viewType);
}

async function newFromTemplate(uri?: vscode.Uri): Promise<void> {
  const pick = await vscode.window.showQuickPick(
    TEMPLATES.map((t) => ({ label: t.label, description: t.id, detail: t.detail, tplId: t.id })),
    { title: 'New Flogo Data Pipeline from Template', placeHolder: 'Choose a starting point', matchOnDetail: true },
  );
  if (!pick) return;
  const tpl = TEMPLATES.find((t) => t.id === pick.tplId);
  if (!tpl) return;

  const dir = await defaultDirUri(uri);
  const saveUri = await vscode.window.showSaveDialog({
    defaultUri: dir ? vscode.Uri.joinPath(dir, tpl.id + '.datapipeline') : undefined,
    filters: { 'Flogo Data Pipeline': ['datapipeline'] },
    title: `New pipeline from the "${tpl.id}" template`,
  });
  if (!saveUri) return;

  const name = path.basename(saveUri.fsPath, '.datapipeline');
  let appObj: Record<string, unknown>;
  try {
    appObj = tpl.make(name);
  } catch (err: any) {
    vscode.window.showErrorMessage(`Template failed: ${err?.message ?? String(err)}`);
    return;
  }
  await vscode.workspace.fs.writeFile(saveUri, Buffer.from(JSON.stringify(appObj, null, 2), 'utf8'));
  await vscode.commands.executeCommand('vscode.openWith', saveUri, PipelineEditorProvider.viewType);
}

async function build(uri?: vscode.Uri): Promise<void> {
  const target = resolvePipelineUri(uri);
  if (!target) {
    vscode.window.showWarningMessage('Select a .datapipeline file first.');
    return;
  }
  const pick = await vscode.window.showQuickPick(
    [
      { label: '$(package) Executable binary', detail: 'Compile a standalone flogo-datapipeline binary', fmt: 'binary' },
      { label: '$(file-code) Dockerfile', detail: 'Generate a Dockerfile for the pipeline', fmt: 'dockerfile' },
      { label: '$(vm) Container image', detail: 'Generate a Dockerfile and build the image (requires Docker)', fmt: 'image' },
      { label: '$(cloud-upload) TIBCO Platform deployment bundle', detail: 'Dockerfile + Kubernetes manifest + README', fmt: 'platform' },
    ],
    { title: `Build ${path.basename(target.fsPath)}`, placeHolder: 'Choose a build target' },
  );
  if (!pick) return;
  try {
    await editor.provider.exportFromUri(target, pick.fmt);
    if (pick.fmt === 'binary') await setBuildConfig(target, { target: 'binary' });
    else if (pick.fmt === 'image') await setBuildConfig(target, { target: 'docker' });
  } catch (err: any) {
    vscode.window.showErrorMessage(`Build failed: ${err?.message ?? String(err)}`);
  }
}

async function runWithOptions(arg: any): Promise<void> {
  const target = resolvePipelineUri(arg);
  if (!target) {
    vscode.window.showWarningMessage('Select a .datapipeline file first.');
    return;
  }
  const build = await promptBuildConfig(target);
  if (!build) return;
  const cfg = vscode.workspace.getConfiguration('flogoDataPipelineStudio');
  const current = cfg.get<number>('apiPort') ?? 9080;
  const answer = await vscode.window.showInputBox({
    title: `Configure and Run ${path.basename(target.fsPath)}`,
    prompt: 'API port for the Flogo Data Pipeline REST endpoint (auto-increments if busy)',
    value: String(current),
    validateInput: (v) => {
      const n = Number(v);
      return Number.isInteger(n) && n >= 1 && n <= 65535 ? undefined : 'Enter a port between 1 and 65535.';
    },
  });
  if (answer === undefined) return;
  const port = Number(answer);
  if (port !== current) {
    const scope = vscode.workspace.workspaceFolders?.length
      ? vscode.ConfigurationTarget.Workspace
      : vscode.ConfigurationTarget.Global;
    await cfg.update('apiPort', port, scope);
  }
  await editor.provider.runBinary(target);
}

async function configure(arg: any): Promise<void> {
  const target = resolvePipelineUri(arg);
  if (!target) {
    vscode.window.showWarningMessage('Select a .datapipeline file first.');
    return;
  }
  const cfg = await promptBuildConfig(target);
  if (cfg) {
    vscode.window.showInformationMessage(
      `Run will ${cfg.target === 'docker' ? 'build a Docker image and run the container' : 'build a native binary and run it'}.`,
    );
  }
}

// ── Status bar ──────────────────────────────────────────────────────────────

function statusRunning(port: number): void {
  statusBar.text = `$(zap) Pipeline: running :${port}`;
  statusBar.color = '#4ec9b0';
  statusBar.tooltip = `Flogo Data Pipeline Studio — engine on port ${port}.`;
}

function statusStarting(): void {
  statusBar.text = '$(sync~spin) Pipeline: starting…';
  statusBar.color = undefined;
  statusBar.tooltip = 'Flogo Data Pipeline Studio — engine starting.';
}

function statusStopped(): void {
  statusBar.text = '$(circuit-board) Data Pipeline';
  statusBar.color = undefined;
  statusBar.tooltip = 'Flogo Data Pipeline Studio';
}
