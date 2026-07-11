import * as vscode from 'vscode';
import * as path from 'path';
import * as crypto from 'crypto';

import { BinaryManager } from '../binaryManager';
import {
  extractPipelineData,
  mergePipelineIntoDoc,
  validate,
  slug,
  PipelineDoc,
  ValidationIssue,
} from '../model/pipeline';
import { ApiClient } from '../engine/apiClient';
import type { WebviewToHost } from '../protocol';

export interface EngineHooks {
  onStarting?: () => void;
  onRunning?: (port: number, name: string) => void;
  onStopped?: () => void;
  onActivePipeline?: (uri: vscode.Uri | null) => void;
}

export class PipelineEditorProvider implements vscode.CustomTextEditorProvider {
  public static readonly viewType = 'flogoDataPipelineStudio.pipelineEditor';

  private readonly panels = new Set<vscode.WebviewPanel>();
  private readonly panelByUri = new Map<string, vscode.WebviewPanel>();
  private activeUri: vscode.Uri | null = null;
  private ignoreNextChange = false;
  private runInFlight = false;
  private readonly diag: vscode.DiagnosticCollection;

  constructor(
    private readonly context: vscode.ExtensionContext,
    private readonly manager: BinaryManager,
    private readonly hooks: EngineHooks = {},
  ) {
    this.diag = vscode.languages.createDiagnosticCollection('flogoDataPipeline');
    context.subscriptions.push(this.diag);
  }

  static register(
    context: vscode.ExtensionContext,
    manager: BinaryManager,
    hooks: EngineHooks,
  ): { provider: PipelineEditorProvider; disposable: vscode.Disposable } {
    const provider = new PipelineEditorProvider(context, manager, hooks);
    const disposable = vscode.window.registerCustomEditorProvider(
      PipelineEditorProvider.viewType,
      provider,
      {
        webviewOptions: { retainContextWhenHidden: true },
        supportsMultipleEditorsPerDocument: false,
      },
    );
    return { provider, disposable };
  }

  get activePipelineUri(): vscode.Uri | null {
    return this.activeUri;
  }

  broadcastStopped(): void {
    for (const p of this.panels) {
      try {
        p.webview.postMessage({ type: 'stopped' });
      } catch {
        /* panel disposed */
      }
    }
  }

  private setActive(uri: vscode.Uri | null): void {
    this.activeUri = uri;
    this.hooks.onActivePipeline?.(uri);
  }

  // ── CustomTextEditorProvider ───────────────────────────────────────────────

  async resolveCustomTextEditor(
    document: vscode.TextDocument,
    webviewPanel: vscode.WebviewPanel,
  ): Promise<void> {
    const extUri = this.context.extensionUri;
    webviewPanel.webview.options = {
      enableScripts: true,
      localResourceRoots: [
        vscode.Uri.joinPath(extUri, 'dist'),
        vscode.Uri.joinPath(extUri, 'media'),
        vscode.Uri.joinPath(extUri, 'resources'),
      ],
    };
    webviewPanel.webview.html = this.buildHtml(webviewPanel.webview, extUri);

    const uriKey = document.uri.toString();
    this.panels.add(webviewPanel);
    this.panelByUri.set(uriKey, webviewPanel);
    this.setActive(document.uri);

    webviewPanel.onDidChangeViewState((e) => {
      if (e.webviewPanel.active) this.setActive(document.uri);
    });

    const changeSub = vscode.workspace.onDidChangeTextDocument((e) => {
      if (e.document.uri.toString() !== uriKey) return;
      if (this.ignoreNextChange) {
        this.ignoreNextChange = false;
        return;
      }
      let appDoc: any = {};
      try {
        appDoc = JSON.parse(e.document.getText());
      } catch {
        /* mid-edit invalid JSON */
      }
      webviewPanel.webview.postMessage({ type: 'update', doc: extractPipelineData(appDoc) });
    });

    webviewPanel.onDidDispose(() => {
      this.panels.delete(webviewPanel);
      this.panelByUri.delete(uriKey);
      changeSub.dispose();
      this.diag.delete(document.uri);
      if (this.activeUri?.toString() === uriKey) this.setActive(null);
    });

    webviewPanel.webview.onDidReceiveMessage((msg: WebviewToHost) => {
      this.onMessage(webviewPanel, document, msg).catch((err) =>
        console.error('[DataPipeline] onMessage error:', err),
      );
    });
  }

  // ── Message handling ───────────────────────────────────────────────────────

  private post(panel: vscode.WebviewPanel, m: unknown): void {
    try {
      panel.webview.postMessage(m);
    } catch {
      /* panel disposed */
    }
  }

  private async onMessage(
    panel: vscode.WebviewPanel,
    doc: vscode.TextDocument,
    msg: WebviewToHost,
  ): Promise<void> {
    switch (msg.type) {
      case 'ready': {
        let appDoc: any = {};
        try {
          appDoc = JSON.parse(doc.getText());
        } catch {
          /* new/invalid */
        }
        const pd = extractPipelineData(appDoc);
        this.post(panel, {
          type: 'init',
          doc: pd,
          fileName: path.basename(doc.uri.fsPath),
          ref: this.refForDoc(doc, pd),
          nonce: crypto.randomBytes(8).toString('hex'),
        });
        if (this.manager.port) this.post(panel, { type: 'runStarted', port: this.manager.port });
        break;
      }

      case 'save': {
        const merged = mergePipelineIntoDoc(doc.getText(), msg.doc as PipelineDoc);
        await this.applyEdit(doc, merged);
        break;
      }

      case 'run':
        await this.runPipeline(doc, panel);
        break;

      case 'stop':
        this.stopPipeline(panel);
        break;

      case 'validate': {
        const issues = validate(doc.getText());
        this.post(panel, { type: 'validationResult', issues });
        this.setDiagnostics(doc, issues);
        break;
      }

      case 'preview':
        await this.previewPipeline(doc, panel, msg.limit ?? 100);
        break;

      case 'export':
        await this.exportFromUri(doc.uri, msg.format || 'binary');
        break;

      case 'openRaw':
        vscode.commands.executeCommand('vscode.openWith', doc.uri, 'default').then(undefined, (err) =>
          console.error('[DataPipeline] openWith failed:', err),
        );
        break;

      case 'definition': {
        const port = this.manager.port;
        if (!port) {
          this.post(panel, { type: 'metricsResult', error: 'Start the pipeline to read its engine definition.' });
          break;
        }
        try {
          const def = await new ApiClient(port).definition(this.refForDoc(doc));
          this.post(panel, { type: 'definition', def });
        } catch (err) {
          this.post(panel, { type: 'metricsResult', error: errText(err) });
        }
        break;
      }

      case 'metrics': {
        const port = this.manager.port;
        if (!port) {
          this.post(panel, { type: 'metricsResult', error: 'Pipeline not running.' });
          break;
        }
        try {
          const st = await new ApiClient(port).status(this.refForDoc(doc));
          this.post(panel, { type: 'metricsResult', data: st });
        } catch (err) {
          this.post(panel, { type: 'metricsResult', error: errText(err) });
        }
        break;
      }

      case 'browse': {
        try {
          const uris = await vscode.window.showOpenDialog({
            canSelectFiles: msg.mode !== 'folder',
            canSelectFolders: msg.mode === 'folder',
            canSelectMany: false,
            openLabel: 'Select',
          });
          if (uris && uris.length > 0) {
            this.post(panel, { type: 'browseResult', id: msg.id, path: uris[0].fsPath });
          } else {
            this.post(panel, { type: 'browseCancelled', id: msg.id });
          }
        } catch {
          this.post(panel, { type: 'browseCancelled', id: msg.id });
        }
        break;
      }

      case 'newFromTemplate':
        vscode.commands.executeCommand('flogoDataPipelineStudio.newFromTemplate');
        break;

      case 'newConnection':
        vscode.window.showInformationMessage('Connections are stored in VS Code Secret Storage (coming soon in this build).');
        break;

      case 'deploy':
        await this.generateDeployArtifacts(doc, msg.replicas);
        break;

      case 'showError':
        if (msg.text) vscode.window.showErrorMessage(msg.text);
        break;

      case 'copyText':
        if (msg.text) vscode.env.clipboard.writeText(msg.text);
        break;

      case 'toast': {
        const fn =
          msg.kind === 'err'
            ? vscode.window.showErrorMessage
            : msg.kind === 'warn'
            ? vscode.window.showWarningMessage
            : vscode.window.showInformationMessage;
        fn(msg.message);
        break;
      }

      default:
        console.warn('[DataPipeline] unhandled message:', (msg as any)?.type);
    }
  }

  // ── Engine lifecycle ───────────────────────────────────────────────────────

  // Compute the pipeline ref (bare slug the REST API accepts) from the document.
  private refForDoc(doc: vscode.TextDocument, pd?: PipelineDoc): string {
    let data = pd;
    if (!data) {
      try {
        data = extractPipelineData(JSON.parse(doc.getText()));
      } catch {
        data = {};
      }
    }
    const name = data?.name || path.basename(doc.uri.fsPath, '.datapipeline');
    return slug(name);
  }

  // Ensure the engine is running for this document; start it if necessary.
  // Returns the bound port, or null if startup failed (error already posted).
  private async ensureEngine(document: vscode.TextDocument, panel: vscode.WebviewPanel): Promise<number | null> {
    if (this.manager.port) return this.manager.port;

    const cfg = vscode.workspace.getConfiguration('flogoDataPipelineStudio');
    const startPort = cfg.get<number>('apiPort') ?? 9080;
    const binPath = cfg.get<string>('binaryPath') ?? '';
    if (binPath) this.manager.setOverrideBinaryPath(binPath);

    this.post(panel, { type: 'runStarting' });
    this.hooks.onStarting?.();

    this.manager.onLog((text) => {
      text.split(/\r?\n/).forEach((line) => {
        if (line.trim()) this.post(panel, { type: 'runLog', line });
      });
    });
    this.post(panel, { type: 'runLog', line: `[engine] Starting flogo-datapipeline on port ${startPort}…` });

    try {
      const port = await this.manager.start(document.uri.fsPath, startPort);
      this.post(panel, { type: 'runLog', line: `[engine] Ready on port ${port}` });
      this.hooks.onRunning?.(port, path.basename(document.uri.fsPath, '.datapipeline'));
      this.post(panel, { type: 'runStarted', port });
      return port;
    } catch (err: any) {
      this.hooks.onStopped?.();
      this.post(panel, { type: 'runError', message: errText(err) });
      if (err && err.code === 'NO_BINARY') {
        const buildCmd = err.buildCmd || this.manager.buildCommand();
        vscode.window
          .showErrorMessage(
            'flogo-datapipeline binary not found. Build it or set the binary path in settings.',
            'Show Build Command',
            'Open Settings',
          )
          .then((choice) => {
            if (choice === 'Show Build Command') {
              const ch = this.manager.channel;
              ch.show(true);
              ch.appendLine('\n══ Build the binary ══');
              ch.appendLine('  ' + buildCmd);
            } else if (choice === 'Open Settings') {
              vscode.commands.executeCommand('workbench.action.openSettings', 'flogoDataPipelineStudio.binaryPath');
            }
          });
      }
      return null;
    } finally {
      this.manager.onLog(null);
    }
  }

  private async runPipeline(document: vscode.TextDocument, panel: vscode.WebviewPanel): Promise<void> {
    if (this.runInFlight) {
      this.post(panel, { type: 'runError', message: 'Another pipeline run is already in progress.' });
      return;
    }
    this.runInFlight = true;
    try {
      const port = await this.ensureEngine(document, panel);
      if (!port) return;

      const client = new ApiClient(port);
      const ref = this.refForDoc(document);

      // Kick off an async run so we can stream status while it executes.
      this.post(panel, { type: 'runLog', line: `[run] POST /v1/pipelines/${ref}/run` });
      let runId: string;
      try {
        const ack = await client.runPipeline(ref, { async: true });
        runId = ack.runId;
        this.post(panel, { type: 'runLog', line: `[run] accepted runId=${runId}` });
        this.post(panel, { type: 'runResult', result: ack, final: false });
      } catch (err) {
        this.post(panel, { type: 'runError', message: 'Run request failed: ' + errText(err) });
        return;
      }

      // Poll status until the run reaches a terminal state.
      const deadline = Date.now() + 30 * 60 * 1000; // 30 min safety cap
      // eslint-disable-next-line no-constant-condition
      while (true) {
        await delay(700);
        if (Date.now() > deadline) {
          this.post(panel, { type: 'runError', message: 'Run polling timed out after 30 minutes.' });
          break;
        }
        let st;
        try {
          st = await client.status(ref);
        } catch (err) {
          this.post(panel, { type: 'runLog', line: '[run] status poll error: ' + errText(err) });
          continue;
        }
        const run = st.runs.find((r) => r.runId === runId) || st.runs[st.runs.length - 1];
        if (!run) continue;
        const terminal = ['completed', 'partial', 'failed', 'cancelled'].includes(run.status);
        this.post(panel, { type: 'runResult', result: run, final: terminal });
        if (terminal) {
          this.post(panel, {
            type: 'runLog',
            line: `[run] ${run.status} — processed=${run.rowsProcessed} written=${run.rowsWritten} failed=${run.rowsFailed} dropped=${run.rowsDropped ?? 0} (${run.durationMs}ms)`,
          });
          break;
        }
      }
    } finally {
      this.runInFlight = false;
    }
  }

  private async previewPipeline(document: vscode.TextDocument, panel: vscode.WebviewPanel, limit: number): Promise<void> {
    const port = await this.ensureEngine(document, panel);
    if (!port) return;
    const client = new ApiClient(port);
    const ref = this.refForDoc(document);
    try {
      const result = await client.preview(ref, limit);
      this.post(panel, { type: 'previewResult', result });
    } catch (err) {
      this.post(panel, { type: 'previewError', message: errText(err) });
    }
  }

  private stopPipeline(panel: vscode.WebviewPanel): void {
    this.manager.stop();
    this.hooks.onStopped?.();
    this.post(panel, { type: 'stopped' });
  }

  // ── Build / export ─────────────────────────────────────────────────────────

  defaultImageTag(uri: vscode.Uri): string {
    return path.basename(uri.fsPath, '.datapipeline').toLowerCase().replace(/[^a-z0-9._-]/g, '-') + ':latest';
  }

  async exportFromUri(uri: vscode.Uri, format: string): Promise<void> {
    // Delegates to the CLI build via the terminal — kept simple and transparent.
    const cfg = vscode.workspace.getConfiguration('flogoDataPipelineStudio');
    const sourcePath = cfg.get<string>('sourcePath') ?? '';
    const ch = this.manager.channel;
    ch.show(true);
    ch.appendLine(`[build] format=${format} pipeline=${path.basename(uri.fsPath)}`);
    if (format === 'binary') {
      if (!sourcePath) {
        vscode.window.showWarningMessage('Set flogoDataPipelineStudio.sourcePath to build a binary from source.');
        return;
      }
      const out = path.join(this.context.extensionPath, 'bin', binaryName());
      await this.manager.buildBinary({ sourcePath, out });
      vscode.window.showInformationMessage(`Built binary → ${out}`);
    } else if (format === 'dockerfile' || format === 'image' || format === 'platform') {
      await this.writeDockerfile(uri);
      if (format === 'platform') {
        await this.generateDeployArtifacts(await vscode.workspace.openTextDocument(uri), 4);
      } else {
        vscode.window.showInformationMessage('Generated Dockerfile next to the pipeline.');
      }
    } else {
      vscode.window.showInformationMessage(`Build target "${format}" — see the Flogo Data Pipeline output channel.`);
    }
  }

  private async writeDockerfile(uri: vscode.Uri): Promise<vscode.Uri> {
    const { dockerfile } = await import('../deploy/manifests');
    const dir = vscode.Uri.joinPath(uri, '..');
    const fileName = path.basename(uri.fsPath);
    const target = vscode.Uri.joinPath(dir, 'Dockerfile');
    await vscode.workspace.fs.writeFile(target, Buffer.from(dockerfile(fileName), 'utf8'));
    return target;
  }

  // Generate a Dockerfile + sharded StatefulSet + README next to the pipeline.
  async generateDeployArtifacts(doc: vscode.TextDocument, replicas: number): Promise<void> {
    const { statefulSet, deployReadme } = await import('../deploy/manifests');
    const dir = vscode.Uri.joinPath(doc.uri, '..');
    const fileName = path.basename(doc.uri.fsPath);
    const pd = extractPipelineData(safeParse(doc.getText()));
    const name = pd.name || path.basename(doc.uri.fsPath, '.datapipeline');
    const image = this.defaultImageTag(doc.uri);

    await this.writeDockerfile(doc.uri);
    const stsUri = vscode.Uri.joinPath(dir, 'k8s-statefulset.yaml');
    await vscode.workspace.fs.writeFile(stsUri, Buffer.from(statefulSet(name, fileName, replicas, image), 'utf8'));
    const readmeUri = vscode.Uri.joinPath(dir, 'DEPLOY.md');
    await vscode.workspace.fs.writeFile(readmeUri, Buffer.from(deployReadme(name, replicas, image), 'utf8'));

    const choice = await vscode.window.showInformationMessage(
      `Generated Dockerfile + k8s-statefulset.yaml (${replicas} replicas) + DEPLOY.md.`,
      'Open Manifest',
    );
    if (choice === 'Open Manifest') {
      await vscode.window.showTextDocument(stsUri);
    }
  }

  async runBinary(uri: vscode.Uri): Promise<void> {
    const panel = this.panelByUri.get(uri.toString());
    const doc = await vscode.workspace.openTextDocument(uri);
    if (panel) await this.runPipeline(doc, panel);
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private async applyEdit(doc: vscode.TextDocument, newText: string): Promise<void> {
    if (doc.getText() === newText) return;
    const edit = new vscode.WorkspaceEdit();
    edit.replace(doc.uri, new vscode.Range(0, 0, doc.lineCount, 0), newText);
    this.ignoreNextChange = true;
    await vscode.workspace.applyEdit(edit);
  }

  private setDiagnostics(document: vscode.TextDocument, issues: ValidationIssue[]): void {
    const text = document.getText();
    const SEV: Record<string, vscode.DiagnosticSeverity> = {
      error: vscode.DiagnosticSeverity.Error,
      warn: vscode.DiagnosticSeverity.Warning,
      info: vscode.DiagnosticSeverity.Information,
    };
    const diags = issues.map((p) => {
      const d = new vscode.Diagnostic(
        this.locateRange(text, p.anchor),
        (p.label ? p.label + ': ' : '') + (p.msg || ''),
        SEV[p.sev] ?? vscode.DiagnosticSeverity.Information,
      );
      d.source = 'Flogo Data Pipeline';
      return d;
    });
    this.diag.set(document.uri, diags);
  }

  private locateRange(text: string, anchor: string | null): vscode.Range {
    if (anchor) {
      const idx = text.indexOf('"' + anchor + '"');
      if (idx >= 0) return new vscode.Range(this.posAt(text, idx), this.posAt(text, idx + anchor.length + 2));
    }
    const nl = text.indexOf('\n');
    return new vscode.Range(0, 0, 0, nl < 0 ? Math.min(text.length, 80) : Math.min(nl, 80));
  }

  private posAt(text: string, offset: number): vscode.Position {
    let line = 0;
    let col = 0;
    const n = Math.min(offset, text.length);
    for (let i = 0; i < n; i++) {
      if (text.charCodeAt(i) === 10) {
        line++;
        col = 0;
      } else {
        col++;
      }
    }
    return new vscode.Position(line, col);
  }

  private buildHtml(webview: vscode.Webview, extUri: vscode.Uri): string {
    const nonce = crypto.randomBytes(16).toString('base64');
    const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(extUri, 'dist', 'webview.js'));
    const styleUri = webview.asWebviewUri(vscode.Uri.joinPath(extUri, 'media', 'webview.css'));
    const csp = [
      `default-src 'none'`,
      `img-src ${webview.cspSource} data:`,
      `style-src ${webview.cspSource} 'unsafe-inline'`,
      `script-src 'nonce-${nonce}'`,
      `font-src ${webview.cspSource}`,
    ].join('; ');
    return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta http-equiv="Content-Security-Policy" content="${csp}" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<link rel="stylesheet" href="${styleUri}" />
<title>Flogo Data Pipeline Studio</title>
</head>
<body>
<div class="win">
  <div class="ide" id="ide">
    <div class="egroup">
      <div class="tabsbar" id="tabsbar"></div>
      <div class="breadcrumb" id="breadcrumb"></div>
      <div class="edmain" id="edmain"></div>
      <div class="panel" id="panel">
        <div class="paneltabs" id="paneltabs"></div>
        <div class="panelbody" id="panelbody"></div>
      </div>
    </div>
  </div>
  <div class="statusbar" id="statusbar"></div>
</div>
<div class="toasts" id="toasts" role="status" aria-live="polite"></div>
<script nonce="${nonce}" src="${scriptUri}"></script>
</body>
</html>`;
  }
}

function binaryName(): string {
  const goArch = process.arch === 'x64' ? 'amd64' : process.arch;
  if (process.platform === 'win32') return `flogo-datapipeline-windows-${goArch}.exe`;
  return `flogo-datapipeline-${process.platform}-${goArch}`;
}

function errText(err: unknown): string {
  return err && (err as any).message ? (err as any).message : String(err);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function safeParse(text: string): any {
  try {
    return JSON.parse(text);
  } catch {
    return {};
  }
}
