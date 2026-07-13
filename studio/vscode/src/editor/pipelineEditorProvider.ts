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
import { DataPreviewView } from '../panel/dataPreviewView';
import { ConnectionStore } from '../connections/store';
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
  // Path of the resolved (connections-injected) sibling file the engine was
  // last started against, so it can be cleaned up on stop/dispose.
  private resolvedPath: string | null = null;
  // Dedicated native Output channel for run/engine logs (replaces the webview's
  // custom Output tab — full IDE integration).
  private readonly runChannel: vscode.OutputChannel;

  constructor(
    private readonly context: vscode.ExtensionContext,
    private readonly manager: BinaryManager,
    private readonly hooks: EngineHooks = {},
    private readonly dataPreview?: DataPreviewView,
    private readonly connections?: ConnectionStore,
  ) {
    this.diag = vscode.languages.createDiagnosticCollection('flogoDataPipeline');
    context.subscriptions.push(this.diag);
    this.runChannel = vscode.window.createOutputChannel('Flogo Data Pipeline Run');
    context.subscriptions.push(this.runChannel);
    if (this.connections) {
      context.subscriptions.push(
        this.connections.onDidChange(() => this.broadcastConnections()),
      );
    }
  }

  static register(
    context: vscode.ExtensionContext,
    manager: BinaryManager,
    hooks: EngineHooks,
    dataPreview?: DataPreviewView,
    connections?: ConnectionStore,
  ): { provider: PipelineEditorProvider; disposable: vscode.Disposable } {
    const provider = new PipelineEditorProvider(context, manager, hooks, dataPreview, connections);
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
    this.clearResolved();
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

  // Push the current connection summaries to one panel.
  private async sendConnections(panel: vscode.WebviewPanel): Promise<void> {
    if (!this.connections) {
      this.post(panel, { type: 'connections', items: [] });
      return;
    }
    const items = await this.connections.summaries();
    this.post(panel, { type: 'connections', items });
  }

  // Push connection summaries to every open panel (on store changes).
  private async broadcastConnections(): Promise<void> {
    const items = this.connections ? await this.connections.summaries() : [];
    for (const p of this.panels) this.post(p, { type: 'connections', items });
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
        this.sendConnections(panel);
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

      case 'showOutput':
        this.runChannel.show(true);
        break;

      case 'showProblems':
        vscode.commands.executeCommand('workbench.actions.view.problems');
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
        vscode.commands.executeCommand('flogoDataPipelineStudio.newConnection');
        break;

      case 'testConnection':
        vscode.commands.executeCommand('flogoDataPipelineStudio.testConnectionById', msg.id);
        break;

      case 'editConnection':
        vscode.commands.executeCommand('flogoDataPipelineStudio.editConnectionById', msg.id);
        break;

      case 'requestConnections':
        this.sendConnections(panel);
        break;

      case 'inferSchema': {
        try {
          const { inferSchema } = await import('../schema/infer');
          const cols = await inferSchema(doc.uri.fsPath, msg.path, {
            sourceType: msg.sourceType,
            delimiter: msg.delimiter,
            hasHeader: msg.hasHeader,
          });
          this.post(panel, { type: 'schemaInferred', columns: cols, count: cols.length });
        } catch (err) {
          this.post(panel, { type: 'schemaInferError', message: errText(err) });
        }
        break;
      }

      case 'sampleStageInput': {
        const port = await this.ensureEngine(doc, panel, false);
        if (!port) {
          this.post(panel, { type: 'stageSampleError', stage: msg.stage, message: 'Engine not available.' });
          break;
        }
        try {
          const res = await new ApiClient(port).sampleInput(this.refForDoc(doc), msg.stage, msg.limit ?? 20);
          this.post(panel, { type: 'stageSample', stage: msg.stage, columns: res.columns, rows: res.rows });
        } catch (err) {
          this.post(panel, { type: 'stageSampleError', stage: msg.stage, message: errText(err) });
        }
        break;
      }

      case 'testStage': {
        const port = await this.ensureEngine(doc, panel, false);
        if (!port) {
          this.post(panel, { type: 'stageTestError', stage: msg.stage, message: 'Engine not available.' });
          break;
        }
        try {
          const result = await new ApiClient(port).testStage(this.refForDoc(doc), msg.stage, msg.columns, msg.rows);
          this.post(panel, { type: 'stageTestResult', stage: msg.stage, result });
        } catch (err) {
          this.post(panel, { type: 'stageTestError', stage: msg.stage, message: errText(err) });
        }
        break;
      }

      case 'fetchDbTables': {
        try {
          const { driver, dsn } = await this.resolveDbTarget(doc, msg.target);
          const port = await this.ensureEngine(doc, panel, false);
          if (!port) {
            this.post(panel, { type: 'dbTablesError', target: msg.target, message: 'Engine not available.' });
            break;
          }
          const res = await new ApiClient(port).introspectTables(driver, dsn);
          this.post(panel, { type: 'dbTables', target: msg.target, tables: res.tables });
        } catch (err) {
          this.post(panel, { type: 'dbTablesError', target: msg.target, message: errText(err) });
        }
        break;
      }

      case 'fetchDbColumns': {
        try {
          const { driver, dsn } = await this.resolveDbTarget(doc, msg.target);
          const port = await this.ensureEngine(doc, panel, false);
          if (!port) {
            this.post(panel, { type: 'dbColumnsError', target: msg.target, message: 'Engine not available.' });
            break;
          }
          const res = await new ApiClient(port).introspectColumns(driver, dsn, msg.table);
          this.post(panel, { type: 'dbColumns', target: msg.target, columns: res.columns });
        } catch (err) {
          this.post(panel, { type: 'dbColumnsError', target: msg.target, message: errText(err) });
        }
        break;
      }

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

  // Append a run/engine log line to the native Output channel.
  private logRun(line: string): void {
    const ts = new Date().toISOString().slice(11, 23);
    this.runChannel.appendLine(`${ts}  ${line}`);
  }

  // Ensure the engine is running for this document; start it if necessary.
  // Returns the bound port, or null if startup failed (error already posted).
  // revealOutput controls whether the native Output channel is focused on start
  // (true for runs; false for preview so the Data Preview panel keeps focus).
  private async ensureEngine(
    document: vscode.TextDocument,
    panel: vscode.WebviewPanel,
    revealOutput = true,
  ): Promise<number | null> {
    if (this.manager.port) return this.manager.port;

    const cfg = vscode.workspace.getConfiguration('flogoDataPipelineStudio');
    const startPort = cfg.get<number>('apiPort') ?? 9080;
    const binPath = cfg.get<string>('binaryPath') ?? '';
    if (binPath) this.manager.setOverrideBinaryPath(binPath);

    this.post(panel, { type: 'runStarting' });
    this.hooks.onStarting?.();

    this.manager.onLog((text) => {
      text.split(/\r?\n/).forEach((line) => {
        if (line.trim()) this.runChannel.appendLine(line);
      });
    });
    if (revealOutput) this.runChannel.show(true);
    this.logRun(`[engine] Starting flogo-datapipeline on port ${startPort}…`);

    try {
      const startPath = await this.materializeResolved(document);
      // Run with the workspace root as the working directory so relative data
      // paths (data/x.csv, out/y.parquet) resolve against the project root even
      // when the pipeline lives in a pipelines/ subfolder.
      const wsRoot = vscode.workspace.getWorkspaceFolder(document.uri)?.uri.fsPath;
      const port = await this.manager.start(startPath, startPort, wsRoot);
      this.logRun(`[engine] Ready on port ${port}`);
      this.hooks.onRunning?.(port, path.basename(document.uri.fsPath, '.datapipeline'));
      this.post(panel, { type: 'runStarted', port });
      return port;
    } catch (err: any) {
      this.hooks.onStopped?.();
      this.post(panel, { type: 'runError', message: errText(err) });
      this.logRun(`[engine] start failed: ${errText(err)}`);
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

  // Resolve any bound connections into concrete engine config and, if any are
  // present, write a sibling temp file the engine reads (keeping secrets out of
  // the on-disk .datapipeline). Returns the path the engine should start from.
  private async materializeResolved(document: vscode.TextDocument): Promise<string> {
    this.clearResolved();
    if (!this.connections) return document.uri.fsPath;

    let app: any;
    try {
      app = safeParse(document.getText());
    } catch {
      return document.uri.fsPath;
    }
    const pipe = app?.resources?.[0]?.data;
    if (!pipe || typeof pipe !== 'object') return document.uri.fsPath;

    // Collect stages that may carry a `connection` binding.
    const stages: any[] = [];
    if (pipe.source) stages.push(pipe.source);
    if (pipe.sink) stages.push(pipe.sink);
    if (pipe.dlq?.options) stages.push(pipe.dlq.options);
    if (Array.isArray(pipe.transforms)) for (const t of pipe.transforms) if (t && typeof t === 'object') stages.push(t);

    let injected = 0;
    for (const stage of stages) {
      const name = stage.connection;
      if (!name || typeof name !== 'string') continue;
      const conn = await this.connections.getByName(name);
      if (!conn) {
        this.logRun(`[connections] WARNING: bound connection "${name}" not found — leaving stage unresolved.`);
        continue;
      }
      const resolved = await this.connections.resolveStageConfig(conn.id);
      Object.assign(stage, resolved);
      delete stage.connection;
      injected++;
    }

    if (injected === 0) return document.uri.fsPath;

    const dir = path.dirname(document.uri.fsPath);
    const base = path.basename(document.uri.fsPath, '.datapipeline');
    const target = path.join(dir, `.${base}.resolved.datapipeline`);
    await vscode.workspace.fs.writeFile(vscode.Uri.file(target), Buffer.from(JSON.stringify(app, null, 2), 'utf8'));
    this.resolvedPath = target;
    this.logRun(
      `[connections] Injected ${injected} connection(s) into a resolved run file (secrets stay out of ${base}.datapipeline).`,
    );
    return target;
  }

  // Resolve the JDBC driver + DSN for a source/sink/dlq target so the DB can be
  // introspected. Uses a bound connection when present, else the inline
  // driver/dsn fields on the stage.
  private async resolveDbTarget(
    doc: vscode.TextDocument,
    target: 'source' | 'sink' | 'dlq',
  ): Promise<{ driver: string; dsn: string }> {
    const app: any = safeParse(doc.getText());
    const pipe = app?.resources?.[0]?.data;
    if (!pipe) throw new Error('Pipeline has no data.');
    const stage: any = target === 'dlq' ? pipe.dlq?.options : pipe[target];
    if (!stage || typeof stage !== 'object') throw new Error(`No ${target} configured.`);

    if (typeof stage.connection === 'string' && this.connections) {
      const conn = await this.connections.getByName(stage.connection);
      if (!conn) throw new Error(`Connection "${stage.connection}" not found.`);
      const cfg = await this.connections.resolveStageConfig(conn.id);
      const driver = String((cfg as any).driver || 'postgres');
      const dsn = String((cfg as any).dsn || '');
      if (!dsn) throw new Error(`Connection "${stage.connection}" is not a database connection.`);
      return { driver, dsn };
    }
    const driver = String(stage.driver || 'postgres');
    const dsn = String(stage.dsn || '');
    if (!dsn) throw new Error('No connection or DSN configured for this stage.');
    return { driver, dsn };
  }

  // Remove the last resolved temp file, if any.
  private clearResolved(): void {
    if (!this.resolvedPath) return;
    try {
      vscode.workspace.fs.delete(vscode.Uri.file(this.resolvedPath));
    } catch {
      /* best effort */
    }
    this.resolvedPath = null;
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
      this.logRun(`[run] POST /v1/pipelines/${ref}/run`);
      let runId: string;
      try {
        const ack = await client.runPipeline(ref, { async: true });
        runId = ack.runId;
        this.logRun(`[run] accepted runId=${runId}`);
        this.post(panel, { type: 'runResult', result: ack, final: false });
      } catch (err) {
        this.post(panel, { type: 'runError', message: 'Run request failed: ' + errText(err) });
        this.logRun(`[run] request failed: ${errText(err)}`);
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
          this.logRun(`[run] status poll error: ${errText(err)}`);
          continue;
        }
        const run = st.runs.find((r) => r.runId === runId) || st.runs[st.runs.length - 1];
        if (!run) continue;
        const terminal = ['completed', 'partial', 'failed', 'cancelled'].includes(run.status);
        this.post(panel, { type: 'runResult', result: run, final: terminal });
        if (terminal) {
          this.logRun(
            `[run] ${run.status} — processed=${run.rowsProcessed} written=${run.rowsWritten} failed=${run.rowsFailed} dropped=${run.rowsDropped ?? 0} (${run.durationMs}ms)`,
          );
          break;
        }
      }
    } finally {
      this.runInFlight = false;
    }
  }

  private async previewPipeline(document: vscode.TextDocument, panel: vscode.WebviewPanel, limit: number): Promise<void> {
    // Reveal the native Data Preview panel and show a loading state there.
    this.dataPreview?.reveal();
    this.dataPreview?.showLoading(`Sampling up to ${limit} rows through the transform chain…`);

    // Start the engine without stealing focus to the Output channel.
    const port = await this.ensureEngine(document, panel, false);
    if (!port) {
      this.dataPreview?.showError('Engine not available.');
      return;
    }
    // Keep the Data Preview panel focused after the engine start.
    this.dataPreview?.reveal();
    const client = new ApiClient(port);
    const ref = this.refForDoc(document);
    try {
      const result = await client.preview(ref, limit);
      // Full table renders in the native panel view; the editor webview also
      // receives it so the Inspector Schema/Test tabs can show a summary.
      this.dataPreview?.showResult(result);
      this.post(panel, { type: 'previewResult', result });
    } catch (err) {
      this.dataPreview?.showError(errText(err));
      this.post(panel, { type: 'previewError', message: errText(err) });
    }
  }

  private stopPipeline(panel: vscode.WebviewPanel): void {
    this.manager.stop();
    this.clearResolved();
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
      const doc = await vscode.workspace.openTextDocument(uri);
      const deploy = await this.buildDeployPipeline(doc);
      await this.writeDockerfile(uri, deploy.deployFileName);
      if (format === 'platform') {
        await this.generateDeployArtifacts(doc, 4, deploy);
      } else {
        const envNote = deploy.env.length
          ? ` Provide ${deploy.env.length} secret env var(s) at runtime — see the generated ${path.basename(deploy.deployFileName)}.env.example.`
          : '';
        vscode.window.showInformationMessage('Generated Dockerfile next to the pipeline.' + envNote);
      }
    } else {
      vscode.window.showInformationMessage(`Build target "${format}" — see the Flogo Data Pipeline output channel.`);
    }
  }

  // Resolve bound connections into a deploy-ready sibling pipeline. Non-secret
  // connection fields are inlined; secret fields become ${ENV_VAR} placeholders
  // the engine expands at runtime. Also writes a .env.example listing the
  // required variables (with placeholder values — the real secrets stay in
  // Secret Storage and are never written to disk). Returns the deploy file name
  // and the env var list.
  private async buildDeployPipeline(
    doc: vscode.TextDocument,
  ): Promise<{ deployFileName: string; env: Array<{ name: string; value: string }> }> {
    const base = path.basename(doc.uri.fsPath, '.datapipeline');
    const deployFileName = `${base}.deploy.datapipeline`;
    const app: any = safeParse(doc.getText());
    const pipe = app?.resources?.[0]?.data;
    const env: Array<{ name: string; value: string }> = [];

    if (pipe && typeof pipe === 'object' && this.connections) {
      const stages: any[] = [];
      if (pipe.source) stages.push(pipe.source);
      if (pipe.sink) stages.push(pipe.sink);
      if (pipe.dlq?.options) stages.push(pipe.dlq.options);
      if (Array.isArray(pipe.transforms)) for (const t of pipe.transforms) if (t && typeof t === 'object') stages.push(t);

      for (const stage of stages) {
        const name = stage.connection;
        if (!name || typeof name !== 'string') continue;
        const conn = await this.connections.getByName(name);
        if (!conn) continue;
        const { config, env: stageEnv } = await this.connections.resolveStageConfigForDeploy(conn.id);
        Object.assign(stage, config);
        delete stage.connection;
        // Record only the var name — real secret values stay in Secret Storage.
        for (const e of stageEnv) if (!env.some((x) => x.name === e.name)) env.push({ name: e.name, value: '' });
      }
    }

    const dir = path.dirname(doc.uri.fsPath);
    const target = path.join(dir, deployFileName);
    await vscode.workspace.fs.writeFile(vscode.Uri.file(target), Buffer.from(JSON.stringify(app, null, 2), 'utf8'));

    if (env.length) {
      const envExample =
        '# Fill in the secret values (kept out of the pipeline file). Do not commit.\n' +
        env.map((e) => `${e.name}=CHANGE_ME`).join('\n') +
        '\n';
      await vscode.workspace.fs.writeFile(
        vscode.Uri.file(path.join(dir, `${deployFileName}.env.example`)),
        Buffer.from(envExample, 'utf8'),
      );
    }
    return { deployFileName, env };
  }

  private async writeDockerfile(uri: vscode.Uri, pipelineFileName?: string): Promise<vscode.Uri> {
    const { dockerfile } = await import('../deploy/manifests');
    const dir = vscode.Uri.joinPath(uri, '..');
    const fileName = pipelineFileName ?? path.basename(uri.fsPath);
    const target = vscode.Uri.joinPath(dir, 'Dockerfile');
    await vscode.workspace.fs.writeFile(target, Buffer.from(dockerfile(fileName), 'utf8'));
    return target;
  }

  // Generate a Dockerfile + sharded StatefulSet + README next to the pipeline.
  async generateDeployArtifacts(
    doc: vscode.TextDocument,
    replicas: number,
    deploy?: { deployFileName: string; env: Array<{ name: string; value: string }> },
  ): Promise<void> {
    const { statefulSet, deployReadme } = await import('../deploy/manifests');
    const dir = vscode.Uri.joinPath(doc.uri, '..');
    const resolved = deploy ?? (await this.buildDeployPipeline(doc));
    const fileName = resolved.deployFileName;
    const envVars = resolved.env;
    const pd = extractPipelineData(safeParse(doc.getText()));
    const name = pd.name || path.basename(doc.uri.fsPath, '.datapipeline');
    const image = this.defaultImageTag(doc.uri);

    await this.writeDockerfile(doc.uri, fileName);
    const stsUri = vscode.Uri.joinPath(dir, 'k8s-statefulset.yaml');
    await vscode.workspace.fs.writeFile(stsUri, Buffer.from(statefulSet(name, fileName, replicas, image, envVars), 'utf8'));
    const readmeUri = vscode.Uri.joinPath(dir, 'DEPLOY.md');
    await vscode.workspace.fs.writeFile(readmeUri, Buffer.from(deployReadme(name, replicas, image, envVars), 'utf8'));

    const secretNote = envVars.length
      ? ` ${envVars.length} secret env var(s) required — a Secret template is included in the manifest and DEPLOY.md.`
      : '';
    const choice = await vscode.window.showInformationMessage(
      `Generated Dockerfile + k8s-statefulset.yaml (${replicas} replicas) + DEPLOY.md.` + secretNote,
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
