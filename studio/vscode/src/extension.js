// @ts-check
'use strict'

const vscode = require('vscode')
const { BinaryManager } = require('./binaryManager')
const { SidebarProvider } = require('./sidebarProvider')
const { PipelineEditorProvider } = require('./pipelineEditorProvider')

let manager = null
let sidebar = null
let statusBar = null
let editor = null  // { provider, disposable }
let extContext = null // extension context (for workspaceState-backed build config)

// ── activate ───────────────────────────────────────────────────────────────

async function activate(context) {
  extContext = context
  manager = new BinaryManager(context.extensionPath)
  sidebar = new SidebarProvider()

  // Status bar item (bottom bar, right side).
  statusBar = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100)
  statusBar.command = 'flogoDataPipelineStudio.focusSidebar'
  _statusStopped()
  statusBar.show()
  context.subscriptions.push(statusBar)

  // Engine-lifecycle hooks — keep the status bar + sidebar in sync. The canvas
  // editor owns the engine (its Run/Stop buttons drive these).
  /** @type {EngineHooks} */
  const hooks = {
    onStarting: () => _statusStarting(),
    onRunning: (port, name, tools) => { sidebar.setRunning(port, name, tools); _statusRunning(port) },
    onStopped: () => { sidebar.setStopped(); _statusStopped() },
    onActivePipeline: (uri) => _syncActivePipeline(uri),
  }

  // Pipeline file editor (Custom Editor for .datapipeline files) — hosts the canvas.
  context.subscriptions.push(manager.channel)  // dispose OutputChannel on deactivate
  editor = PipelineEditorProvider.register(context, manager, hooks)
  context.subscriptions.push(editor.disposable)

  // If the engine crashes after a successful start, reflect it everywhere.
  manager.onCrash(() => {
    hooks.onStopped?.()
    editor?.provider?.broadcastStopped?.()
  })

  // ── Commands ──────────────────────────────────────────────────────────────

  context.subscriptions.push(
    vscode.commands.registerCommand('flogoDataPipelineStudio.runPipeline', uri => _runPipelineFile(uri)),
    vscode.commands.registerCommand('flogoDataPipelineStudio.stopPipeline', () => _stopPipeline()),
    vscode.commands.registerCommand('flogoDataPipelineStudio.newPipeline', uri => _newPipelineFile(uri)),
    vscode.commands.registerCommand('flogoDataPipelineStudio.newFromTemplate', uri => _newFromTemplate(uri)),
    vscode.commands.registerCommand('flogoDataPipelineStudio.build', uri => _build(uri)),
    vscode.commands.registerCommand('flogoDataPipelineStudio.runWithOptions', arg => _runWithOptions(arg)),
    vscode.commands.registerCommand('flogoDataPipelineStudio.configure', arg => _configure(arg)),
    vscode.commands.registerCommand('flogoDataPipelineStudio.focusSidebar', () =>
      vscode.commands.executeCommand('flogoDataPipelineStudio.pipelinesExplorer.focus')),
  )

  // ── Sidebar ───────────────────────────────────────────────────────────────

  context.subscriptions.push(sidebar)  // dispose EventEmitter on deactivate
  context.subscriptions.push(
    vscode.window.registerTreeDataProvider('flogoDataPipelineStudio.pipelinesExplorer', sidebar)
  )

  // Keep the sidebar's action target in sync when a .datapipeline file is focused as
  // raw text (the canvas editor reports its own active state via the hook).
  context.subscriptions.push(
    vscode.window.onDidChangeActiveTextEditor(ed => {
      const u = ed?.document?.uri
      if (u && u.fsPath.endsWith('.datapipeline')) _syncActivePipeline(u)
    })
  )
}

// ── deactivate ─────────────────────────────────────────────────────────────

function deactivate() {
  manager?.stop()
}

// ── Command implementations ──────────────────────────────────────────────────

// Open a .datapipeline file in the canvas editor.
async function _runPipelineFile(uri) {
  const target = _resolvePipelineUri(uri)
  if (!target) {
    vscode.window.showWarningMessage('Select a .datapipeline file first.')
    return
  }
  await vscode.commands.executeCommand('vscode.openWith', target, PipelineEditorProvider.viewType)
}

// Resolve the .datapipeline file an action targets: an explicit argument
// (a URI from a context menu / editor title, or a sidebar tree item carrying
// `pipelineUri`), else the active canvas editor's file, else the active text editor.
// Returns null when nothing suitable is focused.
function _resolvePipelineUri(arg) {
  const isPipelinePath = p => typeof p === 'string' && p.endsWith('.datapipeline')
  if (arg && isPipelinePath(arg.fsPath)) return arg
  const treeUri = arg && (arg.pipelineUri || arg.resourceUri)
  if (treeUri && isPipelinePath(treeUri.fsPath)) return treeUri
  const active = editor?.provider?.activePipelineUri
  if (active) return active
  const ed = vscode.window.activeTextEditor?.document?.uri
  if (ed && isPipelinePath(ed.fsPath)) return ed
  return null
}

// Build & Run following the pipeline's build configuration: "binary" builds a native
// binary and runs it; "docker" builds a container image and runs it. If no config is
// set yet, prompt for one.
async function _runConfigured(uri) {
  const target = _resolvePipelineUri(uri)
  if (!target || !target.fsPath.endsWith('.datapipeline')) {
    vscode.window.showWarningMessage('Select a .datapipeline file first.')
    return
  }
  if (!editor || !editor.provider) {
    vscode.window.showWarningMessage('Flogo Data Pipeline Studio is not ready yet.')
    return
  }
  let cfg = _getBuildConfig(target)
  if (!cfg) {
    cfg = await _promptBuildConfig(target)
    if (!cfg) return
  }
  if (cfg.target === 'docker') {
    await editor.provider.runDocker(target, cfg.imageTag)
  } else {
    await editor.provider.runBinary(target)
  }
}

// ── Per-pipeline build configuration (what Run follows) ──────────────────────
// Stored in workspace state so it survives reloads. Shape: { target: 'binary' | 'docker' }.
function _buildConfigKey(uri) { return 'flogoDataPipelineStudio.buildConfig:' + uri.fsPath }

function _getBuildConfig(uri) {
  return (extContext && extContext.workspaceState.get(_buildConfigKey(uri))) || null
}

async function _setBuildConfig(uri, cfg) {
  if (extContext) await extContext.workspaceState.update(_buildConfigKey(uri), cfg)
  _syncActivePipeline(uri)
}

// Ask how Run should build & launch this pipeline, and remember the choice.
async function _promptBuildConfig(uri) {
  const pick = await vscode.window.showQuickPick([
    { label: '$(package) Executable binary', detail: 'Run builds a native binary from source and launches it', target: 'binary' },
    { label: '$(vm) Container image (Docker)', detail: 'Run builds a Docker image and runs the container', target: 'docker' },
  ], { title: `Configure Run — ${require('path').basename(uri.fsPath)}`, placeHolder: 'How should Run build & launch this pipeline?' })
  if (!pick) return null
  const cfg = { target: pick.target }
  await _setBuildConfig(uri, cfg)
  return cfg
}

// Point the sidebar at `uri` and reflect its configured build target.
function _syncActivePipeline(uri) {
  if (!sidebar) return
  sidebar.setActivePipeline(uri || null)
  const cfg = uri ? _getBuildConfig(uri) : null
  sidebar.setBuildTarget(cfg ? cfg.target : null)
}

// Stop the running engine (sidebar Stop button / command palette).
function _stopPipeline() {
  if (manager) manager.stop()
  if (sidebar) sidebar.setStopped()
  _statusStopped()
  editor?.provider?.broadcastStopped?.()
}

// _defaultDirUri resolves the directory a "new pipeline" file should default to.
// When invoked from the Explorer context menu VS Code passes the right-clicked
// resource: a folder is used directly, a file uses its parent directory. When
// invoked from the command palette or a view title (no uri) it falls back to
// the first workspace folder.
/** @param {vscode.Uri} [uri] */
async function _defaultDirUri(uri) {
  if (uri && uri.scheme === 'file') {
    try {
      const stat = await vscode.workspace.fs.stat(uri)
      if (stat.type & vscode.FileType.Directory) return uri
      return vscode.Uri.joinPath(uri, '..')
    } catch { /* fall through to workspace root */ }
  }
  return vscode.workspace.workspaceFolders?.[0]?.uri
}

// Create a new .datapipeline file from the built-in template.
/** @param {vscode.Uri} [uri] */
async function _newPipelineFile(uri) {
  const defaultUri = await _defaultDirUri(uri)

  const saveUri = await vscode.window.showSaveDialog({
    defaultUri: defaultUri
      ? vscode.Uri.joinPath(defaultUri, 'my-pipeline.datapipeline')
      : undefined,
    filters: { 'Flogo Data Pipeline': ['datapipeline'] },
    title: 'Create new Flogo Data Pipeline file',
  })
  if (!saveUri) return

  const name = require('path').basename(saveUri.fsPath, '.datapipeline')
  const template = JSON.stringify({
    name,
    description: '',
    version: '1.0.0',
    mode: 'etl',
    source: {
      type: 'csv',
      path: '',
    },
    transforms: [],
    sink: {
      type: 'jdbc',
      driver: 'postgres',
      dsn: '',
      table: '',
    },
    performance: {
      batchSize: 5000,
      maxMemoryMB: 512,
    },
    scaling: {
      mode: 'single',
    },
    checkpoint: {
      type: 'none',
    },
  }, null, 2)

  await vscode.workspace.fs.writeFile(saveUri, Buffer.from(template, 'utf8'))
  await vscode.commands.executeCommand('vscode.openWith', saveUri, PipelineEditorProvider.viewType)
  vscode.window.showInformationMessage(
    `Created ${require('path').basename(saveUri.fsPath)}. Configure your source and sink, then click "Run" to launch.`
  )
}

// Create a new .datapipeline file from a starter template (QuickPick gallery).
/** @param {vscode.Uri} [uri] */
async function _newFromTemplate(uri) {
  const { TEMPLATES } = require('./templates')
  const pick = await vscode.window.showQuickPick(
    TEMPLATES.map(t => ({ label: t.label, description: t.id, detail: t.detail, tplId: t.id })),
    { title: 'New Flogo Data Pipeline from Template', placeHolder: 'Choose a starting point', matchOnDetail: true }
  )
  if (!pick) return
  const tpl = TEMPLATES.find(t => t.id === pick.tplId)
  if (!tpl) return

  const defaultUri = await _defaultDirUri(uri)
  const saveUri = await vscode.window.showSaveDialog({
    defaultUri: defaultUri ? vscode.Uri.joinPath(defaultUri, tpl.id + '.datapipeline') : undefined,
    filters: { 'Flogo Data Pipeline': ['datapipeline'] },
    title: `New pipeline from the "${tpl.id}" template`,
  })
  if (!saveUri) return

  const name = require('path').basename(saveUri.fsPath, '.datapipeline')
  let appObj
  try {
    appObj = tpl.make(name)
  } catch (err) {
    vscode.window.showErrorMessage(`Template failed: ${err && err.message ? err.message : String(err)}`)
    return
  }
  await vscode.workspace.fs.writeFile(saveUri, Buffer.from(JSON.stringify(appObj, null, 2), 'utf8'))
  await vscode.commands.executeCommand('vscode.openWith', saveUri, PipelineEditorProvider.viewType)
  vscode.window.showInformationMessage(
    `Created ${require('path').basename(saveUri.fsPath)} from the "${tpl.id}" template. Configure your source and sink, then click "Run" to launch.`
  )
}

// Build the pipeline. Choose an executable binary, a Dockerfile, or a container
// image — the artifacts used for TIBCO Platform (or any OCI runtime) deployment.
async function _build(uri) {
  const target = _resolvePipelineUri(uri)
  if (!target || !target.fsPath.endsWith('.datapipeline')) {
    vscode.window.showWarningMessage('Select a .datapipeline file first.')
    return
  }
  if (!editor || !editor.provider) {
    vscode.window.showWarningMessage('Flogo Data Pipeline Studio is not ready yet.')
    return
  }
  const pick = await vscode.window.showQuickPick([
    { label: '$(package) Executable binary', detail: 'Compile a standalone flogo-datapipeline binary', fmt: 'binary' },
    { label: '$(file-code) Dockerfile', detail: 'Generate a Dockerfile for the pipeline', fmt: 'dockerfile' },
    { label: '$(vm) Container image', detail: 'Generate a Dockerfile and build the image (requires Docker)', fmt: 'image' },
    { label: '$(cloud-upload) TIBCO Platform deployment bundle', detail: 'Dockerfile + Kubernetes manifest + README for a Data Plane namespace', fmt: 'platform' },
  ], { title: `Build ${require('path').basename(target.fsPath)}`, placeHolder: 'Choose a build target' })
  if (!pick) return
  try {
    await editor.provider.exportFromUri(target, pick.fmt)
    if (pick.fmt === 'binary') await _setBuildConfig(target, { target: 'binary' })
    else if (pick.fmt === 'image') await _setBuildConfig(target, { target: 'docker' })
  } catch (err) {
    vscode.window.showErrorMessage(`Build failed: ${err && err.message ? err.message : String(err)}`)
  }
}

// Configure and Run — the "with options" entry point. Always asks HOW to build &
// launch (native binary vs Docker container), then the options that apply, then runs.
// A binary launches directly on the chosen API port; Docker builds the image and runs
// the container. The choice is remembered for subsequent runs.
async function _runWithOptions(arg) {
  const target = _resolvePipelineUri(arg)
  if (!target || !target.fsPath.endsWith('.datapipeline')) {
    vscode.window.showWarningMessage('Select a .datapipeline file first.')
    return
  }
  if (!editor || !editor.provider) {
    vscode.window.showWarningMessage('Flogo Data Pipeline Studio is not ready yet.')
    return
  }

  // Remember prior options (e.g. the last Docker image name) to prepopulate prompts.
  const prev = _getBuildConfig(target)

  // 1. Build type — native binary vs Docker container (remembered for subsequent runs).
  const build = await _promptBuildConfig(target)
  if (!build) return

  // 2. Docker: ask for the image name (prepopulated + editable), then build the image
  //    and run the container.
  if (build.target === 'docker') {
    const defaultTag = (prev && prev.imageTag) || editor.provider.defaultImageTag(target)
    const image = await vscode.window.showInputBox({
      title: `Configure and Run ${require('path').basename(target.fsPath)}`,
      prompt: 'Docker image name to build and run (repository:tag)',
      value: defaultTag,
      validateInput: v => {
        const s = (v || '').trim()
        if (!s) return 'Enter a Docker image name, e.g. my-pipeline:latest.'
        if (s.indexOf(' ') >= 0) return 'Docker image names cannot contain spaces.'
        if (!/^[a-zA-Z0-9._:/@-]+$/.test(s)) return 'Use only letters, digits and . _ - / : @'
        return undefined
      },
    })
    if (image === undefined) return  // cancelled
    const tag = image.trim()
    await _setBuildConfig(target, { target: 'docker', imageTag: tag })  // remember the name
    await editor.provider.runDocker(target, tag)
    return
  }

  // 3. Binary: offer the API port, persist it, then build & launch the binary.
  const cfg = vscode.workspace.getConfiguration('flogoDataPipelineStudio')
  const current = cfg.get('apiPort') ?? 8888
  const answer = await vscode.window.showInputBox({
    title: `Configure and Run ${require('path').basename(target.fsPath)}`,
    prompt: 'API port for the Flogo Data Pipeline REST endpoint (auto-increments if busy)',
    value: String(current),
    validateInput: v => {
      const n = Number(v)
      return Number.isInteger(n) && n >= 1 && n <= 65535 ? undefined : 'Enter a port between 1 and 65535.'
    },
  })
  if (answer === undefined) return  // cancelled
  const port = Number(answer)
  if (port !== current) {
    const hasWorkspace = !!vscode.workspace.workspaceFolders?.length
    const scope = hasWorkspace ? vscode.ConfigurationTarget.Workspace : vscode.ConfigurationTarget.Global
    await cfg.update('apiPort', port, scope)
  }
  await editor.provider.runBinary(target)
}

// Configure how Run builds & launches this pipeline (native binary vs Docker container).
// Run follows this setting the next time it is invoked.
async function _configure(arg) {
  const target = _resolvePipelineUri(arg)
  if (!target || !target.fsPath.endsWith('.datapipeline')) {
    vscode.window.showWarningMessage('Select a .datapipeline file first.')
    return
  }
  const cfg = await _promptBuildConfig(target)
  if (cfg) {
    vscode.window.showInformationMessage(
      `Run will ${cfg.target === 'docker' ? 'build a Docker image and run the container' : 'build a native binary and run it'}.`
    )
  }
}

// ── Status bar helpers ───────────────────────────────────────────────────────

function _statusRunning(port) {
  if (!statusBar) return
  statusBar.text = `$(zap) Pipeline: running :${port}`
  statusBar.color = '#4ec9b0'
  statusBar.tooltip = `Flogo Data Pipeline Studio — engine on port ${port}.`
  statusBar.backgroundColor = undefined
}

function _statusStarting() {
  if (!statusBar) return
  statusBar.text = '$(sync~spin) Pipeline: starting…'
  statusBar.color = '#cca700'
  statusBar.tooltip = 'Flogo Data Pipeline Studio is starting the engine…'
}

function _statusStopped() {
  if (!statusBar) return
  statusBar.text = '$(circle-slash) Pipeline: stopped'
  statusBar.color = undefined
  statusBar.tooltip = 'Flogo Data Pipeline Studio — engine not running. Open a .datapipeline file and click Run.'
}

module.exports = { activate, deactivate }
