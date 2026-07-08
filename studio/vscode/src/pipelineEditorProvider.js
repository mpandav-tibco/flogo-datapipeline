// @ts-check
'use strict'

const vscode = require('vscode')
const fs = require('fs')
const path = require('path')
const http = require('http')
const crypto = require('crypto')

class PipelineEditorProvider {
  static viewType = 'flogoDataPipelineStudio.pipelineEditor'

  // @param hooks { onStarting(), onRunning(port,name), onStopped(), onActivePipeline(uri) }
  constructor(context, manager, hooks) {
    this._context = context
    this._manager = manager
    this._hooks = hooks ?? {}
    this._panels = new Set()            // active webview panels, for crash broadcast
    this._panelByUri = new Map()        // document uri string -> webview panel
    this._docByUri = new Map()          // document uri string -> TextDocument
    this._activeUri = null              // the .datapipeline whose canvas is active
    // Diagnostics surfaced in the Problems panel, mirrored from canvas validation.
    this._diag = vscode.languages.createDiagnosticCollection('flogoDataPipeline')
    if (context && context.subscriptions) context.subscriptions.push(this._diag)
  }

  static register(context, manager, hooks) {
    const provider = new PipelineEditorProvider(context, manager, hooks)
    const disposable = vscode.window.registerCustomEditorProvider(
      PipelineEditorProvider.viewType,
      provider,
      { webviewOptions: { retainContextWhenHidden: true }, supportsMultipleEditorsPerDocument: false }
    )
    return { provider, disposable }
  }

  // Notify every open canvas that the engine went away (called on crash).
  broadcastStopped() {
    for (const p of this._panels) {
      try { p.webview.postMessage({ type: 'stopped' }) } catch (_) { }
    }
  }

  get activePipelineUri() { return this._activeUri }

  _setActive(uri) {
    this._activeUri = uri
    if (this._hooks.onActivePipeline) this._hooks.onActivePipeline(uri)
  }

  // Wait for the canvas panel of `uri` to exist (created asynchronously by openWith).
  async _awaitPanel(uri) {
    const key = uri.toString()
    let panel = this._panelByUri.get(key)
    for (let i = 0; !panel && i < 30; i++) {
      await new Promise(r => setTimeout(r, 100))
      panel = this._panelByUri.get(key)
    }
    return panel || null
  }

  // ── Mirror canvas validation problems into the Problems panel ───────────────

  _setDiagnostics(document, problems) {
    if (!this._diag) return
    const text = document.getText()
    const SEV = {
      error: vscode.DiagnosticSeverity.Error,
      warn: vscode.DiagnosticSeverity.Warning,
      info: vscode.DiagnosticSeverity.Information,
    }
    const list = Array.isArray(problems) ? problems : []
    const diags = list.map(p => {
      const d = new vscode.Diagnostic(
        this._locateRange(text, p.anchor),
        (p.label ? p.label + ': ' : '') + (p.msg || ''),
        SEV[p.sev] || vscode.DiagnosticSeverity.Information
      )
      d.source = 'Flogo Data Pipeline'
      return d
    })
    this._diag.set(document.uri, diags)
  }

  _locateRange(text, anchor) {
    if (anchor) {
      const idx = text.indexOf('"' + anchor + '"')
      if (idx >= 0) return new vscode.Range(this._posAt(text, idx), this._posAt(text, idx + anchor.length + 2))
    }
    const nl = text.indexOf('\n')
    return new vscode.Range(0, 0, 0, nl < 0 ? Math.min(text.length, 80) : Math.min(nl, 80))
  }

  _posAt(text, offset) {
    let line = 0, col = 0
    const n = Math.min(offset, text.length)
    for (let i = 0; i < n; i++) {
      if (text.charCodeAt(i) === 10) { line++; col = 0 } else col++
    }
    return new vscode.Position(line, col)
  }

  // ── CustomEditorProvider impl ───────────────────────────────────────────────

  async resolveCustomTextEditor(document, webviewPanel, _token) {
    const extUri = this._context.extensionUri

    webviewPanel.webview.options = {
      enableScripts: true,
      localResourceRoots: [vscode.Uri.joinPath(extUri, 'src')]
    }

    webviewPanel.webview.html = this._buildHtml(webviewPanel.webview, extUri, document.getText())

    this._panels.add(webviewPanel)
    const uriKey = document.uri.toString()
    this._panelByUri.set(uriKey, webviewPanel)
    this._docByUri.set(uriKey, document)
    this._setActive(document.uri)

    webviewPanel.onDidChangeViewState(e => {
      if (e.webviewPanel.active) this._setActive(document.uri)
    })

    // If the engine is already running, inform this freshly-opened canvas.
    if (this._manager && this._manager.port) {
      webviewPanel.webview.postMessage({ type: 'runStarted', port: this._manager.port })
    }

    // Push fresh content when the file changes externally (git checkout, raw edit, etc.).
    const changeSub = vscode.workspace.onDidChangeTextDocument(e => {
      if (e.document.uri.toString() === document.uri.toString()) {
        let appDoc = {}
        try { appDoc = JSON.parse(e.document.getText()) } catch (_) { }
        webviewPanel.webview.postMessage({ type: 'update', doc: this._extractPipelineData(appDoc) })
      }
    })

    webviewPanel.onDidDispose(() => {
      this._panels.delete(webviewPanel)
      this._panelByUri.delete(uriKey)
      this._docByUri.delete(uriKey)
      changeSub.dispose()
      if (this._diag) this._diag.delete(document.uri)
    })

    webviewPanel.webview.onDidReceiveMessage(msg => this._onMessage(webviewPanel, document, msg))
  }

  // ── Message handler ─────────────────────────────────────────────────────────

  _onMessage(panel, doc, msg) {
    const post = m => { try { panel.webview.postMessage(m) } catch (_) { } }

    switch (msg.type) {

      case 'ready': {
        let appDoc = {}
        try { appDoc = JSON.parse(doc.getText()) } catch (_) { }
        post({ type: 'init', doc: this._extractPipelineData(appDoc), nonce: this._nonce() })
        break
      }

      case 'update': {
        if (typeof msg.text !== 'string') break
        let appDoc = {}
        try { appDoc = JSON.parse(doc.getText()) } catch (_) { }
        let pd = {}
        try { pd = JSON.parse(msg.text) } catch (_) { break }
        if (appDoc.type === 'flogo:app' && Array.isArray(appDoc.resources) && appDoc.resources[0]) {
          // Update resources[0].data in-place; keep resource id in sync with pipeline name
          appDoc.resources[0].data = pd
          if (pd.name) {
            appDoc.name = pd.name
            const newId = 'pipeline:' + pd.name.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '')
            if (appDoc.resources[0].id !== newId) {
              appDoc.resources[0].id = newId
              const h = appDoc.triggers && appDoc.triggers[0] && appDoc.triggers[0].handlers && appDoc.triggers[0].handlers[0]
              if (h && h.action && h.action.settings) h.action.settings.pipelineRef = 'res://' + newId
            }
          }
          this._applyEdit(doc, JSON.stringify(appDoc, null, 2))
        } else {
          // Auto-migrate old flat format to flogo:app on first save
          this._applyEdit(doc, JSON.stringify(this._wrapInAppJson(pd), null, 2))
        }
        break
      }

      case 'run': {
        this._runPipeline(doc, panel)
        break
      }

      case 'stop': {
        this._stopPipeline(panel)
        break
      }

      case 'validate': {
        const issues = this._validate(doc)
        post({ type: 'validationResult', issues })
        this._setDiagnostics(doc, issues.map(i => ({
          sev: i.sev,
          label: i.label,
          msg: i.msg,
          anchor: i.anchor,
        })))
        break
      }

      case 'export': {
        this._export(doc, msg.format || 'binary')
        break
      }

      case 'openRaw': {
        vscode.commands.executeCommand('vscode.openWith', doc.uri, 'default')
          .then(undefined, err => console.error('[PipelineEditorProvider] openWith failed:', err))
        break
      }

      case 'metrics': {
        const port = this._manager && this._manager.port
        if (!port) { post({ type: 'metricsResult', error: 'Pipeline not running.' }); break }
        this._fetchMetrics(port).then(
          data => post({ type: 'metricsResult', data }),
          err => post({ type: 'metricsResult', error: err && err.message ? err.message : String(err) })
        )
        break
      }

      case 'showError': {
        if (typeof msg.text === 'string' && msg.text) {
          vscode.window.showErrorMessage(msg.text)
        }
        break
      }

      case 'validationResults': {
        // Canvas-driven validation — mirror into Problems panel.
        this._setDiagnostics(doc, msg.problems)
        break
      }

      case 'copyText': {
        if (typeof msg.text === 'string' && msg.text) {
          vscode.env.clipboard.writeText(msg.text)
        }
        break
      }

      default:
        console.warn(`[PipelineEditorProvider] unhandled message type: ${msg && msg.type}`)
        break
    }
  }

  // ── Engine lifecycle ────────────────────────────────────────────────────────

  async _runPipeline(document, webviewPanel) {
    if (!this._manager) return
    if (this._runInFlight) {
      try { webviewPanel.webview.postMessage({ type: 'error', message: 'Another pipeline is already starting.' }) } catch (_) { }
      return
    }
    this._runInFlight = true
    const post = m => { try { webviewPanel.webview.postMessage(m) } catch (_) { } }

    try {
      const cfg = vscode.workspace.getConfiguration('flogoDataPipelineStudio')
      const startPort = cfg.get('apiPort') ?? 8888
      const binPath = cfg.get('binaryPath') ?? ''

      post({ type: 'runStarting' })
      if (this._hooks.onStarting) this._hooks.onStarting()

      try {
        this._manager.stop()
        const port = await this._manager.start(document.uri.fsPath, startPort)
        if (this._hooks.onRunning) this._hooks.onRunning(port, path.basename(document.uri.fsPath, '.datapipeline'))
        post({ type: 'runStarted', port })
      } catch (err) {
        if (this._hooks.onStopped) this._hooks.onStopped()
        post({ type: 'error', message: err && err.message ? err.message : String(err) })
      }
    } finally {
      this._runInFlight = false
    }
  }

  _stopPipeline(webviewPanel) {
    if (this._manager) this._manager.stop()
    if (this._hooks.onStopped) this._hooks.onStopped()
    try { webviewPanel.webview.postMessage({ type: 'stopped' }) } catch (_) { }
  }

  // ── flogo:app wrap / unwrap helpers ─────────────────────────────────────────

  // Extract the pipeline data (resources[0].data) from a flogo:app JSON.
  // Falls back to the whole object for old flat-format files (auto-migration on next save).
  _extractPipelineData(appDoc) {
    if (appDoc && appDoc.type === 'flogo:app') {
      const res = Array.isArray(appDoc.resources) ? appDoc.resources[0] : null
      return (res && res.data) || {}
    }
    return appDoc || {}
  }

  // Wrap a pipeline data object into a complete flogo:app JSON.
  // Mirrors the pattern from flogo-agent: the .datapipeline file IS the flogo:app.
  _wrapInAppJson(pd) {
    const name = pd.name || 'untitled-pipeline'
    const safeId = name.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '')
    const resourceId = 'pipeline:' + safeId
    return {
      name,
      type: 'flogo:app',
      version: pd.version || '1.0.0',
      description: pd.description || '',
      appModel: '1.1.1',
      imports: [
        'github.com/mpandav-tibco/flogo-datapipeline/action/pipeline',
        'github.com/project-flogo/contrib/trigger/timer'
      ],
      triggers: [{
        id: 'pipeline-timer',
        ref: 'github.com/project-flogo/contrib/trigger/timer',
        settings: { repeating: true, startDelay: 0, interval: 86400000 },
        handlers: [{
          settings: {},
          action: {
            ref: 'github.com/mpandav-tibco/flogo-datapipeline/action/pipeline',
            settings: { pipelineRef: 'res://' + resourceId }
          }
        }]
      }],
      resources: [{ id: resourceId, data: pd }]
    }
  }

  // ── Validation ──────────────────────────────────────────────────────────────

  _validate(document) {
    const issues = []
    let appDoc = {}
    try { appDoc = JSON.parse(document.getText()) } catch (_) {
      return [{ sev: 'error', label: 'JSON', msg: 'Pipeline file contains invalid JSON.', anchor: null }]
    }
    const doc = this._extractPipelineData(appDoc)

    const source = doc.source || {}
    const sink = doc.sink || {}
    const transforms = Array.isArray(doc.transforms) ? doc.transforms : []
    const mode = (doc.mode || 'etl').toLowerCase()

    // Source checks
    if (!source.type) {
      issues.push({ sev: 'error', label: 'source', msg: 'Source type is required.', anchor: 'source' })
    } else {
      if (source.type === 'jdbc' && !source.dsn) {
        issues.push({ sev: 'error', label: 'source.dsn', msg: 'JDBC source requires a DSN connection string.', anchor: 'dsn' })
      }
      if (source.type === 'kafka' && !source.brokers) {
        issues.push({ sev: 'error', label: 'source.brokers', msg: 'Kafka source requires at least one broker address.', anchor: 'brokers' })
      }
      if ((source.type === 'kafka' || source.type === 'jdbc') && !source.topic && !source.query) {
        issues.push({ sev: 'warn', label: 'source', msg: 'Source has no query or topic configured.', anchor: 'source' })
      }
      if (source.type === 'file' && !source.path) {
        issues.push({ sev: 'error', label: 'source.path', msg: 'File source requires a path.', anchor: 'path' })
      }
      if (source.type === 'rest' && !source.url) {
        issues.push({ sev: 'error', label: 'source.url', msg: 'REST source requires a URL.', anchor: 'url' })
      }
    }

    // Sink checks
    if (!sink.type) {
      issues.push({ sev: 'error', label: 'sink', msg: 'Sink type is required.', anchor: 'sink' })
    } else {
      if (sink.type === 'jdbc' && !sink.dsn) {
        issues.push({ sev: 'error', label: 'sink.dsn', msg: 'JDBC sink requires a DSN connection string.', anchor: 'dsn' })
      }
      if (sink.type === 'kafka' && !sink.brokers) {
        issues.push({ sev: 'error', label: 'sink.brokers', msg: 'Kafka sink requires at least one broker address.', anchor: 'brokers' })
      }
      if (sink.type === 'file' && !sink.path) {
        issues.push({ sev: 'error', label: 'sink.path', msg: 'File sink requires an output path.', anchor: 'path' })
      }
      if (sink.type === 'rest' && !sink.url) {
        issues.push({ sev: 'error', label: 'sink.url', msg: 'REST sink requires a target URL.', anchor: 'url' })
      }
    }

    // Mode-specific checks
    if (mode === 'etl' && transforms.length === 0) {
      issues.push({ sev: 'warn', label: 'mode', msg: 'ETL mode is selected but no transforms are defined. Add at least one transform or switch to EL mode.', anchor: 'transforms' })
    }
    if (mode === 'el' && transforms.length > 0) {
      issues.push({ sev: 'warn', label: 'mode', msg: 'EL (Extract-Load) mode is selected but transforms are present. They will be ignored at runtime.', anchor: 'transforms' })
    }

    // Transform checks
    for (let i = 0; i < transforms.length; i++) {
      const t = transforms[i]
      if (!t || !t.type) {
        issues.push({ sev: 'error', label: `transforms[${i}]`, msg: `Transform at index ${i} has no type.`, anchor: 'transforms' })
      }
    }

    // Performance sanity
    const perf = doc.performance || {}
    if (perf.batchSize && Number(perf.batchSize) > 100000) {
      issues.push({ sev: 'warn', label: 'batchSize', msg: 'Batch size above 100,000 may cause excessive memory pressure.', anchor: 'batchSize' })
    }
    if (perf.workers && Number(perf.workers) > 64) {
      issues.push({ sev: 'warn', label: 'workers', msg: 'Worker count above 64 is unusual — verify the target system supports this concurrency.', anchor: 'workers' })
    }

    return issues
  }

  // ── Metrics fetch ───────────────────────────────────────────────────────────

  _fetchMetrics(port) {
    return new Promise((resolve, reject) => {
      const req = http.get({
        hostname: '127.0.0.1',
        port,
        path: '/api/pipeline/status',
        headers: { Accept: 'application/json' },
      }, res => {
        let raw = ''
        res.on('data', chunk => { raw += chunk })
        res.on('end', () => {
          try { resolve(JSON.parse(raw)) } catch (_) { reject(new Error('Invalid JSON from metrics endpoint.')) }
        })
        res.on('error', reject)
      })
      req.on('error', reject)
      req.setTimeout(5000, () => { req.destroy(); reject(new Error('Metrics request timed out.')) })
    })
  }

  // ── Export ──────────────────────────────────────────────────────────────────

  async _export(document, format) {
    const pipelinePath = document.uri.fsPath
    const pipelineDir = path.dirname(pipelinePath)
    const pipelineName = path.basename(pipelinePath, '.datapipeline')
    const safeName = this._sanitizeName(pipelineName)
    let pipelineJson = {}
    try { pipelineJson = JSON.parse(fs.readFileSync(pipelinePath, 'utf8')) } catch (_) { }

    const port = (pipelineJson.run && pipelineJson.run.apiPort) || 8888

    try {
      if (format === 'binary') {
        await this._buildBinary(pipelinePath, pipelineName)
      } else if (format === 'dockerfile') {
        const dockerfile = this._generateDockerfile(safeName, path.basename(pipelinePath), port)
        const outPath = path.join(pipelineDir, 'Dockerfile')
        fs.writeFileSync(outPath, dockerfile, 'utf8')
        vscode.window.showTextDocument(vscode.Uri.file(outPath))
          .then(undefined, err => console.error('[PipelineEditorProvider] showTextDocument failed:', err))
      } else if (format === 'image') {
        const dockerfile = this._generateDockerfile(safeName, path.basename(pipelinePath), port)
        fs.writeFileSync(path.join(pipelineDir, 'Dockerfile'), dockerfile, 'utf8')
        const term = vscode.window.createTerminal({ name: `Flogo Pipeline — docker build`, cwd: pipelineDir })
        term.show(true)
        term.sendText(`docker build -t ${safeName}:latest .`)
      }
    } catch (err) {
      vscode.window.showErrorMessage(`Export failed: ${err && err.message ? err.message : String(err)}`)
    }
  }

  async _buildBinary(pipelinePath, pipelineName) {
    const targets = [
      { label: 'macOS · Apple Silicon (arm64)', goos: 'darwin', goarch: 'arm64' },
      { label: 'macOS · Intel (amd64)', goos: 'darwin', goarch: 'amd64' },
      { label: 'Linux · x86-64 (amd64)', goos: 'linux', goarch: 'amd64' },
      { label: 'Linux · ARM64 (arm64)', goos: 'linux', goarch: 'arm64' },
      { label: 'Windows · x86-64 (amd64)', goos: 'windows', goarch: 'amd64' },
      { label: 'Windows · ARM64 (arm64)', goos: 'windows', goarch: 'arm64' },
    ]
    const hostGoos = process.platform === 'win32' ? 'windows' : process.platform
    const hostGoarch = process.arch === 'x64' ? 'amd64' : process.arch
    for (const t of targets) {
      if (t.goos === hostGoos && t.goarch === hostGoarch) { t.label += '  ·  this machine'; t.host = true }
    }
    targets.sort((a, b) => (b.host ? 1 : 0) - (a.host ? 1 : 0))

    const pick = await vscode.window.showQuickPick(targets, {
      title: `Build binary — ${pipelineName}`,
      placeHolder: 'Select the target platform & architecture',
    })
    if (!pick) return

    const safeName = this._sanitizeName(pipelineName)
    const binName = safeName + `-${pick.goos}-${pick.goarch}` + (pick.goos === 'windows' ? '.exe' : '')
    const out = path.join(path.dirname(pipelinePath), binName)

    const sourcePath = this._resolveModuleRoot()
    if (!sourcePath) {
      const sel = await vscode.window.showErrorMessage(
        'Cannot build: the flogo-datapipeline Go module was not found. Set "flogoDataPipelineStudio.sourcePath" to the module root, then try again.',
        'Open Settings'
      )
      if (sel === 'Open Settings') vscode.commands.executeCommand('workbench.action.openSettings', 'flogoDataPipelineStudio.sourcePath')
      return
    }

    try {
      await vscode.window.withProgress(
        { location: vscode.ProgressLocation.Notification, title: `Building ${binName}…`, cancellable: false },
        () => this._manager.buildBinary({ sourcePath, out, goos: pick.goos, goarch: pick.goarch, pipelinePath })
      )
    } catch (err) {
      vscode.window.showErrorMessage(`Build failed: ${err && err.message ? err.message : String(err)}`)
      return
    }

    const sel = await vscode.window.showInformationMessage(`Built ${binName}`, 'Reveal', 'Copy Path')
    if (sel === 'Reveal') vscode.commands.executeCommand('revealFileInOS', vscode.Uri.file(out))
    else if (sel === 'Copy Path') vscode.env.clipboard.writeText(out)
  }

  _resolveModuleRoot() {
    const isRoot = d => !!d && fs.existsSync(path.join(d, 'go.mod'))
    const sp = (vscode.workspace.getConfiguration('flogoDataPipelineStudio').get('sourcePath') || '').trim()
    if (isRoot(sp)) return sp
    for (const f of (vscode.workspace.workspaceFolders || [])) {
      if (isRoot(f.uri.fsPath)) return f.uri.fsPath
    }
    return null
  }

  _generateDockerfile(safeName, pipelineFileName, apiPort) {
    return [
      `# Dockerfile generated by Flogo Data Pipeline Studio for "${safeName}".`,
      `# Build: docker build -t ${safeName}:latest .`,
      `# Run:   docker run -p ${apiPort}:${apiPort} ${safeName}:latest`,
      '',
      'FROM gcr.io/distroless/static-debian12:nonroot',
      'ARG TARGETARCH',
      'WORKDIR /app',
      `COPY flogo-datapipeline-linux-\${TARGETARCH} /app/flogo-datapipeline`,
      `COPY ${pipelineFileName} /app/pipeline.datapipeline`,
      `EXPOSE ${apiPort}`,
      `ENTRYPOINT ["/app/flogo-datapipeline", "-f", "/app/pipeline.datapipeline", "-api-port", "${apiPort}"]`,
      '',
    ].join('\n')
  }

  // ── Edit helpers ────────────────────────────────────────────────────────────

  _applyEdit(document, text) {
    const edit = new vscode.WorkspaceEdit()
    edit.replace(document.uri, new vscode.Range(0, 0, document.lineCount, 0), text)
    vscode.workspace.applyEdit(edit).then(ok => {
      if (!ok) vscode.window.showWarningMessage('Flogo Data Pipeline: edit could not be applied.')
    }, err => {
      vscode.window.showErrorMessage(`Flogo Data Pipeline: edit failed — ${err && err.message ? err.message : String(err)}`)
    })
  }

  // ── Public façade methods called by extension.js commands ───────────────────

  // Open the canvas for `uri` then trigger a binary run.
  async runBinary(uri) {
    const panel = await this._awaitPanel(uri)
    if (panel) {
      try { panel.webview.postMessage({ type: 'run' }) } catch (_) { }
    }
  }

  // Build a Docker image with `imageTag` for the pipeline at `uri`.
  async runDocker(uri, imageTag) {
    const pipelinePath = uri.fsPath
    const pipelineName = path.basename(pipelinePath, '.datapipeline')
    const safeName = this._sanitizeName(imageTag || pipelineName)
    const dockerfile = this._generateDockerfile(safeName, path.basename(pipelinePath), 9080)
    const pipelineDir = path.dirname(pipelinePath)
    const fs2 = require('fs')
    fs2.writeFileSync(path.join(pipelineDir, 'Dockerfile'), dockerfile, 'utf8')
    const term = vscode.window.createTerminal({ name: 'Flogo Pipeline — docker build', cwd: pipelineDir })
    term.show(true)
    term.sendText(`docker build -t ${safeName}:latest . && docker run -p 9080:9080 ${safeName}:latest`)
  }

  // Export pipeline at `uri` using the given format string ('binary'|'dockerfile'|'image'|'platform').
  async exportFromUri(uri, format) {
    const uriKey = uri.toString()
    const doc = this._docByUri.get(uriKey)
    if (doc) {
      await this._export(doc, format)
    } else {
      // Fall back to reading the file directly
      const content = require('fs').readFileSync(uri.fsPath, 'utf8')
      const fakeDoc = {
        uri,
        getText: () => content,
        lineCount: content.split('\n').length,
      }
      await this._export(/** @type {any} */ (fakeDoc), format)
    }
  }

  // Returns a default Docker image tag derived from the pipeline filename.
  defaultImageTag(uri) {
    const name = path.basename(uri.fsPath, '.datapipeline')
    return this._sanitizeName(name) + ':latest'
  }

  // ── HTML helpers ────────────────────────────────────────────────────────────

  _htmlAttr(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/"/g, '&quot;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
  }

  _nonce() {
    return crypto.randomBytes(24).toString('base64url')
  }

  _sanitizeName(rawName) {
    const safe = String(rawName || 'pipeline')
      .toLowerCase()
      .replace(/[^a-z0-9-]/g, '-')
      .replace(/^-+|-+$/g, '')
      .slice(0, 63)
    return safe || 'pipeline'
  }

  // ── HTML builder ────────────────────────────────────────────────────────────

  _buildHtml(webview, extUri, docText) {
    const nonce = this._nonce()
    const cspSrc = webview.cspSource
    const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(extUri, 'src', 'pipelineCanvas.js'))

    const csp = [
      `default-src 'none'`,
      `style-src 'unsafe-inline' ${cspSrc}`,
      `script-src 'nonce-${nonce}' ${cspSrc}`,
      `img-src data: ${cspSrc}`,
      `connect-src http://localhost:* http://127.0.0.1:*`,
    ].join('; ')

    const docAttr = this._htmlAttr(docText)

    return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta http-equiv="Content-Security-Policy" content="${csp}">
<style>
:root{
  --bg:var(--vscode-editor-background,#1e1e1e);
  --bg2:var(--vscode-sideBar-background,#252526);
  --bg3:var(--vscode-input-background,#3c3c3c);
  --border:var(--vscode-panel-border,#404040);
  --fg:var(--vscode-editor-foreground,#ccc);
  --fg2:var(--vscode-descriptionForeground,#888);
  --text:var(--vscode-editor-foreground,#ccc);
  --text2:var(--vscode-descriptionForeground,#888);
  --accent:#0078d4;
  --accentH:#1184d9;
  --accent-dim:rgba(0,120,212,.18);
  --inbdr:var(--vscode-input-border,#555);
  --focus:var(--vscode-focusBorder,#007fd4);
  --err:#f85149;--warn:#d29922;--ok:#3fb950;
  --c-src:#1e7d5c;--c-src-dim:rgba(30,125,92,.18);
  --c-xform:#1774e5;--c-xform-dim:rgba(23,116,229,.14);
  --c-sink:#c87a10;--c-sink-dim:rgba(200,122,16,.16);
  --c-dlq:#c0392b;--c-dlq-dim:rgba(192,57,43,.14);
  --teal:#4ec9b0;
  --danger:#f48771;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:var(--vscode-font-family,'Segoe UI',sans-serif);
  font-size:13px;height:100vh;display:flex;flex-direction:column;overflow:hidden;user-select:none}

/* ── Meta bar ── */
.meta-bar{display:flex;align-items:center;gap:8px;height:44px;padding:0 12px;
  background:var(--bg2);border-bottom:1px solid var(--border);flex-shrink:0}
.meta-icon{width:24px;height:24px;background:linear-gradient(135deg,#006040,#00a060);border-radius:5px;
  display:flex;align-items:center;justify-content:center;flex-shrink:0;box-shadow:0 1px 4px rgba(0,0,0,.4)}
.meta-name{background:transparent;border:none;color:var(--text);font-size:13px;font-weight:600;
  min-width:80px;max-width:220px;outline:none;border-radius:3px;padding:2px 5px;font-family:inherit}
.meta-name:hover,.meta-name:focus{background:var(--bg3)}
.meta-desc{background:transparent;border:none;color:var(--text2);font-size:11px;min-width:80px;
  outline:none;border-radius:3px;padding:2px 5px;flex:1;max-width:380px;font-family:inherit}
.meta-desc:hover,.meta-desc:focus{background:var(--bg3)}
.meta-spacer{flex:1}
.meta-vsep{width:1px;height:18px;background:var(--border);margin:0 2px;flex-shrink:0}
/* Mode badge — ETL/ELT/EL cycles on click */
.meta-mode{font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;
  padding:3px 10px;border-radius:10px;cursor:pointer;user-select:none;border:1px solid;
  transition:background .15s,color .15s,border-color .15s;flex-shrink:0}
.meta-mode[data-mode="etl"]{background:rgba(0,120,212,.16);color:#4da3ff;border-color:rgba(0,120,212,.45)}
.meta-mode[data-mode="elt"]{background:rgba(78,201,176,.14);color:#4ec9b0;border-color:rgba(78,201,176,.4)}
.meta-mode[data-mode="el"]{background:rgba(210,153,34,.14);color:#d29922;border-color:rgba(210,153,34,.4)}
.meta-bar button{font-size:11px;padding:4px 10px}

/* ── Top-level tab bar ── */
.top-tabs{display:flex;align-items:center;height:36px;padding:0 6px;
  background:var(--bg2);border-bottom:1px solid var(--border);flex-shrink:0}
.top-tab{padding:0 16px;height:36px;line-height:36px;font-size:11px;font-weight:500;
  color:var(--text2);cursor:pointer;border-bottom:2px solid transparent;
  white-space:nowrap;user-select:none;transition:color .15s}
.top-tab:hover{color:var(--text)}
.top-tab.active{color:var(--accent);border-bottom-color:var(--accent)}
.top-tabs-spacer{flex:1}

/* ── Tab content containers ── */
.ttab-content{display:none;flex:1;min-height:0;overflow:hidden;flex-direction:column}
.ttab-content.active{display:flex}

/* ── Workspace layout (Design tab) ── */
.ws{display:flex;flex-direction:column;flex:1;overflow:hidden}
.ws-top{display:flex;flex:1;overflow:hidden}

/* ── Sidebar rail ── */
.sidebar{display:flex;flex-direction:row;flex-shrink:0}
.sidebar-rail{width:68px;flex-shrink:0;background:var(--bg2);border-right:1px solid var(--border);
  display:flex;flex-direction:column;align-items:stretch;padding:6px 0;gap:1px;z-index:10}
.rail-btn{width:100%;height:60px;border:none;border-radius:0;background:transparent;
  color:var(--text2);cursor:pointer;display:flex;flex-direction:column;align-items:center;
  justify-content:center;gap:4px;transition:background .12s,color .12s;position:relative;
  padding:0 4px;line-height:1}
.rail-btn:hover{background:var(--bg3);color:var(--text)}
.rail-btn.active{background:rgba(0,120,212,.10);color:var(--accent)}
.rail-btn.active::before{content:'';position:absolute;left:0;top:10px;bottom:10px;
  width:2px;background:var(--accent);border-radius:0 2px 2px 0}
.rb-ico{font-size:20px;line-height:1;display:block}
.rb-lbl{font-size:10px;line-height:1;text-align:center;display:block;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:60px}
.sidebar-panel{width:0;overflow:hidden;background:var(--bg2);border-right:1px solid transparent;
  transition:width .15s ease,border-color .15s}
.sidebar-panel.open{border-right-color:var(--border)}
.spane{display:none;flex-direction:column;padding:10px 8px;gap:4px;overflow-y:auto;height:100%;width:200px}
.spane.active{display:flex}

/* Palette */
.pal-title{font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:var(--text2);
  padding:4px 4px 8px;border-bottom:1px solid var(--border);margin-bottom:4px;flex-shrink:0}
.pal-sub{font-size:10px;color:var(--text2);margin-bottom:6px;flex-shrink:0}
.pal-item{display:flex;align-items:center;gap:8px;padding:7px 10px;border-radius:4px;
  border:1px solid var(--border);cursor:grab;font-size:11px;color:var(--text2);
  background:var(--bg);transition:border-color .15s,color .15s,background .15s;position:relative}
.pal-item:hover{border-color:var(--accent);color:var(--text);background:rgba(0,120,212,.06)}
.pal-item:active{cursor:grabbing}
.pal-sep{height:1px;background:var(--border);margin:6px 0}
/* Source palette items — green accent */
.pal-src{border-left:2px solid rgba(30,125,92,.5)}
.pal-src:hover{border-left-color:var(--c-src)}
/* Transform palette items — blue accent */
.pal-xform{border-left:2px solid rgba(23,116,229,.4)}
.pal-xform:hover{border-left-color:var(--c-xform)}
/* Sink palette items — orange accent */
.pal-snk{border-left:2px solid rgba(200,122,16,.45)}
.pal-snk:hover{border-left-color:var(--c-sink)}
.pal-ico{font-size:16px;flex-shrink:0}
.pal-hint{margin-top:auto;border-top:1px solid var(--border);padding-top:10px;
  font-size:10px;color:var(--text2);text-align:center;line-height:1.5}

/* ── Canvas wrapper (flogo3 pattern) ── */
.cwrap{flex:1;position:relative;overflow:hidden;background:var(--bg);cursor:default}
.cwrap::before{content:'';position:absolute;inset:0;z-index:0;
  background-image:radial-gradient(circle,rgba(255,255,255,.06) 1px,transparent 1px);
  background-size:24px 24px;pointer-events:none}
.cwrap.panning{cursor:grabbing}
/* Scrollable canvas layer — overflow:auto lets nodes go anywhere (like flogo3 wi-diagram-container) */
.canvas-scroll{position:absolute;inset:0;overflow:auto;z-index:1}
/* #canvas is the positioning root for all nodes and SVG edges */
#canvas{position:relative;min-width:1200px;min-height:600px;transform-origin:0 0}
/* SVG edge layer — 1px intrinsic so it doesn't expand scroll; overflow:visible handles long edges */
#esvg{position:absolute;top:0;left:0;width:1px;height:1px;pointer-events:none;overflow:visible}
#nodes-layer{position:absolute;top:0;left:0;width:100%;height:100%}
.zoom-lvl{position:absolute;right:12px;bottom:12px;font-size:10px;color:rgba(255,255,255,.22);
  pointer-events:none;user-select:none;font-variant-numeric:tabular-nums;letter-spacing:.04em;
  background:rgba(0,0,0,.25);padding:2px 6px;border-radius:4px}

/* ── Pipeline node cards ── */
.node{position:absolute;width:220px;border-radius:7px;border:1.5px solid transparent;
  box-shadow:0 2px 10px rgba(0,0,0,.4);cursor:pointer;transition:border-color .15s,box-shadow .15s}
.node:hover{box-shadow:0 4px 18px rgba(0,0,0,.55)}
.node.selected{box-shadow:0 0 0 2px rgba(0,120,212,.3)!important}
/* Node type: Source */
.node-source{border-color:rgba(30,125,92,.55)}.node-source.selected{border-color:var(--c-src)!important}
/* Node type: Transform */
.node-transform{border-color:rgba(23,116,229,.45)}.node-transform.selected{border-color:var(--c-xform)!important}
/* Node type: Sink */
.node-sink{border-color:rgba(200,122,16,.5)}.node-sink.selected{border-color:var(--c-sink)!important}
/* Node type: DLQ */
.node-dlq{border:1.5px dashed rgba(192,57,43,.6)}.node-dlq.selected{border-color:var(--c-dlq)!important}
/* Node header */
.nh{padding:8px 11px;border-radius:6px 6px 0 0;display:flex;align-items:center;gap:8px;position:relative}
.nh-ico{font-size:15px;flex-shrink:0;opacity:.9}
.nh-lbl{font-size:11px;font-weight:700;letter-spacing:.04em;flex:1;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:#fff}
.nh-typ{font-size:9px;padding:2px 6px;border-radius:8px;background:rgba(0,0,0,.3);
  color:rgba(255,255,255,.75);letter-spacing:.04em}
/* Header gradient variants */
.nh-src{background:linear-gradient(135deg,#0d4a30,#1a7a50)}
.nh-xform{background:linear-gradient(135deg,#0a2a5c,#1258a8)}
.nh-sink{background:linear-gradient(135deg,#4a2800,#8a4f10)}
.nh-dlq{background:linear-gradient(135deg,#4a0808,#9c1c1c)}
/* Node body */
.nb{padding:10px 12px;background:var(--bg2);border-radius:0 0 6px 6px;font-size:11px}
.nb-row{display:flex;align-items:flex-start;gap:6px;margin-bottom:4px;overflow:hidden}
.nb-row:last-child{margin-bottom:0}
.nb-k{color:var(--text2);flex-shrink:0;width:56px;text-align:right;font-size:10px;padding-top:1px}
.nb-v{color:var(--text);font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
/* Delete button — top-right corner of card, hidden until hover (flogo-agent pattern) */
.node-del-btn{position:absolute;top:5px;right:5px;width:18px;height:18px;border-radius:50%;
  background:rgba(0,0,0,.3);border:1px solid transparent;
  color:rgba(255,255,255,0);font-size:11px;line-height:1;cursor:pointer;
  display:flex;align-items:center;justify-content:center;padding:0;z-index:12;
  transition:color .12s,border-color .12s,background .12s}
.node:hover .node-del-btn{color:rgba(255,255,255,.55);border-color:rgba(255,255,255,.2)}
.node-del-btn:hover{color:#fff!important;background:rgba(244,135,113,.8)!important;border-color:transparent!important}
/* Cursor: grab when hovering idle node, grabbing while dragging */
.node{cursor:grab}.node.dragging{cursor:grabbing}
/* Connection ports */
.port-out{position:absolute;right:-7px;top:50%;transform:translateY(-50%);
  width:13px;height:13px;border-radius:50%;border:2px solid rgba(78,201,176,.3);
  background:var(--bg2);z-index:5;transition:border-color .15s}
.port-in{position:absolute;left:-7px;top:50%;transform:translateY(-50%);
  width:13px;height:13px;border-radius:50%;border:2px solid rgba(78,201,176,.3);
  background:var(--bg2);z-index:5;transition:border-color .15s}
.node:hover .port-out,.node:hover .port-in{border-color:var(--teal)}
/* SVG edges */
.edge{stroke:rgba(255,255,255,.15);stroke-width:2;fill:none}
.edge-src{stroke:rgba(30,125,92,.5)}
.edge-xform{stroke:rgba(23,116,229,.4)}
.edge-sink{stroke:rgba(200,122,16,.45)}
.edge-dlq{stroke:rgba(192,57,43,.5);stroke-dasharray:6 4}
/* Running flow animation */
.edge-flowing{animation:edgeFlow .6s linear infinite}
@keyframes edgeFlow{to{stroke-dashoffset:-24}}

/* ── Properties panel (bottom drawer, resizable) ── */
.pp{position:absolute;left:0;right:0;bottom:0;height:0;border-top:0 solid var(--border);
  background:var(--bg2);display:flex;flex-direction:column;overflow:hidden;z-index:25;
  transition:height .18s ease,border-top-width 0s .18s}
.pp.pp-open{height:300px;border-top-width:1px;transition:height .18s ease}
/* Top-edge drag handle for height resize */
.pp-resize{position:absolute;top:0;left:0;right:0;height:5px;cursor:ns-resize;z-index:26;
  background:transparent;transition:background .12s}
.pp-resize:hover,.pp-resize.dragging{background:var(--accent);opacity:.4}
/* Static header bar: title + close button */
.pp-hdr-bar{display:flex;align-items:center;gap:8px;padding:0 10px 0 14px;height:36px;
  border-bottom:1px solid var(--border);flex-shrink:0;background:var(--bg2)}
.pp-hdr-title{font-size:11px;font-weight:700;flex:1;white-space:nowrap;overflow:hidden;
  text-overflow:ellipsis;text-transform:uppercase;letter-spacing:.06em;color:var(--text2)}
.pp-close-btn{background:transparent;border:1px solid transparent;color:var(--text2);
  width:24px;height:24px;font-size:18px;line-height:1;border-radius:3px;cursor:pointer;
  font-family:inherit;display:flex;align-items:center;justify-content:center;flex-shrink:0;padding:0}
.pp-close-btn:hover{color:var(--text);border-color:var(--inbdr);background:var(--bg3)}
/* Scrollable form area */
#pp-content{display:flex;flex-direction:column;flex:1;overflow-y:auto;min-height:0;padding:10px 14px 14px}
/* Field grid — wraps horizontally inside the wide bottom drawer */
.pp-fields-wrap{display:flex;flex-wrap:wrap;gap:8px 20px;align-items:flex-start;align-content:flex-start}
.pp-field{flex:1;min-width:200px;max-width:320px}
.pp-field-full{flex:0 0 100%;max-width:100%}
/* Action bar (Save / Discard / Delete) at the bottom of the form */
.pp-action-bar{display:flex;align-items:center;gap:8px;padding-top:10px;margin-top:4px;
  border-top:1px solid var(--border);flex-shrink:0;flex-wrap:wrap}
.pp-empty{display:flex;flex-direction:column;align-items:center;justify-content:center;
  width:100%;color:var(--text2);font-size:11px;text-align:center;gap:8px}
.pp-empty-ico{font-size:2rem;opacity:.35}
.section{font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:var(--text2);
  padding-bottom:6px;border-bottom:1px solid var(--border);margin-bottom:12px}

/* ── Form fields ── */
.field{margin-bottom:12px}
.field label{display:flex;align-items:center;font-size:11px;color:var(--text2);margin-bottom:4px}
.field-hint{font-size:10px;color:var(--text2);margin-top:3px}
.frow{display:flex;gap:8px}.frow .field{flex:1;margin-bottom:0}
input[type=text],input[type=number],input[type=password],select,textarea{
  width:100%;background:var(--bg3);color:var(--text);border:1px solid var(--inbdr);
  border-radius:3px;padding:5px 8px;font-family:inherit;font-size:12px;outline:none}
input:focus,select:focus,textarea:focus{border-color:var(--focus)}
select option{background:var(--bg2)}
textarea{resize:vertical;min-height:70px;line-height:1.5}
input[type=range]{width:100%;accent-color:var(--accent);cursor:pointer}
.check-row{display:flex;align-items:center;gap:7px;font-size:11px;color:var(--text2)}
input[type=checkbox]{width:14px;height:14px;cursor:pointer;accent-color:var(--accent)}
.perf-num{width:120px!important;text-align:right;font-family:var(--vscode-editor-font-family,monospace);
  font-size:12px;font-weight:600}

/* ── Buttons ── */
button{font-family:inherit;font-size:12px;cursor:pointer;border:none;border-radius:3px;padding:5px 12px}
.btn-p{background:var(--accent);color:#fff}.btn-p:hover{background:var(--accentH)}
.btn-g{background:transparent;border:1px solid var(--inbdr);color:var(--text2)}
.btn-g:hover{border-color:var(--text2);color:var(--text)}
.btn-d{background:transparent;border:1px solid var(--danger);color:var(--danger);padding:4px 10px;font-size:11px}
.btn-d:hover{background:var(--danger);color:#fff}
.btn-full{width:100%;margin-bottom:6px}
.meta-bar button{font-size:11px;padding:4px 10px}

/* ── Validation panel (slide-up from bottom of canvas) ── */
.val-panel{position:absolute;left:0;right:0;bottom:0;height:180px;display:flex;
  flex-direction:column;background:var(--bg2);border-top:1px solid var(--border);
  box-shadow:0 -4px 16px rgba(0,0,0,.28);z-index:30}
.val-head{display:flex;align-items:center;gap:8px;padding:6px 12px;
  border-bottom:1px solid var(--border);flex-shrink:0}
.val-title{font-size:11px;font-weight:600;color:var(--text)}
.val-head-spacer{flex:1}
.val-list{overflow:auto;padding:4px 0;flex:1}
.val-empty{padding:22px;text-align:center;color:var(--text2);font-size:12px}
.val-row{display:flex;align-items:flex-start;gap:9px;padding:5px 12px;font-size:11px;
  cursor:default;border-left:2px solid transparent}
.val-row:hover{background:var(--bg3)}
.val-row.val-error{border-left-color:var(--err)}
.val-row.val-warn{border-left-color:var(--warn)}
.val-row.val-info{border-left-color:#58a6ff}
.val-sev{flex-shrink:0;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.4px;
  padding:1px 6px;border-radius:3px;min-width:48px;text-align:center;line-height:1.5}
.val-sev-error{color:var(--err);background:rgba(248,81,73,.13);border:1px solid rgba(248,81,73,.4)}
.val-sev-warn{color:var(--warn);background:rgba(210,153,34,.13);border:1px solid rgba(210,153,34,.4)}
.val-sev-info{color:#58a6ff;background:rgba(88,166,255,.13);border:1px solid rgba(88,166,255,.4)}
.val-label{flex-shrink:0;font-weight:600;color:var(--text);min-width:72px;max-width:140px;
  overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.val-msg{color:var(--text2);flex:1;line-height:1.4}
/* Validate button state variants */
.val-btn{transition:color .15s,border-color .15s}
.val-btn.val-ok{color:var(--ok);border-color:rgba(63,185,80,.4)}
.val-btn.val-warn{color:var(--warn);border-color:rgba(210,153,34,.55)}
.val-btn.val-err{color:var(--err);border-color:rgba(248,81,73,.55)}

/* ── Source columns grid ── */
.mapper-toolbar{display:flex;align-items:center;justify-content:space-between;flex-shrink:0}
.mapper-toolbar-label{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:var(--text2)}
.cols-grid,.mapper-host3{flex:0 0 100%;max-width:100%}
.cols-grid{display:grid;grid-template-columns:1fr 110px 26px;gap:3px 6px;align-items:center}
.col-row{display:contents}
.col-del-row{width:22px;height:22px;padding:0!important;font-size:13px;line-height:1;
  text-align:center;border:1px solid transparent;border-radius:3px;background:transparent;
  color:var(--text2);cursor:pointer;display:flex;align-items:center;justify-content:center}
.col-del-row:hover{border-color:var(--danger);color:var(--danger)}
.mapper-col-hdr{font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;
  color:var(--text2);padding-bottom:3px;border-bottom:1px solid var(--border);margin-bottom:2px;
  position:sticky;top:0;background:var(--bg2);z-index:1}

/* ── 3-panel mapper host ── */
.mapper-host3{display:flex;height:280px;border:1px solid var(--border);border-radius:4px;overflow:hidden}

/* Panel 1: Available Fields */
.mapper-avail{width:140px;flex-shrink:0;border-right:1px solid var(--border);
  display:flex;flex-direction:column;background:var(--bg3);overflow:hidden}
.mapper-panel-hdr{font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;
  color:var(--text2);padding:5px 8px;border-bottom:1px solid var(--border);flex-shrink:0}
.avail-list{flex:1;overflow-y:auto;padding:2px 0}
.avail-section{font-size:9px;text-transform:uppercase;letter-spacing:.06em;
  color:var(--text2);padding:4px 8px 2px;opacity:.7}
.avail-field{display:flex;align-items:center;gap:5px;padding:3px 8px;cursor:grab;
  font-size:11px;color:var(--text);border-left:2px solid transparent;user-select:none}
.avail-field:hover{background:var(--bg2);border-left-color:var(--accent)}
.avail-field:active{cursor:grabbing}
.avail-field.af-dragging{opacity:.35}
.avail-type{font-size:9px;font-family:var(--vscode-editor-font-family,monospace);
  color:var(--text2);background:var(--bg2);border:1px solid var(--border);
  border-radius:2px;padding:0 3px;min-width:24px;text-align:center;flex-shrink:0}
.avail-name{font-family:var(--vscode-editor-font-family,monospace);font-size:11px;
  color:var(--accent);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.avail-empty{font-size:10px;color:var(--text2);padding:8px;font-style:italic}

/* Panel 2: Output Fields */
.mapper-outputs{width:155px;flex-shrink:0;border-right:1px solid var(--border);
  display:flex;flex-direction:column;background:var(--bg3);overflow:hidden}
.output-list{flex:1;overflow-y:auto;padding:2px 0}
.output-field{display:flex;align-items:center;gap:6px;padding:4px 8px;cursor:pointer;
  font-size:11px;color:var(--text);border-left:2px solid transparent;user-select:none}
.output-field:hover{background:var(--bg2);border-left-color:var(--accent)}
.output-field.selected{background:rgba(0,120,212,.12);border-left-color:var(--accent)}
.of-dot{width:8px;height:8px;border-radius:50%;border:1.5px solid var(--text2);flex-shrink:0;transition:background .2s,border-color .2s}
.of-dot.mapped{background:var(--accent);border-color:var(--accent)}
.of-name{font-family:var(--vscode-editor-font-family,monospace);font-size:11px;
  overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.new-output-row{display:flex;gap:4px;padding:4px 6px;border-top:1px solid var(--border);flex-shrink:0}
.new-output-row .pp-input{min-width:0;flex:1;font-size:11px}

/* Panel 3: Expression Editor */
.mapper-expr-panel{flex:1;display:flex;flex-direction:column;overflow:hidden;min-width:0}
.fn-strip{display:flex;flex-wrap:wrap;gap:3px;padding:4px 6px;border-bottom:1px solid var(--border);
  flex-shrink:0;background:var(--bg3)}
.fn-strip-disabled .fn-btn{opacity:.4;pointer-events:none}
.fn-btn{font-size:10px;padding:2px 6px;border:1px solid var(--border);border-radius:3px;
  background:var(--bg2);color:var(--text);cursor:pointer;white-space:nowrap;line-height:1.4}
.fn-btn:hover{border-color:var(--accent);color:var(--accent);background:rgba(0,120,212,.08)}
.expr-editor-wrap{flex:1;display:flex;flex-direction:column;padding:6px;gap:4px;overflow:hidden}
.map-expr3{flex:1;resize:none;font-family:var(--vscode-editor-font-family,monospace);font-size:11px;
  min-height:0;transition:border-color .15s}
.map-expr3.drop-over{border-color:var(--accent)!important;background:rgba(0,120,212,.08)!important}
.expr-hint{font-size:10px;color:var(--text2);font-style:italic;flex-shrink:0;line-height:1.4}

/* ── Performance tab ── */
.perf-wrap{flex:1;overflow-y:auto;padding:24px;max-width:640px;display:flex;flex-direction:column;gap:20px}
.perf-section{background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:16px}
.perf-title{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;
  color:var(--text2);margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid var(--border)}

/* ── Scaling tab ── */
.scale-wrap{flex:1;overflow-y:auto;padding:24px;max-width:640px;display:flex;flex-direction:column;gap:20px}
.scale-section{background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:16px}
.scale-title{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;
  color:var(--text2);margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid var(--border)}
.radio-group{display:flex;gap:8px;flex-wrap:wrap}
.radio-card{flex:1;min-width:100px;border:1.5px solid var(--border);border-radius:5px;
  padding:10px 12px;cursor:pointer;transition:border-color .15s,background .15s}
.radio-card:hover{border-color:var(--accent);background:rgba(0,120,212,.06)}
.radio-card.active{border-color:var(--accent);background:rgba(0,120,212,.12)}
.radio-card label{cursor:pointer;font-size:12px;font-weight:600;color:var(--text)}
.radio-card .rc-desc{font-size:10px;color:var(--text2);margin-top:3px;line-height:1.45}
input[type=radio]{accent-color:var(--accent)}

/* ── Run tab ── */
.run-wrap{flex:1;display:flex;flex-direction:column;overflow:hidden;padding:16px;gap:12px}
.run-controls{display:flex;align-items:center;gap:8px;flex-shrink:0;
  padding:12px 16px;background:var(--bg2);border:1px solid var(--border);border-radius:6px}
.run-status-badge{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;
  padding:3px 10px;border-radius:10px;border:1px solid;transition:all .2s}
.run-status-badge.idle{background:var(--bg3);color:var(--text2);border-color:var(--inbdr)}
.run-status-badge.starting{background:rgba(210,153,34,.14);color:var(--warn);border-color:rgba(210,153,34,.45)}
.run-status-badge.running{background:rgba(63,185,80,.12);color:var(--ok);border-color:rgba(63,185,80,.4)}
.run-status-badge.done{background:rgba(0,120,212,.14);color:#4da3ff;border-color:rgba(0,120,212,.45)}
.run-status-badge.error{background:rgba(248,81,73,.12);color:var(--err);border-color:rgba(248,81,73,.4)}
.run-status-dot{width:8px;height:8px;border-radius:50%;background:rgba(255,255,255,.2);flex-shrink:0;transition:background .2s}
.run-status-dot.running{background:var(--ok);animation:pulse 1.2s ease-in-out infinite}
.run-status-dot.starting{background:var(--warn);animation:pulse 1s ease-in-out infinite}
.run-status-dot.error{background:var(--err)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
/* Progress bar */
.run-progress-wrap{flex-shrink:0;background:var(--bg2);border:1px solid var(--border);
  border-radius:6px;padding:12px 16px}
.run-progress-hdr{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.run-progress-lbl{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:var(--text2)}
.run-progress-pct{font-size:11px;font-family:var(--vscode-editor-font-family,monospace);color:var(--text)}
.run-progress-bar{height:6px;background:var(--bg3);border-radius:3px;overflow:hidden}
.run-progress-fill{height:100%;background:linear-gradient(90deg,var(--accent),var(--teal));
  border-radius:3px;transition:width .4s ease;width:0}
/* Metrics table */
.run-metrics-wrap{flex-shrink:0;background:var(--bg2);border:1px solid var(--border);border-radius:6px;overflow:hidden}
.run-metrics-hdr{padding:8px 14px;background:var(--bg3);border-bottom:1px solid var(--border);
  font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--text2);
  display:flex;align-items:center;justify-content:space-between}
.metrics-table{width:100%;border-collapse:collapse;font-size:12px}
.metrics-table th{padding:6px 14px;font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.05em;
  color:var(--text2);border-bottom:1px solid var(--border);text-align:left}
.metrics-table td{padding:6px 14px;border-bottom:1px solid var(--border);font-variant-numeric:tabular-nums}
.metrics-table tr:last-child td{border-bottom:none}
.metrics-table td:nth-child(2){color:var(--text);font-weight:600;text-align:right}
.metric-ok{color:var(--ok)}.metric-err{color:var(--err)}.metric-warn{color:var(--warn)}
/* Run log */
.run-log-wrap{flex:1;min-height:0;background:var(--bg2);border:1px solid var(--border);
  border-radius:6px;display:flex;flex-direction:column;overflow:hidden}
.run-log-hdr{padding:6px 12px;background:var(--bg3);border-bottom:1px solid var(--border);
  display:flex;align-items:center;justify-content:space-between;flex-shrink:0}
.run-log-title{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--text2)}
#run-log{flex:1;overflow-y:auto;padding:8px 12px;font-family:var(--vscode-editor-font-family,monospace);
  font-size:11px;line-height:1.55;color:var(--text2)}
.log-entry{padding:1px 0;white-space:pre-wrap;word-break:break-word}
.log-entry.log-info{color:var(--text2)}
.log-entry.log-warn{color:var(--warn)}
.log-entry.log-error{color:var(--err)}
.log-entry.log-ok{color:var(--ok)}

/* ── Keyboard focus ring ── */
:focus-visible{outline:2px solid var(--focus);outline-offset:1px;border-radius:2px}
.top-tab:focus-visible{outline-offset:-2px}

/* ── Reduce motion ── */
@media (prefers-reduced-motion: reduce){
  *,*::before,*::after{animation-duration:.001ms!important;animation-iteration-count:1!important;
    transition-duration:.001ms!important;scroll-behavior:auto!important}
}
</style>
</head>
<body>

<!-- Document data — read by pipelineCanvas.js via data-json attribute. -->
<div id="doc-data" data-json="${docAttr}" style="display:none"></div>

<!-- ── Meta bar ── -->
<div class="meta-bar" id="meta-bar">
  <div class="meta-icon" title="Flogo Data Pipeline Studio">
    <svg width="13" height="14" viewBox="0 0 13 14" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path d="M2 4L6.5 1.5L11 4V10L6.5 12.5L2 10V4Z" fill="rgba(255,255,255,0.85)" stroke="rgba(255,255,255,0.2)" stroke-width="0.5"/>
      <path d="M4 7H9M9 7L7 5M9 7L7 9" stroke="rgba(0,64,40,0.9)" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/>
    </svg>
  </div>
  <input class="meta-name" id="meta-name" placeholder="Pipeline name" value="" aria-label="Pipeline name"/>
  <span class="meta-vsep"></span>
  <input class="meta-desc" id="meta-desc" placeholder="Description…" value="" aria-label="Pipeline description"/>
  <div class="meta-spacer"></div>
  <span class="meta-mode" id="meta-mode" data-mode="etl" title="Pipeline mode — click to cycle ETL / ELT / EL" role="button" tabindex="0" aria-label="Pipeline mode: ETL">ETL</span>
  <span class="meta-vsep"></span>
  <button class="btn-g" id="btn-raw" title="Open raw JSON" aria-label="Open raw JSON">{ }</button>
  <button class="btn-g val-btn val-ok" id="btn-validate" title="Validate pipeline configuration">&#10003; Validate</button>
  <button class="btn-p" id="btn-run" title="Run pipeline">&#9654; Run Pipeline</button>
</div>

<!-- ── Top-level tab bar: Design | Performance | Scaling | Run ── -->
<div class="top-tabs" id="top-tabs" role="tablist" aria-label="Editor sections">
  <div class="top-tab active" data-ttab="design" role="tab" tabindex="0" aria-selected="true">&#9670; Design</div>
  <div class="top-tab" data-ttab="performance" role="tab" tabindex="-1" aria-selected="false">&#9881; Performance</div>
  <div class="top-tab" data-ttab="scaling" role="tab" tabindex="-1" aria-selected="false">&#9650; Scaling</div>
  <div class="top-tab" data-ttab="run" role="tab" tabindex="-1" aria-selected="false">&#9654; Run</div>
  <div class="top-tabs-spacer"></div>
  <button class="btn-g" id="btn-fit" title="Fit canvas to view" aria-label="Fit canvas to view" style="font-size:11px;padding:3px 9px">&#8633; Fit</button>
</div>

<!-- ═══════════════════════════════════════════════════════════════════════
     TAB 1 — Design (canvas + sidebar + properties panel)
═══════════════════════════════════════════════════════════════════════ -->
<div class="ttab-content active" id="ttab-design">
<div class="ws">
  <div class="ws-top">

    <!-- Sidebar rail + palette panels -->
    <div class="sidebar">
      <div class="sidebar-rail">
        <button class="rail-btn" id="rail-sources" data-panel="sources" title="Sources">
          <span class="rb-ico">&#8595;</span><span class="rb-lbl">Sources</span>
        </button>
        <button class="rail-btn" id="rail-transforms" data-panel="transforms" title="Transforms">
          <span class="rb-ico">&#9654;</span><span class="rb-lbl">Transforms</span>
        </button>
        <button class="rail-btn" id="rail-sinks" data-panel="sinks" title="Sinks">
          <span class="rb-ico">&#8593;</span><span class="rb-lbl">Sinks</span>
        </button>
      </div>
      <div class="sidebar-panel" id="sidebar-panel">

        <!-- Sources palette -->
        <div class="spane" id="spane-sources">
          <div class="pal-title">Sources</div>
          <div class="pal-sub">Drag onto canvas or click to add</div>
          <div class="pal-item pal-src" data-node-type="source" data-source-type="jdbc" draggable="true" title="JDBC Source — query a relational database"><span class="pal-ico">&#128200;</span><span>JDBC</span></div>
          <div class="pal-item pal-src" data-node-type="source" data-source-type="kafka" draggable="true" title="Kafka Source — consume from a topic"><span class="pal-ico">&#128233;</span><span>Kafka</span></div>
          <div class="pal-item pal-src" data-node-type="source" data-source-type="file" draggable="true" title="File Source — CSV, JSON, Parquet"><span class="pal-ico">&#128196;</span><span>File</span></div>
          <div class="pal-item pal-src" data-node-type="source" data-source-type="rest" draggable="true" title="REST Source — paginated HTTP API"><span class="pal-ico">&#127760;</span><span>REST API</span></div>
          <div class="pal-item pal-src" data-node-type="source" data-source-type="s3" draggable="true" title="S3 / Object Store Source"><span class="pal-ico">&#9729;</span><span>S3 / Object</span></div>
          <div class="pal-sep"></div>
          <div class="pal-hint">Sources read records.<br/>One source per pipeline.</div>
        </div>

        <!-- Transforms palette -->
        <div class="spane" id="spane-transforms">
          <div class="pal-title">Transforms</div>
          <div class="pal-sub">Drag or click to add; drag cards to reorder</div>
          <div class="pal-item pal-xform" data-node-type="transform" data-xform-type="map" draggable="true" title="Map — rename or remap fields"><span class="pal-ico">&#8644;</span><span>Map</span></div>
          <div class="pal-item pal-xform" data-node-type="transform" data-xform-type="filter" draggable="true" title="Filter — drop records by expression"><span class="pal-ico">&#11092;</span><span>Filter</span></div>
          <div class="pal-item pal-xform" data-node-type="transform" data-xform-type="aggregate" draggable="true" title="Aggregate — group-by + sum/count/avg"><span class="pal-ico">&#10564;</span><span>Aggregate</span></div>
          <div class="pal-item pal-xform" data-node-type="transform" data-xform-type="join" draggable="true" title="Join — enrich with a lookup dataset"><span class="pal-ico">&#8860;</span><span>Join</span></div>
          <div class="pal-item pal-xform" data-node-type="transform" data-xform-type="dedupe" draggable="true" title="Deduplicate — remove duplicate records"><span class="pal-ico">&#10013;</span><span>Deduplicate</span></div>
          <div class="pal-item pal-xform" data-node-type="transform" data-xform-type="flatten" draggable="true" title="Flatten — expand nested arrays into rows"><span class="pal-ico">&#9636;</span><span>Flatten</span></div>
          <div class="pal-item pal-xform" data-node-type="transform" data-xform-type="script" draggable="true" title="Script — custom expression or JS function"><span class="pal-ico">&#9166;</span><span>Script</span></div>
          <div class="pal-sep"></div>
          <div class="pal-hint">Transforms are applied in order.<br/>Drag to reorder.</div>
        </div>

        <!-- Sinks palette -->
        <div class="spane" id="spane-sinks">
          <div class="pal-title">Sinks</div>
          <div class="pal-sub">Drag onto canvas or click to add</div>
          <div class="pal-item pal-snk" data-node-type="sink" data-sink-type="jdbc" draggable="true" title="JDBC Sink — write to a relational database"><span class="pal-ico">&#128200;</span><span>JDBC</span></div>
          <div class="pal-item pal-snk" data-node-type="sink" data-sink-type="kafka" draggable="true" title="Kafka Sink — produce to a topic"><span class="pal-ico">&#128233;</span><span>Kafka</span></div>
          <div class="pal-item pal-snk" data-node-type="sink" data-sink-type="file" draggable="true" title="File Sink — write CSV, JSON, Parquet"><span class="pal-ico">&#128196;</span><span>File</span></div>
          <div class="pal-item pal-snk" data-node-type="sink" data-sink-type="rest" draggable="true" title="REST Sink — POST/PUT to an HTTP endpoint"><span class="pal-ico">&#127760;</span><span>REST API</span></div>
          <div class="pal-item pal-snk" data-node-type="sink" data-sink-type="s3" draggable="true" title="S3 / Object Store Sink"><span class="pal-ico">&#9729;</span><span>S3 / Object</span></div>
          <div class="pal-sep"></div>
          <div class="pal-hint">Sinks write records.<br/>One primary sink per pipeline.<br/>Optional DLQ sink for failed records.</div>
        </div>

      </div><!-- .sidebar-panel -->
    </div><!-- .sidebar -->

    <!-- Canvas area — pipeline flow visualization -->
    <div class="cwrap" id="cwrap">
      <!-- Scrollable layer (flogo3 wi-diagram-container pattern) -->
      <div class="canvas-scroll" id="canvas-scroll">
        <div id="canvas">
          <svg id="esvg"></svg>
          <div id="nodes-layer"></div>
        </div>
      </div>
      <div class="zoom-lvl" id="zoom-lvl">100%</div>

      <!-- Validation panel — slides up from canvas bottom -->
      <div class="val-panel" id="val-panel" style="display:none">
        <div class="val-head">
          <span class="val-title" id="val-title">Validation</span>
          <div class="val-head-spacer"></div>
          <button class="btn-g" id="btn-val-close" title="Close" aria-label="Close validation results" style="font-size:11px;padding:3px 9px">&#10005; Close</button>
        </div>
        <div class="val-list" id="val-list"></div>
      </div>

      <!-- Properties panel — bottom drawer, slides up from canvas bottom -->
      <div class="pp" id="pp">
        <div class="pp-resize" id="pp-resize"></div>
        <div class="pp-hdr-bar">
          <span class="pp-hdr-title" id="pp-hdr-title">Properties</span>
          <button class="pp-close-btn" id="btn-pp-close" title="Close">&#10005;</button>
        </div>
        <div id="pp-content">
          <div class="pp-empty" id="pp-empty">
            <span class="pp-empty-ico">&#9670;</span>
            <span>Select a node to configure it</span>
          </div>
        </div>
      </div>

    </div><!-- #cwrap -->

  </div><!-- .ws-top -->
</div><!-- .ws -->
</div><!-- #ttab-design -->

<!-- ═══════════════════════════════════════════════════════════════════════
     TAB 2 — Performance
═══════════════════════════════════════════════════════════════════════ -->
<div class="ttab-content" id="ttab-performance">
  <div class="perf-wrap" id="perf-wrap">

    <div class="perf-section">
      <div class="perf-title">Throughput</div>

      <div class="field">
        <label for="perf-batchsize">Batch size<span style="font-weight:400;margin-left:6px;font-size:10px;color:var(--text2)">— records per batch</span></label>
        <input type="number" class="perf-num" id="perf-batchsize" min="100" max="50000" step="100" value="1000" aria-label="Batch size"/>
        <div class="field-hint">Larger batches improve throughput but increase memory usage. 1,000–5,000 is a good starting range.</div>
      </div>

      <div class="field">
        <label for="perf-workers">Parallel workers<span style="font-weight:400;margin-left:6px;font-size:10px;color:var(--text2)">— concurrent transform goroutines</span></label>
        <input type="number" class="perf-num" id="perf-workers" min="1" max="32" step="1" value="4" aria-label="Workers"/>
        <div class="field-hint">Set to the number of available CPU cores for CPU-bound transforms. For I/O-bound workloads, a higher value may help.</div>
      </div>
    </div>

    <div class="perf-section">
      <div class="perf-title">Memory</div>

      <div class="field">
        <label for="perf-memory">Memory limit (MB)<span style="font-weight:400;margin-left:6px;font-size:10px;color:var(--text2)">— soft RSS ceiling</span></label>
        <input type="number" class="perf-num" id="perf-memory" min="128" max="65536" step="128" value="512" aria-label="Memory limit"/>
        <div class="field-hint">When RSS exceeds this value, the pipeline pauses ingestion and flushes the current batch to the sink before resuming.</div>
      </div>
    </div>

    <div class="perf-section">
      <div class="perf-title">Buffering</div>

      <div class="field">
        <label for="perf-src-buf">Source read-ahead buffer (batches)</label>
        <input type="number" class="perf-num" id="perf-src-buf" min="1" max="64" step="1" value="4" aria-label="Source buffer"/>
        <div class="field-hint">How many batches to pre-fetch from the source while transforms are running. Higher values hide source latency.</div>
      </div>

      <div class="field">
        <label for="perf-snk-buf">Sink write-queue depth (batches)</label>
        <input type="number" class="perf-num" id="perf-snk-buf" min="1" max="64" step="1" value="4" aria-label="Sink buffer"/>
        <div class="field-hint">How many transformed batches to queue for the sink writer. Higher values hide sink commit latency.</div>
      </div>
    </div>

    <div style="padding-bottom:8px;flex-shrink:0">
      <button class="btn-p" id="btn-perf-save">Save Performance Config</button>
    </div>
  </div>
</div><!-- #ttab-performance -->

<!-- ═══════════════════════════════════════════════════════════════════════
     TAB 3 — Scaling
═══════════════════════════════════════════════════════════════════════ -->
<div class="ttab-content" id="ttab-scaling">
  <div class="scale-wrap" id="scale-wrap">

    <div class="scale-section">
      <div class="scale-title">Execution mode</div>
      <div class="radio-group" id="scale-mode" role="radiogroup" aria-label="Scaling mode">
        <div class="radio-card active" data-mode="single">
          <label><input type="radio" name="scale-mode" value="single" checked/> Single-process</label>
          <div class="rc-desc">All stages run in a single Go binary. Best for moderate data volumes and simple deployments.</div>
        </div>
        <div class="radio-card" data-mode="sharded">
          <label><input type="radio" name="scale-mode" value="sharded"/> Sharded</label>
          <div class="rc-desc">Multiple instances each process a shard of the source. Coordinator assigns shards and tracks progress.</div>
        </div>
        <div class="radio-card" data-mode="distributed">
          <label><input type="radio" name="scale-mode" value="distributed"/> Distributed</label>
          <div class="rc-desc">Worker pool with a central coordinator. Suitable for very large datasets and Kubernetes deployments.</div>
        </div>
      </div>
    </div>

    <div class="scale-section" id="scale-instance-section">
      <div class="scale-title">Instance count (sharded / distributed)</div>
      <div class="field">
        <label for="scale-instance-count">Number of worker instances</label>
        <input type="number" class="perf-num" id="scale-instance-count" min="2" max="256" step="1" value="4" aria-label="Instance count"/>
        <div class="field-hint">Target number of concurrent worker instances. The scheduler will place them across available nodes.</div>
      </div>
    </div>

    <div class="scale-section" id="scale-coordinator-section">
      <div class="scale-title">Coordinator</div>
      <div class="field">
        <label for="scale-coordinator-type">Coordinator backend</label>
        <select id="scale-coordinator-type" aria-label="Coordinator type">
          <option value="postgres">PostgreSQL</option>
          <option value="mysql">MySQL</option>
          <option value="redis">Redis</option>
          <option value="etcd">etcd</option>
        </select>
        <div class="field-hint">The coordinator stores shard assignments and progress. Use a HA-ready backend in production.</div>
      </div>
      <div class="field">
        <label for="scale-coordinator-dsn">Coordinator DSN / URL</label>
        <input type="text" id="scale-coordinator-dsn" placeholder="postgres://user:pass@host:5432/dbname" aria-label="Coordinator DSN"/>
      </div>
      <div class="field">
        <label for="scale-lease-seconds">Shard lease duration (seconds)</label>
        <input type="number" class="perf-num" id="scale-lease-seconds" min="10" max="600" step="10" value="60" aria-label="Lease duration"/>
        <div class="field-hint">A worker that fails to renew its lease within this window will have its shard reassigned to another worker.</div>
      </div>
    </div>

    <div style="padding-bottom:8px;flex-shrink:0">
      <button class="btn-p" id="btn-scale-save">Save Scaling Config</button>
    </div>
  </div>
</div><!-- #ttab-scaling -->

<!-- ═══════════════════════════════════════════════════════════════════════
     TAB 4 — Run (execute + live metrics)
═══════════════════════════════════════════════════════════════════════ -->
<div class="ttab-content" id="ttab-run">
  <div class="run-wrap">

    <!-- Run controls -->
    <div class="run-controls">
      <div class="run-status-dot" id="run-status-dot"></div>
      <span class="run-status-badge idle" id="run-status" aria-live="polite" aria-label="Run status">idle</span>
      <div style="flex:1"></div>
      <button class="btn-p" id="btn-run-start" title="Run the pipeline">&#9654; Run Pipeline</button>
      <button class="btn-d" id="btn-run-stop" style="display:none" title="Stop the running pipeline">&#9632; Stop</button>
      <button class="btn-g" id="btn-run-clear" title="Clear log" style="font-size:11px;padding:4px 9px">&#128465;&#xFE0E; Clear</button>
    </div>

    <!-- Progress bar -->
    <div class="run-progress-wrap">
      <div class="run-progress-hdr">
        <span class="run-progress-lbl">Progress</span>
        <span class="run-progress-pct" id="run-progress-pct">—</span>
      </div>
      <div class="run-progress-bar">
        <div class="run-progress-fill" id="run-progress" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0"></div>
      </div>
    </div>

    <!-- Live metrics table -->
    <div class="run-metrics-wrap">
      <div class="run-metrics-hdr">
        <span>Live Metrics</span>
        <button class="btn-g" id="btn-metrics-refresh" title="Refresh metrics" style="font-size:10px;padding:2px 8px">&#8635; Refresh</button>
      </div>
      <table class="metrics-table" id="run-metrics" aria-label="Pipeline metrics">
        <thead>
          <tr>
            <th>Metric</th>
            <th style="text-align:right">Value</th>
          </tr>
        </thead>
        <tbody id="run-metrics-tbody">
          <tr><td>Rows processed</td><td id="m-rows-processed">—</td></tr>
          <tr><td>Rows written</td><td id="m-rows-written">—</td></tr>
          <tr><td>Rows failed (DLQ)</td><td id="m-rows-failed">—</td></tr>
          <tr><td>Duration</td><td id="m-duration">—</td></tr>
          <tr><td>Throughput</td><td id="m-throughput">—</td></tr>
          <tr><td>Memory RSS</td><td id="m-memory">—</td></tr>
        </tbody>
      </table>
    </div>

    <!-- Scrolling run log -->
    <div class="run-log-wrap">
      <div class="run-log-hdr">
        <span class="run-log-title">Run Log</span>
        <button class="btn-g" id="btn-log-scroll" title="Scroll to bottom" style="font-size:10px;padding:2px 8px">&#8615; Bottom</button>
      </div>
      <div id="run-log" role="log" aria-live="polite" aria-label="Run log"></div>
    </div>

  </div>
</div><!-- #ttab-run -->

<!-- External canvas script — loaded via webview.asWebviewUri, allowed by nonce CSP. -->
<script nonce="${nonce}" src="${scriptUri}"></script>
</body>
</html>`
  }
}

module.exports = { PipelineEditorProvider }
