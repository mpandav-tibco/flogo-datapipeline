// @ts-check
'use strict'

const vscode = require('vscode')
const path = require('path')

// Single tree item for the "Flogo Pipelines" explorer view.
class PipelineItem extends vscode.TreeItem {
  constructor(label, tooltip, collapsible, contextValue, iconId, description, command) {
    super(label, collapsible ?? vscode.TreeItemCollapsibleState.None)
    this.tooltip = tooltip ?? label
    this.contextValue = contextValue ?? ''
    if (iconId) this.iconPath = new vscode.ThemeIcon(iconId)
    if (description) this.description = description
    if (command) this.command = command
    /** @type {vscode.Uri | undefined} — set on runnable rows so commands can target the file. */
    this.pipelineUri = undefined
  }
}

// Provides the "Flogo Pipelines" Explorer view. Shows the active .datapipeline
// file, a "Pipelines" section for inline run/configure actions, and live engine
// status. Mirrors the shape of flogo-agent's SidebarProvider so the extension.js
// wiring is identical in structure.
class SidebarProvider {
  constructor() {
    /** @type {vscode.EventEmitter<void>} */
    this._onDidChangeTreeData = new vscode.EventEmitter()
    this.onDidChangeTreeData = this._onDidChangeTreeData.event

    this._running = false
    this._port = null
    this._pipelineName = null   // display name of the running pipeline
    this._activeUri = null      // the .datapipeline file Build/Run actions target
    this._workspacePipelines = []  // vscode.Uri[] — .datapipeline files in workspace
  }

  // ── State setters (called by extension.js) ────────────────────────────────

  // Mark the engine as running on `port` for the given pipeline.
  setRunning(port, pipelineName) {
    this._running = true
    this._port = port
    this._pipelineName = pipelineName
    this.refresh()
  }

  setStopped() {
    this._running = false
    this._port = null
    this._pipelineName = null
    this.refresh()
  }

  // Track the .datapipeline file being edited/viewed so actions target it.
  setActiveAgent(uri) {
    this._activeUri = uri || null
    this.refresh()
  }

  // Convenience alias so extension.js can call either name.
  setActivePipeline(uri) { this.setActiveAgent(uri) }

  // Record the configured build target ('binary' | 'docker' | null) for display
  // in the tree view description. Called from extension.js _syncActivePipeline().
  setBuildTarget(target) {
    this._buildTarget = target || null
    this.refresh()
  }

  // Update the list of .datapipeline files found in the workspace (passed in by
  // extension.js after a workspace glob). Calling this triggers a tree refresh.
  setWorkspacePipelines(uris) {
    this._workspacePipelines = Array.isArray(uris) ? uris : []
    this.refresh()
  }

  refresh() {
    this._onDidChangeTreeData.fire()
  }

  dispose() {
    this._onDidChangeTreeData.dispose()
  }

  // ── TreeDataProvider ──────────────────────────────────────────────────────

  getTreeItem(element) { return element }

  getChildren(element) {
    if (element) {
      if (element.contextValue === 'pipelinesTitle') return this._pipelineRows()
      return []
    }
    return this._rootItems()
  }

  _rootItems() {
    const uri = this._activeUri
    const activeName = uri ? path.basename(uri.fsPath) : null
    const items = []

    // App header — the active .datapipeline file (click opens the designer).
    items.push(new PipelineItem(
      activeName || 'No pipeline selected',
      activeName ? uri.fsPath : 'Open a .datapipeline file to select it',
      vscode.TreeItemCollapsibleState.None,
      'pipelineHeader',
      activeName ? 'database' : 'circle-slash',
      activeName ? undefined : 'open a .datapipeline file',
      activeName
        ? { command: 'flogoDataPipeline.openPipeline', title: 'Open Designer', arguments: [uri] }
        : undefined,
    ))

    // "Pipelines" section — runnable pipeline actions with inline Run buttons.
    // The section is only shown when there is an active pipeline file.
    if (activeName) {
      const section = new PipelineItem(
        'Pipelines',
        'Run builds and launches this pipeline. Use the gear icon to configure.',
        vscode.TreeItemCollapsibleState.Expanded,
        'pipelinesTitle',
        'symbol-event',
        this._running ? `running :${this._port}` : undefined,
      )
      section.pipelineUri = uri  // so the inline Configure action targets this pipeline
      items.push(section)
    }

    // Runtime status — running or stopped.
    if (this._running) {
      items.push(new PipelineItem(
        this._pipelineName ?? 'pipeline',
        `Local pipeline engine on port ${this._port}`,
        vscode.TreeItemCollapsibleState.None,
        'pipelineRunning',
        'run',
        `running :${this._port}`,
      ))
    } else {
      items.push(new PipelineItem(
        'Not running',
        'The pipeline engine is stopped. Use Run to start it.',
        vscode.TreeItemCollapsibleState.None,
        'pipelineStopped',
        'circle-slash',
      ))
    }

    // Workspace pipeline list — all .datapipeline files found in the workspace,
    // so the user can switch between them without navigating the file explorer.
    if (this._workspacePipelines.length > 0) {
      const listSection = new PipelineItem(
        'Workspace Pipelines',
        'All .datapipeline files found in the workspace',
        vscode.TreeItemCollapsibleState.Expanded,
        'workspacePipelinesTitle',
        'folder-library',
      )
      items.push(listSection)
    }

    return items
  }

  // Children of the "Pipelines" section: one runnable row per active pipeline.
  // Inline Run / Configure buttons are contributed via view/item/context in
  // package.json on `pipelineAction`; `pipelineUri` lets those commands target
  // the right file.
  _pipelineRows() {
    const uri = this._activeUri
    if (!uri) return []
    const name = path.basename(uri.fsPath, path.extname(uri.fsPath))
    const item = new PipelineItem(
      name,
      'Build and run this pipeline, then open the tester',
      vscode.TreeItemCollapsibleState.None,
      'pipelineAction',
      'database',
      this._running ? `:${this._port}` : undefined,
    )
    item.pipelineUri = uri
    return [item]
  }

  // Children of the "Workspace Pipelines" section — one row per discovered file.
  // Clicking a row sets it as the active pipeline via flogoDataPipeline.selectPipeline.
  _workspacePipelineItems() {
    if (this._workspacePipelines.length === 0) {
      return [new PipelineItem(
        'No .datapipeline files found',
        'Add a .datapipeline file to this workspace',
        vscode.TreeItemCollapsibleState.None,
        '',
        'dash',
      )]
    }
    return this._workspacePipelines.map(u => {
      const name = path.basename(u.fsPath)
      const isActive = this._activeUri && u.fsPath === this._activeUri.fsPath
      const item = new PipelineItem(
        name,
        u.fsPath,
        vscode.TreeItemCollapsibleState.None,
        'workspacePipeline',
        isActive ? 'database' : 'file-code',
        isActive ? 'active' : undefined,
        { command: 'flogoDataPipeline.selectPipeline', title: 'Select Pipeline', arguments: [u] },
      )
      item.pipelineUri = u
      return item
    })
  }

  // Override getChildren to also serve the workspace pipeline list section.
  // (Redefines the routing added in the main getChildren above.)
}

// Patch getChildren to handle the workspacePipelinesTitle node after the class
// body is fully defined (avoids a forward-reference to _workspacePipelineItems).
const _originalGetChildren = SidebarProvider.prototype.getChildren
SidebarProvider.prototype.getChildren = function (element) {
  if (element && element.contextValue === 'workspacePipelinesTitle') {
    return this._workspacePipelineItems()
  }
  return _originalGetChildren.call(this, element)
}

module.exports = { SidebarProvider }
