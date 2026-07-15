// Copyright (c) 2024-2026. Cloud Software Group, Inc. All Rights Reserved. Confidential & Proprietary
/**
 * Renders the self-contained pipeline canvas HTML for the custom editor webview.
 *
 * This is a lightweight, dependency-free canvas (themed with `--vscode-*`
 * variables). A production version would move this into an Angular app under
 * `packages/wi-contributions/projects/datapipeline-ui` reusing `wi-diagram` and
 * `@tibco/flogo-mapper`; the message contract below stays the same.
 */
import * as vscode from 'vscode';
import { PipelineGraphVM } from './types';

export function getCanvasHtml(
  webview: vscode.Webview,
  nonce: string,
): string {
  const csp = [
    `default-src 'none'`,
    `style-src ${webview.cspSource} 'unsafe-inline'`,
    `script-src 'nonce-${nonce}'`,
    `font-src ${webview.cspSource}`,
  ].join('; ');

  return /* html */ `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta http-equiv="Content-Security-Policy" content="${csp}" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<style>
  :root { color-scheme: light dark; }
  * { box-sizing: border-box; }
  body {
    margin: 0; font-family: var(--vscode-font-family); font-size: 13px;
    color: var(--vscode-foreground); background: var(--vscode-editor-background);
  }
  .toolbar {
    display: flex; align-items: center; gap: 8px; padding: 8px 12px;
    border-bottom: 1px solid var(--vscode-panel-border);
    background: var(--vscode-editorGroupHeader-tabsBackground);
  }
  .toolbar .title { font-weight: 600; }
  .toolbar .mode {
    font-size: 11px; padding: 1px 7px; border-radius: 10px;
    background: var(--vscode-badge-background); color: var(--vscode-badge-foreground);
  }
  .toolbar .grow { flex: 1; }
  button {
    font-family: inherit; font-size: 12px; border-radius: 4px; padding: 4px 10px;
    border: 1px solid var(--vscode-button-border, transparent); cursor: pointer;
    color: var(--vscode-button-foreground); background: var(--vscode-button-background);
  }
  button.secondary {
    color: var(--vscode-button-secondaryForeground);
    background: var(--vscode-button-secondaryBackground);
  }
  .canvas {
    padding: 24px 20px; overflow: auto; min-height: 220px;
    background-image: radial-gradient(var(--vscode-panel-border) 1px, transparent 1px);
    background-size: 22px 22px;
  }
  .row { display: flex; align-items: center; flex-wrap: nowrap; overflow-x: auto; padding-bottom: 10px; }
  .node {
    flex: none; width: 158px; padding: 10px 12px; border-radius: 9px;
    background: var(--vscode-editorWidget-background);
    border: 1px solid var(--vscode-editorWidget-border, var(--vscode-panel-border));
  }
  .node.selected { border-color: var(--vscode-focusBorder); box-shadow: 0 0 0 1px var(--vscode-focusBorder); }
  .node .kind { font-size: 10px; text-transform: uppercase; letter-spacing: .06em; opacity: .7; }
  .node .name { font-weight: 600; margin: 3px 0 5px; }
  .node .meta { font-family: var(--vscode-editor-font-family); font-size: 11px; opacity: .8; word-break: break-word; }
  .node.source { border-top: 2px solid #4a90d9; }
  .node.sink { border-top: 2px solid #4caf7d; }
  .node.dlq { border-style: dashed; }
  .conn { flex: none; width: 30px; height: 2px; background: var(--vscode-panel-border); position: relative; }
  .conn::after {
    content: ""; position: absolute; right: -1px; top: -4px;
    border: 5px solid transparent; border-left-color: var(--vscode-panel-border);
  }
  .branch { margin: 10px 0 0 194px; display: flex; align-items: flex-start; gap: 8px; }
  .branch .down { width: 2px; height: 22px; background: var(--vscode-panel-border); margin-left: 16px; }
  .empty { opacity: .7; padding: 30px; text-align: center; }
  .footer { padding: 8px 14px; border-top: 1px solid var(--vscode-panel-border); font-size: 11px; opacity: .75; }
  .footer a { color: var(--vscode-textLink-foreground); cursor: pointer; }
</style>
</head>
<body>
  <div class="toolbar">
    <span class="title" id="pname">pipeline</span>
    <span class="mode" id="pmode">—</span>
    <span class="grow"></span>
    <button class="secondary" id="btn-preview">Dry-run Preview</button>
    <button id="btn-run">▷ Run</button>
  </div>
  <div class="canvas" id="canvas"><div class="empty">Loading pipeline…</div></div>
  <div class="footer">
    Editing the <code>.datapipeline</code> JSON directly? <a id="reopen">Reopen with Text Editor</a>.
  </div>

  <script nonce="${nonce}">
    const vscode = acquireVsCodeApi();
    let selected = null;

    document.getElementById('btn-run').addEventListener('click', () => vscode.postMessage({ type: 'run' }));
    document.getElementById('btn-preview').addEventListener('click', () => vscode.postMessage({ type: 'preview' }));
    document.getElementById('reopen').addEventListener('click', () => vscode.postMessage({ type: 'reopenAsText' }));

    window.addEventListener('message', (event) => {
      const msg = event.data;
      if (msg.type === 'model') { render(msg.graph); }
    });

    function esc(s) { return String(s == null ? '' : s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }

    function nodeEl(n) {
      const div = document.createElement('div');
      div.className = 'node ' + n.kind + (n.id === selected ? ' selected' : '');
      div.innerHTML =
        '<div class="kind">' + esc(n.kind) + '</div>' +
        '<div class="name">' + esc(n.title) + '</div>' +
        '<div class="meta">' + esc(n.meta) + '</div>';
      div.addEventListener('click', () => { selected = n.id; vscode.postMessage({ type: 'selectNode', nodeId: n.id }); });
      return div;
    }

    function conn() { const c = document.createElement('div'); c.className = 'conn'; return c; }

    function render(graph) {
      document.getElementById('pname').textContent = graph.name || 'pipeline';
      document.getElementById('pmode').textContent = graph.mode || '—';
      const canvas = document.getElementById('canvas');
      canvas.innerHTML = '';
      if (!graph.nodes || !graph.nodes.length) {
        canvas.innerHTML = '<div class="empty">No <code>pipeline:*</code> resource found in this file.</div>';
        return;
      }
      const row = document.createElement('div');
      row.className = 'row';
      graph.nodes.forEach((n, i) => {
        if (i > 0) row.appendChild(conn());
        row.appendChild(nodeEl(n));
      });
      canvas.appendChild(row);
      if (graph.hasDlq && graph.dlq) {
        const branch = document.createElement('div');
        branch.className = 'branch';
        const down = document.createElement('div');
        down.className = 'down';
        branch.appendChild(down);
        branch.appendChild(nodeEl(graph.dlq));
        canvas.appendChild(branch);
      }
    }

    vscode.postMessage({ type: 'ready' });
  </script>
</body>
</html>`;
}

/** Cryptographically-random nonce for the webview CSP. */
export function makeNonce(): string {
  const chars =
    'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let out = '';
  for (let i = 0; i < 32; i++) {
    out += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return out;
}

/** Type guard for the graph object (kept here to co-locate with the renderer). */
export function isGraph(x: unknown): x is PipelineGraphVM {
  return !!x && typeof x === 'object' && Array.isArray((x as PipelineGraphVM).nodes);
}
