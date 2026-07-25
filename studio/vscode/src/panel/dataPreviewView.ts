import * as vscode from 'vscode';
import * as crypto from 'crypto';
import type { PreviewResult } from '../engine/apiClient';

// DataPreviewView is a native VS Code panel view (docked next to Problems /
// Output / Terminal) that renders dry-run preview rows as a table. The pipeline
// editor pushes results here instead of drawing its own bottom panel, so the
// Studio integrates with the IDE chrome rather than reimplementing it.
export class DataPreviewView implements vscode.WebviewViewProvider {
  public static readonly viewType = 'flogoDataPipelineStudio.dataPreview';

  private view?: vscode.WebviewView;
  private lastState?: { kind: 'loading' | 'result' | 'error' | 'empty'; payload?: unknown; meta?: string };

  constructor(private readonly extensionUri: vscode.Uri) {}

  resolveWebviewView(webviewView: vscode.WebviewView): void {
    this.view = webviewView;
    webviewView.webview.options = {
      enableScripts: true,
      localResourceRoots: [vscode.Uri.joinPath(this.extensionUri, 'media')],
    };
    webviewView.webview.html = this.html(webviewView.webview);
    // Re-send the last state whenever the view is (re)created or becomes visible,
    // since VS Code disposes a hidden panel webview's DOM.
    if (this.lastState) this.post(this.lastState);
    webviewView.onDidChangeVisibility(() => {
      if (webviewView.visible && this.lastState) this.post(this.lastState);
    });
  }

  // Reveal the panel view (used when a preview is requested from the editor).
  reveal(): void {
    if (this.view) {
      this.view.show?.(true);
    } else {
      void vscode.commands.executeCommand('flogoDataPipelineStudio.dataPreview.focus');
    }
  }

  showLoading(meta: string): void {
    this.send({ kind: 'loading', meta });
  }

  showResult(result: PreviewResult): void {
    if (!result.rows || result.rows.length === 0) {
      this.send({
        kind: 'empty',
        meta: `Read ${result.rowsRead} · dropped ${result.rowsDropped} · failed ${result.rowsFailed}`,
      });
      return;
    }
    this.send({ kind: 'result', payload: result });
  }

  showError(message: string): void {
    this.send({ kind: 'error', meta: message });
  }

  private send(state: { kind: 'loading' | 'result' | 'error' | 'empty'; payload?: unknown; meta?: string }): void {
    this.lastState = state;
    this.post(state);
  }

  private post(state: unknown): void {
    try {
      this.view?.webview.postMessage(state);
    } catch {
      /* view disposed */
    }
  }

  private html(webview: vscode.Webview): string {
    const nonce = crypto.randomBytes(16).toString('base64');
    const styleUri = webview.asWebviewUri(vscode.Uri.joinPath(this.extensionUri, 'media', 'webview.css'));
    const csp = [
      `default-src 'none'`,
      `style-src ${webview.cspSource} 'unsafe-inline'`,
      `script-src 'nonce-${nonce}'`,
      `font-src ${webview.cspSource}`,
    ].join('; ');
    return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta http-equiv="Content-Security-Policy" content="${csp}" />
<link rel="stylesheet" href="${styleUri}" />
<style>
  html, body { height: 100%; }
  body { padding: 8px 10px; box-sizing: border-box; }
  #root { height: 100%; }
  .empty { color: var(--dim); font-size: 12.5px; padding: 10px 2px; }
  /* Fill the panel and let the table area scroll, so all sampled rows are
     reachable without maximizing the panel. The header row stays pinned. */
  .pv { display: flex; flex-direction: column; height: 100%; min-height: 0; }
  .pv .pvtop { flex: 0 0 auto; }
  .pvscroll { flex: 1 1 auto; overflow: auto; min-height: 0; }
  .pvscroll thead th { position: sticky; top: 0; z-index: 3; }
  /* Left-hand row-number gutter; stays visible while scrolling both axes. */
  .pv th.idx, .pv td.idx { position: sticky; left: 0; width: 1%; text-align: right;
    color: var(--dim); font-variant-numeric: tabular-nums; user-select: none; }
  .pv td.idx { background: var(--editor); z-index: 2; }
  .pvscroll thead th.idx { z-index: 4; }
</style>
</head>
<body>
<div id="root"><div class="empty">Run <b style="color:var(--text)">Dry-run Preview</b> from a pipeline to sample rows here.</div></div>
<script nonce="${nonce}">
  const root = document.getElementById('root');
  const esc = (s) => String(s == null ? 'null' : s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const fmt = (n) => Number(n).toLocaleString('en-US');
  const cell = (v) => v == null ? 'null' : (typeof v === 'object' ? JSON.stringify(v) : String(v));
  window.addEventListener('message', (ev) => {
    const m = ev.data;
    if (m.kind === 'loading') { root.innerHTML = '<div class="empty">' + esc(m.meta || 'Sampling rows…') + '</div>'; return; }
    if (m.kind === 'error') { root.innerHTML = '<div class="empty" style="color:var(--red)">Preview failed: ' + esc(m.meta) + '</div>'; return; }
    if (m.kind === 'empty') { root.innerHTML = '<div class="empty">No sample rows produced. ' + esc(m.meta || '') + '</div>'; return; }
    if (m.kind === 'result') {
      const pr = m.payload;
      const cols = (pr.columns && pr.columns.length) ? pr.columns : Object.keys(pr.rows[0] || {});
      const ths = '<th class="idx">#</th>' + cols.map((c) => '<th>' + esc(c) + '</th>').join('');
      const trs = pr.rows.map((row, i) => '<tr><td class="idx">' + (i + 1) + '</td>' + cols.map((c) => '<td>' + esc(cell(row[c])) + '</td>').join('') + '</tr>').join('');
      root.innerHTML =
        '<div class="pv"><div class="pvtop"><span><b style="color:var(--text)">Live preview</b> · ' + pr.rows.length +
        ' sampled' + (pr.truncated ? ' (truncated)' : '') + ' · read ' + fmt(pr.rowsRead) + '</span><span class="muted">after transforms</span></div>' +
        '<div class="pvscroll"><table><thead><tr>' + ths + '</tr></thead><tbody>' + trs + '</tbody></table></div></div>';
    }
  });
</script>
</body>
</html>`;
  }
}
