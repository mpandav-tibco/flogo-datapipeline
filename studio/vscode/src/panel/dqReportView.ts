import * as vscode from 'vscode';
import * as crypto from 'crypto';
import type { RunResponse } from '../engine/apiClient';

export interface DqReject {
  id: string;
  name: string;
  rowsOut: number;
  rowsErrored: number;
}

// DqReportView is a native VS Code panel view (docked next to Problems / Data
// Preview / Output / Terminal) that summarises the last run's data quality —
// rows read/written/rejected and, per rejecting stage, the reject count.
// Clicking a stage posts back to the extension host so the active pipeline
// editor can select that node on the canvas, mirroring how VS Code's own
// Problems panel jumps to the offending location in the active editor.
export class DqReportView implements vscode.WebviewViewProvider {
  public static readonly viewType = 'flogoDataPipelineStudio.dqReport';

  private view?: vscode.WebviewView;
  private lastState?: { kind: 'empty' | 'result'; run?: RunResponse; rejects?: DqReject[] };

  // Set by the extension host to route a stage click to the active editor.
  public onSelectNode?: (id: string) => void;

  constructor(private readonly extensionUri: vscode.Uri) { }

  resolveWebviewView(webviewView: vscode.WebviewView): void {
    this.view = webviewView;
    webviewView.webview.options = {
      enableScripts: true,
      localResourceRoots: [vscode.Uri.joinPath(this.extensionUri, 'media')],
    };
    webviewView.webview.html = this.html(webviewView.webview);
    webviewView.webview.onDidReceiveMessage((msg) => {
      if (msg && msg.command === 'selectNode' && typeof msg.id === 'string') this.onSelectNode?.(msg.id);
    });
    // Re-send the last state whenever the view is (re)created or becomes visible,
    // since VS Code disposes a hidden panel webview's DOM.
    if (this.lastState) this.post(this.lastState);
    webviewView.onDidChangeVisibility(() => {
      if (webviewView.visible && this.lastState) this.post(this.lastState);
    });
  }

  reveal(): void {
    if (this.view) {
      this.view.show?.(true);
    } else {
      void vscode.commands.executeCommand('flogoDataPipelineStudio.dqReport.focus');
    }
  }

  showResult(run: RunResponse, rejects: DqReject[]): void {
    this.send({ kind: 'result', run, rejects });
  }

  showEmpty(): void {
    this.send({ kind: 'empty' });
  }

  private send(state: { kind: 'empty' | 'result'; run?: RunResponse; rejects?: DqReject[] }): void {
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
  .dqsum { display: flex; gap: 16px; padding: 4px 2px 12px; flex-wrap: wrap; }
  .dqm { font-size: 12px; color: var(--mute); }
  .dqm b { font-size: 13.5px; color: var(--text); }
  .dqm.ok b { color: var(--green, #4caf7d); }
  .dqm.err b { color: var(--red, #d9534f); }
  .dqrow { display: flex; justify-content: space-between; align-items: center;
    padding: 7px 8px; border-radius: 5px; cursor: pointer; font-size: 12px; }
  .dqrow:hover { background: var(--hover); }
  .dqn { color: var(--text); font-weight: 600; }
  .dqv { color: var(--mute); }
  .dqv .err { color: var(--red, #d9534f); }
</style>
</head>
<body>
<div id="root"><div class="empty">Run the pipeline to see a data-quality report — rows read, written and rejected, per stage.</div></div>
<script nonce="${nonce}">
  const vscode = acquireVsCodeApi();
  const root = document.getElementById('root');
  const esc = (s) => String(s == null ? '' : s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const fmtK = (n) => (n >= 1e6 ? (n/1e6).toFixed(2)+'M' : n >= 1e3 ? Math.round(n/1e3)+'k' : String(Math.round(n)));
  window.addEventListener('message', (ev) => {
    const m = ev.data;
    if (m.kind === 'empty') { root.innerHTML = '<div class="empty">Run the pipeline to see a data-quality report — rows read, written and rejected, per stage.</div>'; return; }
    if (m.kind === 'result') {
      const r = m.run;
      const read = r.rowsProcessed || 0, wrote = r.rowsWritten || 0, failed = r.rowsFailed || 0;
      const passRate = read ? Math.round((wrote / read) * 1000) / 10 : 0;
      let html = '<div class="dqsum">'
        + '<span class="dqm"><b>' + fmtK(read) + '</b> read</span>'
        + '<span class="dqm ok"><b>' + fmtK(wrote) + '</b> written</span>'
        + '<span class="dqm err"><b>' + fmtK(failed) + '</b> rejected</span>'
        + '<span class="dqm"><b>' + passRate + '%</b> pass rate</span>'
        + '</div>';
      const rejects = m.rejects || [];
      if (rejects.length) {
        html += rejects.map((s) => (
          '<div class="dqrow" data-id="' + esc(s.id) + '">'
          + '<span class="dqn">' + esc(s.name) + '</span>'
          + '<span class="dqv">' + fmtK(s.rowsOut) + ' passed · <span class="err">' + fmtK(s.rowsErrored) + ' rejected</span></span>'
          + '</div>'
        )).join('');
      } else if (!failed) {
        html += '<div class="empty">No rows were rejected in the last run — clean data.</div>';
      }
      root.innerHTML = html;
      root.querySelectorAll('.dqrow').forEach((row) => {
        row.addEventListener('click', () => vscode.postMessage({ command: 'selectNode', id: row.getAttribute('data-id') }));
      });
    }
  });
</script>
</body>
</html>`;
  }
}
