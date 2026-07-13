// Visual editor for `.fgconn` connection files.
//
// Registered as a CustomTextEditor for *.fgconn, so double-clicking a connection
// in the tree (or the file) opens a friendly form instead of raw JSON. Secrets
// are shown masked and only sent to Secret Storage when changed — the .fgconn
// file itself never carries secret values.

import * as vscode from 'vscode';
import * as path from 'path';

import { ConnectionStore, connType, CONN_TYPES, familyName } from './store';

export class ConnectionEditorProvider implements vscode.CustomTextEditorProvider {
  public static readonly viewType = 'flogoDataPipelineStudio.connectionEditor';

  static register(context: vscode.ExtensionContext, store: ConnectionStore): vscode.Disposable {
    const provider = new ConnectionEditorProvider(context, store);
    return vscode.window.registerCustomEditorProvider(ConnectionEditorProvider.viewType, provider, {
      webviewOptions: { retainContextWhenHidden: true },
      supportsMultipleEditorsPerDocument: false,
    });
  }

  constructor(
    private readonly context: vscode.ExtensionContext,
    private readonly store: ConnectionStore,
  ) {}

  async resolveCustomTextEditor(document: vscode.TextDocument, panel: vscode.WebviewPanel): Promise<void> {
    panel.webview.options = { enableScripts: true };
    panel.webview.html = this.html(panel.webview);

    const idFromFile = path.basename(document.uri.fsPath).replace(/\.fgconn$/, '');

    const send = () => this.sendState(panel, document, idFromFile);

    const changeSub = vscode.workspace.onDidChangeTextDocument((e) => {
      if (e.document.uri.toString() === document.uri.toString()) send();
    });
    const storeSub = this.store.onDidChange(() => send());
    panel.onDidDispose(() => {
      changeSub.dispose();
      storeSub.dispose();
    });

    panel.webview.onDidReceiveMessage(async (msg) => {
      try {
        await this.onMessage(panel, document, idFromFile, msg);
      } catch (err: any) {
        vscode.window.showErrorMessage(`Connection editor: ${err?.message ?? String(err)}`);
      }
    });
  }

  private parse(document: vscode.TextDocument): any {
    try {
      return JSON.parse(document.getText());
    } catch {
      return {};
    }
  }

  private async sendState(panel: vscode.WebviewPanel, document: vscode.TextDocument, id: string): Promise<void> {
    const data = this.parse(document);
    const type: string = data.type || 'postgres';
    const ct = connType(type) || CONN_TYPES[0];
    const values: Record<string, string> = data.fields || {};
    const secretKeys: string[] = Array.isArray(data.secretKeys) ? data.secretKeys : [];
    panel.webview.postMessage({
      type: 'init',
      id,
      name: data.name || id,
      connType: type,
      label: ct.label,
      family: ct.family,
      familyLabel: familyName(ct.family),
      fields: ct.fields,
      values,
      secretKeys,
    });
  }

  private async onMessage(
    panel: vscode.WebviewPanel,
    document: vscode.TextDocument,
    id: string,
    msg: any,
  ): Promise<void> {
    switch (msg?.type) {
      case 'ready':
        await this.sendState(panel, document, id);
        break;

      case 'save': {
        const data = this.parse(document);
        const type: string = data.type || 'postgres';
        const ct = connType(type) || CONN_TYPES[0];
        // Merge: existing non-secret values + edited values. Secrets only come
        // through when the user typed a new value (msg.secrets has the key).
        const merged: Record<string, string> = { ...(msg.values || {}) };
        for (const [k, v] of Object.entries(msg.secrets || {})) {
          if (typeof v === 'string' && v !== '') merged[k] = v;
        }
        // Preserve unchanged secrets already in Secret Storage.
        const existing = await this.store.resolveValues(id);
        for (const f of ct.fields) {
          if (f.secret && !(f.key in merged) && existing[f.key]) merged[f.key] = existing[f.key];
        }
        await this.store.upsert({ id, name: msg.name || id, type, family: ct.family }, merged);
        vscode.window.showInformationMessage(`Connection "${msg.name || id}" saved.`);
        break;
      }

      case 'test':
        await vscode.commands.executeCommand('flogoDataPipelineStudio.testConnectionById', id);
        break;

      case 'delete':
        await vscode.commands.executeCommand('flogoDataPipelineStudio.deleteConnectionById', id);
        break;

      case 'openRaw':
        vscode.commands.executeCommand('vscode.openWith', document.uri, 'default');
        break;
    }
  }

  private html(webview: vscode.Webview): string {
    const nonce = String(Math.random()).slice(2);
    const csp = [
      `default-src 'none'`,
      `style-src 'unsafe-inline'`,
      `script-src 'nonce-${nonce}'`,
      `connect-src ${webview.cspSource}`,
      `img-src ${webview.cspSource} data:`,
    ].join('; ');
    return /* html */ `<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8" />
<meta http-equiv="Content-Security-Policy" content="${csp}" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<style>
  :root { color-scheme: dark light; }
  body { font-family: var(--vscode-font-family); color: var(--vscode-foreground); padding: 18px 22px; max-width: 620px; }
  h2 { margin: 0 0 2px; font-size: 17px; display:flex; align-items:center; gap:8px; }
  .sub { color: var(--vscode-descriptionForeground); font-size: 12px; margin-bottom: 16px; }
  .badge { font-size: 11px; background: var(--vscode-badge-background); color: var(--vscode-badge-foreground); border-radius: 10px; padding: 1px 8px; }
  .fld { margin-bottom: 12px; }
  .k { font-size: 11px; text-transform: uppercase; letter-spacing: .04em; color: var(--vscode-descriptionForeground); margin-bottom: 4px; }
  .k .secret { text-transform: none; letter-spacing: 0; color: var(--vscode-charts-yellow); margin-left: 6px; }
  input { width: 100%; box-sizing: border-box; background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border, transparent); border-radius: 4px; padding: 6px 8px; font-size: 13px; }
  input:focus { outline: 1px solid var(--vscode-focusBorder); }
  .row { display: flex; gap: 8px; margin-top: 18px; }
  button { background: var(--vscode-button-background); color: var(--vscode-button-foreground); border: none; border-radius: 4px; padding: 7px 14px; font-size: 13px; cursor: pointer; }
  button.secondary { background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); }
  button:hover { opacity: .92; }
  .spacer { flex: 1; }
  .note { margin-top: 18px; font-size: 11.5px; color: var(--vscode-descriptionForeground); border-top: 1px solid var(--vscode-panel-border); padding-top: 12px; }
  code { color: var(--vscode-charts-orange); }
</style>
</head><body>
  <h2>⛁ <span id="title">Connection</span> <span class="badge" id="fam"></span></h2>
  <div class="sub" id="typeLabel"></div>
  <div id="form"></div>
  <div class="row">
    <button id="save">Save Connection</button>
    <button id="test" class="secondary">⟿ Test</button>
    <span class="spacer"></span>
    <button id="raw" class="secondary">View JSON</button>
    <button id="del" class="secondary">Delete</button>
  </div>
  <div class="note">Secrets are stored in VS Code <b>Secret Storage</b> — never written to the <code>.fgconn</code> file. Bind this connection by name on any ${''}source/sink of the matching family.</div>
<script nonce="${nonce}">
  const vscode = acquireVsCodeApi();
  let STATE = null;
  const form = document.getElementById('form');

  function render(s) {
    STATE = s;
    document.getElementById('title').textContent = s.name;
    document.getElementById('fam').textContent = s.familyLabel;
    document.getElementById('typeLabel').textContent = s.label;
    form.innerHTML = '';
    // Name row
    form.appendChild(field('__name__', 'Connection name', s.name, false, false));
    for (const f of s.fields) {
      const isSet = s.secretKeys.includes(f.key);
      const val = f.secret ? '' : (s.values[f.key] || '');
      form.appendChild(field(f.key, f.label, val, !!f.secret, isSet, f.placeholder));
    }
  }

  function field(key, label, value, secret, isSet, placeholder) {
    const wrap = document.createElement('div'); wrap.className = 'fld';
    const k = document.createElement('div'); k.className = 'k';
    k.textContent = label;
    if (secret) { const s = document.createElement('span'); s.className = 'secret'; s.textContent = '· Secret Storage'; k.appendChild(s); }
    wrap.appendChild(k);
    const inp = document.createElement('input');
    inp.dataset.key = key;
    inp.dataset.secret = secret ? '1' : '0';
    if (secret) { inp.type = 'password'; inp.placeholder = isSet ? '•••••••• (unchanged)' : (placeholder || ''); }
    else { inp.value = value; inp.placeholder = placeholder || ''; }
    wrap.appendChild(inp);
    return wrap;
  }

  function collect() {
    const values = {}; const secrets = {}; let name = STATE.name;
    form.querySelectorAll('input').forEach((inp) => {
      const key = inp.dataset.key;
      if (key === '__name__') { name = inp.value; return; }
      if (inp.dataset.secret === '1') { if (inp.value) secrets[key] = inp.value; }
      else if (inp.value) values[key] = inp.value;
    });
    return { name, values, secrets };
  }

  document.getElementById('save').onclick = () => { const c = collect(); vscode.postMessage({ type: 'save', name: c.name, values: c.values, secrets: c.secrets }); };
  document.getElementById('test').onclick = () => vscode.postMessage({ type: 'test' });
  document.getElementById('raw').onclick = () => vscode.postMessage({ type: 'openRaw' });
  document.getElementById('del').onclick = () => vscode.postMessage({ type: 'delete' });

  window.addEventListener('message', (e) => { if (e.data && e.data.type === 'init') render(e.data); });
  vscode.postMessage({ type: 'ready' });
</script>
</body></html>`;
  }
}
