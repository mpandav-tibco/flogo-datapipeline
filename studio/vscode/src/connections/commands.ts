// Native (QuickPick + InputBox) flows for creating, editing, testing and
// deleting connections. Secrets are entered via password InputBoxes and go
// straight to Secret Storage — they never pass through the webview.

import * as vscode from 'vscode';
import * as net from 'net';

import {
  ConnectionStore,
  CONN_TYPES,
  connType,
  connectionsFilePath,
  ConnectionMeta,
} from './store';

// Prompt for every field of a connection type, pre-filling from `initial`.
async function promptFields(
  typeId: string,
  initial: Record<string, string>,
): Promise<Record<string, string> | undefined> {
  const ct = connType(typeId);
  if (!ct) return undefined;
  const values: Record<string, string> = {};
  for (const f of ct.fields) {
    const input = await vscode.window.showInputBox({
      title: `${ct.label} — ${f.label}${f.secret ? '  (Secret Storage)' : ''}`,
      prompt: f.secret ? 'Stored securely in VS Code Secret Storage' : f.optional ? 'Optional' : '',
      value: initial[f.key] ?? '',
      placeHolder: f.placeholder || '',
      password: !!f.secret,
      ignoreFocusOut: true,
    });
    if (input === undefined) return undefined; // cancelled
    if (input !== '') values[f.key] = input;
  }
  return values;
}

export async function createConnectionFlow(store: ConnectionStore): Promise<ConnectionMeta | undefined> {
  if (!vscode.workspace.workspaceFolders?.length) {
    vscode.window.showWarningMessage('Open a workspace folder to save connections.');
    return;
  }
  const pick = await vscode.window.showQuickPick(
    CONN_TYPES.map((c) => ({ label: c.label, description: c.desc, typeId: c.type })),
    { title: 'New connection — pick a type', placeHolder: 'Where does the data live?' },
  );
  if (!pick) return;

  const name = await vscode.window.showInputBox({
    title: `New ${pick.label} connection — name`,
    prompt: 'A short name you will bind on source/sink stages',
    value: `${pick.typeId}-conn`,
    ignoreFocusOut: true,
    validateInput: async (v) => {
      if (!v.trim()) return 'Enter a name.';
      const existing = await store.getByName(v.trim());
      return existing ? 'A connection with that name already exists.' : undefined;
    },
  });
  if (!name) return;

  const values = await promptFields(pick.typeId, {});
  if (!values) return;

  const ct = connType(pick.typeId)!;
  const record = await store.upsert({ name: name.trim(), type: pick.typeId, family: ct.family }, values);
  vscode.window.showInformationMessage(
    `Connection "${record.name}" saved — now selectable on any ${ct.family} source/sink. Secrets are in Secret Storage.`,
  );
  return record;
}

export async function editConnectionFlow(store: ConnectionStore, id: string): Promise<void> {
  const record = await store.get(id);
  if (!record) {
    vscode.window.showWarningMessage('Connection not found.');
    return;
  }
  const action = await vscode.window.showQuickPick(
    [
      { label: '$(edit) Edit fields', a: 'edit' },
      { label: '$(pulse) Test connection', a: 'test' },
      { label: '$(files) Duplicate', a: 'dup' },
      { label: '$(trash) Delete', a: 'del' },
    ],
    { title: `Connection "${record.name}"`, placeHolder: connType(record.type)?.label },
  );
  if (!action) return;

  if (action.a === 'test') {
    await testConnectionFlow(store, id);
    return;
  }
  if (action.a === 'del') {
    await deleteConnectionFlow(store, id);
    return;
  }
  if (action.a === 'dup') {
    const values = await store.resolveValues(id);
    const newName = await vscode.window.showInputBox({
      title: 'Duplicate connection — new name',
      value: `${record.name}-copy`,
      ignoreFocusOut: true,
      validateInput: async (v) => (!v.trim() ? 'Enter a name.' : (await store.getByName(v.trim())) ? 'Name already exists.' : undefined),
    });
    if (!newName) return;
    await store.upsert({ name: newName.trim(), type: record.type, family: record.family }, values);
    vscode.window.showInformationMessage(`Connection "${newName.trim()}" created.`);
    return;
  }

  // Edit fields — pre-fill from resolved values (incl. secrets).
  const current = await store.resolveValues(id);
  const values = await promptFields(record.type, current);
  if (!values) return;
  await store.upsert({ id: record.id, name: record.name, type: record.type, family: record.family }, values);
  vscode.window.showInformationMessage(`Connection "${record.name}" updated.`);
}

export async function deleteConnectionFlow(store: ConnectionStore, id: string): Promise<void> {
  const record = await store.get(id);
  if (!record) return;
  const yes = await vscode.window.showWarningMessage(
    `Delete connection "${record.name}"? Its secrets will be removed from Secret Storage.`,
    { modal: true },
    'Delete',
  );
  if (yes !== 'Delete') return;
  await store.delete(id);
  vscode.window.showInformationMessage(`Connection "${record.name}" deleted.`);
}

// A best-effort reachability test. For db/kafka we do a TCP connect to
// host:port; for object stores / http we probe the endpoint host. We avoid
// bundling driver clients — this verifies the endpoint is reachable, not auth.
export async function testConnectionFlow(store: ConnectionStore, id: string): Promise<void> {
  const record = await store.get(id);
  if (!record) return;
  const v = await store.resolveValues(id);

  const target = tcpTarget(record, v);
  if (!target) {
    vscode.window.showInformationMessage(`"${record.name}": no network endpoint to probe (config looks complete).`);
    return;
  }

  await vscode.window.withProgress(
    { location: vscode.ProgressLocation.Notification, title: `Testing "${record.name}" — ${target.host}:${target.port}…` },
    async () => {
      const started = Date.now();
      const ok = await tcpProbe(target.host, target.port, 4000);
      const ms = Date.now() - started;
      if (ok) {
        vscode.window.showInformationMessage(`Connection "${record.name}" — reachable (${target.host}:${target.port}, ${ms}ms).`);
      } else {
        vscode.window.showErrorMessage(`Connection "${record.name}" — could not reach ${target.host}:${target.port}.`);
      }
    },
  );
}

function tcpTarget(record: ConnectionMeta, v: Record<string, string>): { host: string; port: number } | null {
  switch (record.type) {
    case 'postgres':
      return v.host ? { host: v.host, port: Number(v.port) || 5432 } : null;
    case 'mysql':
      return v.host ? { host: v.host, port: Number(v.port) || 3306 } : null;
    case 'kafka': {
      const first = (v.brokers || '').split(',')[0]?.trim();
      if (!first) return null;
      const [h, p] = first.split(':');
      return { host: h, port: Number(p) || 9092 };
    }
    case 's3':
    case 'http': {
      const url = v.endpoint || v.baseUrl;
      if (!url) return null;
      try {
        const u = new URL(url);
        return { host: u.hostname, port: Number(u.port) || (u.protocol === 'https:' ? 443 : 80) };
      } catch {
        return null;
      }
    }
    default:
      return null;
  }
}

function tcpProbe(host: string, port: number, timeoutMs: number): Promise<boolean> {
  return new Promise((resolve) => {
    const sock = new net.Socket();
    let done = false;
    const finish = (ok: boolean) => {
      if (done) return;
      done = true;
      sock.destroy();
      resolve(ok);
    };
    sock.setTimeout(timeoutMs);
    sock.once('connect', () => finish(true));
    sock.once('timeout', () => finish(false));
    sock.once('error', () => finish(false));
    sock.connect(port, host);
  });
}

export function revealConnectionsFile(): void {
  const p = connectionsFilePath();
  if (p) vscode.window.showInformationMessage(`Connections are stored at ${p} (secrets excluded).`);
}
