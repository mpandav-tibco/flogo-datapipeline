import * as vscode from 'vscode';

import { ConnectionStore, connType } from './store';

type ConnNodeKind = 'connection' | 'message';

export class ConnectionTreeItem extends vscode.TreeItem {
  constructor(
    label: string,
    public readonly kind: ConnNodeKind,
    public readonly connId?: string,
  ) {
    super(label, vscode.TreeItemCollapsibleState.None);
  }
}

export class ConnectionsSidebarProvider
  implements vscode.TreeDataProvider<ConnectionTreeItem>, vscode.Disposable
{
  private readonly emitter = new vscode.EventEmitter<ConnectionTreeItem | undefined | void>();
  readonly onDidChangeTreeData = this.emitter.event;
  private readonly sub: vscode.Disposable;

  constructor(private readonly store: ConnectionStore) {
    this.sub = store.onDidChange(() => this.refresh());
  }

  dispose(): void {
    this.emitter.dispose();
    this.sub.dispose();
  }

  refresh(): void {
    this.emitter.fire();
  }

  getTreeItem(element: ConnectionTreeItem): vscode.TreeItem {
    return element;
  }

  async getChildren(element?: ConnectionTreeItem): Promise<ConnectionTreeItem[]> {
    if (element) return [];
    if (!vscode.workspace.workspaceFolders?.length) {
      const msg = new ConnectionTreeItem('Open a folder to manage connections', 'message');
      return [msg];
    }
    const list = await this.store.list();
    if (!list.length) {
      const msg = new ConnectionTreeItem('No connections yet', 'message');
      msg.description = 'Add one with ⛁ New Connection';
      return [msg];
    }
    return list.map((c) => {
      const item = new ConnectionTreeItem(c.name, 'connection', c.id);
      const ct = connType(c.type);
      item.description = ct?.desc || c.type;
      item.iconPath = new vscode.ThemeIcon('database');
      item.contextValue = 'flogoConnection';
      const host = c.fields.host || c.fields.bucket || c.fields.brokers || c.fields.file || c.fields.baseUrl || '';
      item.tooltip = `${ct?.label || c.type}${host ? '\n' + host : ''}\nSecrets in VS Code Secret Storage`;
      const fileUri = this.store.fsPathFor(c.id);
      item.command = fileUri
        ? {
            command: 'vscode.openWith',
            title: 'Open Connection',
            arguments: [vscode.Uri.file(fileUri), 'flogoDataPipelineStudio.connectionEditor'],
          }
        : {
            command: 'flogoDataPipelineStudio.editConnection',
            title: 'Edit Connection',
            arguments: [item],
          };
      return item;
    });
  }
}
