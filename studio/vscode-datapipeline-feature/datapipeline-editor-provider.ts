// Copyright (c) 2024-2026. Cloud Software Group, Inc. All Rights Reserved. Confidential & Proprietary
/**
 * Custom editor for `.datapipeline` files. Modeled directly on the extension's
 * `FlowCustomTextEditorProvider`: the JSON document remains the source of truth
 * (so undo/redo, dirty state, autosave and Git diff are native), and the webview
 * renders a canvas *onto* it.
 */
import * as vscode from 'vscode';
import { buildGraph, getPipelineDefinition, parseApp } from './model';
import { getCanvasHtml, makeNonce } from './webview';
import { DataPipelineDiagnostics } from './diagnostics';
import { WebviewInbound } from './types';

export class DataPipelineEditorProvider
  implements vscode.CustomTextEditorProvider
{
  public static readonly viewType = 'flogo3.datapipeline-editor';

  public static register(
    context: vscode.ExtensionContext,
    diagnostics: DataPipelineDiagnostics,
  ): vscode.Disposable {
    const provider = new DataPipelineEditorProvider(context, diagnostics);
    return vscode.window.registerCustomEditorProvider(
      DataPipelineEditorProvider.viewType,
      provider,
      {
        webviewOptions: { retainContextWhenHidden: true },
        supportsMultipleEditorsPerDocument: false,
      },
    );
  }

  constructor(
    private readonly context: vscode.ExtensionContext,
    private readonly diagnostics: DataPipelineDiagnostics,
  ) {}

  public async resolveCustomTextEditor(
    document: vscode.TextDocument,
    webviewPanel: vscode.WebviewPanel,
    _token: vscode.CancellationToken,
  ): Promise<void> {
    webviewPanel.webview.options = { enableScripts: true };
    const nonce = makeNonce();
    webviewPanel.webview.html = getCanvasHtml(webviewPanel.webview, nonce);

    const postModel = () => {
      const label = pipelineName(document);
      const def = getPipelineDefinition(parseApp(document.getText()));
      void webviewPanel.webview.postMessage({
        type: 'model',
        graph: buildGraph(def, label),
      });
    };

    const changeSub = vscode.workspace.onDidChangeTextDocument((e) => {
      if (e.document.uri.toString() === document.uri.toString()) {
        postModel();
        this.diagnostics.validate(document);
      }
    });

    const msgSub = webviewPanel.webview.onDidReceiveMessage(
      (msg: WebviewInbound) => {
        switch (msg.type) {
          case 'ready':
            postModel();
            this.diagnostics.validate(document);
            break;
          case 'run':
            void vscode.commands.executeCommand(
              'flogo3.datapipeline.run',
              document.uri,
            );
            break;
          case 'preview':
            void vscode.commands.executeCommand(
              'flogo3.datapipeline.preview',
              document.uri,
            );
            break;
          case 'reopenAsText':
            void vscode.commands.executeCommand(
              'vscode.openWith',
              document.uri,
              'default',
            );
            break;
          case 'selectNode':
            // Reserved: reveal the selected stage's config in an inspector
            // (future: embed @tibco/flogo-mapper for map/expression stages).
            break;
        }
      },
    );

    webviewPanel.onDidDispose(() => {
      changeSub.dispose();
      msgSub.dispose();
    });
  }
}

function pipelineName(document: vscode.TextDocument): string {
  const base = document.uri.path.split('/').pop() ?? 'pipeline';
  return base.replace(/\.datapipeline$/, '');
}
