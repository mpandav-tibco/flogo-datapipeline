// Copyright (c) 2024-2026. Cloud Software Group, Inc. All Rights Reserved. Confidential & Proprietary
/**
 * Static validation for `.datapipeline` documents, surfaced through the native
 * Problems panel. This is the data-pipeline analogue of the flow editor's
 * `buildDiagnostis()` helper — parse the definition and emit `Diagnostic`s the
 * user can click to navigate.
 */
import * as vscode from 'vscode';
import { getPipelineDefinition, parseApp } from './model';

const SOURCE = 'flogo-dp';

export class DataPipelineDiagnostics {
  private readonly collection =
    vscode.languages.createDiagnosticCollection('flogo-datapipeline');

  /** Re-validate a document if it is a `.datapipeline` file. */
  validate(doc: vscode.TextDocument): void {
    if (!isDataPipeline(doc)) {
      return;
    }
    this.collection.set(doc.uri, buildDiagnostics(doc));
  }

  clear(uri: vscode.Uri): void {
    this.collection.delete(uri);
  }

  dispose(): void {
    this.collection.dispose();
  }
}

export function isDataPipeline(doc: vscode.TextDocument): boolean {
  return doc.fileName.endsWith('.datapipeline');
}

/** Produce diagnostics for a `.datapipeline` document. */
export function buildDiagnostics(doc: vscode.TextDocument): vscode.Diagnostic[] {
  const text = doc.getText();
  const out: vscode.Diagnostic[] = [];

  const app = parseApp(text);
  if (!app) {
    out.push(wholeDoc(doc, 'Invalid JSON — cannot parse .datapipeline document'));
    return out;
  }

  const def = getPipelineDefinition(app);
  if (!def) {
    out.push(
      at(doc, /"resources"/, 'No resource with an id starting "pipeline:" was found'),
    );
    return out;
  }

  if (!def.source?.type) {
    out.push(at(doc, /"source"/, 'Pipeline is missing source.type'));
  }
  if (!def.sink?.type) {
    out.push(at(doc, /"sink"/, 'Pipeline is missing sink.type'));
  }
  (def.transforms ?? []).forEach((t, i) => {
    if (!t?.type) {
      out.push(
        at(
          doc,
          /"transforms"/,
          `transform[${i}] is missing a "type"`,
          vscode.DiagnosticSeverity.Warning,
        ),
      );
    }
  });

  // Engine-true guardrail: the Parquet sink stringifies all columns.
  if (def.sink?.type === 'parquet') {
    out.push(
      at(
        doc,
        /"sink"/,
        'Parquet sink writes all columns as strings — numeric fields will be stringified',
        vscode.DiagnosticSeverity.Information,
      ),
    );
  }

  return out;
}

function at(
  doc: vscode.TextDocument,
  needle: RegExp,
  message: string,
  severity: vscode.DiagnosticSeverity = vscode.DiagnosticSeverity.Error,
): vscode.Diagnostic {
  const idx = doc.getText().search(needle);
  const start = idx >= 0 ? doc.positionAt(idx) : new vscode.Position(0, 0);
  const end = idx >= 0 ? doc.positionAt(idx + (needle.source.length - 2)) : start;
  const d = new vscode.Diagnostic(new vscode.Range(start, end), message, severity);
  d.source = SOURCE;
  return d;
}

function wholeDoc(doc: vscode.TextDocument, message: string): vscode.Diagnostic {
  const lastLine = Math.max(0, doc.lineCount - 1);
  const range = new vscode.Range(0, 0, lastLine, 0);
  const d = new vscode.Diagnostic(range, message, vscode.DiagnosticSeverity.Error);
  d.source = SOURCE;
  return d;
}
