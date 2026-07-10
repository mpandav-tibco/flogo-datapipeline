// Copyright (c) 2024-2026. Cloud Software Group, Inc. All Rights Reserved. Confidential & Proprietary
/**
 * Always-on status-bar pulse: shows the engine/last-run state and doubles as a
 * shortcut to the Data Pipeline output channel.
 */
import * as vscode from 'vscode';
import { RunSummary } from './types';

export class DataPipelineStatusBar {
  private readonly item: vscode.StatusBarItem;

  constructor() {
    this.item = vscode.window.createStatusBarItem(
      vscode.StatusBarAlignment.Right,
      100,
    );
    this.item.command = 'flogo3.datapipeline.showOutput';
    this.setIdle();
  }

  setIdle(name?: string): void {
    this.item.text = `$(pulse) Data Pipeline${name ? ` · ${name}` : ''}`;
    this.item.tooltip = 'Flogo Data Pipeline — click to show output';
    this.item.show();
  }

  setRunning(name: string): void {
    this.item.text = `$(sync~spin) ${name} · running`;
    this.item.tooltip = 'Pipeline run in progress';
    this.item.show();
  }

  setResult(name: string, summary: RunSummary): void {
    const icon =
      summary.status === 'completed'
        ? '$(pass)'
        : summary.status === 'partial'
          ? '$(warning)'
          : '$(error)';
    this.item.text = `${icon} ${name} · ${fmt(summary.rowsWritten)} written`;
    this.item.tooltip = new vscode.MarkdownString(
      [
        `**${name}** — ${summary.status}`,
        '',
        `- processed: ${fmt(summary.rowsProcessed)}`,
        `- written: ${fmt(summary.rowsWritten)}`,
        `- dropped: ${fmt(summary.rowsDropped)}`,
        `- routed → DLQ: ${fmt(summary.rowsRouted)}`,
        `- failed: ${fmt(summary.rowsFailed)}`,
        `- duration: ${summary.durationMs} ms`,
      ].join('\n'),
    );
    this.item.show();
  }

  dispose(): void {
    this.item.dispose();
  }
}

function fmt(n: number): string {
  return n.toLocaleString('en-US');
}
