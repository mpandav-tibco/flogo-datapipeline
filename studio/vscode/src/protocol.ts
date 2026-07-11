// Message protocol between the extension host and the pipeline webview.

import type { PipelineDoc, ValidationIssue } from './model/pipeline';
import type { RunResponse, PreviewResult, DefinitionSummary } from './engine/apiClient';

// Host -> Webview
export type HostToWebview =
  | { type: 'init'; doc: PipelineDoc; fileName: string; ref: string; nonce: string }
  | { type: 'update'; doc: PipelineDoc }
  | { type: 'runStarting' }
  | { type: 'runStarted'; port: number }
  | { type: 'runLog'; line: string }
  | { type: 'runResult'; result: RunResponse; final: boolean }
  | { type: 'runError'; message: string }
  | { type: 'stopped' }
  | { type: 'validationResult'; issues: ValidationIssue[] }
  | { type: 'previewResult'; result: PreviewResult }
  | { type: 'previewError'; message: string }
  | { type: 'definition'; def: DefinitionSummary }
  | { type: 'metricsResult'; data?: unknown; error?: string }
  | { type: 'browseResult'; id: string; path: string }
  | { type: 'browseCancelled'; id: string };

// Webview -> Host
export type WebviewToHost =
  | { type: 'ready' }
  | { type: 'save'; doc: PipelineDoc }
  | { type: 'run' }
  | { type: 'stop' }
  | { type: 'validate' }
  | { type: 'preview'; limit?: number }
  | { type: 'export'; format: string }
  | { type: 'openRaw' }
  | { type: 'metrics' }
  | { type: 'definition' }
  | { type: 'browse'; id: string; mode?: 'file' | 'folder' }
  | { type: 'newFromTemplate' }
  | { type: 'newConnection' }
  | { type: 'deploy'; replicas: number }
  | { type: 'showError'; text: string }
  | { type: 'copyText'; text: string }
  | { type: 'toast'; kind: 'ok' | 'err' | 'warn' | 'info'; message: string };
