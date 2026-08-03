// Message protocol between the extension host and the pipeline webview.

import type { PipelineDoc, ValidationIssue } from './model/pipeline';
import type { RunResponse, PreviewResult, DefinitionSummary, StageTestResult } from './engine/apiClient';
import type { ConnectionSummary } from './connections/store';

// Host -> Webview
export type HostToWebview =
  | { type: 'init'; doc: PipelineDoc; fileName: string; ref: string; nonce: string }
  | { type: 'update'; doc: PipelineDoc }
  | { type: 'connections'; items: ConnectionSummary[] }
  | { type: 'runStarting' }
  | { type: 'runStarted'; port: number }
  | { type: 'runResult'; result: RunResponse; final: boolean }
  | { type: 'runError'; message: string }
  | { type: 'stopped' }
  | { type: 'validationResult'; issues: ValidationIssue[] }
  | { type: 'previewResult'; result: PreviewResult }
  | { type: 'previewError'; message: string }
  | { type: 'definition'; def: DefinitionSummary }
  | { type: 'metricsResult'; data?: unknown; error?: string }
  | { type: 'browseResult'; id: string; path: string }
  | { type: 'browseCancelled'; id: string }
  | { type: 'schemaInferred'; columns: Array<{ name: string; type: string }>; count: number }
  | { type: 'schemaInferError'; message: string }
  | { type: 'stageSample'; stage: number; columns: string[]; rows: Array<Record<string, unknown>> }
  | { type: 'stageSampleError'; stage: number; message: string }
  | { type: 'stageTestResult'; stage: number; result: StageTestResult }
  | { type: 'stageTestError'; stage: number; message: string }
  | { type: 'dbTables'; target: 'source' | 'sink' | 'dlq'; tables: string[] }
  | { type: 'dbTablesError'; target: 'source' | 'sink' | 'dlq'; message: string }
  | { type: 'dbColumns'; target: 'source' | 'sink' | 'dlq'; columns: Array<{ name: string; type: string; dbType?: string; nullable?: boolean }> }
  | { type: 'dbColumnsError'; target: 'source' | 'sink' | 'dlq'; message: string }
  | { type: 'sourceSchema'; columns: Array<{ name: string; type: string }> }
  | { type: 'sourceSchemaError'; message: string }
  | { type: 'objectList'; target: 'source' | 'sink' | 'dlq'; objects: Array<{ key: string; size: number }> }
  | { type: 'objectListError'; target: 'source' | 'sink' | 'dlq'; message: string }
  | { type: 'kafkaTopics'; target: 'source' | 'sink' | 'dlq'; topics: string[] }
  | { type: 'kafkaTopicsError'; target: 'source' | 'sink' | 'dlq'; message: string }
  | { type: 'schemaRegistry'; columns: Array<{ name: string; type: string; nullable?: boolean }>; subject: string }
  | { type: 'schemaRegistryError'; message: string }
  | { type: 'xmlSchema'; recordElement: string; fields: Array<{ name: string; xpath: string; type: string }>; columns: Array<{ name: string; type: string }> }
  | { type: 'xmlSchemaError'; message: string };

// Webview -> Host
export type WebviewToHost =
  | { type: 'ready' }
  | { type: 'save'; doc: PipelineDoc }
  | { type: 'run'; fresh?: boolean }
  | { type: 'stop' }
  | { type: 'validate' }
  | { type: 'preview'; limit?: number }
  | { type: 'export'; format: string }
  | { type: 'openRaw' }
  | { type: 'metrics' }
  | { type: 'definition' }
  | { type: 'showOutput' }
  | { type: 'showProblems' }
  | { type: 'showDataPreview' }
  | { type: 'showTerminal' }
  | { type: 'browse'; id: string; mode?: 'file' | 'folder' }
  | { type: 'newFromTemplate' }
  | { type: 'newConnection'; family?: string }
  | { type: 'testConnection'; id: string }
  | { type: 'editConnection'; id: string }
  | { type: 'requestConnections' }
  | { type: 'inferSchema'; path: string; sourceType?: string; delimiter?: string; hasHeader?: boolean }
  | { type: 'sampleStageInput'; stage: number; limit?: number }
  | { type: 'testStage'; stage: number; columns: string[]; rows: Array<Record<string, unknown>> }
  | { type: 'fetchDbTables'; target: 'source' | 'sink' | 'dlq' }
  | { type: 'fetchDbColumns'; target: 'source' | 'sink' | 'dlq'; table: string }
  | { type: 'fetchSourceSchema' }
  | { type: 'fetchObjects'; target: 'source' | 'sink' | 'dlq' }
  | { type: 'fetchKafkaTopics'; target: 'source' | 'sink' | 'dlq' }
  | { type: 'fetchSchemaRegistry' }
  | { type: 'fetchXmlSchema' }
  | { type: 'deploy'; replicas: number }
  | { type: 'showError'; text: string }
  | { type: 'copyText'; text: string }
  | { type: 'toast'; kind: 'ok' | 'err' | 'warn' | 'info'; message: string };
