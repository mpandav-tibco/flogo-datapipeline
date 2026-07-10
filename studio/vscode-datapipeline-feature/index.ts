// Copyright (c) 2024-2026. Cloud Software Group, Inc. All Rights Reserved. Confidential & Proprietary
/**
 * Public surface of the Data Pipeline feature. Import `registerDataPipelineFeature`
 * from `extension.ts::activate()`.
 */
export {
  registerDataPipelineFeature,
  DataPipelineCommands,
} from './register';
export { DataPipelineEditorProvider } from './datapipeline-editor-provider';
export { PipelinesTreeProvider } from './pipelines-tree-provider';
export { DataPipelineEngineClient, parseSummaryLine } from './engine-client';
export { DataPipelineDiagnostics, buildDiagnostics } from './diagnostics';
export * from './types';
