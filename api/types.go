// Package api defines the REST management API for flogo-datapipeline.
// The server is optional — it is only started when PIPELINE_API_ADDR is set
// (or --api-addr is passed on the command line). When active it runs alongside
// the Flogo engine in a separate goroutine and shares the factory's action cache.
package api

import "time"

// PipelineRunRequest is the body of POST /v1/pipelines/{ref}/run.
type PipelineRunRequest struct {
	// PipelineRef is the Flogo resource reference, e.g. "res://pipeline:daily-tx".
	// When set it overrides the {ref} path parameter (useful for proxy callers).
	PipelineRef string `json:"pipelineRef,omitempty"`

	// RunID is an optional caller-supplied idempotency token.
	// When empty the server generates a random 16-hex-char ID.
	RunID string `json:"runId,omitempty"`

	// Params are key-value pairs injected into the source path template,
	// e.g. {"date": "2026-07-07"} expands "${date}" in the pipeline path.
	Params map[string]interface{} `json:"params,omitempty"`

	// InputPath overrides the source path declared in the pipeline definition
	// for this run only. Useful for ad-hoc file processing.
	InputPath string `json:"inputPath,omitempty"`
}

// PipelineRunResponse is returned by POST /v1/pipelines/{ref}/run (sync) and
// stored in the per-ref ring buffer for GET /v1/pipelines/{ref}/status.
type PipelineRunResponse struct {
	// RunID is the identifier for this run — either caller-supplied or server-generated.
	RunID string `json:"runId"`

	// Status is one of: "running", "completed", "partial", "failed", "cancelled".
	Status string `json:"status"`

	// RowsProcessed is the total number of rows read from the source.
	RowsProcessed int64 `json:"rowsProcessed"`

	// RowsWritten is the number of rows successfully written to the sink.
	RowsWritten int64 `json:"rowsWritten"`

	// RowsFailed is the number of rows that encountered a processing error.
	RowsFailed int64 `json:"rowsFailed"`

	// RowsDropped is the number of rows intentionally dropped by a transform
	// (e.g. filter, schemadrift policy=drop). Distinct from RowsFailed.
	RowsDropped int64 `json:"rowsDropped,omitempty"`

	// DurationMs is the wall-clock duration of the run in milliseconds.
	DurationMs int64 `json:"durationMs"`

	// Error holds the error message if Status is "failed".
	Error string `json:"error,omitempty"`

	// StartedAt is the UTC wall-clock time the run began.
	StartedAt time.Time `json:"startedAt"`

	// FinishedAt is the UTC wall-clock time the run ended (zero while running).
	FinishedAt time.Time `json:"finishedAt,omitempty"`

	// QualityReport holds DQ expectations results when the pipeline has a DQ step.
	QualityReport *DQReport `json:"qualityReport,omitempty"`

	// StageStats holds per-transform row counters (RowsIn/RowsOut) for the graph
	// run overlay, in transform order. Present for single-worker runs.
	StageStats []StageRowStat `json:"stageStats,omitempty"`

	// GraphStats holds per-node row counters for a DAG (graph-runner) run overlay.
	GraphStats []GraphNodeStat `json:"graphStats,omitempty"`
}

// StageRowStat reports one transform stage's row throughput for a run.
type StageRowStat struct {
	Name    string `json:"name"`
	RowsIn  int64  `json:"rowsIn"`
	RowsOut int64  `json:"rowsOut"`
}

// GraphNodeStat reports one graph node's row throughput for a DAG run.
type GraphNodeStat struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	RowsIn      int64  `json:"rowsIn"`
	RowsOut     int64  `json:"rowsOut"`
	RowsWritten int64  `json:"rowsWritten"`
	RowsErrored int64  `json:"rowsErrored"`
}

// DQReport carries the data-quality expectation results for a single run.
type DQReport struct {
	// Passed is the number of DQ expectations that were satisfied.
	Passed int `json:"passed"`
	// Failed is the number of DQ expectations that were violated.
	Failed int `json:"failed"`
	// Violations is a human-readable list of violated expectation descriptions.
	Violations []string `json:"violations,omitempty"`
}

// PipelineListResponse is returned by GET /v1/pipelines.
type PipelineListResponse struct {
	Pipelines []PipelineInfo `json:"pipelines"`
}

// PipelineInfo summarises a registered pipeline and its most recent run.
type PipelineInfo struct {
	// Ref is the Flogo resource reference, e.g. "res://pipeline:daily-tx".
	Ref string `json:"ref"`

	// SourceType is the source format declared in the pipeline definition (e.g. "csv", "jdbc").
	SourceType string `json:"sourceType"`

	// SinkType is the sink type declared in the pipeline definition (e.g. "jdbc", "parquet").
	SinkType string `json:"sinkType"`

	// Status is the last-known lifecycle status:
	//   "idle"     — never run or no runs on record
	//   "running"  — a run is currently in flight
	//   "completed"— last run finished successfully
	//   "partial"  — last run finished with some failed rows
	//   "failed"   — last run aborted with an error
	Status string `json:"status"`

	// LastRunAt is the start time of the most recent run (zero if never run).
	LastRunAt time.Time `json:"lastRunAt,omitempty"`

	// LastRunDurationMs is the duration of the most recent completed run.
	LastRunDurationMs int64 `json:"lastRunDurationMs,omitempty"`

	// LastRunStatus is the terminal status of the most recent run.
	LastRunStatus string `json:"lastRunStatus,omitempty"`
}

// PipelineStatusResponse is returned by GET /v1/pipelines/{ref}/status.
// It includes both the live PipelineInfo and the last N run records.
type PipelineStatusResponse struct {
	PipelineInfo
	// Runs is the most recent run records (up to the ring buffer capacity).
	Runs []PipelineRunResponse `json:"runs"`
}

// ErrorResponse is the standard error body for all 4xx/5xx responses.
type ErrorResponse struct {
	// Code is a machine-readable error code (e.g. "not_found", "bad_request").
	Code string `json:"code"`
	// Message is a human-readable description of the error.
	Message string `json:"message"`
}

// HealthResponse is returned by GET /v1/health.
type HealthResponse struct {
	// Status is always "ok" when the server is reachable.
	Status string `json:"status"`
	// Pipelines is the number of registered pipeline actions.
	Pipelines int `json:"pipelines"`
}
