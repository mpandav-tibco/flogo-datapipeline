// Package pipeline implements the "pipeline:" resource type and the
// DataPipelineAction — a first-class Flogo action whose runtime is a
// multi-format ETL/ELT engine rather than a flow DAG.
package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"

	"github.com/project-flogo/core/app/resource"
)

// ResType is the Flogo resource type for a pipeline definition.
// Pipeline definitions are declared as "pipeline:<id>" resources —
// a first-class peer of "flow:" and "agent:".
const ResType = "pipeline"

// ColumnDef declares a column for sources that require an explicit schema
// (CSV without auto-inference, fixed-width, delimited flat files).
type ColumnDef struct {
	Name           string `json:"name"`
	Type           string `json:"type"`                     // string | int | float | bool | date | decimal
	Scale          int    `json:"scale,omitempty"`          // decimal places
	Format         string `json:"format,omitempty"`         // date format e.g. "2006-01-02"
	Start          int    `json:"start,omitempty"`          // fixed-width: 1-based column start position
	Length         int    `json:"length,omitempty"`         // fixed-width: column byte length
	ImpliedDecimal int    `json:"impliedDecimal,omitempty"` // fixed-width: implied decimal places
	Trim           bool   `json:"trim,omitempty"`           // trim whitespace
	Nullable       bool   `json:"nullable,omitempty"`
}

// SourceDef configures the data source for the pipeline.
// The "type" field selects the registered source factory.
type SourceDef struct {
	Type     string                 `json:"type"`               // csv | parquet | fixedwidth | xml | jsonlines | jdbc
	Path     string                 `json:"path,omitempty"`     // file path or glob pattern
	Encoding string                 `json:"encoding,omitempty"` // utf-8 | ebcdic-037 | latin-1
	Columns  []ColumnDef            `json:"columns,omitempty"`  // explicit schema (optional for self-describing formats)
	Options  map[string]interface{} `json:"options,omitempty"`  // format-specific options

	// ErrorSink routes deserialization failures from streaming sources (malformed
	// JSON, schema-mismatch, encoding errors) to a DLQ rather than aborting the
	// source.  Expressed as an inline sink definition: the "type" field selects
	// the registered sink factory and "options" (or any other key) is passed
	// through verbatim.  When omitted, a deserialization error aborts the run.
	//
	// Example:
	//   "errorSink": { "type": "dlq", "innerType": "file", "innerOptions": { "path": "/tmp/src-errors" } }
	ErrorSink map[string]interface{} `json:"errorSink,omitempty"`
}

// UnmarshalJSON flattens the entire source object into Options so that
// source-specific keys can be written either at the top level (e.g. "dsn",
// "driver", "table", "brokers", "topic") or nested under "options" — matching
// the ergonomics of SinkDef. The dedicated struct fields (Type, Path, Encoding,
// Columns, ErrorSink) are also populated for typed access. buildSourceConfig
// copies Options over the config map, so flat keys reach the source factory.
func (s *SourceDef) UnmarshalJSON(data []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if typ, ok := raw["type"].(string); ok {
		s.Type = typ
	}
	if p, ok := raw["path"].(string); ok {
		s.Path = p
	}
	if e, ok := raw["encoding"].(string); ok {
		s.Encoding = e
	}
	// Decode columns (if present) into the typed slice.
	if _, ok := raw["columns"]; ok {
		var wrapper struct {
			Columns []ColumnDef `json:"columns"`
		}
		if err := json.Unmarshal(data, &wrapper); err != nil {
			return err
		}
		s.Columns = wrapper.Columns
	}
	if es, ok := raw["errorSink"].(map[string]interface{}); ok {
		s.ErrorSink = es
	}
	// Flatten everything (except the reserved structural keys) into Options so
	// flat top-level keys reach the source factory. Keys nested under an explicit
	// "options" object are merged in as well and take precedence.
	opts := make(map[string]interface{})
	for k, v := range raw {
		switch k {
		case "type", "path", "encoding", "columns", "errorSink", "options":
			continue
		}
		opts[k] = v
	}
	if nested, ok := raw["options"].(map[string]interface{}); ok {
		for k, v := range nested {
			opts[k] = v
		}
	}
	if len(opts) > 0 {
		s.Options = opts
	}
	return nil
}

// TransformDef describes a single transform stage.
// The "type" field selects the registered transform factory.
// All other fields are passed to the factory as the config map.
type TransformDef struct {
	Type string `json:"type"` // filter | map | lookup | validate | aggregate
	// Inline the remaining fields so the full TransformDef map is passed to the factory.
	Extra map[string]interface{} `json:"-"`
}

func (t *TransformDef) UnmarshalJSON(data []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if typ, ok := raw["type"].(string); ok {
		t.Type = typ
	}
	t.Extra = raw
	return nil
}

// SinkDef configures the data sink for the pipeline.
type SinkDef struct {
	Type    string                 `json:"type"`              // jdbc | parquet | csv | kafka | null
	Options map[string]interface{} `json:"options,omitempty"` // type-specific options (flattened at parse time)
}

func (s *SinkDef) UnmarshalJSON(data []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if typ, ok := raw["type"].(string); ok {
		s.Type = typ
	}
	s.Options = raw
	return nil
}

// CheckpointDef configures the checkpoint backend.
type CheckpointDef struct {
	Type            string `json:"type"`                      // file | redis | none
	Path            string `json:"path,omitempty"`            // file: directory for checkpoint files
	Addr            string `json:"addr,omitempty"`            // redis: host:port
	PasswordEnv     string `json:"passwordEnv,omitempty"`     // redis: env var holding password
	KeyPrefix       string `json:"keyPrefix,omitempty"`       // redis: key namespace
	IntervalRows    int    `json:"intervalRows,omitempty"`    // checkpoint every N rows (default 10000)
	IntervalSeconds int    `json:"intervalSeconds,omitempty"` // checkpoint every N seconds (default 30)
	DeleteOnSuccess bool   `json:"deleteOnSuccess,omitempty"` // delete checkpoint on successful completion
}

// PerformanceDef tunes the pipeline execution parameters.
type PerformanceDef struct {
	BatchSize           int `json:"batchSize,omitempty"`           // rows per RecordBatch (default 5000)
	TransformWorkers    int `json:"transformWorkers,omitempty"`    // parallel transform goroutines (default 1)
	SourceBufferBatches int `json:"sourceBufferBatches,omitempty"` // channel depth between source and transforms (default 4)
	SinkBufferBatches   int `json:"sinkBufferBatches,omitempty"`   // channel depth between transforms and sink (default 4)
	MaxMemoryMB         int `json:"maxMemoryMB,omitempty"`         // backpressure: block source when in-flight batches exceed this (default 512)
	RowGroupParallelism int `json:"rowGroupParallelism,omitempty"` // parquet: concurrent row-group readers (default 4)
}

// IncrementalDef configures incremental-load / CDC behaviour for the pipeline.
// When Mode is "hwm", the source injects a WHERE clause on the watermark column
// so only rows newer than the last saved mark are read.  The hwm transform must
// be present in the Transforms list to track the in-batch maximum; the engine
// persists the mark to the configured store at each checkpoint interval and at
// pipeline completion.
//
// On the very first run (or after a pipeline reset), no mark exists.  The
// source performs a full-table load.  Subsequent runs only fetch deltas.
// HWM-mode pipelines MUST use onConflict:upsert at the sink to guarantee
// idempotent re-processing of the last batch in the event of a partial failure.
type IncrementalDef struct {
	// Mode selects the incremental strategy.  "hwm" enables high-water-mark
	// delta loading.  "none" (or omitted) disables incremental behaviour and
	// the pipeline always performs a full load.
	Mode string `json:"mode,omitempty"` // "hwm" | "none"
	// Column is the watermark column name (e.g. "updated_at", "seq_id").
	// Required when Mode is "hwm".
	Column string `json:"column,omitempty"`
	// ValueType is the column's logical type.  One of:
	//   "datetime" — the column contains timestamps (time.Time / TIMESTAMPTZ).
	//                Values are stored as RFC3339Nano strings.
	//   "int"      — the column contains integer sequence numbers.
	//   "string"   — lexicographic ordering (e.g. UUIDs with monotonic generation).
	// Defaults to "string" if omitted.
	ValueType string `json:"valueType,omitempty"` // "datetime" | "int" | "string"
	// InitialValue is the watermark to use on the very first run when the store
	// has no prior mark.  When empty and no stored mark exists the pipeline
	// performs a full-table load without any WHERE predicate.
	InitialValue string `json:"initialValue,omitempty"`
	// StoreType selects the watermark backend.  One of "file" | "redis" | "postgres".
	// Defaults to "file".
	StoreType string `json:"storeType,omitempty"` // "file" | "redis" | "postgres"
	// StoreOptions passes backend-specific configuration (DSN, addr, path, etc.)
	// to the watermark store factory.
	StoreOptions map[string]interface{} `json:"storeOptions,omitempty"`
}

// ScalingDef controls multi-instance horizontal scaling.
type ScalingDef struct {
	Mode               string          `json:"mode"`                         // single | sharded | distributed
	InstanceIndex      int             `json:"instanceIndex,omitempty"`      // sharded: 0-based index of this instance
	InstanceCount      int             `json:"instanceCount,omitempty"`      // sharded: total number of instances
	Coordinator        *CoordinatorDef `json:"coordinator,omitempty"`        // distributed: work queue config
	PartitionStrategy  string          `json:"partitionStrategy,omitempty"`  // rowgroup | file | byterange
	PartitionsPerClaim int             `json:"partitionsPerClaim,omitempty"` // distributed: claim batch size
}

// CoordinatorDef configures the distributed work queue.
type CoordinatorDef struct {
	Type             string `json:"type"`                       // redis | postgres
	Addr             string `json:"addr,omitempty"`             // redis: host:port
	DSN              string `json:"dsn,omitempty"`              // postgres: connection string
	PasswordEnv      string `json:"passwordEnv,omitempty"`      // redis: env var for password
	LeaseSeconds     int    `json:"leaseSeconds,omitempty"`     // claim lease TTL (default 120)
	HeartbeatSeconds int    `json:"heartbeatSeconds,omitempty"` // lease renewal interval (default 30)
	MaxRetries       int    `json:"maxRetries,omitempty"`       // partition retry limit before marking failed (default 3)
}

// SchemaDriftDef configures the schema evolution / drift handling behaviour
// for the pipeline. When Policy is non-empty, a schemadrift transform is
// automatically prepended to the transform pipeline before any user-defined
// transforms are applied.
//
// Policy values:
//
//	"project"  — (default) only the baseline schema columns pass downstream;
//	             new source columns are ignored, dropped columns emit nil.
//	"additive" — new source columns are merged into the baseline and propagated.
//	             Type changes are adopted (type promotion). JDBC sink must have
//	             autoMigrate:true for new columns to reach the database.
//	"strict"   — any schema change aborts the pipeline run with an error.
//	"drop"     — any batch with a schema deviation is silently discarded in full.
//
// OnDrift values:
//
//	"log"    — (default) emit a Warn-level log entry for each detected change.
//	"alert"  — reserved; currently behaves identically to "log".
//	"silent" — suppress all drift-related log output.
type SchemaDriftDef struct {
	// Policy selects the drift remediation strategy.
	Policy string `json:"policy"` // "project" | "additive" | "strict" | "drop"

	// OnDrift controls how detected drift is surfaced to operators.
	OnDrift string `json:"onDrift,omitempty"` // "log" | "alert" | "silent"
}

// DLQDef configures the Dead-Letter Queue destination for write-time failures.
//
// When a sink's WriteBatch call fails, the pipeline falls back to per-row
// isolation: each row is retried individually.  Rows that still fail after
// isolation are appended with an error-envelope JSON column and forwarded to the
// sink described here.
//
// The DLQ sink is opened lazily on the first bad-row event; pipelines with no
// write-time failures pay zero overhead.
//
// The "type" and "options" fields here are passed to sink.New as the inner sink
// of the "dlq" wrapper.  Alternatively, set Type to "dlq" and supply the full
// dlq configuration (including innerType / innerOptions) in Options.
//
// Example (DLQ to Postgres table):
//
//	"dlq": {
//	  "type":            "jdbc",
//	  "options":         { "driver": "postgres", "dsn": "...", "table": "pipeline_dlq" },
//	  "maxRowsPerBatch": 1000,
//	  "onFull":          "drop"
//	}
//
// Example (DLQ to S3 object store, via the "dlq" type wrapper):
//
//	"dlq": {
//	  "type":    "dlq",
//	  "options": {
//	    "innerType":    "objstore",
//	    "innerOptions": { "bucket": "my-dlq", "prefix": "pipeline/errors" }
//	  }
//	}
type DLQDef struct {
	// Type is the sink type for the DLQ destination (e.g. "jdbc", "file", "null").
	// Required when the DLQ block is present; if empty the DLQ is disabled.
	Type string `json:"type,omitempty"`

	// Options is passed verbatim to sink.New as the config map.
	Options map[string]interface{} `json:"options,omitempty"`

	// MaxRowsPerBatch limits how many DLQ rows are forwarded per write call.
	// 0 (default) means unlimited — all rows in the bad-row accumulation batch
	// are written in a single call to the inner sink.
	MaxRowsPerBatch int `json:"maxRowsPerBatch,omitempty"`

	// OnFull controls behaviour when MaxRowsPerBatch is exceeded.
	//   "block" (default) — write all rows even if the batch exceeds the limit;
	//                       the inner sink may split internally.
	//   "drop"            — silently discard overflow rows beyond MaxRowsPerBatch.
	OnFull string `json:"onFull,omitempty"`
}

// ObservabilityDef configures the metrics backend for a pipeline.
// Set "metrics" to "prometheus", "otel", or "none" (default).
// The corresponding backend sub-package must be imported (blank import) for the
// named backend to be available in the registry.
//
// Example JSON:
//
//	"observability": {
//	  "metrics": "prometheus",
//	  "options": { "listenAddr": ":9090", "path": "/metrics" },
//	  "stallSeconds": 30
//	}
type ObservabilityDef struct {
	// Metrics selects the backend: "prometheus" | "otel" | "none" (default).
	Metrics string `json:"metrics,omitempty"`
	// Options is passed verbatim to the backend factory (listenAddr, endpoint, …).
	Options map[string]interface{} `json:"options,omitempty"`
	// StallSeconds is the per-batch wall-clock threshold (seconds) after which a
	// Stall event is emitted. Zero means use the default (30 s). Negative disables.
	StallSeconds int `json:"stallSeconds,omitempty"`
}

// Definition is the declarative pipeline spec carried by a "pipeline:" resource.
// All fields map directly to the JSON structure of the pipeline resource file.
type Definition struct {
	Name        string         `json:"name,omitempty"`
	Description string         `json:"description,omitempty"`
	Source      SourceDef      `json:"source"`
	Transforms  []TransformDef `json:"transforms,omitempty"`
	Sink        SinkDef        `json:"sink"`
	Checkpoint  CheckpointDef  `json:"checkpoint,omitempty"`
	Performance PerformanceDef `json:"performance,omitempty"`
	Scaling     ScalingDef     `json:"scaling,omitempty"`
	// SchemaDrift configures automatic schema evolution handling. When
	// Policy is non-empty, a schemadrift transform is prepended to the
	// transform pipeline automatically; the schemadrift package must be
	// imported (blank import) in the main package alongside this package.
	SchemaDrift SchemaDriftDef `json:"schemaDrift,omitempty"`
	// Observability configures the metrics backend. When Metrics is empty or
	// "none", a zero-cost NoopRecorder is used so there is no overhead.
	Observability ObservabilityDef `json:"observability,omitempty"`
	// Incremental configures high-water-mark incremental loading.  When Mode is
	// "hwm", the source injects a WHERE clause and only reads rows whose
	// watermark column value exceeds the last persisted mark.  An hwm transform
	// must be present in Transforms to track the in-batch maximum.
	Incremental IncrementalDef `json:"incremental,omitempty"`

	// DLQ configures the Dead-Letter Queue for write-time row failures.
	// When a batch write fails, the engine falls back to per-row isolation:
	// each row is retried individually.  Rows that still fail are routed to this
	// sink with an error-envelope column added.  When omitted or Type is empty,
	// failed rows are counted in RowsFailed but not forwarded anywhere.
	DLQ DLQDef `json:"dlq,omitempty"`
}

// Defaults is the public alias of defaults, exposed for use by the SDK builder.
func (d *Definition) Defaults() { d.defaults() }

// Validate is the public alias of validate, exposed for use by the SDK builder.
func (d *Definition) Validate() error { return d.validate() }

// defaults fills in zero values with sensible production-ready defaults.
func (d *Definition) defaults() {
	if d.Performance.BatchSize == 0 {
		d.Performance.BatchSize = 5000
	}
	if d.Performance.SourceBufferBatches == 0 {
		d.Performance.SourceBufferBatches = 4
	}
	if d.Performance.SinkBufferBatches == 0 {
		d.Performance.SinkBufferBatches = 4
	}
	if d.Performance.MaxMemoryMB == 0 {
		d.Performance.MaxMemoryMB = 512
	}
	if d.Performance.RowGroupParallelism == 0 {
		d.Performance.RowGroupParallelism = 4
	}
	if d.Checkpoint.IntervalRows == 0 {
		d.Checkpoint.IntervalRows = 10000
	}
	if d.Checkpoint.IntervalSeconds == 0 {
		d.Checkpoint.IntervalSeconds = 30
	}
	if d.Scaling.Mode == "" {
		d.Scaling.Mode = "single"
	}
	if d.Observability.StallSeconds == 0 {
		d.Observability.StallSeconds = 30
	}
	if d.Incremental.Mode == "hwm" {
		if d.Incremental.ValueType == "" {
			d.Incremental.ValueType = "string"
		}
		if d.Incremental.StoreType == "" {
			d.Incremental.StoreType = "file"
		}
	}
	if d.Scaling.Coordinator != nil {
		if d.Scaling.Coordinator.LeaseSeconds == 0 {
			d.Scaling.Coordinator.LeaseSeconds = 120
		}
		if d.Scaling.Coordinator.HeartbeatSeconds == 0 {
			d.Scaling.Coordinator.HeartbeatSeconds = 30
		}
		if d.Scaling.Coordinator.MaxRetries == 0 {
			d.Scaling.Coordinator.MaxRetries = 3
		}
	}
	if d.DLQ.Type != "" && d.DLQ.OnFull == "" {
		d.DLQ.OnFull = "block"
	}
}

func (d *Definition) validate() error {
	if d.Source.Type == "" {
		return fmt.Errorf("pipeline definition: source.type is required")
	}
	if d.Sink.Type == "" {
		return fmt.Errorf("pipeline definition: sink.type is required")
	}
	if d.Incremental.Mode == "hwm" {
		if d.Incremental.Column == "" {
			return fmt.Errorf("pipeline definition: incremental.column is required when mode is 'hwm'")
		}
		switch d.Incremental.ValueType {
		case "datetime", "int", "string", "":
			// valid
		default:
			return fmt.Errorf("pipeline definition: incremental.valueType must be 'datetime', 'int', or 'string'")
		}
	}
	if d.DLQ.Type != "" {
		switch d.DLQ.OnFull {
		case "block", "drop":
			// valid
		default:
			return fmt.Errorf("pipeline definition: dlq.onFull must be 'block' or 'drop', got %q", d.DLQ.OnFull)
		}
	}
	return nil
}

// defLoader implements resource.Loader for the "pipeline:" resource type.
type defLoader struct{}

// Compile-time assertion that defLoader satisfies resource.Loader.
var _ resource.Loader = defLoader{}

func (defLoader) LoadResource(config *resource.Config) (*resource.Resource, error) {
	var def Definition
	if err := json.Unmarshal(config.Data, &def); err != nil {
		return nil, err
	}
	// Expand ${ENV_VAR} placeholders in string config values so deployed
	// pipelines can inject secrets (DSNs, keys, tokens) via environment
	// variables — e.g. a container references "${DB_PASSWORD}" and Kubernetes
	// supplies it from a Secret. Unset variables expand to "" (like a shell).
	def.expandEnv()
	def.defaults()
	if err := def.validate(); err != nil {
		return nil, err
	}
	return resource.New(ResType, &def), nil
}

// envVarRe matches ${NAME} placeholders (NAME = letters, digits, underscore).
var envVarRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// reservedTemplateVars are engine-internal placeholders expanded elsewhere
// (incremental high-water mark, date/path templating). They must NOT be treated
// as environment variables.
var reservedTemplateVars = map[string]bool{
	"hwm": true, "date": true, "year": true, "month": true, "day": true,
	"uuid": true, "runId": true,
}

// expandEnvString replaces ${VAR} in s with the environment value of VAR — but
// only when VAR is actually set in the environment and is not a reserved
// engine template (e.g. ${hwm}, ${date}). Unset / reserved placeholders are
// left untouched so their dedicated substitution still runs.
func expandEnvString(s string) string {
	return envVarRe.ReplaceAllStringFunc(s, func(m string) string {
		name := envVarRe.FindStringSubmatch(m)[1]
		if reservedTemplateVars[name] {
			return m
		}
		if v, ok := os.LookupEnv(name); ok {
			return v
		}
		return m
	})
}

// expandEnvValue recursively expands ${ENV} placeholders in any string values
// held in a config value (string, map, or slice).
func expandEnvValue(v interface{}) interface{} {
	switch t := v.(type) {
	case string:
		return expandEnvString(t)
	case map[string]interface{}:
		for k, val := range t {
			t[k] = expandEnvValue(val)
		}
		return t
	case []interface{}:
		for i, val := range t {
			t[i] = expandEnvValue(val)
		}
		return t
	default:
		return v
	}
}

// expandEnv expands ${ENV} placeholders across the source, sink, dlq and
// transform config maps (and the source path). Structural/numeric fields are
// left untouched.
func (d *Definition) expandEnv() {
	d.Source.Path = expandEnvString(d.Source.Path)
	if d.Source.Options != nil {
		expandEnvValue(d.Source.Options)
	}
	if d.Sink.Options != nil {
		expandEnvValue(d.Sink.Options)
	}
	if d.DLQ.Options != nil {
		expandEnvValue(d.DLQ.Options)
	}
	for i := range d.Transforms {
		if d.Transforms[i].Extra != nil {
			expandEnvValue(d.Transforms[i].Extra)
		}
	}
}

