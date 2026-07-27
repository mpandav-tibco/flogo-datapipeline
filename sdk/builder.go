package sdk

import (
	"encoding/json"
	"fmt"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"
)

// ---------------------------------------------------------------------------
// Source option types
// ---------------------------------------------------------------------------

// CSVOption is a functional option for the FromCSV builder method.
type CSVOption func(opts map[string]interface{})

// WithCSVHasHeader declares whether the CSV file has a header row (default true).
func WithCSVHasHeader(v bool) CSVOption {
	return func(opts map[string]interface{}) { opts["hasHeader"] = v }
}

// WithCSVDelimiter sets the field delimiter (default ",").
func WithCSVDelimiter(d string) CSVOption {
	return func(opts map[string]interface{}) { opts["delimiter"] = d }
}

// WithCSVEncoding sets the file encoding (e.g. "utf-8", "ebcdic-037").
func WithCSVEncoding(enc string) CSVOption {
	return func(opts map[string]interface{}) { opts["encoding"] = enc }
}

// WithCSVColumns sets an explicit column schema for the CSV source.
func WithCSVColumns(cols []pipeline.ColumnDef) CSVOption {
	return func(opts map[string]interface{}) { opts["columns"] = cols }
}

// JDBCOption is a functional option for FromJDBC / ToJDBC builder methods.
type JDBCOption func(opts map[string]interface{})

// WithJDBCDriver sets the database driver name (e.g. "postgres", "mysql", "sqlite3").
func WithJDBCDriver(driver string) JDBCOption {
	return func(opts map[string]interface{}) { opts["driver"] = driver }
}

// WithJDBCWhere adds a SQL WHERE predicate to the JDBC source query.
func WithJDBCWhere(where string) JDBCOption {
	return func(opts map[string]interface{}) { opts["where"] = where }
}

// WithJDBCOrderKey sets the keyset-pagination column for the JDBC source.
func WithJDBCOrderKey(col string) JDBCOption {
	return func(opts map[string]interface{}) { opts["orderKey"] = col }
}

// WithJDBCBatchSize sets the SQL fetch batch size (default 5000).
func WithJDBCBatchSize(n int) JDBCOption {
	return func(opts map[string]interface{}) { opts["batchSize"] = n }
}

// WithJDBCOnConflict sets the sink conflict-resolution strategy ("upsert", "ignore", "fail").
func WithJDBCOnConflict(strategy string) JDBCOption {
	return func(opts map[string]interface{}) { opts["onConflict"] = strategy }
}

// WithJDBCConflictKey sets the list of column names that form the upsert key.
func WithJDBCConflictKey(cols []string) JDBCOption {
	return func(opts map[string]interface{}) { opts["conflictKey"] = cols }
}

// WithJDBCColumns sets an explicit column list for the JDBC source SELECT.
func WithJDBCColumns(cols []string) JDBCOption {
	return func(opts map[string]interface{}) { opts["columns"] = cols }
}

// KafkaOption is a functional option for FromKafka / ToKafka builder methods.
type KafkaOption func(opts map[string]interface{})

// WithKafkaGroupID sets the Kafka consumer group ID.
func WithKafkaGroupID(id string) KafkaOption {
	return func(opts map[string]interface{}) { opts["groupId"] = id }
}

// WithKafkaOffsetReset sets the auto.offset.reset policy ("earliest" | "latest").
func WithKafkaOffsetReset(policy string) KafkaOption {
	return func(opts map[string]interface{}) { opts["autoOffsetReset"] = policy }
}

// WithKafkaFormat sets the message encoding format ("json" | "avro" | "protobuf").
func WithKafkaFormat(format string) KafkaOption {
	return func(opts map[string]interface{}) { opts["format"] = format }
}

// WithKafkaTLS enables TLS and sets the CA certificate path.
func WithKafkaTLS(caPath string) KafkaOption {
	return func(opts map[string]interface{}) {
		opts["tls"] = true
		opts["tlsCACert"] = caPath
	}
}

// WithKafkaSASL sets SASL/PLAIN credentials.
func WithKafkaSASL(mechanism, username, passwordEnv string) KafkaOption {
	return func(opts map[string]interface{}) {
		opts["saslMechanism"] = mechanism
		opts["saslUsername"] = username
		opts["saslPasswordEnv"] = passwordEnv
	}
}

// S3Option is a functional option for ToS3 builder method.
type S3Option func(opts map[string]interface{})

// WithS3Region sets the AWS region (default "us-east-1").
func WithS3Region(region string) S3Option {
	return func(opts map[string]interface{}) { opts["region"] = region }
}

// WithS3Format sets the object format ("parquet" | "csv" | "json").
func WithS3Format(format string) S3Option {
	return func(opts map[string]interface{}) { opts["format"] = format }
}

// WithS3Credentials sets static AWS credentials. Prefer IAM roles in production.
func WithS3Credentials(keyID, secretEnv string) S3Option {
	return func(opts map[string]interface{}) {
		opts["accessKeyId"] = keyID
		opts["secretAccessKeyEnv"] = secretEnv
	}
}

// WithS3Endpoint overrides the S3 endpoint URL (for MinIO and other S3-compatible stores).
func WithS3Endpoint(url string) S3Option {
	return func(opts map[string]interface{}) { opts["endpoint"] = url }
}

// WithS3PartSize sets the multipart upload part size in MiB (default 8).
func WithS3PartSize(mib int) S3Option {
	return func(opts map[string]interface{}) { opts["partSizeMiB"] = mib }
}

// ---------------------------------------------------------------------------
// Source builder methods
// ---------------------------------------------------------------------------

// FromCSV configures the pipeline to read from a CSV or TSV file at path.
// Functional options (CSVOption) override the defaults.
func (b *PipelineBuilder) FromCSV(path string, opts ...CSVOption) *PipelineBuilder {
	if b.def.Source.Type != "" {
		b.addErr("source already set to %q; cannot set FromCSV", b.def.Source.Type)
		return b
	}
	options := map[string]interface{}{
		"path":      path,
		"hasHeader": true,
	}
	for _, o := range opts {
		o(options)
	}
	b.def.Source = pipeline.SourceDef{
		Type:    "csv",
		Path:    path,
		Options: options,
	}
	return b
}

// FromJDBC configures the pipeline to read from a SQL database table.
// dsn is the database connection string; table is the source table name.
// Functional options (JDBCOption) override the defaults.
func (b *PipelineBuilder) FromJDBC(dsn, table string, opts ...JDBCOption) *PipelineBuilder {
	if b.def.Source.Type != "" {
		b.addErr("source already set to %q; cannot set FromJDBC", b.def.Source.Type)
		return b
	}
	if dsn == "" {
		b.addErr("FromJDBC: dsn is required")
		return b
	}
	if table == "" {
		b.addErr("FromJDBC: table is required")
		return b
	}
	options := map[string]interface{}{
		"dsn":    dsn,
		"table":  table,
		"driver": "postgres",
	}
	for _, o := range opts {
		o(options)
	}
	b.def.Source = pipeline.SourceDef{
		Type:    "jdbc",
		Options: options,
	}
	return b
}

// FromKafka configures the pipeline to read from a Kafka topic.
// brokers is a list of broker addresses; topic is the Kafka topic name.
func (b *PipelineBuilder) FromKafka(brokers []string, topic string, opts ...KafkaOption) *PipelineBuilder {
	if b.def.Source.Type != "" {
		b.addErr("source already set to %q; cannot set FromKafka", b.def.Source.Type)
		return b
	}
	if len(brokers) == 0 {
		b.addErr("FromKafka: at least one broker address is required")
		return b
	}
	if topic == "" {
		b.addErr("FromKafka: topic is required")
		return b
	}
	options := map[string]interface{}{
		"brokers": brokers,
		"topic":   topic,
	}
	for _, o := range opts {
		o(options)
	}
	b.def.Source = pipeline.SourceDef{
		Type:    "kafka",
		Options: options,
	}
	return b
}

// ---------------------------------------------------------------------------
// Sink builder methods
// ---------------------------------------------------------------------------

// ToJDBC configures the pipeline to write to a SQL database table.
// dsn is the database connection string; table is the target table name.
func (b *PipelineBuilder) ToJDBC(dsn, table string, opts ...JDBCOption) *PipelineBuilder {
	if b.def.Sink.Type != "" {
		b.addErr("sink already set to %q; cannot set ToJDBC", b.def.Sink.Type)
		return b
	}
	if dsn == "" {
		b.addErr("ToJDBC: dsn is required")
		return b
	}
	if table == "" {
		b.addErr("ToJDBC: table is required")
		return b
	}
	options := map[string]interface{}{
		"type":   "jdbc",
		"dsn":    dsn,
		"table":  table,
		"driver": "postgres",
	}
	for _, o := range opts {
		o(options)
	}
	b.def.Sink = pipeline.SinkDef{
		Type:    "jdbc",
		Options: options,
	}
	return b
}

// ToKafka configures the pipeline to publish to a Kafka topic.
// brokers is a list of broker addresses; topic is the target Kafka topic.
func (b *PipelineBuilder) ToKafka(brokers []string, topic string, opts ...KafkaOption) *PipelineBuilder {
	if b.def.Sink.Type != "" {
		b.addErr("sink already set to %q; cannot set ToKafka", b.def.Sink.Type)
		return b
	}
	if len(brokers) == 0 {
		b.addErr("ToKafka: at least one broker address is required")
		return b
	}
	if topic == "" {
		b.addErr("ToKafka: topic is required")
		return b
	}
	options := map[string]interface{}{
		"type":    "kafka",
		"brokers": brokers,
		"topic":   topic,
	}
	for _, o := range opts {
		o(options)
	}
	b.def.Sink = pipeline.SinkDef{
		Type:    "kafka",
		Options: options,
	}
	return b
}

// ToS3 configures the pipeline to write objects to an S3 (or S3-compatible) bucket.
// bucket is the S3 bucket name; key is the object key prefix or full key.
func (b *PipelineBuilder) ToS3(bucket, key string, opts ...S3Option) *PipelineBuilder {
	if b.def.Sink.Type != "" {
		b.addErr("sink already set to %q; cannot set ToS3", b.def.Sink.Type)
		return b
	}
	if bucket == "" {
		b.addErr("ToS3: bucket is required")
		return b
	}
	if key == "" {
		b.addErr("ToS3: key is required")
		return b
	}
	options := map[string]interface{}{
		"type":   "s3",
		"bucket": bucket,
		"key":    key,
		"format": "parquet",
		"region": "us-east-1",
	}
	for _, o := range opts {
		o(options)
	}
	b.def.Sink = pipeline.SinkDef{
		Type:    "s3",
		Options: options,
	}
	return b
}

// ---------------------------------------------------------------------------
// Transform builder methods
// ---------------------------------------------------------------------------

// Filter adds a filter transform stage that keeps only rows satisfying expr.
// expr is a boolean expression using field names (e.g. "amount > 0 AND status != 'VOID'").
func (b *PipelineBuilder) Filter(expr string) *PipelineBuilder {
	if expr == "" {
		b.addErr("Filter: expr is required")
		return b
	}
	b.def.Transforms = append(b.def.Transforms, pipeline.TransformDef{
		Type: "filter",
		Extra: map[string]interface{}{
			"type": "filter",
			"expr": expr,
		},
	})
	return b
}

// Map adds a field-mapping transform stage.
// mappings is a map from destination field name to a Flogo expression string
// (e.g. {"amount_usd": "=divide($.amount, 100)", "id": "=$txn_id"}).
func (b *PipelineBuilder) Map(mappings map[string]string) *PipelineBuilder {
	if len(mappings) == 0 {
		b.addErr("Map: mappings must not be empty")
		return b
	}
	fields := make(map[string]interface{}, len(mappings))
	for k, v := range mappings {
		fields[k] = v
	}
	b.def.Transforms = append(b.def.Transforms, pipeline.TransformDef{
		Type: "map",
		Extra: map[string]interface{}{
			"type":   "map",
			"fields": fields,
		},
	})
	return b
}

// ValidateRule describes a single field validation rule.
// It mirrors the validate transform config shape so both the SDK and the JSON
// config share the same semantics.
type ValidateRule struct {
	// Field is the column name to validate (required).
	Field string `json:"field"`
	// NotNull rejects null/missing values when true.
	NotNull bool `json:"notNull,omitempty"`
	// Min is the minimum allowed numeric value (inclusive).
	Min *float64 `json:"min,omitempty"`
	// Max is the maximum allowed numeric value (inclusive).
	Max *float64 `json:"max,omitempty"`
	// MinLength is the minimum allowed string length (inclusive).
	MinLength *int `json:"minLength,omitempty"`
	// MaxLength is the maximum allowed string length (inclusive).
	MaxLength *int `json:"maxLength,omitempty"`
	// Pattern is a Go regexp that the field value must fully match.
	Pattern string `json:"pattern,omitempty"`
	// In is an allowlist of permitted values (case-insensitive comparison).
	In []string `json:"in,omitempty"`
}

// Validate adds a validation transform stage.
// rules is a list of field-level validation rules.
// onViolation controls row disposition: "route" (DLQ), "drop" (silent), or "fail" (abort).
func (b *PipelineBuilder) Validate(rules []ValidateRule, onViolation string) *PipelineBuilder {
	if len(rules) == 0 {
		b.addErr("Validate: rules must not be empty")
		return b
	}
	if onViolation == "" {
		onViolation = "route"
	}
	rawRules := make([]interface{}, len(rules))
	for i, r := range rules {
		m := map[string]interface{}{
			"field":   r.Field,
			"notNull": r.NotNull,
		}
		if r.Min != nil {
			m["min"] = *r.Min
		}
		if r.Max != nil {
			m["max"] = *r.Max
		}
		if r.MinLength != nil {
			m["minLength"] = *r.MinLength
		}
		if r.MaxLength != nil {
			m["maxLength"] = *r.MaxLength
		}
		if r.Pattern != "" {
			m["pattern"] = r.Pattern
		}
		if len(r.In) > 0 {
			inIface := make([]interface{}, len(r.In))
			for j, v := range r.In {
				inIface[j] = v
			}
			m["in"] = inIface
		}
		rawRules[i] = m
	}
	b.def.Transforms = append(b.def.Transforms, pipeline.TransformDef{
		Type: "validate",
		Extra: map[string]interface{}{
			"type":        "validate",
			"rules":       rawRules,
			"onViolation": onViolation,
		},
	})
	return b
}

// DQExpectation describes a data-quality expectation for the WithDQ method.
type DQExpectation struct {
	// Name is a human-readable description of the expectation (required).
	Name string `json:"name"`
	// Type selects the expectation kind: "notNull" | "unique" | "range" | "pattern" | "custom".
	Type string `json:"type"`
	// Column is the target column name.
	Column string `json:"column,omitempty"`
	// Params carries type-specific parameters (e.g. {"min": 0, "max": 100}).
	Params map[string]interface{} `json:"params,omitempty"`
}

// DQOption is a functional option for the WithDQ builder method.
type DQOption func(opts map[string]interface{})

// WithDQFailThreshold sets the failure rate (0.0–1.0) above which the run is
// marked as failed (default 0.0 — any violation fails the run).
func WithDQFailThreshold(rate float64) DQOption {
	return func(opts map[string]interface{}) { opts["failThreshold"] = rate }
}

// WithDQSampleSize sets the number of rows sampled per expectation (default all).
func WithDQSampleSize(n int) DQOption {
	return func(opts map[string]interface{}) { opts["sampleSize"] = n }
}

// WithDQ adds a data-quality check transform stage.
// expectations is a list of DQ checks to evaluate.
// Functional options (DQOption) tune evaluation behaviour.
func (b *PipelineBuilder) WithDQ(expectations []DQExpectation, opts ...DQOption) *PipelineBuilder {
	if len(expectations) == 0 {
		b.addErr("WithDQ: expectations must not be empty")
		return b
	}
	options := map[string]interface{}{
		"type": "dq",
	}
	for _, o := range opts {
		o(options)
	}
	rawExp := make([]interface{}, len(expectations))
	for i, e := range expectations {
		m := map[string]interface{}{
			"name":   e.Name,
			"type":   e.Type,
			"column": e.Column,
		}
		for k, v := range e.Params {
			m[k] = v
		}
		rawExp[i] = m
	}
	options["expectations"] = rawExp

	b.def.Transforms = append(b.def.Transforms, pipeline.TransformDef{
		Type:  "dq",
		Extra: options,
	})
	return b
}

// WithHWM enables high-water-mark incremental loading.
// column is the watermark column name; valueType is one of "datetime", "int", "string", "decimal".
// This method sets both the Incremental config (mode=hwm) and appends an hwm
// transform stage that tracks the in-batch maximum.
func (b *PipelineBuilder) WithHWM(column, valueType string) *PipelineBuilder {
	if column == "" {
		b.addErr("WithHWM: column is required")
		return b
	}
	if valueType == "" {
		valueType = "string"
	}
	switch valueType {
	case "datetime", "int", "string", "decimal":
		// valid
	default:
		b.addErr("WithHWM: valueType must be 'datetime', 'int', 'string', or 'decimal', got %q", valueType)
		return b
	}

	b.def.Incremental = pipeline.IncrementalDef{
		Mode:      "hwm",
		Column:    column,
		ValueType: valueType,
		StoreType: "file",
	}

	b.def.Transforms = append(b.def.Transforms, pipeline.TransformDef{
		Type: "hwm",
		Extra: map[string]interface{}{
			"type":      "hwm",
			"column":    column,
			"valueType": valueType,
		},
	})
	return b
}

// WithDLQ configures a dead-letter queue sink for rows that fail validation or
// other error-routing transforms.
// sinkType is the target sink type (e.g. "jdbc", "kafka"); opts is the sink config map.
//
// The DLQ is applied at two levels:
//   - The Definition.DLQ field is set so the sink engine routes write-time row
//     failures to the DLQ sink automatically.
//   - Each validate/dq transform already in the builder's transform list is
//     updated with errorSinkType + errorSinkConfig so validation violations are
//     also routed to the same DLQ.
func (b *PipelineBuilder) WithDLQ(sinkType string, opts map[string]interface{}) *PipelineBuilder {
	if sinkType == "" {
		b.addErr("WithDLQ: sinkType is required")
		return b
	}
	dlqOpts := make(map[string]interface{}, len(opts))
	for k, v := range opts {
		dlqOpts[k] = v
	}

	// Set the definition-level DLQ config (handles write-time row isolation).
	b.def.DLQ = pipeline.DLQDef{
		Type:    sinkType,
		Options: dlqOpts,
	}

	// Also inject into any validate/dq transforms already in the chain so that
	// transform-level violations are routed to the same destination.
	for i := range b.def.Transforms {
		switch b.def.Transforms[i].Type {
		case "validate", "dq":
			if b.def.Transforms[i].Extra == nil {
				b.def.Transforms[i].Extra = map[string]interface{}{}
			}
			b.def.Transforms[i].Extra["errorSinkType"] = sinkType
			b.def.Transforms[i].Extra["errorSinkConfig"] = dlqOpts
		}
	}
	return b
}

// WithMetrics configures the metrics backend for the pipeline.
// backend is one of "prometheus", "otel", or "none"; opts is backend-specific config
// (e.g. {"listenAddr": ":9090"} for prometheus, {"endpoint": "otel-collector:4317"} for otel).
func (b *PipelineBuilder) WithMetrics(backend string, opts map[string]interface{}) *PipelineBuilder {
	b.def.Observability = pipeline.ObservabilityDef{
		Metrics:      backend,
		Options:      opts,
		StallSeconds: 30,
	}
	return b
}

// ---------------------------------------------------------------------------
// JSON serialisation helper
// ---------------------------------------------------------------------------

// flogoResource is the wire shape of a Flogo resource file.
type flogoResource struct {
	ID   string               `json:"id"`
	Data *pipeline.Definition `json:"data"`
}

// marshalDefinition serialises a pipeline Definition as a Flogo resource JSON object
// ready for embedding in an app.json "resources" array or writing as a standalone file.
func marshalDefinition(def *pipeline.Definition) ([]byte, error) {
	name := def.Name
	if name == "" {
		name = "pipeline"
	}
	res := flogoResource{
		ID:   fmt.Sprintf("pipeline:%s", name),
		Data: def,
	}
	b, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("pipeline: marshal definition: %w", err)
	}
	return b, nil
}
