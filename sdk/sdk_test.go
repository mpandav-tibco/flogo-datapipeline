package sdk_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/lib/pq"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"
	"github.com/mpandav-tibco/flogo-datapipeline/sdk"

	// Registry activation for the end-to-end Run() test.
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jdbc"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/csv"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/dq"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/filter"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/mapper"
)

// ─── builder validation (pure, no infra) ────────────────────────────────────

func TestBuilder_ValidationErrors(t *testing.T) {
	cases := []struct {
		name    string
		build   func() (*sdk.PipelineBuilder, error)
		wantErr string
	}{
		{"duplicate source", func() (*sdk.PipelineBuilder, error) {
			return sdk.NewPipeline("p").FromCSV("a.csv").FromJDBC("dsn", "t"), nil
		}, "source already set"},
		{"kafka no brokers", func() (*sdk.PipelineBuilder, error) {
			return sdk.NewPipeline("p").FromKafka(nil, "topic"), nil
		}, "at least one broker"},
		{"kafka no topic", func() (*sdk.PipelineBuilder, error) {
			return sdk.NewPipeline("p").FromKafka([]string{"b:9092"}, ""), nil
		}, "topic is required"},
		{"jdbc no dsn", func() (*sdk.PipelineBuilder, error) {
			return sdk.NewPipeline("p").FromJDBC("", "t"), nil
		}, "dsn is required"},
		{"empty filter", func() (*sdk.PipelineBuilder, error) {
			return sdk.NewPipeline("p").FromCSV("a.csv").Filter("").ToKafka([]string{"b:9092"}, "t"), nil
		}, "expr is required"},
		{"empty map", func() (*sdk.PipelineBuilder, error) {
			return sdk.NewPipeline("p").FromCSV("a.csv").Map(nil).ToKafka([]string{"b:9092"}, "t"), nil
		}, "must not be empty"},
		{"no source", func() (*sdk.PipelineBuilder, error) {
			return sdk.NewPipeline("p").ToKafka([]string{"b:9092"}, "t"), nil
		}, "source is required"},
		{"no sink", func() (*sdk.PipelineBuilder, error) {
			return sdk.NewPipeline("p").FromCSV("a.csv"), nil
		}, "sink is required"},
		{"duplicate sink", func() (*sdk.PipelineBuilder, error) {
			return sdk.NewPipeline("p").FromCSV("a.csv").ToKafka([]string{"b:9092"}, "t").ToS3("bucket", "key"), nil
		}, "sink already set"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := tc.build()
			_, err := b.Build()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestBuilder_BuildAppliesDefaults(t *testing.T) {
	def, err := sdk.NewPipeline("orders").
		FromCSV("orders.csv").
		Filter("amount > 0").
		Map(map[string]string{"id": "=$id"}).
		ToKafka([]string{"broker:9092"}, "orders-out").
		Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if def.Mode != "etl" {
		t.Errorf("Mode = %q, want etl (default)", def.Mode)
	}
	if def.Performance.BatchSize != 5000 {
		t.Errorf("BatchSize = %d, want 5000 (default)", def.Performance.BatchSize)
	}
	if def.Source.Type != "csv" || def.Sink.Type != "kafka" {
		t.Errorf("source/sink = %s/%s, want csv/kafka", def.Source.Type, def.Sink.Type)
	}
	if len(def.Transforms) != 2 {
		t.Errorf("transforms = %d, want 2", len(def.Transforms))
	}
}

func TestBuilder_MarshalJSON(t *testing.T) {
	raw, err := sdk.NewPipeline("daily-load").
		FromCSV("in.csv").
		ToS3("my-bucket", "out/data").
		MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	var res struct {
		ID   string `json:"id"`
		Data struct {
			Source struct {
				Type string `json:"type"`
			} `json:"source"`
			Sink struct {
				Type string `json:"type"`
			} `json:"sink"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("unmarshal resource: %v\n%s", err, raw)
	}
	if res.ID != "pipeline:daily-load" {
		t.Errorf("id = %q, want pipeline:daily-load", res.ID)
	}
	if res.Data.Source.Type != "csv" || res.Data.Sink.Type != "s3" {
		t.Errorf("source/sink = %s/%s, want csv/s3", res.Data.Source.Type, res.Data.Sink.Type)
	}
}

func TestBuilder_MarshalJSON_FailsOnIncomplete(t *testing.T) {
	// No sink → Build (called by MarshalJSON) must fail.
	if _, err := sdk.NewPipeline("p").FromCSV("a.csv").MarshalJSON(); err == nil {
		t.Fatal("expected MarshalJSON to fail on a pipeline with no sink")
	}
}

// TestBuilder_MarshalJSON_RoundTrips guards the serialization fix: a definition
// produced by MarshalJSON must reload with every transform, sink, and source
// option intact. Before the TransformDef/SinkDef MarshalJSON methods existed,
// transform config was dropped (Extra is json:"-") and sink options were nested
// under "options" where the flat-key loader could not find them.
func TestBuilder_MarshalJSON_RoundTrips(t *testing.T) {
	raw, err := sdk.NewPipeline("rt").
		FromCSV("in.csv", sdk.WithCSVDelimiter("|")).
		Filter("amount > 0").
		Map(map[string]string{"id": "=$id"}).
		ToJDBC("dsn", "orders", sdk.WithJDBCOnConflict("upsert")).
		MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}

	var res struct {
		Data pipeline.Definition `json:"data"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("reload definition: %v\n%s", err, raw)
	}
	d := res.Data
	if len(d.Transforms) != 2 {
		t.Fatalf("transforms after round-trip = %d, want 2\n%s", len(d.Transforms), raw)
	}
	if d.Transforms[0].Extra["expr"] != "amount > 0" {
		t.Fatalf("filter expr lost on round-trip: %v", d.Transforms[0].Extra)
	}
	if d.Transforms[1].Extra["fields"] == nil {
		t.Fatalf("mapper fields lost on round-trip: %v", d.Transforms[1].Extra)
	}
	if d.Sink.Type != "jdbc" || d.Sink.Options["onConflict"] != "upsert" {
		t.Fatalf("sink options lost on round-trip: %v", d.Sink.Options)
	}
	if d.Source.Type != "csv" || d.Source.Options["delimiter"] != "|" {
		t.Fatalf("source options lost on round-trip: %v", d.Source.Options)
	}
}

// ─── functional options land in the Definition ──────────────────────────────

func TestBuilder_CSVAndJDBCOptions(t *testing.T) {
	def, err := sdk.NewPipeline("p").
		FromJDBC("src-dsn", "orders",
			sdk.WithJDBCDriver("mysql"),
			sdk.WithJDBCWhere("amount > 0"),
			sdk.WithJDBCOrderKey("id"),
			sdk.WithJDBCBatchSize(1000),
			sdk.WithJDBCColumns([]string{"id", "amount"})).
		ToJDBC("dst-dsn", "orders_out",
			sdk.WithJDBCOnConflict("upsert"),
			sdk.WithJDBCConflictKey([]string{"id"})).
		Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if def.Source.Type != "jdbc" || def.Sink.Type != "jdbc" {
		t.Fatalf("source/sink = %s/%s, want jdbc/jdbc", def.Source.Type, def.Sink.Type)
	}
	if def.Source.Options["driver"] != "mysql" || def.Source.Options["where"] != "amount > 0" {
		t.Fatalf("source jdbc options not applied: %v", def.Source.Options)
	}
	if def.Sink.Options["onConflict"] != "upsert" {
		t.Fatalf("sink onConflict not applied: %v", def.Sink.Options)
	}
}

func TestBuilder_CSVKafkaS3AndGovernanceOptions(t *testing.T) {
	maxAmt := 1000.0
	def, err := sdk.NewPipeline("p").
		FromCSV("in.csv",
			sdk.WithCSVDelimiter("|"),
			sdk.WithCSVHasHeader(false),
			sdk.WithCSVEncoding("ebcdic-037"),
			sdk.WithCSVColumns([]pipeline.ColumnDef{{Name: "amount", Type: "decimal"}})).
		Validate([]sdk.ValidateRule{{Field: "amount", NotNull: true, Max: &maxAmt}}, "route").
		WithHWM("seq", "int").
		WithDLQ("jsonlines", map[string]interface{}{"path": "dlq.jsonl"}).
		WithMetrics("prometheus", map[string]interface{}{"listenAddr": ":9090"}).
		ToS3("bucket", "key",
			sdk.WithS3Region("eu-west-1"),
			sdk.WithS3Format("csv"),
			sdk.WithS3Credentials("AKIA", "SECRET_ENV"),
			sdk.WithS3Endpoint("http://minio:9000"),
			sdk.WithS3PartSize(16)).
		Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if def.Incremental.Mode != "hwm" || def.Incremental.Column != "seq" {
		t.Fatalf("HWM not applied: %+v", def.Incremental)
	}
	if def.DLQ.Type != "jsonlines" {
		t.Fatalf("DLQ not applied: %+v", def.DLQ)
	}
	if def.Observability.Metrics != "prometheus" {
		t.Fatalf("metrics not applied: %+v", def.Observability)
	}
	if def.Sink.Options["region"] != "eu-west-1" || def.Sink.Options["endpoint"] != "http://minio:9000" {
		t.Fatalf("S3 options not applied: %v", def.Sink.Options)
	}
	// validate + hwm transforms both present.
	var haveValidate, haveHWM bool
	for _, tr := range def.Transforms {
		switch tr.Type {
		case "validate":
			haveValidate = true
		case "hwm":
			haveHWM = true
		}
	}
	if !haveValidate || !haveHWM {
		t.Fatalf("expected validate+hwm transforms, got %d transforms", len(def.Transforms))
	}
}

// TestBuilder_WithDQ_FriendlyTypesTranslated guards the fix for the SDK/engine
// naming mismatch: the SDK documents friendly expectation names ("notNull",
// "range"), but the dq transform's registry only knows the canonical
// "expectColumn…" names. WithDQ must translate, or a pipeline built exactly as
// documented fails at runtime with "unknown expectation type".
func TestBuilder_WithDQ_FriendlyTypesTranslated(t *testing.T) {
	raw, err := sdk.NewPipeline("p").
		FromCSV("in.csv").
		WithDQ([]sdk.DQExpectation{
			{Name: "amount not null", Type: "notNull", Column: "amount"},
			{Name: "amount range", Type: "range", Column: "amount", Params: map[string]interface{}{"min": 0.0, "max": 1e9}},
		}).
		ToKafka([]string{"b:9092"}, "out").
		MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	s := string(raw)
	if !strings.Contains(s, "expectColumnNotNull") || !strings.Contains(s, "expectColumnValuesBetween") {
		t.Fatalf("friendly DQ types were not translated to canonical names:\n%s", s)
	}
}

// TestPipeline_Run_WithDQFriendlyType runs a DQ gate declared with the friendly
// "notNull" name end-to-end, proving the dq transform's expectation registry
// accepts the translated type (the failure surfaced by the mismatch above).
func TestPipeline_Run_WithDQFriendlyType(t *testing.T) {
	dsn := os.Getenv("DP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_PG_DSN to run the SDK DQ end-to-end pipeline")
	}
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "orders.csv")
	if err := os.WriteFile(csvPath, []byte("id,amount\n1,100\n2,200\n"), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`DROP TABLE IF EXISTS sdk_dq_orders`); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE sdk_dq_orders (id TEXT, amount TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { db.Exec(`DROP TABLE IF EXISTS sdk_dq_orders`) })

	resp, err := sdk.NewPipeline("dq-run").
		FromCSV(csvPath).
		WithDQ([]sdk.DQExpectation{{Name: "amount present", Type: "notNull", Column: "amount"}}).
		ToJDBC(dsn, "sdk_dq_orders", sdk.WithJDBCDriver("postgres")).
		Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed (err=%s) — friendly DQ type likely not translated", resp.Status, resp.Error)
	}
}

func TestBuilder_KafkaSourceOptions(t *testing.T) {
	def, err := sdk.NewPipeline("p").
		FromKafka([]string{"b:9092"}, "in",
			sdk.WithKafkaGroupID("g1"),
			sdk.WithKafkaOffsetReset("earliest"),
			sdk.WithKafkaFormat("json"),
			sdk.WithKafkaTLS("/etc/ca.pem"),
			sdk.WithKafkaSASL("PLAIN", "user", "PW_ENV")).
		ToKafka([]string{"b:9092"}, "out").
		Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if def.Source.Options["groupId"] != "g1" || def.Source.Options["autoOffsetReset"] != "earliest" {
		t.Fatalf("kafka source options not applied: %v", def.Source.Options)
	}
	if def.Source.Options["tls"] != true || def.Source.Options["saslMechanism"] != "PLAIN" {
		t.Fatalf("kafka TLS/SASL options not applied: %v", def.Source.Options)
	}
}

// ─── end-to-end pipeline-as-code Run() via live Postgres ────────────────────

func TestPipeline_Run_CSVtoJDBC(t *testing.T) {
	dsn := os.Getenv("DP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_PG_DSN to run the CSV to JDBC end-to-end pipeline")
	}
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "orders.csv")
	if err := os.WriteFile(csvPath, []byte("id,name,amount\n1,Alice,100\n2,Bob,-5\n3,Carol,200\n"), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	// Pre-create the target table.
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`DROP TABLE IF EXISTS sdk_csv_orders`); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE sdk_csv_orders (customer_id TEXT, total TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { db.Exec(`DROP TABLE IF EXISTS sdk_csv_orders`) })

	resp, err := sdk.NewPipeline("csv-to-jdbc").
		FromCSV(csvPath).
		Filter("amount > 0").
		Map(map[string]string{"customer_id": "=$id", "total": "=$amount"}).
		ToJDBC(dsn, "sdk_csv_orders", sdk.WithJDBCDriver("postgres")).
		Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed (err=%s)", resp.Status, resp.Error)
	}
	if resp.RowsWritten != 2 {
		t.Fatalf("rowsWritten = %d, want 2 (amount>0 keeps Alice, Carol)", resp.RowsWritten)
	}

	// Confirm the rows actually landed in the target table.
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sdk_csv_orders`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Fatalf("row count in table = %d, want 2", count)
	}
}
