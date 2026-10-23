package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"

	// Registry activation via blank imports — these are the components exercised
	// end-to-end by the orchestration tests below.
	_ "github.com/mpandav-tibco/flogo-datapipeline/checkpoint/file"
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/dlq"
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jsonlines"
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/null"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/csv"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/dq"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/filter"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/hwm"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/mapper"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/schemadrift"
	_ "github.com/mpandav-tibco/flogo-datapipeline/watermark/file"
)

// ─── test helpers ────────────────────────────────────────────────────────────

const ordersCSV = `id,name,amount,currency
1,Alice,100,USD
2,Bob,-50,EUR
3,Carol,200,USD
4,Dave,0,GBP
5,Eve,300,USD
`

// writeTemp writes content to a uniquely named file under t.TempDir().
func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return p
}

// csvSource builds a SourceDef for a headered CSV file at path.
func csvSource(path string) SourceDef {
	return SourceDef{Type: "csv", Path: path, Options: map[string]interface{}{"hasHeader": true}}
}

// jsonlSink builds a SinkDef that writes NDJSON to path.
func jsonlSink(path string) SinkDef {
	return SinkDef{Type: "jsonlines", Options: map[string]interface{}{"path": path}}
}

// runInline applies defaults, builds an inline action and runs it synchronously.
func runInline(t *testing.T, def *Definition) (*Output, error) {
	t.Helper()
	def.Defaults()
	act, err := NewInline(def, "sdk://test")
	if err != nil {
		t.Fatalf("NewInline: %v", err)
	}
	return act.RunDirect(context.Background(), &Input{RunID: "test-run"})
}

// readJSONL parses each line of an NDJSON file into a map.
func readJSONL(t *testing.T, path string) []map[string]interface{} {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var out []map[string]interface{}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("parse jsonl line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

// ─── basic ETL through the real orchestration path ──────────────────────────

func TestRunDirect_BasicETL(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	outPath := filepath.Join(t.TempDir(), "out.jsonl")

	def := &Definition{
		Source: csvSource(src),
		Transforms: []TransformDef{
			{Type: "filter", Extra: map[string]interface{}{"type": "filter", "expr": "amount > 0"}},
			{Type: "mapper", Extra: map[string]interface{}{
				"type": "mapper",
				"fields": map[string]interface{}{
					"customerId": "=$id",
					"fullName":   "=$name",
				},
				"fieldOrder": []interface{}{"customerId", "fullName"},
			}},
		},
		Sink: jsonlSink(outPath),
	}

	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.Status != "completed" {
		t.Fatalf("status = %q, want completed (err=%s)", out.Status, out.Error)
	}
	// 5 read; filter keeps amount>0 (Alice 100, Carol 200, Eve 300) = 3 written.
	if out.RowsProcessed != 5 || out.RowsWritten != 3 {
		t.Fatalf("processed=%d written=%d, want 5/3", out.RowsProcessed, out.RowsWritten)
	}
	rows := readJSONL(t, outPath)
	if len(rows) != 3 {
		t.Fatalf("output rows = %d, want 3", len(rows))
	}
	// Mapper projected exactly customerId + fullName.
	if rows[0]["customerId"] != "1" || rows[0]["fullName"] != "Alice" {
		t.Fatalf("row0 = %v, want customerId=1 fullName=Alice", rows[0])
	}
	if _, ok := rows[0]["currency"]; ok {
		t.Fatalf("row0 unexpectedly retained un-mapped field: %v", rows[0])
	}
}

// ─── EL / ELT modes bypass the in-flight transform stage ────────────────────

func TestRunDirect_ELMode_BypassesTransforms(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	outPath := filepath.Join(t.TempDir(), "out.jsonl")
	def := &Definition{
		Mode:   "el",
		Source: csvSource(src),
		Transforms: []TransformDef{ // must be IGNORED in el mode
			{Type: "filter", Extra: map[string]interface{}{"type": "filter", "expr": "amount > 100000"}},
		},
		Sink: jsonlSink(outPath),
	}
	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	// Filter would have dropped everything; el mode ignores it → all 5 written raw.
	if out.RowsWritten != 5 {
		t.Fatalf("el mode written=%d, want 5 (transforms must be bypassed)", out.RowsWritten)
	}
}

func TestRunDirect_ELTMode_BypassesTransforms(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	outPath := filepath.Join(t.TempDir(), "out.jsonl")
	def := &Definition{
		Mode:       "elt",
		Source:     csvSource(src),
		Transforms: []TransformDef{{Type: "filter", Extra: map[string]interface{}{"type": "filter", "expr": "amount > 100000"}}},
		Sink:       jsonlSink(outPath),
	}
	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.RowsWritten != 5 {
		t.Fatalf("elt mode written=%d, want 5", out.RowsWritten)
	}
}

// ─── ordered-parallel transform path (transformWorkers > 1) ─────────────────

func TestRunDirect_ParallelWorkers_OrderedOutput(t *testing.T) {
	// 200 rows so several batches flow through the worker pool.
	var sb strings.Builder
	sb.WriteString("id,amount\n")
	for i := 1; i <= 200; i++ {
		fmt.Fprintf(&sb, "%d,%d\n", i, i)
	}
	src := writeTemp(t, "big.csv", sb.String())
	outPath := filepath.Join(t.TempDir(), "out.jsonl")

	def := &Definition{
		Source:     csvSource(src),
		Transforms: []TransformDef{{Type: "filter", Extra: map[string]interface{}{"type": "filter", "expr": "amount > 100"}}},
		Sink:       jsonlSink(outPath),
		Performance: PerformanceDef{
			BatchSize:        16,
			TransformWorkers: 4, // engages drainParallel
		},
	}
	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.RowsProcessed != 200 || out.RowsWritten != 100 {
		t.Fatalf("processed=%d written=%d, want 200/100", out.RowsProcessed, out.RowsWritten)
	}
	rows := readJSONL(t, outPath)
	if len(rows) != 100 {
		t.Fatalf("output rows=%d, want 100", len(rows))
	}
	// Ordered output: the collector must reassemble batches in read order, so the
	// first surviving row is id=101 and ids increase monotonically.
	if rows[0]["id"] != "101" {
		t.Fatalf("first row id=%v, want 101 (ordered)", rows[0]["id"])
	}
	prev := 100
	for _, r := range rows {
		var id int
		fmt.Sscanf(fmt.Sprintf("%v", r["id"]), "%d", &id)
		if id <= prev {
			t.Fatalf("output not ordered: id %d after %d", id, prev)
		}
		prev = id
	}
}

// ─── small batches force many read→transform→write cycles ───────────────────

func TestRunDirect_SmallBatch_MultipleBatches(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	outPath := filepath.Join(t.TempDir(), "out.jsonl")
	def := &Definition{
		Source:      csvSource(src),
		Sink:        jsonlSink(outPath),
		Performance: PerformanceDef{BatchSize: 2}, // 5 rows → 3 batches
	}
	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.RowsProcessed != 5 || out.RowsWritten != 5 {
		t.Fatalf("processed=%d written=%d, want 5/5", out.RowsProcessed, out.RowsWritten)
	}
}

// ─── checkpoint save + resume (file backend) ────────────────────────────────

func TestRunDirect_Checkpoint_SaveAndResume(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	ckptDir := t.TempDir()

	mkDef := func() *Definition {
		return &Definition{
			Source: csvSource(src),
			Sink:   SinkDef{Type: "null", Options: map[string]interface{}{}},
			Checkpoint: CheckpointDef{
				Type:            "file",
				Path:            ckptDir,
				Key:             "resume-key",
				IntervalRows:    1, // checkpoint aggressively
				DeleteOnSuccess: false,
			},
		}
	}

	// Run 1: processes all 5 and persists the final checkpoint.
	out1, err := runInline(t, mkDef())
	if err != nil {
		t.Fatalf("run1: %v", err)
	}
	if out1.RowsProcessed != 5 {
		t.Fatalf("run1 processed=%d, want 5", out1.RowsProcessed)
	}

	// Run 2 with the same checkpoint key must resume and skip the 5 already-processed rows.
	out2, err := runInline(t, mkDef())
	if err != nil {
		t.Fatalf("run2: %v", err)
	}
	if out2.RowsWritten != 0 {
		t.Fatalf("run2 written=%d, want 0 (all rows already processed, should be skipped)", out2.RowsWritten)
	}
}

// ─── empty source (header only) ─────────────────────────────────────────────

func TestRunDirect_EmptySource(t *testing.T) {
	src := writeTemp(t, "empty.csv", "id,name,amount\n")
	outPath := filepath.Join(t.TempDir(), "out.jsonl")
	out, err := runInline(t, &Definition{Source: csvSource(src), Sink: jsonlSink(outPath)})
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.Status != "completed" || out.RowsProcessed != 0 || out.RowsWritten != 0 {
		t.Fatalf("empty run: status=%s processed=%d written=%d, want completed/0/0", out.Status, out.RowsProcessed, out.RowsWritten)
	}
}

// ─── context cancellation aborts the run ────────────────────────────────────

func TestRunDirect_ContextCancelled(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	def := &Definition{Source: csvSource(src), Sink: SinkDef{Type: "null", Options: map[string]interface{}{}}}
	def.Defaults()
	act, err := NewInline(def, "sdk://cancel")
	if err != nil {
		t.Fatalf("NewInline: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the run starts
	out, runErr := act.RunDirect(ctx, &Input{RunID: "cancel-run"})
	if runErr == nil {
		t.Fatalf("expected a cancellation error, got nil (status=%s)", out.Status)
	}
	if out.Status != "failed" {
		t.Fatalf("status=%q, want failed on cancellation", out.Status)
	}
}

// ─── schemadrift prepend (project policy) ───────────────────────────────────

func TestRunDirect_SchemaDrift_ProjectPolicy(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	outPath := filepath.Join(t.TempDir(), "out.jsonl")
	def := &Definition{
		Source:      csvSource(src),
		Sink:        jsonlSink(outPath),
		SchemaDrift: SchemaDriftDef{Policy: "project", OnDrift: "silent"},
	}
	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	// project policy on a stable schema is a pass-through: all rows survive.
	if out.RowsProcessed != 5 || out.RowsWritten != 5 {
		t.Fatalf("schemadrift project processed=%d written=%d, want 5/5", out.RowsProcessed, out.RowsWritten)
	}
}

// ─── DLQ: batch write failure falls back to per-row isolation ───────────────

// failingSink fails a whole-batch WriteBatch whenever the batch contains a row
// named "FAIL", forcing the engine onto the per-row isolation path. Individual
// rows named "FAIL" also fail so they are routed to the DLQ.
type failingSink struct{}

func init() {
	sink.Register("failtest", func(map[string]interface{}) (sink.Sink, error) { return &failingSink{}, nil })
}

func (s *failingSink) WriteBatch(b *record.RecordBatch) error {
	for i := 0; i < b.Len(); i++ {
		if fmt.Sprintf("%v", b.RowMap(i)["name"]) == "FAIL" {
			return fmt.Errorf("simulated write failure")
		}
	}
	return nil
}
func (s *failingSink) Flush() error { return nil }
func (s *failingSink) Close() error { return nil }

func TestRunDirect_DLQ_RowIsolation(t *testing.T) {
	csv := "id,name,amount\n1,Alice,100\n2,FAIL,200\n3,Carol,300\n"
	src := writeTemp(t, "in.csv", csv)
	dlqPath := filepath.Join(t.TempDir(), "dlq.jsonl")

	def := &Definition{
		Source: csvSource(src),
		Sink:   SinkDef{Type: "failtest", Options: map[string]interface{}{}},
		DLQ:    DLQDef{Type: "jsonlines", Options: map[string]interface{}{"path": dlqPath}},
	}
	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	// Alice + Carol succeed on the per-row retry; FAIL is routed to the DLQ.
	if out.RowsWritten != 2 {
		t.Fatalf("written=%d, want 2 (Alice, Carol)", out.RowsWritten)
	}
	if out.RowsFailed != 1 {
		t.Fatalf("failed=%d, want 1 (FAIL row)", out.RowsFailed)
	}
	if out.Status != "partial" {
		t.Fatalf("status=%q, want partial", out.Status)
	}
	dlqRows := readJSONL(t, dlqPath)
	if len(dlqRows) != 1 {
		t.Fatalf("dlq rows=%d, want 1", len(dlqRows))
	}
}

// recordingFailSink behaves like failingSink but records every row it accepts,
// so the test can detect a row being delivered more than once.
type recordingFailSink struct{ accepted []string }

var lastRecordingSink *recordingFailSink

func init() {
	sink.Register("failrecord", func(map[string]interface{}) (sink.Sink, error) {
		lastRecordingSink = &recordingFailSink{}
		return lastRecordingSink, nil
	})
}

func (s *recordingFailSink) WriteBatch(b *record.RecordBatch) error {
	for i := 0; i < b.Len(); i++ {
		if fmt.Sprintf("%v", b.Value(i, "name")) == "FAIL" {
			return fmt.Errorf("simulated write failure")
		}
	}
	for i := 0; i < b.Len(); i++ {
		s.accepted = append(s.accepted, fmt.Sprintf("%v", b.Value(i, "name")))
	}
	return nil
}
func (s *recordingFailSink) Flush() error { return nil }
func (s *recordingFailSink) Close() error { return nil }

func TestRunDirect_DLQ_RowIsolation_NoDuplicateWrites(t *testing.T) {
	csv := "id,name,amount\n1,Alice,100\n2,FAIL,200\n3,Carol,300\n"
	src := writeTemp(t, "in.csv", csv)
	def := &Definition{
		Source: csvSource(src),
		Sink:   SinkDef{Type: "failrecord", Options: map[string]interface{}{}},
		DLQ:    DLQDef{Type: "jsonlines", Options: map[string]interface{}{"path": filepath.Join(t.TempDir(), "dlq.jsonl")}},
	}
	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.RowsWritten != 2 || out.RowsFailed != 1 {
		t.Fatalf("written=%d failed=%d, want 2/1", out.RowsWritten, out.RowsFailed)
	}
	if got := strings.Join(lastRecordingSink.accepted, ","); got != "Alice,Carol" {
		t.Fatalf("sink received %q, want each good row exactly once (\"Alice,Carol\")", got)
	}
}

// ─── DQ gate report is surfaced in Output.Metadata ──────────────────────────

func TestRunDirect_DQ_ReportInMetadata(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	outPath := filepath.Join(t.TempDir(), "out.jsonl")
	def := &Definition{
		Source: csvSource(src),
		Transforms: []TransformDef{{Type: "dq", Extra: map[string]interface{}{
			"type":        "dq",
			"onViolation": "warn", // never aborts — we just want the report
			"expectations": []interface{}{
				map[string]interface{}{"type": "expectColumnNotNull", "column": "amount"},
				map[string]interface{}{"type": "expectTableRowCountBetween", "min": 1.0, "max": 1000.0},
			},
		}}},
		Sink: jsonlSink(outPath),
	}
	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.Metadata == nil || out.Metadata["dqReport"] == nil {
		t.Fatalf("expected dqReport in Output.Metadata, got %v", out.Metadata)
	}
}

// ─── HWM incremental persists the high-water mark ───────────────────────────

func TestRunDirect_HWM_PersistsWatermark(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	wmDir := t.TempDir()
	outPath := filepath.Join(t.TempDir(), "out.jsonl")
	def := &Definition{
		Source: csvSource(src),
		Transforms: []TransformDef{{Type: "hwm", Extra: map[string]interface{}{
			"type": "hwm", "column": "id", "valueType": "int",
		}}},
		Sink: jsonlSink(outPath),
		Incremental: IncrementalDef{
			Mode: "hwm", Column: "id", ValueType: "int",
			StoreType: "file", StoreOptions: map[string]interface{}{"path": wmDir},
		},
	}
	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.RowsProcessed != 5 {
		t.Fatalf("processed=%d, want 5", out.RowsProcessed)
	}
	// The hwm transform tracks max(id)=5; the engine persists it on completion.
	entries, _ := os.ReadDir(wmDir)
	var found bool
	for _, e := range entries {
		b, _ := os.ReadFile(filepath.Join(wmDir, e.Name()))
		if strings.Contains(string(b), "\"5\"") || strings.Contains(string(b), ":5") {
			found = true
		}
	}
	if !found {
		t.Fatalf("watermark file with max value 5 not found in %s (entries=%d)", wmDir, len(entries))
	}
}
