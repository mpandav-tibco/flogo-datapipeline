// Package dq registers the "dq" (Data Quality) transform.
// Import this package with a blank import to activate it:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/transform/dq"
//
// # Config shape
//
//	{
//	  "type": "dq",
//	  "expectations": [
//	    { "type": "expectColumnNotNull",       "column": "customer_id",   "threshold": 1.0 },
//	    { "type": "expectColumnValuesBetween", "column": "amount",        "min": 0, "max": 1000000 },
//	    { "type": "expectTableRowCountBetween","min": 1, "max": 10000000 }
//	  ],
//	  "onFail": "route",
//	  "qualityThreshold": 0.95,
//	  "scoreColumn": "_dq_score",
//	  "errorSinkType":   "dlq",
//	  "errorSinkConfig": { ... },
//	  "reportSinkType":  "jdbc",
//	  "reportSinkConfig": { "table": "pipeline_dq_reports", "dsn": "..." }
//	}
//
// # onFail behaviour (triggered when overallScore < qualityThreshold)
//
//	"route"  — row-level pass/fail: failing rows → errorSink, passing rows → downstream
//	           (a row passes if all row-level expectations evaluate correctly for it).
//	           Dataset-level expectation failures are logged but do not route rows.
//	"fail"   — abort the pipeline with a descriptive error listing failed expectations.
//	"warn"   — pass all rows downstream; log the quality report as a warning.
//	"drop"   — discard the entire batch; return nil to the pipeline engine.
//
// # scoreColumn
//
// When the "scoreColumn" config key is set to a non-empty string, a float64
// column with that name is appended to every passing row containing the
// individual row quality score (1.0 = all row-level expectations passed,
// fractional = some failed). Dataset-level expectation scores do not affect
// the per-row score.
//
// # Report sink
//
// When reportSinkType is non-empty, one synthetic row is written per batch
// containing all DQReport fields (matching the column names in DQReport's
// JSON tags). This enables a Grafana dashboard over a JDBC report table.
//
// # DQGateProvider
//
// dqTransform implements DQGateProvider so the engine can include the last
// run's DQReport in Output.Metadata["dqReport"].
package dq

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

var dqlog = corelog.ChildLogger(corelog.RootLogger(), "dq")

func init() {
	transform.Register("dq", newDQTransform)
}

// DQGateProvider is implemented by dqTransform and exposed to the engine via a
// type assertion on each pipeline stage. The engine calls LastRunReport() after
// pipeline completion and includes the result in Output.Metadata["dqReport"].
type DQGateProvider interface {
	LastRunReport() *DQReport
}

// DQReport is the end-of-batch data quality summary.
// All fields are JSON-tagged so they can be serialised into Output.Metadata
// and written to the report sink table (column names = JSON tag names).
type DQReport struct {
	PipelineRef        string              `json:"pipelineRef"`
	BatchSeq           int64               `json:"batchSeq"`
	RunAt              time.Time           `json:"runAt"`
	RowsEvaluated      int                 `json:"rowsEvaluated"`
	RowsPassed         int                 `json:"rowsPassed"`
	RowsFailed         int                 `json:"rowsFailed"`
	ExpectationResults []ExpectationResult `json:"expectationResults"`
	OverallScore       float64             `json:"overallScore"`
	PassedGate         bool                `json:"passedGate"`
}

// dqTransform implements transform.Transform and DQGateProvider.
type dqTransform struct {
	expectations     []Expectation
	onFail           string  // "route" | "fail" | "warn" | "drop"
	qualityThreshold float64 // default 0.95
	scoreColumn      string  // optional; appends _dq_score to passing rows when non-empty
	errorSink        sink.Sink
	reportSink       reportSinkWriter // nil when no reportSinkType is configured
	pipelineRef      string           // set by SetPipelineRef; stamped on every report

	mu         sync.Mutex
	lastReport *DQReport
	batchSeq   int64
}

// reportSinkWriter is a narrow interface used for the optional report sink.
// Using an interface here keeps the implementation flexible (JDBC, custom, etc.)
// without exposing the full sink.Sink surface to the report-writing logic.
type reportSinkWriter interface {
	writeReport(report *DQReport) error
	close() error
}

func newDQTransform(cfg map[string]interface{}) (transform.Transform, error) {
	// --- expectations ---
	rawExpectations, ok := cfg["expectations"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("dq transform: 'expectations' must be an array")
	}
	expectations := make([]Expectation, 0, len(rawExpectations))
	for i, raw := range rawExpectations {
		m, ok := raw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("dq transform: expectations[%d] must be an object", i)
		}
		exp, err := buildExpectation(m)
		if err != nil {
			return nil, fmt.Errorf("dq transform: expectations[%d]: %w", i, err)
		}
		expectations = append(expectations, exp)
	}
	if len(expectations) == 0 {
		return nil, fmt.Errorf("dq transform: at least one expectation is required")
	}

	// --- onFail ---
	onFail, _ := cfg["onFail"].(string)
	if onFail == "" {
		onFail = "route"
	}
	switch onFail {
	case "route", "fail", "warn", "drop":
		// valid
	default:
		return nil, fmt.Errorf("dq transform: onFail must be 'route', 'fail', 'warn', or 'drop', got %q", onFail)
	}

	// --- qualityThreshold ---
	qualityThreshold := 0.95
	if qt, ok := cfg["qualityThreshold"]; ok {
		if f, ok2 := toFloat64(qt); ok2 {
			qualityThreshold = f
		}
	}
	if qualityThreshold < 0 || qualityThreshold > 1 {
		return nil, fmt.Errorf("dq transform: qualityThreshold must be in [0.0, 1.0], got %v", qualityThreshold)
	}

	// --- scoreColumn ---
	scoreColumn, _ := cfg["scoreColumn"].(string)

	// --- error sink ---
	var errSink sink.Sink
	var buildErr error
	if sinkType, ok := cfg["errorSinkType"].(string); ok && sinkType != "" {
		sinkCfg, _ := cfg["errorSinkConfig"].(map[string]interface{})
		if sinkCfg == nil {
			sinkCfg = map[string]interface{}{}
		}
		errSink, buildErr = sink.New(sinkType, sinkCfg)
		if buildErr != nil {
			return nil, fmt.Errorf("dq transform: error sink: %w", buildErr)
		}
	} else {
		// Default: discard bad rows silently via built-in noop.
		errSink = &noopSink{}
	}

	// --- report sink ---
	var repSink reportSinkWriter
	if repType, ok := cfg["reportSinkType"].(string); ok && repType != "" {
		repCfg, _ := cfg["reportSinkConfig"].(map[string]interface{})
		if repCfg == nil {
			repCfg = map[string]interface{}{}
		}
		var err error
		repSink, err = newJDBCReportSink(repType, repCfg)
		if err != nil {
			return nil, fmt.Errorf("dq transform: report sink: %w", err)
		}
	}

	return &dqTransform{
		expectations:     expectations,
		onFail:           onFail,
		qualityThreshold: qualityThreshold,
		scoreColumn:      scoreColumn,
		errorSink:        errSink,
		reportSink:       repSink,
	}, nil
}

func (d *dqTransform) Name() string { return "dq" }

// LastRunReport returns the DQReport from the most recent Apply call.
// Returns nil before the first batch is processed.
// Thread-safe: the engine may call this concurrently with the next Apply.
func (d *dqTransform) LastRunReport() *DQReport {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastReport
}

// Apply runs two passes over the batch:
//  1. Dataset-level: compute BatchProfile → evaluate all dataset-level expectations.
//  2. Row-level: for each row, evaluate row-level expectations (those that declare
//     a column), compute a per-row score, and route/filter according to onFail.
//
// The output batch carries the same schema as the input, plus the optional
// scoreColumn appended at the end.
func (d *dqTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	d.mu.Lock()
	d.batchSeq++
	seq := d.batchSeq
	d.mu.Unlock()

	// --- Pass 1: compute profile and evaluate dataset-level expectations ---
	var profile BatchProfile
	profile.Compute(in)

	datasetResults := make([]ExpectationResult, 0, len(d.expectations))
	rowLevelExpectations := make([]Expectation, 0, len(d.expectations))
	for _, exp := range d.expectations {
		// Dataset-level expectations are those that do not reference a specific
		// column (e.g. expectTableRowCountBetween) or those that operate on
		// aggregate profile data. We evaluate all expectations at the dataset
		// level here; row-level scoring is handled separately in pass 2.
		result := exp.Evaluate(in, &profile)
		datasetResults = append(datasetResults, result)
		// Row-level expectations are those that accept a column name — they can
		// also be evaluated per-row for the per-row score.
		if isRowLevel(exp) {
			rowLevelExpectations = append(rowLevelExpectations, exp)
		}
	}

	// Compute overall dataset score = mean of all expectation scores.
	overallScore := meanScores(datasetResults)
	passedGate := overallScore >= d.qualityThreshold

	// --- Handle onFail: "fail" and "drop" don't need row-level work ---
	if !passedGate {
		switch d.onFail {
		case "fail":
			// Build a descriptive error listing which expectations failed.
			var failed []string
			for _, r := range datasetResults {
				if !r.Passed {
					failed = append(failed, fmt.Sprintf("%s (observed=%v, expected=%v)",
						r.Name, r.Observed, r.Expected))
				}
			}
			return nil, fmt.Errorf("dq gate: overallScore=%.4f < qualityThreshold=%.4f; failed expectations: %s",
				overallScore, d.qualityThreshold, strings.Join(failed, "; "))

		case "drop":
			dqlog.Warnf("dq gate: overallScore=%.4f < qualityThreshold=%.4f — dropping entire batch (%d rows)",
				overallScore, d.qualityThreshold, in.Len())
			d.storeReport(&DQReport{
				BatchSeq:           seq,
				RunAt:              time.Now().UTC(),
				RowsEvaluated:      in.Len(),
				RowsPassed:         0,
				RowsFailed:         in.Len(),
				ExpectationResults: datasetResults,
				OverallScore:       overallScore,
				PassedGate:         false,
			})
			return nil, nil
		}
	}

	// --- Pass 2: row-level evaluation ---
	// Build output schema (possibly adding scoreColumn).
	outCols := in.Columns
	addScoreCol := d.scoreColumn != ""
	if addScoreCol {
		outCols = append(append([]record.Column(nil), in.Columns...), record.Column{
			Name: d.scoreColumn,
			Type: record.TypeFloat,
		})
	}

	goodBatch := record.Get()
	goodBatch.Columns = outCols
	goodBatch.Rows = goodBatch.Rows[:0]

	var badRows [][]interface{}
	rowsPassed, rowsFailed := 0, 0

	for i := range in.Rows {
		rowScore, rowPassed := d.evaluateRow(in, i, rowLevelExpectations, &profile)
		if rowPassed {
			rowsPassed++
			row := in.Rows[i]
			if addScoreCol {
				extended := make([]interface{}, len(row)+1)
				copy(extended, row)
				extended[len(row)] = rowScore
				goodBatch.Rows = append(goodBatch.Rows, extended)
			} else {
				goodBatch.Rows = append(goodBatch.Rows, row)
			}
		} else {
			rowsFailed++
			switch d.onFail {
			case "route":
				// Route failing rows to the error sink; exclude from goodBatch.
				badRows = append(badRows, in.Rows[i])
			case "warn":
				// Pass failing rows downstream with a warning; include in goodBatch.
				row := in.Rows[i]
				if addScoreCol {
					extended := make([]interface{}, len(row)+1)
					copy(extended, row)
					extended[len(row)] = rowScore
					goodBatch.Rows = append(goodBatch.Rows, extended)
				} else {
					goodBatch.Rows = append(goodBatch.Rows, row)
				}
				// default ("fail"): rows are dropped; gate failure already handled above.
			}
		}
	}

	// For "warn" mode: log the quality report but pass all rows downstream.
	if d.onFail == "warn" {
		dqlog.Warnf("dq gate: overallScore=%.4f (passedGate=%v) — passing all rows downstream (warn mode)",
			overallScore, passedGate)
		// In warn mode all rows are already in goodBatch (including failing rows).
		// Recalculate rowsPassed to reflect actual output.
	}

	// Route bad rows to the error sink.
	if len(badRows) > 0 && d.onFail == "route" {
		errBatch := record.Get()
		errBatch.Columns = in.Columns // no scoreColumn in error sink
		errBatch.Rows = badRows
		if err := d.errorSink.WriteBatch(errBatch); err != nil {
			dqlog.Warnf("dq: error sink write failed (%d bad rows may be lost): %v", len(badRows), err)
		}
		record.Put(errBatch)
	}

	// Store the report.
	report := &DQReport{
		BatchSeq:           seq,
		RunAt:              time.Now().UTC(),
		RowsEvaluated:      in.Len(),
		RowsPassed:         rowsPassed,
		RowsFailed:         rowsFailed,
		ExpectationResults: datasetResults,
		OverallScore:       overallScore,
		PassedGate:         passedGate,
	}
	d.storeReport(report)

	// Write to the report sink if configured.
	if d.reportSink != nil {
		if err := d.reportSink.writeReport(report); err != nil {
			dqlog.Warnf("dq: report sink write failed: %v", err)
		}
	}

	// Emit dataset-level log summary.
	if passedGate {
		dqlog.Infof("dq gate: overallScore=%.4f >= threshold=%.4f — passed (rows=%d, good=%d, bad=%d)",
			overallScore, d.qualityThreshold, in.Len(), rowsPassed, rowsFailed)
	} else {
		dqlog.Warnf("dq gate: overallScore=%.4f < threshold=%.4f — %s (rows=%d, good=%d, bad=%d)",
			overallScore, d.qualityThreshold, d.onFail, in.Len(), rowsPassed, rowsFailed)
	}

	if len(goodBatch.Rows) == 0 {
		record.Put(goodBatch)
		return nil, nil
	}
	return goodBatch, nil
}

// Close flushes and closes the error and report sinks.
func (d *dqTransform) Close() error {
	var errs []string
	if d.errorSink != nil {
		if err := d.errorSink.Flush(); err != nil {
			errs = append(errs, fmt.Sprintf("error sink flush: %v", err))
		}
		if err := d.errorSink.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("error sink close: %v", err))
		}
	}
	if d.reportSink != nil {
		if err := d.reportSink.close(); err != nil {
			errs = append(errs, fmt.Sprintf("report sink close: %v", err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("dq transform close: %s", strings.Join(errs, "; "))
	}
	return nil
}

// storeReport persists the report for LastRunReport() and optionally sets the
// pipelineRef on the report when it is injected from outside (the engine sets it
// after the pipeline builds the transform). Protected by the dqTransform mutex.
func (d *dqTransform) storeReport(r *DQReport) {
	d.mu.Lock()
	r.PipelineRef = d.pipelineRef
	d.lastReport = r
	d.mu.Unlock()
}

// SetPipelineRef injects the pipeline reference string into subsequent reports.
// Called by the engine after it locates the DQGateProvider stage.
func (d *dqTransform) SetPipelineRef(ref string) {
	d.mu.Lock()
	d.pipelineRef = ref
	if d.lastReport != nil {
		d.lastReport.PipelineRef = ref
	}
	d.mu.Unlock()
}

// ----------------------------------------------------------------------------
// Row-level evaluation
// ----------------------------------------------------------------------------

// evaluateRow computes a per-row quality score based on the row-level subset of
// expectations. Returns (score, passed) where passed is true when all row-level
// expectations evaluate correctly for this specific row (or when there are no
// row-level expectations).
//
// Row-level expectations are approximated by re-checking the column value for
// each row against the declared constraints (notNull, between, inSet). This is
// faster than re-running each Expectation.Evaluate per row because it avoids
// constructing a single-row BatchProfile; instead we use inline predicates that
// mirror the expectation logic.
func (d *dqTransform) evaluateRow(
	batch *record.RecordBatch,
	rowIdx int,
	rowLevelExp []Expectation,
	_ *BatchProfile,
) (score float64, passed bool) {
	if len(rowLevelExp) == 0 {
		return 1.0, true
	}
	total, pass := 0, 0
	for _, exp := range rowLevelExp {
		total++
		if rowSatisfiesExpectation(batch, rowIdx, exp) {
			pass++
		}
	}
	if total == 0 {
		return 1.0, true
	}
	rowScore := float64(pass) / float64(total)
	return rowScore, pass == total
}

// rowSatisfiesExpectation tests whether the value in the given row satisfies a
// row-level expectation inline (without allocating a per-row profile).
func rowSatisfiesExpectation(batch *record.RecordBatch, rowIdx int, exp Expectation) bool {
	switch e := exp.(type) {
	case *expectColumnNotNull:
		val := batch.Value(rowIdx, e.column)
		return val != nil

	case *expectColumnValuesBetween:
		val := batch.Value(rowIdx, e.column)
		if val == nil {
			return true // null is excluded from range check
		}
		f, ok := toFloat64(val)
		if !ok {
			return true // non-numeric: skip
		}
		return f >= e.lo && f <= e.hi

	case *expectColumnValuesInSet:
		val := batch.Value(rowIdx, e.column)
		if val == nil {
			return true
		}
		s := fmt.Sprintf("%v", val)
		key := s
		if !e.caseSensitive {
			key = strings.ToUpper(s)
		}
		_, ok := e.allowed[key]
		return ok

	case *expectColumnUnique:
		// Uniqueness is a dataset-level property; at the row level we cannot
		// determine if a value is unique without scanning the whole batch.
		// For row-level scoring, treat uniqueness expectations as always passing.
		return true

	case *expectColumnMeanBetween, *expectColumnStddevBelow, *expectTableRowCountBetween:
		// These are purely dataset-level; they do not affect per-row scores.
		return true

	default:
		// Unknown expectation type: pass through.
		return true
	}
}

// isRowLevel returns true when an expectation can contribute to per-row scoring.
// Dataset-only expectations (rowCount, mean, stddev) return false.
func isRowLevel(exp Expectation) bool {
	switch exp.(type) {
	case *expectTableRowCountBetween, *expectColumnMeanBetween, *expectColumnStddevBelow:
		return false
	default:
		return true
	}
}

// ----------------------------------------------------------------------------
// Score helpers
// ----------------------------------------------------------------------------

// meanScores returns the arithmetic mean of all expectation scores.
// Returns 1.0 when results is empty.
func meanScores(results []ExpectationResult) float64 {
	if len(results) == 0 {
		return 1.0
	}
	var sum float64
	for _, r := range results {
		sum += r.Score
	}
	return sum / float64(len(results))
}

// ----------------------------------------------------------------------------
// Report sink — JDBC implementation
// ----------------------------------------------------------------------------

// jdbcReportSink writes one DQReport row per batch to a SQL table.
// The sink is intentionally thin: it uses database/sql directly rather than
// the full sink.Sink abstraction because the report table schema is fixed and
// determined by DQReport — it does not need the full jdbc sink machinery.
type jdbcReportSink struct {
	db     *sql.DB
	table  string
	driver string
}

func newJDBCReportSink(sinkType string, cfg map[string]interface{}) (reportSinkWriter, error) {
	// Only "jdbc" is supported as report sink type for now.
	// Future: "kafka", "pubsub", etc.
	if sinkType != "jdbc" {
		return nil, fmt.Errorf("reportSinkType %q is not supported; only 'jdbc' is supported", sinkType)
	}
	dsn, _ := cfg["dsn"].(string)
	if dsn == "" {
		return nil, fmt.Errorf("dq report sink (jdbc): 'dsn' is required")
	}
	table, _ := cfg["table"].(string)
	if table == "" {
		table = "pipeline_dq_reports"
	}
	driver, _ := cfg["driver"].(string)
	if driver == "" {
		driver = "postgres"
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("dq report sink (jdbc): open db: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("dq report sink (jdbc): ping db: %w", err)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	return &jdbcReportSink{db: db, table: table, driver: driver}, nil
}

// writeReport serialises the DQReport and inserts one row into the report table.
// The expectationResults column is stored as a JSON string for flexibility.
func (r *jdbcReportSink) writeReport(report *DQReport) error {
	resultsJSON, err := json.Marshal(report.ExpectationResults)
	if err != nil {
		return fmt.Errorf("dq report sink: marshal expectation results: %w", err)
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (pipeline_ref, batch_seq, run_at, rows_evaluated, rows_passed, rows_failed, overall_score, passed_gate, expectation_results)
		 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)`,
		quoteIdent(r.table, r.driver),
		r.ph(1), r.ph(2), r.ph(3), r.ph(4), r.ph(5), r.ph(6), r.ph(7), r.ph(8), r.ph(9),
	)

	_, insertErr := r.db.Exec(query,
		report.PipelineRef,
		report.BatchSeq,
		report.RunAt,
		report.RowsEvaluated,
		report.RowsPassed,
		report.RowsFailed,
		report.OverallScore,
		report.PassedGate,
		string(resultsJSON),
	)
	return insertErr
}

func (r *jdbcReportSink) close() error {
	return r.db.Close()
}

// ph returns a placeholder for position n appropriate to the driver.
func (r *jdbcReportSink) ph(n int) string {
	if r.driver == "postgres" {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}

// quoteIdent quotes a table or column identifier for the given driver.
func quoteIdent(name, driver string) string {
	switch driver {
	case "mysql":
		return "`" + name + "`"
	default: // postgres, sqlite, etc.
		return `"` + name + `"`
	}
}

// ----------------------------------------------------------------------------
// Inline noop sink (used when errorSinkType is not configured)
// ----------------------------------------------------------------------------

type noopSink struct{}

func (*noopSink) WriteBatch(_ *record.RecordBatch) error { return nil }
func (*noopSink) Flush() error                           { return nil }
func (*noopSink) Close() error                           { return nil }
