package integration

// Enterprise scenario / architecture-safety regression: proves a delete-
// capable jdbc sink (opColumn mode) is safe to use together with
// transformWorkers > 1 (action/pipeline's ordered-parallel transform path,
// "drainParallel" / parallelEligible in action.go).
//
// Audit finding: parallelEligible's exclusion list (HWM/DQ/window/schemadrift/
// checkpoint/SinkOwner) does NOT explicitly test for opColumn. This is safe by
// construction, not by accident: drainParallel only parallelizes the
// TRANSFORM stage across worker goroutines (each with its own private
// transform.Pipeline) — the actual sink write (and therefore a delete-capable
// sink's rowsDeleted accumulator) always happens from the single ordered
// "collector" goroutine that reassembles worker results in read order before
// calling rowLevelWrite. So no sink ever sees concurrent writes, regardless of
// transformWorkers.
//
// This test doesn't just assert the resulting counts — it runs under `-race`
// (see the Makefile's live-infra targets), so if the collector's ordering
// invariant were ever broken by a future change (e.g. writing straight from a
// worker goroutine instead of the collector), Go's race detector would catch
// the resulting concurrent, unsynchronized access to jdbcSink.rowsDeleted.
//
// Requires a live Postgres (DP_TEST_PG_DSN).

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"

	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jdbc"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/jdbc"
)

func TestEnterprise_CDC_DeleteCapableSink_ParallelTransformWorkers(t *testing.T) {
	dsn := os.Getenv("DP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_PG_DSN to run the parallel-transform CDC delete scenario")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	ts := time.Now().UnixNano()
	opsTable := fmt.Sprintf("ent_par_ops_%d", ts)
	targetTable := fmt.Sprintf("ent_par_target_%d", ts)
	exec := func(q string) {
		t.Helper()
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup exec %q: %v", q, err)
		}
	}
	t.Cleanup(func() {
		db.Exec("DROP TABLE IF EXISTS " + opsTable)
		db.Exec("DROP TABLE IF EXISTS " + targetTable)
	})

	// Target starts fully seeded with 500 rows (as if a prior full load ran).
	exec(fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, v TEXT)`, targetTable))
	var seedVals []string
	for i := 1; i <= 500; i++ {
		seedVals = append(seedVals, fmt.Sprintf("(%d,'v%d')", i, i))
	}
	exec(fmt.Sprintf(`INSERT INTO %s (id, v) VALUES %s`, targetTable, strings.Join(seedVals, ",")))

	// The "ops" feed: 300 delete instructions (ids 1..300) for a bulk
	// retention/purge job — a shape a real CDC/bulk-delete batch job produces.
	// __op is metadata for the sink, not a target-table column.
	exec(fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, v TEXT, __op TEXT)`, opsTable))
	var opVals []string
	for i := 1; i <= 300; i++ {
		opVals = append(opVals, fmt.Sprintf("(%d,'','d')", i))
	}
	exec(fmt.Sprintf(`INSERT INTO %s (id, v, __op) VALUES %s`, opsTable, strings.Join(opVals, ",")))

	def := &pipeline.Definition{
		Name: "enterprise-cdc-parallel-delete",
		Source: pipeline.SourceDef{
			Type: "jdbc",
			Options: map[string]interface{}{
				"dsn": dsn, "driver": "postgres", "table": opsTable, "orderKey": "id",
			},
		},
		Sink: pipeline.SinkDef{
			Type: "jdbc",
			Options: map[string]interface{}{
				"type": "jdbc", "dsn": dsn, "driver": "postgres", "table": targetTable,
				"onConflict": "upsert", "conflictKey": []interface{}{"id"}, "opColumn": "__op",
			},
		},
		// Small batches + multiple workers force many batches to flow through
		// drainParallel's worker pool concurrently — the scenario that would
		// expose a data race in the sink's rowsDeleted accumulator if the write
		// path were not, in fact, fully serialized through the collector.
		Performance: pipeline.PerformanceDef{
			BatchSize:        8,
			TransformWorkers: 4,
		},
	}
	def.Defaults()

	act, err := pipeline.NewInline(def, "sdk://enterprise-cdc-parallel-delete")
	if err != nil {
		t.Fatalf("NewInline: %v", err)
	}
	out, err := act.RunDirect(context.Background(), &pipeline.Input{})
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.Status != "completed" {
		t.Fatalf("status=%q want completed (error=%s)", out.Status, out.Error)
	}
	if out.RowsProcessed != 300 {
		t.Errorf("RowsProcessed = %d, want 300", out.RowsProcessed)
	}
	if out.RowsDeleted != 300 {
		t.Errorf("RowsDeleted = %d, want 300 (parallel transform path must not lose or duplicate delete counts)", out.RowsDeleted)
	}

	var remaining int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + targetTable).Scan(&remaining); err != nil {
		t.Fatalf("count target: %v", err)
	}
	if remaining != 200 {
		t.Errorf("target row count = %d, want 200 (500 seeded - 300 deleted)", remaining)
	}
}
