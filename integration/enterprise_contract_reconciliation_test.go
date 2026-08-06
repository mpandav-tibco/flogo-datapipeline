package integration

// Data-contract reconciliation scenario: proves the optional post-run
// reconciliation check (ContractDef.Reconciliation) actually catches a REAL
// mismatch — not a contrived one — using the exact failure mode this feature
// exists for: a transform silently changing the row count between source and
// sink, independent of the pipeline's own internal RowsProcessed/RowsWritten
// counters (which "correctly" report what the pipeline itself did; the point
// of reconciliation is verifying that matches the ACTUAL row counts at both
// ends via fresh, independent queries).
//
// Requires live Postgres (DP_TEST_PG_DSN) since only the jdbc source/sink
// implement source.Reconciler / sink.Reconciler today.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"

	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jdbc"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/jdbc"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/filter"
)

func TestEnterprise_ContractReconciliation_JDBC(t *testing.T) {
	dsn := os.Getenv("DP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_PG_DSN to run the contract reconciliation scenario")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	ts := time.Now().UnixNano()
	srcTable := fmt.Sprintf("ent_recon_src_%d", ts)
	sinkTable := fmt.Sprintf("ent_recon_sink_%d", ts)
	exec := func(q string) {
		t.Helper()
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup exec %q: %v", q, err)
		}
	}
	t.Cleanup(func() {
		db.Exec("DROP TABLE IF EXISTS " + srcTable)
		db.Exec("DROP TABLE IF EXISTS " + sinkTable)
	})

	exec(fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, amount NUMERIC(10,2))`, srcTable))
	// 5 rows; one (id=3) has a negative amount that a "filter" transform
	// downstream will drop -- an intentional, realistic row-count-changing
	// transform, not a bug -- but exactly the shape reconciliation exists to
	// surface if a contract author forgot their pipeline isn't a 1:1 mirror.
	exec(fmt.Sprintf(`INSERT INTO %s VALUES (1,100),(2,200),(3,-50),(4,300),(5,400)`, srcTable))

	runCopy := func(withFilter bool) *pipeline.Output {
		t.Helper()
		exec("DROP TABLE IF EXISTS " + sinkTable)
		def := &pipeline.Definition{
			Name:   "enterprise-contract-reconciliation",
			Source: pipeline.SourceDef{Type: "jdbc", Options: map[string]interface{}{"dsn": dsn, "driver": "postgres", "table": srcTable, "orderKey": "id"}},
			Sink: pipeline.SinkDef{Type: "jdbc", Options: map[string]interface{}{
				"type": "jdbc", "dsn": dsn, "driver": "postgres", "table": sinkTable,
				"autoMigrate": true, "onConflict": "upsert", "conflictKey": []interface{}{"id"},
			}},
			Contract: &pipeline.ContractDef{
				Reconciliation: &pipeline.ReconciliationDef{
					Enabled:          true,
					SourceCountQuery: "SELECT COUNT(*) FROM " + srcTable,
					SinkCountQuery:   "SELECT COUNT(*) FROM " + sinkTable,
					OnMismatch:       "fail",
				},
			},
		}
		if withFilter {
			def.Transforms = []pipeline.TransformDef{
				{Type: "filter", Extra: map[string]interface{}{"type": "filter", "expr": "amount > 0"}},
			}
		}
		def.Defaults()
		act, err := pipeline.NewInline(def, "sdk://enterprise-contract-reconciliation")
		if err != nil {
			t.Fatalf("NewInline: %v", err)
		}
		out, err := act.RunDirect(context.Background(), &pipeline.Input{})
		if err != nil {
			t.Fatalf("RunDirect: %v", err)
		}
		return out
	}

	t.Run("1:1 copy reconciles cleanly", func(t *testing.T) {
		out := runCopy(false)
		if out.Status != "completed" {
			t.Fatalf("status=%q, want completed (error=%s)", out.Status, out.Error)
		}
		rec, ok := out.Metadata["reconciliation"].(map[string]interface{})
		if !ok {
			t.Fatal("expected out.Metadata[\"reconciliation\"] to be populated")
		}
		if mismatched, _ := rec["mismatched"].(bool); mismatched {
			t.Errorf("reconciliation reported a mismatch for a true 1:1 copy: %+v", rec)
		}
	})

	t.Run("filter changes the row count, reconciliation catches it and marks partial", func(t *testing.T) {
		out := runCopy(true)
		if out.Status != "partial" {
			t.Fatalf("status=%q, want partial (reconciliation mismatch with onMismatch=fail); error=%s", out.Status, out.Error)
		}
		rec, ok := out.Metadata["reconciliation"].(map[string]interface{})
		if !ok {
			t.Fatal("expected out.Metadata[\"reconciliation\"] to be populated")
		}
		if mismatched, _ := rec["mismatched"].(bool); !mismatched {
			t.Errorf("expected reconciliation to report a mismatch (source has 5 rows, sink has 4 after the filter drops id=3): %+v", rec)
		}
		if sc, _ := rec["sourceCount"].(int64); sc != 5 {
			t.Errorf("sourceCount = %v, want 5", rec["sourceCount"])
		}
		if nk, _ := rec["sinkCount"].(int64); nk != 4 {
			t.Errorf("sinkCount = %v, want 4", rec["sinkCount"])
		}
	})
}
