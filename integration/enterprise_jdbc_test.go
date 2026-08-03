package integration

// Enterprise scenario: idempotent incremental ETL from one Postgres table to
// another via a JDBC source → filter → JDBC upsert sink, executed through the
// real DataPipelineAction orchestration (NewInline + RunDirect).
//
// This doubles as shippable reference collateral for the "database-to-database
// upsert" pattern: keyset-paginated read, an in-flight quality filter, and an
// idempotent ON CONFLICT DO UPDATE load that is safe to re-run (at-least-once
// delivery + upsert = exactly-once effect at the target).
//
// Live Postgres is required; the test is skipped unless DP_TEST_PG_DSN is set,
// e.g.  DP_TEST_PG_DSN="postgres://postgres:postgres@127.0.0.1:55432/testdb?sslmode=disable"

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/lib/pq"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"

	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jdbc"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/jdbc"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/filter"
)

func TestEnterprise_JDBC_IncrementalUpsertETL(t *testing.T) {
	dsn := os.Getenv("DP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_PG_DSN to run the enterprise JDBC ETL scenario")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	// --- fixture: a source table with a mix of valid and invalid orders ---
	exec := func(q string) {
		t.Helper()
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup exec %q: %v", q, err)
		}
	}
	exec(`DROP TABLE IF EXISTS ent_orders_src`)
	exec(`DROP TABLE IF EXISTS ent_orders_dw`)
	exec(`CREATE TABLE ent_orders_src (order_id INT PRIMARY KEY, customer TEXT, amount NUMERIC(12,2))`)
	exec(`CREATE TABLE ent_orders_dw  (order_id INT PRIMARY KEY, customer TEXT, amount NUMERIC(12,2))`)
	exec(`INSERT INTO ent_orders_src VALUES
		(1,'Alice',100.50),
		(2,'Bob',-5.00),
		(3,'Carol',200.00),
		(4,'Dave',0.00),
		(5,'Eve',300.25)`)
	t.Cleanup(func() {
		db.Exec(`DROP TABLE IF EXISTS ent_orders_src`)
		db.Exec(`DROP TABLE IF EXISTS ent_orders_dw`)
	})

	def := &pipeline.Definition{
		Name: "enterprise-orders-etl",
		Source: pipeline.SourceDef{
			Type: "jdbc",
			Options: map[string]interface{}{
				"dsn":      dsn,
				"driver":   "postgres",
				"table":    "ent_orders_src",
				"orderKey": "order_id", // keyset pagination
			},
		},
		Transforms: []pipeline.TransformDef{
			{Type: "filter", Extra: map[string]interface{}{"type": "filter", "expr": "amount > 0"}},
		},
		Sink: pipeline.SinkDef{
			Type: "jdbc",
			Options: map[string]interface{}{
				"type":        "jdbc",
				"dsn":         dsn,
				"driver":      "postgres",
				"table":       "ent_orders_dw",
				"onConflict":  "upsert",
				"conflictKey": []interface{}{"order_id"},
			},
		},
	}
	def.Defaults()

	run := func() *pipeline.Output {
		t.Helper()
		act, err := pipeline.NewInline(def, "sdk://enterprise-orders-etl")
		if err != nil {
			t.Fatalf("NewInline: %v", err)
		}
		out, err := act.RunDirect(context.Background(), &pipeline.Input{})
		if err != nil {
			t.Fatalf("RunDirect: %v", err)
		}
		if out.Status != "completed" {
			t.Fatalf("status=%q want completed (err=%s)", out.Status, out.Error)
		}
		return out
	}

	// --- first load: 5 read, 2 filtered (amount<=0), 3 upserted ---
	out := run()
	if out.RowsProcessed != 5 || out.RowsWritten != 3 {
		t.Fatalf("first load processed=%d written=%d, want 5/3", out.RowsProcessed, out.RowsWritten)
	}
	assertDWCount(t, db, 3)

	var amount string
	if err := db.QueryRow(`SELECT amount::text FROM ent_orders_dw WHERE order_id=1`).Scan(&amount); err != nil {
		t.Fatalf("query order 1: %v", err)
	}
	if amount != "100.50" {
		t.Fatalf("order 1 amount=%q, want 100.50 (decimal fidelity)", amount)
	}

	// --- second load: idempotent upsert must NOT duplicate rows ---
	out2 := run()
	if out2.RowsWritten != 3 {
		t.Fatalf("second load written=%d, want 3", out2.RowsWritten)
	}
	assertDWCount(t, db, 3) // still 3 — upsert, not append
}

func assertDWCount(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ent_orders_dw`).Scan(&n); err != nil {
		t.Fatalf("count dw: %v", err)
	}
	if n != want {
		t.Fatalf("ent_orders_dw row count = %d, want %d", n, want)
	}
}
