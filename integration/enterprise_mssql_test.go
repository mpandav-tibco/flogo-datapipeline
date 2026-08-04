package integration

// Enterprise scenario on SQL Server: incremental ETL from one table to another
// via a JDBC source (keyset pagination using OFFSET/FETCH) → filter → JDBC
// upsert sink (T-SQL MERGE). This exercises the SQL Server dialect end-to-end
// against a live database: [bracket] identifier quoting, @pN placeholders, the
// OFFSET … ROWS FETCH NEXT form, and the MERGE-based upsert.
//
// Live SQL Server is required; the test is skipped unless DP_TEST_MSSQL_DSN is
// set, e.g.
//
//	DP_TEST_MSSQL_DSN="sqlserver://sa:Flogo-Pass-123@localhost:11433?database=master"

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"

	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jdbc"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/jdbc"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/filter"
)

func TestEnterprise_MSSQL_IncrementalUpsertETL(t *testing.T) {
	dsn := os.Getenv("DP_TEST_MSSQL_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_MSSQL_DSN to run the enterprise SQL Server ETL scenario")
	}
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		t.Fatalf("open sqlserver: %v", err)
	}
	defer db.Close()

	exec := func(q string) {
		t.Helper()
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup exec %q: %v", q, err)
		}
	}
	exec(`DROP TABLE IF EXISTS ent_orders_src_ms`)
	exec(`DROP TABLE IF EXISTS ent_orders_dw_ms`)
	exec(`CREATE TABLE ent_orders_src_ms (order_id INT PRIMARY KEY, customer NVARCHAR(100), amount DECIMAL(12,2))`)
	exec(`CREATE TABLE ent_orders_dw_ms  (order_id INT PRIMARY KEY, customer NVARCHAR(100), amount DECIMAL(12,2))`)
	exec(`INSERT INTO ent_orders_src_ms VALUES
		(1,'Alice',100.50),
		(2,'Bob',-5.00),
		(3,'Carol',200.00),
		(4,'Dave',0.00),
		(5,'Eve',300.25)`)
	t.Cleanup(func() {
		db.Exec(`DROP TABLE IF EXISTS ent_orders_src_ms`)
		db.Exec(`DROP TABLE IF EXISTS ent_orders_dw_ms`)
	})

	def := &pipeline.Definition{
		Name: "enterprise-orders-etl-mssql",
		Source: pipeline.SourceDef{
			Type: "jdbc",
			Options: map[string]interface{}{
				"dsn":      dsn,
				"driver":   "sqlserver",
				"table":    "ent_orders_src_ms",
				"orderKey": "order_id", // keyset pagination (OFFSET/FETCH needs ORDER BY)
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
				"driver":      "sqlserver",
				"table":       "ent_orders_dw_ms",
				"onConflict":  "upsert",
				"conflictKey": []interface{}{"order_id"},
			},
		},
	}
	def.Defaults()

	run := func() *pipeline.Output {
		t.Helper()
		act, err := pipeline.NewInline(def, "sdk://enterprise-orders-etl-mssql")
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

	// --- first load: 5 read, 2 filtered (amount<=0), 3 upserted via MERGE ---
	out := run()
	if out.RowsProcessed != 5 || out.RowsWritten != 3 {
		t.Fatalf("first load processed=%d written=%d, want 5/3", out.RowsProcessed, out.RowsWritten)
	}
	assertMSCount(t, db, 3)

	// Decimal fidelity: 100.50 must round-trip exactly (numeric comparison).
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ent_orders_dw_ms WHERE order_id=1 AND amount=100.50`).Scan(&n); err != nil {
		t.Fatalf("query order 1: %v", err)
	}
	if n != 1 {
		t.Fatalf("order 1 amount did not round-trip to 100.50")
	}

	// --- second load: idempotent MERGE upsert must NOT duplicate rows ---
	out2 := run()
	if out2.RowsWritten != 3 {
		t.Fatalf("second load written=%d, want 3", out2.RowsWritten)
	}
	assertMSCount(t, db, 3) // still 3 — upsert, not append
}

func assertMSCount(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ent_orders_dw_ms`).Scan(&got); err != nil {
		t.Fatalf("count dw: %v", err)
	}
	if got != want {
		t.Fatalf("ent_orders_dw_ms row count = %d, want %d", got, want)
	}
}
