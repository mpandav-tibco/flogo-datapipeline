package integration

// Enterprise scenario on Oracle: incremental ETL from one table to another via a
// JDBC source (keyset pagination using OFFSET/FETCH) → filter → JDBC upsert sink
// (Oracle MERGE … USING (SELECT … FROM dual)). This exercises the Oracle dialect
// end-to-end against a live database: "double-quote" identifier quoting, :N
// placeholders, the OFFSET … ROWS FETCH NEXT form, and the MERGE-from-dual upsert.
//
// The fixtures are created with quoted lower-case identifiers so their stored
// case matches the (lower-case) pipeline config — mirroring how Postgres folds
// unquoted names to lower case — which keeps this test parallel to the Postgres
// and SQL Server scenarios.
//
// Live Oracle is required; the test is skipped unless DP_TEST_ORACLE_DSN is set,
// e.g.
//
//	DP_TEST_ORACLE_DSN="oracle://dpuser:FlogoPass123@localhost:11521/FREEPDB1"

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/sijms/go-ora/v2"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"

	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jdbc"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/jdbc"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/filter"
)

func TestEnterprise_Oracle_IncrementalUpsertETL(t *testing.T) {
	dsn := os.Getenv("DP_TEST_ORACLE_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_ORACLE_DSN to run the enterprise Oracle ETL scenario")
	}
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		t.Fatalf("open oracle: %v", err)
	}
	defer db.Close()

	exec := func(q string) {
		t.Helper()
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup exec %q: %v", q, err)
		}
	}
	// Oracle (< 23c) has no DROP TABLE IF EXISTS; swallow ORA-00942 (table missing).
	drop := func(quotedTable string) {
		db.Exec(`BEGIN EXECUTE IMMEDIATE 'DROP TABLE ` + quotedTable + `'; EXCEPTION WHEN OTHERS THEN NULL; END;`)
	}
	drop(`"ent_orders_src_or"`)
	drop(`"ent_orders_dw_or"`)
	exec(`CREATE TABLE "ent_orders_src_or" ("order_id" NUMBER(10) PRIMARY KEY, "customer" VARCHAR2(100), "amount" NUMBER(12,2))`)
	exec(`CREATE TABLE "ent_orders_dw_or"  ("order_id" NUMBER(10) PRIMARY KEY, "customer" VARCHAR2(100), "amount" NUMBER(12,2))`)
	// Oracle has no multi-row VALUES; insert fixtures individually.
	for _, r := range []string{
		`(1,'Alice',100.50)`,
		`(2,'Bob',-5.00)`,
		`(3,'Carol',200.00)`,
		`(4,'Dave',0.00)`,
		`(5,'Eve',300.25)`,
	} {
		exec(`INSERT INTO "ent_orders_src_or" ("order_id","customer","amount") VALUES ` + r)
	}
	t.Cleanup(func() {
		drop(`"ent_orders_src_or"`)
		drop(`"ent_orders_dw_or"`)
	})

	def := &pipeline.Definition{
		Name: "enterprise-orders-etl-oracle",
		Source: pipeline.SourceDef{
			Type: "jdbc",
			Options: map[string]interface{}{
				"dsn":      dsn,
				"driver":   "oracle",
				"table":    "ent_orders_src_or",
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
				"driver":      "oracle",
				"table":       "ent_orders_dw_or",
				"onConflict":  "upsert",
				"conflictKey": []interface{}{"order_id"},
			},
		},
	}
	def.Defaults()

	run := func() *pipeline.Output {
		t.Helper()
		act, err := pipeline.NewInline(def, "sdk://enterprise-orders-etl-oracle")
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
	assertORCount(t, db, 3)

	// Decimal fidelity: 100.50 must round-trip (numeric comparison; NUMBER(12,2)).
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "ent_orders_dw_or" WHERE "order_id"=1 AND "amount"=100.50`).Scan(&n); err != nil {
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
	assertORCount(t, db, 3) // still 3 — upsert, not append
}

func assertORCount(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "ent_orders_dw_or"`).Scan(&got); err != nil {
		t.Fatalf("count dw: %v", err)
	}
	if got != want {
		t.Fatalf("ent_orders_dw_or row count = %d, want %d", got, want)
	}
}
