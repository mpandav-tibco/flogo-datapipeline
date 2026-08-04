package jdbc

import "testing"

// These tests lock the exact SQL generated for each dialect. They are the
// primary correctness gate for the SQL Server and Oracle paths, which cannot be
// executed against a live database in this environment.

func TestBuildInsert_SQLServer(t *testing.T) {
	s := &jdbcSink{table: "t", columns: []string{"id", "amount"}, driver: "sqlserver", onConflict: conflictInsert}
	q, args := s.buildInsert([][]interface{}{{int64(1), 1.5}, {int64(2), 2.5}})
	want := "INSERT INTO [t] ([id],[amount]) VALUES (@p1,@p2),(@p3,@p4)"
	if q != want {
		t.Errorf("SQL Server insert:\n got  %q\n want %q", q, want)
	}
	if len(args) != 4 {
		t.Errorf("args = %d, want 4", len(args))
	}
}

func TestBuildInsert_OracleInsertAll(t *testing.T) {
	s := &jdbcSink{table: "orders", columns: []string{"id", "amt"}, driver: "oracle", onConflict: conflictInsert}
	q, args := s.buildInsert([][]interface{}{{int64(1), 2}, {int64(3), 4}})
	want := `INSERT ALL INTO "orders" ("id","amt") VALUES (:1,:2) INTO "orders" ("id","amt") VALUES (:3,:4) SELECT 1 FROM dual`
	if q != want {
		t.Errorf("Oracle INSERT ALL:\n got  %q\n want %q", q, want)
	}
	if len(args) != 4 {
		t.Errorf("args = %d, want 4", len(args))
	}
}

func TestBuildMerge_SQLServerUpsert(t *testing.T) {
	s := &jdbcSink{table: "t", columns: []string{"id", "amount"}, driver: "sqlserver", onConflict: conflictUpsert, conflictKey: []string{"id"}}
	q, _ := s.buildInsert([][]interface{}{{int64(1), 1.5}})
	want := "MERGE INTO [t] tgt USING (VALUES (@p1,@p2)) AS src ([id],[amount]) " +
		"ON (tgt.[id]=src.[id]) " +
		"WHEN MATCHED THEN UPDATE SET tgt.[amount]=src.[amount] " +
		"WHEN NOT MATCHED THEN INSERT ([id],[amount]) VALUES (src.[id],src.[amount]);"
	if q != want {
		t.Errorf("SQL Server MERGE upsert:\n got  %q\n want %q", q, want)
	}
}

func TestBuildMerge_SQLServerIgnore(t *testing.T) {
	s := &jdbcSink{table: "t", columns: []string{"id", "amount"}, driver: "sqlserver", onConflict: conflictIgnore, conflictKey: []string{"id"}}
	q, _ := s.buildInsert([][]interface{}{{int64(1), 1.5}})
	want := "MERGE INTO [t] tgt USING (VALUES (@p1,@p2)) AS src ([id],[amount]) " +
		"ON (tgt.[id]=src.[id]) " +
		"WHEN NOT MATCHED THEN INSERT ([id],[amount]) VALUES (src.[id],src.[amount]);"
	if q != want {
		t.Errorf("SQL Server MERGE ignore:\n got  %q\n want %q", q, want)
	}
}

func TestBuildMerge_OracleUpsert(t *testing.T) {
	s := &jdbcSink{table: "t", columns: []string{"id", "amount"}, driver: "oracle", onConflict: conflictUpsert, conflictKey: []string{"id"}}
	q, _ := s.buildInsert([][]interface{}{{int64(1), 1.5}, {int64(2), 2.5}})
	want := `MERGE INTO "t" tgt USING (SELECT :1 "id",:2 "amount" FROM dual UNION ALL SELECT :3,:4 FROM dual) src ` +
		`ON (tgt."id"=src."id") ` +
		`WHEN MATCHED THEN UPDATE SET tgt."amount"=src."amount" ` +
		`WHEN NOT MATCHED THEN INSERT ("id","amount") VALUES (src."id",src."amount")`
	if q != want {
		t.Errorf("Oracle MERGE upsert:\n got  %q\n want %q", q, want)
	}
}

func TestMaxRowsPerInsert(t *testing.T) {
	cases := []struct {
		driver string
		ncols  int
		want   int
	}{
		{"sqlserver", 3, (2100 - 8) / 3},
		{"oracle", 5, (30000 - 8) / 5},
		{"postgres", 10, (65535 - 8) / 10},
		{"mysql", 2, (65535 - 8) / 2},
		{"sqlserver", 5000, 1}, // more columns than the cap → at least one row
	}
	for _, c := range cases {
		if got := maxRowsPerInsert(c.driver, c.ncols); got != c.want {
			t.Errorf("maxRowsPerInsert(%q,%d) = %d, want %d", c.driver, c.ncols, got, c.want)
		}
	}
}
