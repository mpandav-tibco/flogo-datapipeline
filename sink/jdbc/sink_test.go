package jdbc

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

func mustDec(t *testing.T, s string) record.Decimal {
	t.Helper()
	d, ok := record.ParseDecimal(s)
	if !ok {
		t.Fatalf("ParseDecimal(%q) failed", s)
	}
	return d
}

// TestBuildInsertPreservesDecimal proves the JDBC sink forwards record.Decimal
// values into the statement arguments unchanged — no float coercion — so the
// database driver receives the exact NUMERIC text through driver.Valuer.
func TestBuildInsertPreservesDecimal(t *testing.T) {
	s := &jdbcSink{
		table:      "payments",
		columns:    []string{"id", "amount"},
		driver:     "postgres",
		onConflict: conflictInsert,
	}
	rows := [][]interface{}{
		{int64(1), mustDec(t, "123.40")},
		{int64(2), mustDec(t, "99999999999999999.99")},
	}
	_, args := s.buildInsert(rows)
	if len(args) != 4 {
		t.Fatalf("args len = %d; want 4", len(args))
	}
	for i, want := range []string{"123.40", "99999999999999999.99"} {
		got, ok := args[2*i+1].(record.Decimal)
		if !ok {
			t.Fatalf("args[%d] = %T; want record.Decimal (float coercion would lose precision)", 2*i+1, args[2*i+1])
		}
		if got.String() != want {
			t.Errorf("args[%d].String() = %q; want %q", 2*i+1, got.String(), want)
		}
		// The exact text the database/sql driver transmits via driver.Valuer.
		v, err := got.Value()
		if err != nil {
			t.Fatalf("Value() error: %v", err)
		}
		if v != want {
			t.Errorf("driver value = %v; want %q", v, want)
		}
	}
}

// TestRecordTypeToSQLDecimal covers the driver-aware DDL: MySQL and SQL Server need
// an explicit precision/scale because bare NUMERIC/DECIMAL defaults to (10,0) and
// truncates the fractional part; Oracle uses NUMBER(38,10); Postgres NUMERIC is
// arbitrary-precision.
func TestRecordTypeToSQLDecimal(t *testing.T) {
	cases := map[string]string{
		"mysql":     "DECIMAL(38,10)",
		"sqlserver": "DECIMAL(38,10)",
		"oracle":    "NUMBER(38,10)",
		"postgres":  "NUMERIC",
	}
	for drv, want := range cases {
		if got := recordTypeToSQL(record.TypeDecimal, drv); got != want {
			t.Errorf("recordTypeToSQL(TypeDecimal, %q) = %q; want %q", drv, got, want)
		}
	}
}
