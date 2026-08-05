package sqlutil

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

func TestQuoteIdent(t *testing.T) {
	cases := []struct{ name, in, driver, want string }{
		{"postgres double-quote", "col", "postgres", `"col"`},
		{"oracle double-quote", "col", "oracle", `"col"`},
		{"mysql backtick", "col", "mysql", "`col`"},
		{"sqlserver bracket", "col", "sqlserver", "[col]"},
		{"mssql bracket alias", "col", "mssql", "[col]"},
		{"postgres embedded quote escaped", `a"b`, "postgres", `"a""b"`},
		{"mysql embedded backtick escaped", "a`b", "mysql", "`a``b`"},
		{"sqlserver embedded bracket escaped", "a]b", "sqlserver", "[a]]b]"},
		{"unknown driver falls back to double-quote", "col", "db2", `"col"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := QuoteIdent(c.in, c.driver); got != c.want {
				t.Errorf("QuoteIdent(%q, %q) = %q, want %q", c.in, c.driver, got, c.want)
			}
		})
	}
}

func TestPlaceholder(t *testing.T) {
	cases := []struct {
		name   string
		n      int
		driver string
		want   string
	}{
		{"postgres n1", 1, "postgres", "$1"},
		{"postgres n3", 3, "postgres", "$3"},
		{"mysql", 2, "mysql", "?"},
		{"sqlserver", 4, "sqlserver", "@p4"},
		{"mssql alias", 4, "mssql", "@p4"},
		{"oracle", 5, "oracle", ":5"},
		{"unknown falls back to postgres form", 7, "db2", "$7"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Placeholder(c.n, c.driver); got != c.want {
				t.Errorf("Placeholder(%d, %q) = %q, want %q", c.n, c.driver, got, c.want)
			}
		})
	}
}

func TestLimitClause(t *testing.T) {
	cases := []struct {
		name          string
		driver        string
		limit, offset int64
		want          string
	}{
		{"postgres limit only", "postgres", 10, 0, " LIMIT 10"},
		{"postgres offset only", "postgres", 0, 5, " OFFSET 5"},
		{"postgres both", "postgres", 10, 5, " LIMIT 10 OFFSET 5"},
		{"postgres none", "postgres", 0, 0, ""},
		{"mysql both", "mysql", 20, 40, " LIMIT 20 OFFSET 40"},
		{"sqlserver both", "sqlserver", 10, 5, " OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY"},
		{"sqlserver offset only", "sqlserver", 0, 5, " OFFSET 5 ROWS"},
		{"mssql both alias", "mssql", 3, 2, " OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY"},
		{"oracle limit implies offset 0", "oracle", 10, 0, " OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY"},
		{"oracle none", "oracle", 0, 0, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := LimitClause(c.driver, c.limit, c.offset); got != c.want {
				t.Errorf("LimitClause(%q, %d, %d) = %q, want %q", c.driver, c.limit, c.offset, got, c.want)
			}
		})
	}
}

func TestSQLTypeToRecord(t *testing.T) {
	cases := []struct {
		in   string
		want record.ColumnType
	}{
		{"INT", record.TypeInt},
		{"int4", record.TypeInt},
		{"BIGINT", record.TypeInt},
		{"SMALLINT", record.TypeInt},
		{"YEAR", record.TypeInt},
		{"FLOAT8", record.TypeFloat},
		{"DOUBLE", record.TypeFloat},
		{"BINARY_DOUBLE", record.TypeFloat},
		{"NUMERIC(38,10)", record.TypeDecimal},
		{"NUMBER", record.TypeDecimal},
		{"MONEY", record.TypeDecimal},
		{"BOOL", record.TypeBool},
		{"BOOLEAN", record.TypeBool},
		{"BIT", record.TypeBool},
		{"DATE", record.TypeDate},
		{"TIMESTAMPTZ", record.TypeDate},
		{"DATETIME2", record.TypeDate},
		{"TIMESTAMP WITH TIME ZONE", record.TypeDate},
		{"VARCHAR(255)", record.TypeString},
		{"JSONB", record.TypeString},
		{"UUID", record.TypeString},
		{"  timestamp  ", record.TypeDate}, // lower-case + surrounding space
		{"", record.TypeString},            // empty falls through to string
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			if got := SQLTypeToRecord(c.in); got != c.want {
				t.Errorf("SQLTypeToRecord(%q) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}
