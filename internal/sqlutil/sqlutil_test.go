package sqlutil

import "testing"

func TestQuoteLiteral(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		driver string
		want   string
	}{
		{"plain postgres", "abc", "postgres", "'abc'"},
		{"single quote postgres", "O'Brien", "postgres", "'O''Brien'"},
		{"backslash postgres kept literal", `C:\foo`, "postgres", `'C:\foo'`},
		{"plain mysql", "abc", "mysql", "'abc'"},
		{"single quote mysql", "O'Brien", "mysql", "'O''Brien'"},
		{"backslash mysql doubled", `C:\foo`, "mysql", `'C:\\foo'`},
		// The injection vector: a trailing backslash must not escape the closing
		// quote on MySQL. With escaping, the closing quote stays intact.
		{"trailing backslash mysql", `x\`, "mysql", `'x\\'`},
		{"quote after backslash mysql", `x\' OR 1=1 -- `, "mysql", `'x\\'' OR 1=1 -- '`},
		{"sqlite standard", `x\`, "sqlite3", `'x\'`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := QuoteLiteral(c.in, c.driver); got != c.want {
				t.Errorf("QuoteLiteral(%q, %q) = %q, want %q", c.in, c.driver, got, c.want)
			}
		})
	}
}
