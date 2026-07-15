package jdbc

import "testing"

// TestHWMPredicate verifies the incremental high-water-mark WHERE predicate that
// the engine-injected hwmColumn/hwmValue/hwmValueType keys produce, including the
// numeric-vs-quoted formatting and single-quote escaping.
func TestHWMPredicate(t *testing.T) {
	cases := []struct {
		name string
		cfg  map[string]interface{}
		want string
	}{
		{"none", map[string]interface{}{}, ""},
		{"no-value", map[string]interface{}{"hwmColumn": "updated_at"}, ""},
		{
			"datetime",
			map[string]interface{}{"hwmColumn": "updated_at", "hwmValue": "2026-07-05 13:00:00", "hwmValueType": "datetime"},
			`"updated_at" > '2026-07-05 13:00:00'`,
		},
		{
			"int",
			map[string]interface{}{"hwmColumn": "id", "hwmValue": "500", "hwmValueType": "int"},
			`"id" > 500`,
		},
		{
			"int-invalid-falls-back-to-quoted",
			map[string]interface{}{"hwmColumn": "id", "hwmValue": "abc", "hwmValueType": "int"},
			`"id" > 'abc'`,
		},
		{
			"string-escapes-quote",
			map[string]interface{}{"hwmColumn": "code", "hwmValue": "O'Brien", "hwmValueType": "string"},
			`"code" > 'O''Brien'`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := hwmPredicate(c.cfg, "postgres"); got != c.want {
				t.Fatalf("hwmPredicate = %q, want %q", got, c.want)
			}
		})
	}
}

// TestBuildBaseQueryWithHWM verifies the HWM predicate is ANDed onto a user WHERE.
func TestBuildBaseQueryWithHWM(t *testing.T) {
	cfg := map[string]interface{}{
		"where":        "status = 'ACTIVE'",
		"hwmColumn":    "updated_at",
		"hwmValue":     "2026-07-05 13:00:00",
		"hwmValueType": "datetime",
	}
	got := buildBaseQuery(cfg, "src_orders", "postgres")
	want := `SELECT * FROM "src_orders" WHERE status = 'ACTIVE' AND "updated_at" > '2026-07-05 13:00:00'`
	if got != want {
		t.Fatalf("buildBaseQuery =\n  %q\nwant\n  %q", got, want)
	}
}
