package jdbc

import (
	"strings"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

func TestNewJDBCSink_OpColumnRequiresConflictKey(t *testing.T) {
	_, err := newJDBCSink(map[string]interface{}{
		"dsn": "postgres://x", "table": "t", "opColumn": "__op",
	})
	if err == nil {
		t.Fatal("expected an error: opColumn without conflictKey")
	}
}

func TestIsDeleteOp(t *testing.T) {
	deleteValues := []string{"d", "delete"}
	cases := []struct {
		v    interface{}
		want bool
	}{
		{"d", true},
		{"D", true},
		{"delete", true},
		{"DELETE", true},
		{"u", false},
		{"c", false},
		{"", false},
		{nil, false},
		{int64(1), false},
	}
	for _, c := range cases {
		if got := isDeleteOp(c.v, deleteValues); got != c.want {
			t.Errorf("isDeleteOp(%v, %v) = %v, want %v", c.v, deleteValues, got, c.want)
		}
	}
}

func TestStripColumn(t *testing.T) {
	cols := []record.Column{
		{Name: "id", Type: record.TypeInt},
		{Name: "__op", Type: record.TypeString},
		{Name: "name", Type: record.TypeString},
	}
	got := stripColumn(cols, "__op")
	if len(got) != 2 || got[0].Name != "id" || got[1].Name != "name" {
		t.Fatalf("stripColumn = %+v, want [id, name]", got)
	}
	// A name that isn't present is a no-op (order/content preserved).
	got2 := stripColumn(cols, "nope")
	if len(got2) != 3 {
		t.Fatalf("stripColumn with absent name changed length: %+v", got2)
	}
}

func TestBuildDelete_SingleColumnKey(t *testing.T) {
	s := &jdbcSink{table: "orders", driver: "postgres", conflictKey: []string{"id"}}
	query, args := s.buildDelete([][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}})
	if !strings.Contains(query, `DELETE FROM "orders"`) {
		t.Errorf("query = %q, want it to start a DELETE FROM \"orders\"", query)
	}
	// OR-of-ANDs form: 3 keys -> 3 parenthesised terms joined by OR, no row-value IN.
	if strings.Count(query, " OR ") != 2 {
		t.Errorf("query = %q, want exactly 2 ORs for 3 keys", query)
	}
	if len(args) != 3 {
		t.Fatalf("args = %v, want 3 values", args)
	}
}

func TestBuildDelete_CompositeKey(t *testing.T) {
	s := &jdbcSink{table: "line_items", driver: "postgres", conflictKey: []string{"order_id", "line_no"}}
	query, args := s.buildDelete([][]interface{}{{int64(1), int64(1)}, {int64(1), int64(2)}})
	if strings.Count(query, " AND ") != 2 {
		t.Errorf("query = %q, want 2 ANDs (2 keys x 2 rows)", query)
	}
	if len(args) != 4 {
		t.Fatalf("args = %v, want 4 values (2 keys x 2 rows)", args)
	}
}

func TestBuildDelete_SQLServerPlaceholders(t *testing.T) {
	// The OR-of-ANDs form must work on SQL Server too, since it has no
	// row-value "(a,b) IN (...)" constructor at all — the whole point of
	// choosing this form over a tuple IN list.
	s := &jdbcSink{table: "orders", driver: "sqlserver", conflictKey: []string{"id"}}
	query, _ := s.buildDelete([][]interface{}{{int64(1)}, {int64(2)}})
	if !strings.Contains(query, "@p1") || !strings.Contains(query, "@p2") {
		t.Errorf("query = %q, want @p1/@p2 SQL Server placeholders", query)
	}
}
