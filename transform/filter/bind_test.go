package filter

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// TestBoundPredicateMatchesMapEval proves the position-bound evaluator used on
// the hot path agrees with the map-based Eval for every expression/row pair,
// including absent columns, ragged rows, duplicate names and mixed cell types.
func TestBoundPredicateMatchesMapEval(t *testing.T) {
	cols := []record.Column{{Name: "id"}, {Name: "status"}, {Name: "amount"}, {Name: "flag"}, {Name: "status"}}
	dec, _ := record.ParseDecimal("12.50")
	rows := [][]interface{}{
		{int64(1), "OPEN", 10.5, true, "VOID"},
		{int64(2), "VOID", int64(0), false, "OPEN"},
		{int32(3), nil, "7", nil, nil},
		{int(4), "10", dec, "true", "x"},
		{int64(5)}, // ragged: everything past id absent
		{},
		{"abc", "OPEN", 3.0e22, 1, "OPEN"},
	}
	exprs := []string{
		"id > 2", "id = 'abc'", "status = 'OPEN'", "status != 'VOID'",
		"amount >= 10", "amount < 7.5", "amount = '7'", "flag = true", "flag != false",
		"status IS NULL", "status IS NOT NULL", "missing IS NULL", "missing IS NOT NULL",
		"missing = 1", "amount = null", "amount != null", "id > 1 AND amount > 5",
		"status = 'X' OR id <= 2", "amount > '5'", "id ~ 3",
	}
	for _, e := range exprs {
		p, err := Compile(e)
		if err != nil {
			t.Fatalf("compile %q: %v", e, err)
		}
		bp := p.Bind(cols)
		b := &record.RecordBatch{Columns: cols, Rows: rows}
		for i, row := range rows {
			want := p.Eval(b.RowMap(i))
			if got := bp.EvalRow(row); got != want {
				t.Errorf("%q row %d %v: bound=%v map=%v", e, i, row, got, want)
			}
		}
	}
}

func TestFilterRebindsOnSchemaChange(t *testing.T) {
	f := &filterTransform{}
	c, err := parseCondition("amount > 5")
	if err != nil {
		t.Fatal(err)
	}
	f.cond = c
	b1 := &record.RecordBatch{Columns: []record.Column{{Name: "amount"}, {Name: "id"}}, Rows: [][]interface{}{{10.0, 1}}}
	b2 := &record.RecordBatch{Columns: []record.Column{{Name: "id"}, {Name: "amount"}}, Rows: [][]interface{}{{1, 10.0}}}
	for _, b := range []*record.RecordBatch{b1, b2} {
		out, err := f.Apply(b)
		if err != nil || out.Len() != 1 {
			t.Fatalf("columns %v: got %v rows, err %v; want 1", b.Columns, out.Len(), err)
		}
	}
}
