package filter

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

// helper: make a batch with columns id(int), status(string), amount(float)
func makeBatch(rows []map[string]interface{}) *record.RecordBatch {
	cols := []record.Column{
		{Name: "id", Type: record.TypeInt},
		{Name: "status", Type: record.TypeString},
		{Name: "amount", Type: record.TypeFloat},
		{Name: "note", Type: record.TypeString},
	}
	b := &record.RecordBatch{Columns: cols}
	for _, row := range rows {
		r := make([]interface{}, len(cols))
		for j, col := range cols {
			r[j] = row[col.Name]
		}
		b.Rows = append(b.Rows, r)
	}
	return b
}

func buildFilter(t *testing.T, expr string) transform.Transform {
	t.Helper()
	f, err := transform.New("filter", map[string]interface{}{"expr": expr})
	if err != nil {
		t.Fatalf("buildFilter(%q): %v", expr, err)
	}
	return f
}

// ─── AND condition ────────────────────────────────────────────────────────────

func TestFilter_AND(t *testing.T) {
	f := buildFilter(t, "amount > 0 AND status != 'VOID'")
	batch := makeBatch([]map[string]interface{}{
		{"id": 1, "status": "OK", "amount": 100.0, "note": nil},
		{"id": 2, "status": "VOID", "amount": 50.0, "note": nil},
		{"id": 3, "status": "OK", "amount": -5.0, "note": nil},
		{"id": 4, "status": "OK", "amount": 75.0, "note": nil},
	})
	out, err := f.Apply(batch)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 2 {
		t.Fatalf("expected 2 rows, got %d", out.Len())
	}
	// Confirm the correct rows survive.
	ids := []interface{}{out.Value(0, "id"), out.Value(1, "id")}
	if ids[0] != 1 || ids[1] != 4 {
		t.Errorf("unexpected row ids: %v", ids)
	}
}

// ─── OR condition ─────────────────────────────────────────────────────────────

func TestFilter_OR(t *testing.T) {
	f := buildFilter(t, "status = 'VOID' OR amount < 0")
	batch := makeBatch([]map[string]interface{}{
		{"id": 1, "status": "OK", "amount": 100.0, "note": nil},
		{"id": 2, "status": "VOID", "amount": 50.0, "note": nil},
		{"id": 3, "status": "OK", "amount": -5.0, "note": nil},
	})
	out, err := f.Apply(batch)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 2 {
		t.Fatalf("expected 2 rows, got %d", out.Len())
	}
}

// ─── IS NULL ──────────────────────────────────────────────────────────────────

func TestFilter_IsNull(t *testing.T) {
	f := buildFilter(t, "note IS NULL")
	batch := makeBatch([]map[string]interface{}{
		{"id": 1, "status": "OK", "amount": 1.0, "note": nil},
		{"id": 2, "status": "OK", "amount": 2.0, "note": "hello"},
	})
	out, err := f.Apply(batch)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 1 {
		t.Fatalf("expected 1 row, got %d", out.Len())
	}
	if out.Value(0, "id") != 1 {
		t.Errorf("expected id=1, got %v", out.Value(0, "id"))
	}
}

// ─── IS NOT NULL ──────────────────────────────────────────────────────────────

func TestFilter_IsNotNull(t *testing.T) {
	f := buildFilter(t, "note IS NOT NULL")
	batch := makeBatch([]map[string]interface{}{
		{"id": 1, "status": "OK", "amount": 1.0, "note": nil},
		{"id": 2, "status": "OK", "amount": 2.0, "note": "hello"},
	})
	out, err := f.Apply(batch)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 1 {
		t.Fatalf("expected 1 row, got %d", out.Len())
	}
	if out.Value(0, "id") != 2 {
		t.Errorf("expected id=2, got %v", out.Value(0, "id"))
	}
}

// ─── Numeric comparisons ──────────────────────────────────────────────────────

func TestFilter_NumericComparisons(t *testing.T) {
	cases := []struct {
		expr    string
		wantIDs []int
	}{
		{"amount >= 50", []int{2, 3}},
		{"amount <= 10", []int{1}},
		{"amount > 10", []int{2, 3}},
		{"amount < 100", []int{1, 2}},
		{"amount = 50", []int{2}},
		{"amount != 50", []int{1, 3}},
	}
	batch := makeBatch([]map[string]interface{}{
		{"id": 1, "status": "A", "amount": 10.0, "note": nil},
		{"id": 2, "status": "B", "amount": 50.0, "note": nil},
		{"id": 3, "status": "C", "amount": 100.0, "note": nil},
	})
	for _, tc := range cases {
		t.Run(tc.expr, func(t *testing.T) {
			f := buildFilter(t, tc.expr)
			out, err := f.Apply(batch)
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if out == nil {
				out = &record.RecordBatch{}
			}
			if out.Len() != len(tc.wantIDs) {
				t.Fatalf("rows = %d, want %d", out.Len(), len(tc.wantIDs))
			}
		})
	}
}

// ─── String comparison ────────────────────────────────────────────────────────

func TestFilter_StringEquality(t *testing.T) {
	f := buildFilter(t, "status = 'ACTIVE'")
	batch := makeBatch([]map[string]interface{}{
		{"id": 1, "status": "ACTIVE", "amount": 1.0, "note": nil},
		{"id": 2, "status": "INACTIVE", "amount": 2.0, "note": nil},
	})
	out, err := f.Apply(batch)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 1 || out.Value(0, "status") != "ACTIVE" {
		t.Errorf("unexpected result: %v rows", out.Len())
	}
}

// ─── All rows filtered → nil result ──────────────────────────────────────────

func TestFilter_AllRowsFiltered(t *testing.T) {
	f := buildFilter(t, "amount > 9999")
	batch := makeBatch([]map[string]interface{}{
		{"id": 1, "status": "OK", "amount": 1.0, "note": nil},
	})
	out, err := f.Apply(batch)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out != nil {
		t.Errorf("expected nil result when all rows filtered, got batch with %d rows", out.Len())
	}
}

// ─── Missing 'expr' config error ─────────────────────────────────────────────

func TestFilter_MissingExprError(t *testing.T) {
	_, err := transform.New("filter", map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing expr, got nil")
	}
}

// ─── Tokenizer edge cases ─────────────────────────────────────────────────────

func TestTokenize_QuotedStringWithSpaces(t *testing.T) {
	tokens := tokenize("status = 'hello world'")
	if len(tokens) != 3 {
		t.Fatalf("expected 3 tokens, got %d: %v", len(tokens), tokens)
	}
	if tokens[2] != "'hello world'" {
		t.Errorf("token[2] = %q, want \"'hello world'\"", tokens[2])
	}
}

func TestTokenize_EmptyExpr(t *testing.T) {
	tokens := tokenize("   ")
	if len(tokens) != 0 {
		t.Errorf("expected 0 tokens for whitespace-only, got %d", len(tokens))
	}
}

func TestTokenize_MultipleSpaces(t *testing.T) {
	tokens := tokenize("a  =  'b'")
	if len(tokens) != 3 {
		t.Fatalf("expected 3 tokens, got %d: %v", len(tokens), tokens)
	}
}

// ─── Close is a no-op ─────────────────────────────────────────────────────────

func TestFilter_Close(t *testing.T) {
	f := buildFilter(t, "amount > 0")
	if err := f.Close(); err != nil {
		t.Errorf("Close() error: %v", err)
	}
}
