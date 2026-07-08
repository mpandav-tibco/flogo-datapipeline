package validate

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"

	// Blank-import null sink so validate can route bad rows without error.
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/null"
)

// helper: build a simple 3-column batch
func txBatch(rows []map[string]interface{}) *record.RecordBatch {
	cols := []record.Column{
		{Name: "amount", Type: record.TypeFloat},
		{Name: "customerId", Type: record.TypeString},
		{Name: "currency", Type: record.TypeString},
	}
	b := &record.RecordBatch{Columns: cols}
	for _, m := range rows {
		row := make([]interface{}, len(cols))
		for j, col := range cols {
			row[j] = m[col.Name]
		}
		b.Rows = append(b.Rows, row)
	}
	return b
}

func buildValidate(t *testing.T, cfg map[string]interface{}) transform.Transform {
	t.Helper()
	v, err := transform.New("validate", cfg)
	if err != nil {
		t.Fatalf("buildValidate: %v", err)
	}
	return v
}

// ─── notNull ──────────────────────────────────────────────────────────────────

func TestValidate_NotNull_Pass(t *testing.T) {
	v := buildValidate(t, map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"field": "amount", "notNull": true},
		},
		"onViolation": "drop",
	})
	in := txBatch([]map[string]interface{}{
		{"amount": 100.0, "customerId": "CUST-000001", "currency": "USD"},
	})
	out, err := v.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out == nil || out.Len() != 1 {
		t.Fatalf("expected 1 row, got %v", out)
	}
}

func TestValidate_NotNull_Fail_Drop(t *testing.T) {
	v := buildValidate(t, map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"field": "amount", "notNull": true},
		},
		"onViolation": "drop",
	})
	in := txBatch([]map[string]interface{}{
		{"amount": nil, "customerId": "CUST-000001", "currency": "USD"},
		{"amount": 50.0, "customerId": "CUST-000002", "currency": "EUR"},
	})
	out, err := v.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out == nil || out.Len() != 1 {
		t.Fatalf("expected 1 row after drop, got %v", out)
	}
}

// ─── min / max ────────────────────────────────────────────────────────────────

func TestValidate_Min(t *testing.T) {
	v := buildValidate(t, map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"field": "amount", "min": float64(0)},
		},
		"onViolation": "drop",
	})
	in := txBatch([]map[string]interface{}{
		{"amount": -1.0, "customerId": "C1", "currency": "USD"},
		{"amount": 0.0, "customerId": "C2", "currency": "USD"},
		{"amount": 10.0, "customerId": "C3", "currency": "USD"},
	})
	out, err := v.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 2 {
		t.Fatalf("expected 2 rows (amount >= 0), got %d", out.Len())
	}
}

func TestValidate_Max(t *testing.T) {
	v := buildValidate(t, map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"field": "amount", "max": float64(100)},
		},
		"onViolation": "drop",
	})
	in := txBatch([]map[string]interface{}{
		{"amount": 50.0, "customerId": "C1", "currency": "USD"},
		{"amount": 150.0, "customerId": "C2", "currency": "USD"},
	})
	out, err := v.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 1 {
		t.Fatalf("expected 1 row (amount <= 100), got %d", out.Len())
	}
}

// ─── minLen / maxLen ─────────────────────────────────────────────────────────

func TestValidate_MinLen(t *testing.T) {
	v := buildValidate(t, map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"field": "customerId", "minLength": float64(5)},
		},
		"onViolation": "drop",
	})
	in := txBatch([]map[string]interface{}{
		{"amount": 1.0, "customerId": "AB", "currency": "USD"},
		{"amount": 2.0, "customerId": "ABCDE", "currency": "USD"},
	})
	out, err := v.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 1 {
		t.Fatalf("expected 1 row, got %d", out.Len())
	}
}

func TestValidate_MaxLen(t *testing.T) {
	v := buildValidate(t, map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"field": "customerId", "maxLength": float64(4)},
		},
		"onViolation": "drop",
	})
	in := txBatch([]map[string]interface{}{
		{"amount": 1.0, "customerId": "AB", "currency": "USD"},
		{"amount": 2.0, "customerId": "ABCDE", "currency": "USD"},
	})
	out, err := v.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 1 {
		t.Fatalf("expected 1 row, got %d", out.Len())
	}
}

// ─── pattern ─────────────────────────────────────────────────────────────────

func TestValidate_Pattern(t *testing.T) {
	v := buildValidate(t, map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"field": "customerId", "pattern": `^CUST-\d{6}$`},
		},
		"onViolation": "drop",
	})
	in := txBatch([]map[string]interface{}{
		{"amount": 1.0, "customerId": "CUST-123456", "currency": "USD"},
		{"amount": 2.0, "customerId": "BAD-FORMAT", "currency": "USD"},
	})
	out, err := v.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 1 {
		t.Fatalf("expected 1 row, got %d", out.Len())
	}
	if out.Value(0, "customerId") != "CUST-123456" {
		t.Errorf("wrong row survived: %v", out.Value(0, "customerId"))
	}
}

// ─── inSet ────────────────────────────────────────────────────────────────────

func TestValidate_InSet(t *testing.T) {
	v := buildValidate(t, map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"field": "currency", "in": []interface{}{"USD", "EUR", "GBP"}},
		},
		"onViolation": "drop",
	})
	in := txBatch([]map[string]interface{}{
		{"amount": 1.0, "customerId": "C1", "currency": "USD"},
		{"amount": 2.0, "customerId": "C2", "currency": "JPY"},
		{"amount": 3.0, "customerId": "C3", "currency": "EUR"},
	})
	out, err := v.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 2 {
		t.Fatalf("expected 2 rows, got %d", out.Len())
	}
}

// ─── onViolation=route ────────────────────────────────────────────────────────

func TestValidate_OnViolation_Route(t *testing.T) {
	// Good rows go downstream; bad rows go to null sink (no error).
	v := buildValidate(t, map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"field": "amount", "notNull": true},
		},
		"onViolation":     "route",
		"errorSinkType":   "null",
		"errorSinkConfig": map[string]interface{}{},
	})
	in := txBatch([]map[string]interface{}{
		{"amount": nil, "customerId": "C1", "currency": "USD"},
		{"amount": 100.0, "customerId": "C2", "currency": "USD"},
	})
	out, err := v.Apply(in)
	if err != nil {
		t.Fatalf("Apply with route: %v", err)
	}
	if out == nil || out.Len() != 1 {
		t.Fatalf("expected 1 good row downstream, got %v", out)
	}
}

// ─── onViolation=fail ─────────────────────────────────────────────────────────

func TestValidate_OnViolation_Fail(t *testing.T) {
	v := buildValidate(t, map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"field": "amount", "notNull": true},
		},
		"onViolation": "fail",
	})
	in := txBatch([]map[string]interface{}{
		{"amount": nil, "customerId": "C1", "currency": "USD"},
	})
	_, err := v.Apply(in)
	if err == nil {
		t.Fatal("expected error for onViolation=fail, got nil")
	}
}

// ─── onViolation=drop ─────────────────────────────────────────────────────────

func TestValidate_OnViolation_Drop_Silent(t *testing.T) {
	v := buildValidate(t, map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"field": "amount", "notNull": true},
		},
		"onViolation": "drop",
	})
	in := txBatch([]map[string]interface{}{
		{"amount": nil, "customerId": "C1", "currency": "USD"},
		{"amount": nil, "customerId": "C2", "currency": "USD"},
	})
	// All rows violate → result should be nil (empty), no error.
	out, err := v.Apply(in)
	if err != nil {
		t.Fatalf("unexpected error for onViolation=drop: %v", err)
	}
	if out != nil {
		t.Errorf("expected nil result when all rows dropped, got batch with %d rows", out.Len())
	}
}

// ─── bad rules config error ───────────────────────────────────────────────────

func TestValidate_BadRulesConfig(t *testing.T) {
	_, err := transform.New("validate", map[string]interface{}{
		"rules": "not-an-array",
	})
	if err == nil {
		t.Fatal("expected error for non-array rules, got nil")
	}
}
