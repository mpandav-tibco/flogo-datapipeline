package record

import "testing"

// ─── RecordBatch helpers ──────────────────────────────────────────────────────

func makeTestBatch() *RecordBatch {
	return &RecordBatch{
		Columns: []Column{
			{Name: "id", Type: TypeInt},
			{Name: "name", Type: TypeString},
			{Name: "amount", Type: TypeFloat},
		},
		Rows: [][]interface{}{
			{int64(1), "Alice", 10.5},
			{int64(2), "Bob", 20.0},
			{int64(3), "Carol", 30.75},
		},
	}
}

// ─── Len ─────────────────────────────────────────────────────────────────────

func TestLen_Normal(t *testing.T) {
	b := makeTestBatch()
	if got := b.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3", got)
	}
}

func TestLen_Empty(t *testing.T) {
	b := &RecordBatch{}
	if got := b.Len(); got != 0 {
		t.Fatalf("Len() on empty batch = %d, want 0", got)
	}
}

func TestLen_Nil(t *testing.T) {
	var b *RecordBatch
	if got := b.Len(); got != 0 {
		t.Fatalf("Len() on nil = %d, want 0", got)
	}
}

// ─── ColIndex ─────────────────────────────────────────────────────────────────

func TestColIndex_Found(t *testing.T) {
	b := makeTestBatch()
	if got := b.ColIndex("name"); got != 1 {
		t.Fatalf("ColIndex(name) = %d, want 1", got)
	}
}

func TestColIndex_NotFound(t *testing.T) {
	b := makeTestBatch()
	if got := b.ColIndex("nonexistent"); got != -1 {
		t.Fatalf("ColIndex(nonexistent) = %d, want -1", got)
	}
}

func TestColIndex_FirstAndLast(t *testing.T) {
	b := makeTestBatch()
	if got := b.ColIndex("id"); got != 0 {
		t.Fatalf("ColIndex(id) = %d, want 0", got)
	}
	if got := b.ColIndex("amount"); got != 2 {
		t.Fatalf("ColIndex(amount) = %d, want 2", got)
	}
}

// ─── Value ────────────────────────────────────────────────────────────────────

func TestValue_Valid(t *testing.T) {
	b := makeTestBatch()
	got := b.Value(1, "name")
	if got != "Bob" {
		t.Fatalf("Value(1, name) = %v, want Bob", got)
	}
}

func TestValue_NilOnMissingCol(t *testing.T) {
	b := makeTestBatch()
	if got := b.Value(0, "nope"); got != nil {
		t.Fatalf("Value(0, nope) = %v, want nil", got)
	}
}

func TestValue_NilOnOutOfBoundsRow(t *testing.T) {
	b := makeTestBatch()
	if got := b.Value(99, "name"); got != nil {
		t.Fatalf("Value(99, name) = %v, want nil", got)
	}
}

// ─── RowMap ───────────────────────────────────────────────────────────────────

func TestRowMap(t *testing.T) {
	b := makeTestBatch()
	m := b.RowMap(0)
	if m["id"] != int64(1) {
		t.Errorf("RowMap[id] = %v, want 1", m["id"])
	}
	if m["name"] != "Alice" {
		t.Errorf("RowMap[name] = %v, want Alice", m["name"])
	}
	if m["amount"] != 10.5 {
		t.Errorf("RowMap[amount] = %v, want 10.5", m["amount"])
	}
	if len(m) != 3 {
		t.Errorf("RowMap len = %d, want 3", len(m))
	}
}

func TestRowMap_LastRow(t *testing.T) {
	b := makeTestBatch()
	m := b.RowMap(2)
	if m["name"] != "Carol" {
		t.Errorf("RowMap[name] = %v, want Carol", m["name"])
	}
}

func TestRowMap_OutOfBoundsReturnsEmptyMap(t *testing.T) {
	b := makeTestBatch() // 3 rows
	for _, i := range []int{-1, 3, 99} {
		m := b.RowMap(i)
		if m == nil {
			t.Errorf("RowMap(%d) = nil, want empty (non-nil) map", i)
		}
		if len(m) != 0 {
			t.Errorf("RowMap(%d) = %v, want empty map", i, m)
		}
	}
}

// ─── Clone ────────────────────────────────────────────────────────────────────

func TestClone_SharesColumns(t *testing.T) {
	b := makeTestBatch()
	c := b.Clone(10)
	if len(c.Columns) != len(b.Columns) {
		t.Errorf("Clone column count mismatch: got %d, want %d", len(c.Columns), len(b.Columns))
	}
	if c.Columns[0].Name != "id" {
		t.Errorf("Clone column[0] = %v, want id", c.Columns[0].Name)
	}
}

func TestClone_IndependentRows(t *testing.T) {
	b := makeTestBatch()
	c := b.Clone(5)
	if len(c.Rows) != 0 {
		t.Errorf("Clone should start with 0 rows, got %d", len(c.Rows))
	}
	// Appending to clone must not affect source.
	c.Rows = append(c.Rows, b.Rows[0])
	if len(b.Rows) != 3 {
		t.Errorf("Source batch row count changed after clone append: %d", len(b.Rows))
	}
}

func TestClone_CapacityHint(t *testing.T) {
	b := makeTestBatch()
	c := b.Clone(100)
	if cap(c.Rows) < 100 {
		t.Errorf("Clone capacity %d < requested 100", cap(c.Rows))
	}
}

// ─── Pool Get/Put ─────────────────────────────────────────────────────────────

func TestPool_GetReturnsNonNil(t *testing.T) {
	b := Get()
	if b == nil {
		t.Fatal("pool.Get() returned nil")
	}
}

func TestPool_PutNilIsSafe(t *testing.T) {
	// Should not panic.
	Put(nil)
}

func TestPool_PutAndGetReuses(t *testing.T) {
	b := Get()
	b.Columns = []Column{{Name: "x", Type: TypeString}}
	b.Rows = append(b.Rows, []interface{}{"val"})
	Put(b) // returns to pool; columns and rows are cleared

	b2 := Get()
	// After Put the columns are nil and rows are empty.
	if b2.Columns != nil {
		t.Errorf("recycled batch should have nil Columns, got %v", b2.Columns)
	}
	if len(b2.Rows) != 0 {
		t.Errorf("recycled batch should have 0 rows, got %d", len(b2.Rows))
	}
}
