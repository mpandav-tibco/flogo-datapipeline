package jdbc

import "testing"

func TestDedupUpsertRows_KeepsLastOccurrence(t *testing.T) {
	s := &jdbcSink{
		columns:     []string{"id", "status"},
		conflictKey: []string{"id"},
		onConflict:  conflictUpsert,
	}
	rows := [][]interface{}{
		{int64(1), "open"},
		{int64(2), "open"},
		{int64(1), "vip"}, // second change to id=1 — must win
	}
	got := s.dedupUpsertRows(rows)
	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2 (deduped by id): %+v", len(got), got)
	}
	byID := map[int64]string{}
	for _, r := range got {
		byID[r[0].(int64)] = r[1].(string)
	}
	if byID[1] != "vip" {
		t.Errorf("id=1 status = %q, want vip (the LAST occurrence must win)", byID[1])
	}
	if byID[2] != "open" {
		t.Errorf("id=2 status = %q, want open", byID[2])
	}
}

func TestDedupUpsertRows_NoDuplicatesIsNoOp(t *testing.T) {
	s := &jdbcSink{
		columns:     []string{"id", "status"},
		conflictKey: []string{"id"},
		onConflict:  conflictUpsert,
	}
	rows := [][]interface{}{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}}
	got := s.dedupUpsertRows(rows)
	if len(got) != 3 {
		t.Fatalf("got %d rows, want 3 (no dedup needed)", len(got))
	}
}

func TestDedupUpsertRows_CompositeKey(t *testing.T) {
	s := &jdbcSink{
		columns:     []string{"order_id", "line_no", "qty"},
		conflictKey: []string{"order_id", "line_no"},
		onConflict:  conflictUpsert,
	}
	rows := [][]interface{}{
		{int64(1), int64(1), int64(5)},
		{int64(1), int64(2), int64(5)},
		{int64(1), int64(1), int64(9)}, // same (order_id,line_no) as row 0 — must win
	}
	got := s.dedupUpsertRows(rows)
	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2: %+v", len(got), got)
	}
	for _, r := range got {
		if r[0].(int64) == 1 && r[1].(int64) == 1 && r[2].(int64) != 9 {
			t.Errorf("(order_id=1,line_no=1) qty = %v, want 9 (last write must win)", r[2])
		}
	}
}

// TestDedupUpsertRows_NotAppliedToIgnoreOrInsert proves the dedup is scoped to
// "upsert" only: Postgres's "cannot affect row a second time" restriction is
// specific to ON CONFLICT DO UPDATE. "ignore" (DO NOTHING) and plain "insert"
// have no such restriction, and silently dropping a legitimate duplicate-key
// error for "insert" mode would hide a real data problem.
func TestDedupUpsertRows_NotAppliedToIgnoreOrInsert(t *testing.T) {
	rows := [][]interface{}{{int64(1), "a"}, {int64(1), "b"}}
	for _, mode := range []conflictStrategy{conflictIgnore, conflictInsert} {
		s := &jdbcSink{columns: []string{"id", "v"}, conflictKey: []string{"id"}, onConflict: mode}
		if got := s.dedupUpsertRows(rows); len(got) != 2 {
			t.Errorf("onConflict=%s: got %d rows, want 2 (unchanged — dedup must not apply)", mode, len(got))
		}
	}
}

func TestDedupUpsertRows_NoConflictKeyIsNoOp(t *testing.T) {
	s := &jdbcSink{columns: []string{"id", "v"}, onConflict: conflictUpsert}
	rows := [][]interface{}{{int64(1), "a"}, {int64(1), "b"}}
	if got := s.dedupUpsertRows(rows); len(got) != 2 {
		t.Errorf("got %d rows, want 2 (no conflictKey configured — dedup can't apply)", len(got))
	}
}

func TestJdbcRowKeyString_NoFalseCollisionAcrossColumnBoundary(t *testing.T) {
	// {"ab","c"} and {"a","bc"} must hash differently even though a naive
	// concatenation ("ab"+"c" == "a"+"bc" == "abc") would collide.
	k1 := jdbcRowKeyString([]interface{}{"ab", "c"}, []int{0, 1})
	k2 := jdbcRowKeyString([]interface{}{"a", "bc"}, []int{0, 1})
	if k1 == k2 {
		t.Errorf("jdbcRowKeyString collided across a column boundary: %q == %q", k1, k2)
	}
}
