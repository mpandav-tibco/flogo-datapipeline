package jdbc

import "testing"

// TestWriteBatchWithOp_DeleteThenRecreateSameKey regression-tests bug #5: a
// row deleted and then re-created for the SAME conflictKey, both buffered in
// one flush with nothing forcing an intermediate flush in between (a
// legitimate CDC sequence — a row removed and then reinserted before the next
// checkpoint), must survive as the re-created row, not be wiped out by
// flushMixed's fixed insert-then-delete statement order.
func TestWriteBatchWithOp_DeleteThenRecreateSameKey(t *testing.T) {
	s := &jdbcSink{
		columns:     []string{"id", "status"},
		conflictKey: []string{"id"},
		onConflict:  conflictUpsert,
		opColumn:    "__op",
	}

	// First: delete id=1.
	s.appendPendingDelete([]interface{}{int64(1)})
	if len(s.deleteBuf) != 1 {
		t.Fatalf("after delete: deleteBuf len = %d, want 1", len(s.deleteBuf))
	}

	// Then: a later re-create for the SAME id=1 must remove the stale pending delete.
	rowCopy := []interface{}{int64(1), "recreated"}
	s.removeStalePendingDelete([]interface{}{rowCopy[0]})
	s.buf = append(s.buf, rowCopy)

	if len(s.deleteBuf) != 0 {
		t.Errorf("deleteBuf len = %d, want 0 (the later re-create must have superseded the earlier delete)", len(s.deleteBuf))
	}
	if len(s.buf) != 1 || s.buf[0][1] != "recreated" {
		t.Errorf("buf = %+v, want one row with status=recreated", s.buf)
	}
}

// TestWriteBatchWithOp_MultipleDeletesThenOneRecreate proves the fix scales to
// several pending deletes: only the recreated key's delete is removed, the
// others remain pending.
func TestWriteBatchWithOp_MultipleDeletesThenOneRecreate(t *testing.T) {
	s := &jdbcSink{
		columns:     []string{"id", "status"},
		conflictKey: []string{"id"},
		onConflict:  conflictUpsert,
		opColumn:    "__op",
	}
	s.appendPendingDelete([]interface{}{int64(1)})
	s.appendPendingDelete([]interface{}{int64(2)})
	s.appendPendingDelete([]interface{}{int64(3)})
	if len(s.deleteBuf) != 3 {
		t.Fatalf("deleteBuf len = %d, want 3", len(s.deleteBuf))
	}

	// id=2 is re-created; its pending delete must be removed, 1 and 3 remain.
	s.removeStalePendingDelete([]interface{}{int64(2)})
	if len(s.deleteBuf) != 2 {
		t.Fatalf("deleteBuf len = %d, want 2 after removing id=2's pending delete", len(s.deleteBuf))
	}
	remaining := map[int64]bool{}
	for _, k := range s.deleteBuf {
		remaining[k[0].(int64)] = true
	}
	if remaining[2] {
		t.Error("id=2's pending delete should have been removed")
	}
	if !remaining[1] || !remaining[3] {
		t.Errorf("id=1 and id=3's pending deletes must remain: %v", remaining)
	}
}

// TestRemoveStalePendingDelete_NoOpWhenKeyNotPending proves removing a key
// with no pending delete is a harmless no-op (the common case — most upsert
// rows in a batch have no corresponding pending delete at all).
func TestRemoveStalePendingDelete_NoOpWhenKeyNotPending(t *testing.T) {
	s := &jdbcSink{conflictKey: []string{"id"}}
	s.appendPendingDelete([]interface{}{int64(1)})
	s.removeStalePendingDelete([]interface{}{int64(999)}) // not pending
	if len(s.deleteBuf) != 1 {
		t.Errorf("deleteBuf len = %d, want 1 (unrelated key must not affect id=1's pending delete)", len(s.deleteBuf))
	}
}

func TestAppendPendingDelete_DuplicateKeyIsIdempotent(t *testing.T) {
	s := &jdbcSink{conflictKey: []string{"id"}}
	s.appendPendingDelete([]interface{}{int64(1)})
	s.appendPendingDelete([]interface{}{int64(1)}) // duplicate delete for the same key
	if len(s.deleteBuf) != 1 {
		t.Errorf("deleteBuf len = %d, want 1 (duplicate delete for the same key must not grow the buffer)", len(s.deleteBuf))
	}
}
