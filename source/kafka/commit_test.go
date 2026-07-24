package kafka

import (
	"encoding/json"
	"testing"
)

// TestCursorReturnsCommitTarget checks the offset cursor JSON (the last committed
// = processed position) stored in the checkpoint token alongside window state.
func TestCursorReturnsCommitTarget(t *testing.T) {
	ks := &kafkaSource{
		cfg:          &kafkaSourceConfig{topic: "t"},
		commitTarget: map[int32]int64{0: 42, 3: 7},
	}
	raw := ks.Cursor()
	var m map[string]int64
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("cursor not valid JSON: %v (%s)", err, raw)
	}
	if m["0"] != 42 || m["3"] != 7 {
		t.Errorf("cursor offsets wrong: %v", m)
	}
}

// TestTargetForRows verifies the read-ahead-safe mapping from the pipeline's
// processed-row boundary to the exact offset to commit (and how many marks prune).
func TestTargetForRows(t *testing.T) {
	marks := []offMark{
		{row: 10, offsets: map[int32]int64{0: 10}},
		{row: 20, offsets: map[int32]int64{0: 20}},
		{row: 30, offsets: map[int32]int64{0: 30}},
	}
	if target, keep := targetForRows(marks, 20); target[0] != 20 || keep != 2 {
		t.Errorf("rows=20 → target=%v keep=%d, want {0:20} keep=2", target, keep)
	}
	if target, keep := targetForRows(marks, 5); target != nil || keep != 0 {
		t.Errorf("rows=5 → target=%v keep=%d, want nil/0", target, keep)
	}
	if target, keep := targetForRows(marks, 1<<62); target[0] != 30 || keep != 3 {
		t.Errorf("rows=huge → target=%v keep=%d, want {0:30} keep=3", target, keep)
	}
}

// TestCommitWithoutSessionIsNoOp ensures Commit is safe before any partition
// session exists (e.g. a checkpoint fires before the first message is read).
func TestCommitWithoutSessionIsNoOp(t *testing.T) {
	ks := &kafkaSource{
		cfg:        &kafkaSourceConfig{topic: "t"},
		commitMode: "checkpoint",
		marks:      []offMark{{row: 1, offsets: map[int32]int64{0: 1}}},
	}
	if err := ks.Commit(1); err != nil {
		t.Errorf("Commit with no session should be nil, got %v", err)
	}
}
