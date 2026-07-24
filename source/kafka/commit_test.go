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

// TestPollTimeoutMsAlias verifies the poll window is read from the documented
// "pollTimeoutMs" key (and the "pollMs" alias), defaulting to 500.
func TestPollTimeoutMsAlias(t *testing.T) {
	base := map[string]interface{}{"brokers": "localhost:9092", "topic": "t", "groupId": "g"}
	for _, key := range []string{"pollTimeoutMs", "pollMs"} {
		cfg := map[string]interface{}{"brokers": "localhost:9092", "topic": "t", "groupId": "g", key: float64(1234)}
		kc, err := parseSourceConfig(cfg)
		if err != nil {
			t.Fatalf("%s: parse: %v", key, err)
		}
		if kc.pollMs != 1234 {
			t.Errorf("%s: pollMs = %d, want 1234", key, kc.pollMs)
		}
	}
	kc, err := parseSourceConfig(base)
	if err != nil {
		t.Fatalf("default: parse: %v", err)
	}
	if kc.pollMs != 500 {
		t.Errorf("default pollMs = %d, want 500", kc.pollMs)
	}
}

// TestParseResumeCursor verifies the checkpoint resume cursor decodes to
// per-partition next-offsets and that empty/absent cursors yield no seek.
func TestParseResumeCursor(t *testing.T) {
	m, err := parseResumeCursor(json.RawMessage(`{"0":100,"2":250}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(m) != 2 || m[0] != 100 || m[2] != 250 {
		t.Errorf("got %v, want {0:100, 2:250}", m)
	}
	// String form is also accepted.
	if m, err := parseResumeCursor(`{"1":5}`); err != nil || m[1] != 5 {
		t.Errorf("string form: got %v err %v", m, err)
	}
	// Nil, "null", empty, and empty-object cursors mean "no seek".
	for _, v := range []interface{}{nil, json.RawMessage("null"), json.RawMessage(""), "", []byte("{}")} {
		if mm, err := parseResumeCursor(v); err != nil || len(mm) != 0 {
			t.Errorf("empty-ish %v: got %v err %v, want empty/no-error", v, mm, err)
		}
	}
	// A non-numeric partition key is an error.
	if _, err := parseResumeCursor(json.RawMessage(`{"x":1}`)); err == nil {
		t.Error("expected error for non-numeric partition key")
	}
}
