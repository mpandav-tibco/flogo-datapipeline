package kafka

import (
	"encoding/json"
	"testing"
)

// TestCursorSerializesPendingOffsets checks the offset cursor JSON that gets
// stored in the checkpoint token alongside the window state.
func TestCursorSerializesPendingOffsets(t *testing.T) {
	ks := &kafkaSource{
		cfg:     &kafkaSourceConfig{topic: "t"},
		pending: map[int32]int64{0: 42, 3: 7},
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

// TestCommitWithoutSessionIsNoOp ensures Commit is safe before any partition
// session exists (e.g. a checkpoint fires before the first message is read).
func TestCommitWithoutSessionIsNoOp(t *testing.T) {
	ks := &kafkaSource{cfg: &kafkaSourceConfig{topic: "t"}, pending: map[int32]int64{0: 1}}
	if err := ks.Commit(); err != nil {
		t.Errorf("Commit with no session should be nil, got %v", err)
	}
}
