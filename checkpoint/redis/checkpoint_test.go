package redis

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/checkpoint"
)

// TestRedisCheckpoint_SaveLoadDelete round-trips a token (payload + window state)
// through a live Redis. Skipped unless DP_TEST_REDIS_ADDR is set.
func TestRedisCheckpoint_SaveLoadDelete(t *testing.T) {
	addr := os.Getenv("DP_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set DP_TEST_REDIS_ADDR to run the Redis checkpoint test")
	}
	// Unique prefix per run → full isolation from any prior run's keys.
	prefix := fmt.Sprintf("flogo:test:ckpt:%d", time.Now().UnixNano())
	ckpt, err := checkpoint.New("redis", map[string]interface{}{"addr": addr, "keyPrefix": prefix})
	if err != nil {
		t.Fatalf("checkpoint.New(redis): %v", err)
	}
	defer ckpt.Close()

	tok := &checkpoint.Token{
		Format:        "csv",
		RowsProcessed: 100,
		Payload:       json.RawMessage(`{"offset":4096}`),
		WindowState:   json.RawMessage(`{"w":1}`),
	}
	if err := ckpt.Save("part-0", tok); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := ckpt.Load("part-0")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got == nil {
		t.Fatal("Load returned nil after Save")
	}
	if got.RowsProcessed != 100 || got.Format != "csv" {
		t.Fatalf("token round-trip mismatch: %+v", got)
	}
	if string(got.Payload) != `{"offset":4096}` {
		t.Fatalf("payload round-trip = %s, want {\"offset\":4096}", got.Payload)
	}
	if string(got.WindowState) != `{"w":1}` {
		t.Fatalf("windowState round-trip = %s, want {\"w\":1}", got.WindowState)
	}

	// Unknown partition → (nil, nil).
	if miss, err := ckpt.Load("no-such-part"); err != nil || miss != nil {
		t.Fatalf("Load(unknown) = %+v,%v want nil,nil", miss, err)
	}

	// Delete → Load returns nil.
	if err := ckpt.Delete("part-0"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if after, err := ckpt.Load("part-0"); err != nil || after != nil {
		t.Fatalf("Load after Delete = %+v,%v want nil,nil", after, err)
	}
}
