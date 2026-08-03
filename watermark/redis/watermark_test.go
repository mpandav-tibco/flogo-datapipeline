package redis

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/watermark"
)

// TestRedisWatermark_SaveLoad round-trips a high-water mark through a live Redis.
// Skipped unless DP_TEST_REDIS_ADDR is set.
func TestRedisWatermark_SaveLoad(t *testing.T) {
	addr := os.Getenv("DP_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set DP_TEST_REDIS_ADDR to run the Redis watermark test")
	}
	prefix := fmt.Sprintf("flogo:test:wm:%d", time.Now().UnixNano())
	store, err := watermark.New("redis", map[string]interface{}{"addr": addr, "keyPrefix": prefix})
	if err != nil {
		t.Fatalf("watermark.New(redis): %v", err)
	}
	defer store.Close()

	pipeID := "res://pipeline:orders"
	// No mark yet → (nil, nil).
	if m, err := store.Load(pipeID); err != nil || m != nil {
		t.Fatalf("Load(fresh) = %+v,%v want nil,nil", m, err)
	}

	mark := &watermark.Mark{
		Column:    "updated_at",
		Value:     "2026-01-15T09:30:00Z",
		ValueType: "datetime",
		SavedAt:   time.Now().UTC(),
	}
	if err := store.Save(pipeID, mark); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := store.Load(pipeID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got == nil {
		t.Fatal("Load returned nil after Save")
	}
	if got.Column != "updated_at" || got.Value != "2026-01-15T09:30:00Z" || got.ValueType != "datetime" {
		t.Fatalf("mark round-trip mismatch: %+v", got)
	}

	// Overwrite with a newer value.
	mark.Value = "2026-02-01T00:00:00Z"
	if err := store.Save(pipeID, mark); err != nil {
		t.Fatalf("Save(update): %v", err)
	}
	if got, _ := store.Load(pipeID); got == nil || got.Value != "2026-02-01T00:00:00Z" {
		t.Fatalf("mark not updated: %+v", got)
	}
}
