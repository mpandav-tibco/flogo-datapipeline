package postgres

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/watermark"
)

// TestPostgresWatermark_SaveLoad round-trips a high-water mark through a live
// Postgres (the store auto-creates flogo_pipeline_watermarks). Skipped unless
// DP_TEST_PG_DSN is set.
func TestPostgresWatermark_SaveLoad(t *testing.T) {
	dsn := os.Getenv("DP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_PG_DSN to run the Postgres watermark test")
	}
	store, err := watermark.New("postgres", map[string]interface{}{"dsn": dsn})
	if err != nil {
		t.Fatalf("watermark.New(postgres): %v", err)
	}
	defer store.Close()

	// Unique pipeline id per run so the row never collides across runs.
	pipeID := fmt.Sprintf("res://pipeline:pg-wm-%d", time.Now().UnixNano())

	if m, err := store.Load(pipeID); err != nil || m != nil {
		t.Fatalf("Load(fresh) = %+v,%v want nil,nil", m, err)
	}

	mark := &watermark.Mark{
		Column:    "seq_id",
		Value:     "42",
		ValueType: "int",
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
	if got.Column != "seq_id" || got.Value != "42" || got.ValueType != "int" {
		t.Fatalf("mark round-trip mismatch: %+v", got)
	}

	// Upsert: saving again for the same pipeline updates in place.
	mark.Value = "99"
	if err := store.Save(pipeID, mark); err != nil {
		t.Fatalf("Save(update): %v", err)
	}
	if got, _ := store.Load(pipeID); got == nil || got.Value != "99" {
		t.Fatalf("mark not upserted: %+v", got)
	}
}
