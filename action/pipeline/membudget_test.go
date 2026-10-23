package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

func TestMemBudget_BlocksUntilRelease(t *testing.T) {
	b := newMemBudget(100)
	b.acquire(100)
	done := make(chan bool)
	go func() { done <- b.waitForRoom(context.Background()) }()
	select {
	case <-done:
		t.Fatal("waitForRoom returned while the budget was full")
	case <-time.After(50 * time.Millisecond):
	}
	b.release(60)
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("waitForRoom = false after release")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitForRoom not woken by release")
	}
}

func TestMemBudget_CancelWakesWaiter(t *testing.T) {
	b := newMemBudget(1)
	b.acquire(1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() { done <- b.waitForRoom(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("waitForRoom = true after cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not wake the waiter")
	}
}

func TestMemBudget_DisabledNeverBlocks(t *testing.T) {
	b := newMemBudget(0)
	b.acquire(1 << 40)
	if !b.waitForRoom(context.Background()) {
		t.Fatal("disabled budget blocked")
	}
}

func TestBatchBytes_ScalesWithValueSize(t *testing.T) {
	mk := func(s string, n int) *record.RecordBatch {
		b := &record.RecordBatch{Columns: []record.Column{{Name: "a"}, {Name: "b"}}}
		for i := 0; i < n; i++ {
			b.Rows = append(b.Rows, []interface{}{int64(i), s})
		}
		return b
	}
	small, wide := batchBytes(mk("x", 1000)), batchBytes(mk(strings.Repeat("x", 4096), 1000))
	if wide < 4096*1000 || wide < 50*small {
		t.Fatalf("estimate ignores string size: small=%d wide=%d", small, wide)
	}
	if got := batchBytes(mk("x", 2000)); got < 19*small/10 || got > 21*small/10 {
		t.Fatalf("estimate not proportional to rows: 1000 rows=%d 2000 rows=%d", small, got)
	}
	if batchBytes(nil) != 0 || batchBytes(&record.RecordBatch{}) != 0 {
		t.Fatal("empty batch must be 0")
	}
}
