package pipeline

import (
	"strings"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

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
