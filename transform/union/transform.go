// Package union registers a UNION transform that appends one or more bounded
// secondary datasets (the `with` sources) to the primary stream, producing a
// single same-schema stream (N→1) — the map-side equivalent of SQL UNION ALL.
//
// The primary stream flows through unchanged; each `with` source (CSV / JDBC /
// Parquet / a Kafka batch drain / …) is read fully into memory and emitted with
// its columns aligned to the primary schema by name. Because the secondary
// sources are buffered, they MUST be bounded (a Kafka `with` source is auto-
// drained to its current end); loading more than `maxRows` rows in total is a
// hard error.
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/transform/union"
//
// Config:
//
//	{
//	  "type": "union",
//	  "with": [
//	    { "type": "csv", "path": "more-orders.csv", "hasHeader": true },
//	    { "type": "jdbc", "dsn": "...", "query": "SELECT ... FROM archive_orders" }
//	  ],
//	  "maxRows": 1000000   // cap on total buffered secondary rows (default 1,000,000)
//	}
//
// Semantics: UNION ALL (no de-duplication). Secondary rows are emitted on the
// first primary batch (union order is not significant). A column present in the
// primary schema but absent from a secondary row is filled with null; secondary
// columns not in the primary schema are ignored (the output keeps the primary
// schema). NOTE: the secondary rows ride along with the first primary batch, so
// if the primary source produces no rows at all they are not emitted — size the
// primary as the driving stream.
package union

import (
	"fmt"
	"io"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func init() {
	transform.Register("union", newUnion)
}

type unionTransform struct {
	extra   []map[string]interface{} // buffered secondary rows (column name → value)
	emitted bool
}

func newUnion(cfg map[string]interface{}) (transform.Transform, error) {
	withRaw, ok := cfg["with"].([]interface{})
	if !ok || len(withRaw) == 0 {
		return nil, fmt.Errorf("union transform: 'with' must be a non-empty array of source configs")
	}

	maxRows := int64(1_000_000)
	switch v := cfg["maxRows"].(type) {
	case float64:
		if v > 0 {
			maxRows = int64(v)
		}
	case int:
		if v > 0 {
			maxRows = int64(v)
		}
	case int64:
		if v > 0 {
			maxRows = v
		}
	}

	var buffered []map[string]interface{}
	var loaded int64
	for i, w := range withRaw {
		wmap, ok := w.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("union transform: with[%d] must be a source-config object", i)
		}
		wtype, _ := wmap["type"].(string)
		if wtype == "" {
			return nil, fmt.Errorf("union transform: with[%d].type is required", i)
		}
		// A Kafka secondary source is drained to its current end so it terminates
		// instead of blocking forever on a live topic.
		if wtype == "kafka" {
			if _, has := wmap["mode"]; !has {
				wmap["mode"] = "batch"
			}
			_, hasEnd := wmap["stopAtCurrentEnd"]
			_, hasOff := wmap["stopOffset"]
			_, hasTS := wmap["stopTimestamp"]
			if !hasEnd && !hasOff && !hasTS {
				wmap["stopAtCurrentEnd"] = true
			}
		}

		s, err := source.New(wtype, wmap)
		if err != nil {
			return nil, fmt.Errorf("union transform: open with[%d] source: %w", i, err)
		}
		for {
			b, rErr := s.ReadBatch(1000)
			if b != nil && b.Len() > 0 {
				if loaded+int64(b.Len()) > maxRows {
					_ = s.Close()
					return nil, fmt.Errorf("union transform: secondary sources exceeded maxRows=%d — they must be bounded (raise 'maxRows' or use bounded sources)", maxRows)
				}
				for r := 0; r < b.Len(); r++ {
					buffered = append(buffered, b.RowMap(r))
				}
				loaded += int64(b.Len())
			}
			if rErr == io.EOF {
				break
			}
			if rErr != nil {
				_ = s.Close()
				return nil, fmt.Errorf("union transform: read with[%d] source: %w", i, rErr)
			}
		}
		_ = s.Close()
	}

	return &unionTransform{extra: buffered}, nil
}

func (u *unionTransform) Name() string { return "union" }

func (u *unionTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	if in == nil {
		return nil, nil
	}
	// Once the secondary rows have been emitted (or there are none), the primary
	// stream passes straight through.
	if u.emitted || len(u.extra) == 0 {
		return in, nil
	}
	u.emitted = true

	out := record.Get()
	out.Columns = append([]record.Column(nil), in.Columns...)
	out.Rows = make([][]interface{}, 0, len(u.extra)+in.Len())

	// Secondary rows first, aligned to the primary schema by column name.
	for _, m := range u.extra {
		row := make([]interface{}, len(in.Columns))
		for c, col := range in.Columns {
			row[c] = m[col.Name]
		}
		out.Rows = append(out.Rows, row)
	}
	// Then the primary batch — copied so the rows survive the input batch being
	// recycled to the pool after this stage returns.
	for _, r := range in.Rows {
		row := make([]interface{}, len(r))
		copy(row, r)
		out.Rows = append(out.Rows, row)
	}

	u.extra = nil // free the buffer
	return out, nil
}

func (u *unionTransform) Close() error { return nil }
