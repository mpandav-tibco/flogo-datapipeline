package integration

import (
	"fmt"
	"io"
	"sort"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
	"github.com/mpandav-tibco/flogo-datapipeline/transform/window"

	_ "github.com/mpandav-tibco/flogo-datapipeline/source/csv"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/join"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/window"
)

const joinOrdersCSV = `orderId,customerId,amount
1,C1,100
2,C2,50
3,C9,20
4,C1,75
`

const joinCustomersCSV = `id,name,tier
C1,Alice,gold
C2,Bob,silver
`

// TestJoin_Inner_And_Left drives a streaming left source (orders) through the
// join transform against a source-loaded right reference table (customers),
// covering inner (drop unmatched) and left (keep unmatched with null right)
// semantics, including the many rows → same customer case.
func TestJoin_Inner_And_Left(t *testing.T) {
	rightPath, cleanupR := writeTempCSV(t, joinCustomersCSV)
	defer cleanupR()

	run := func(joinType string) []string {
		leftPath, cleanupL := writeTempCSV(t, joinOrdersCSV)
		defer cleanupL()
		src, err := source.New("csv", map[string]interface{}{"path": leftPath, "hasHeader": true})
		if err != nil {
			t.Fatalf("source.New(csv left): %v", err)
		}
		defer src.Close()

		p, err := transform.Build([]map[string]interface{}{
			{
				"type":         "join",
				"leftKey":      "customerId",
				"rightKey":     "id",
				"joinType":     joinType,
				"right":        map[string]interface{}{"type": "csv", "path": rightPath, "hasHeader": true},
				"rightColumns": []interface{}{"name", "tier"},
			},
		})
		if err != nil {
			t.Fatalf("transform.Build(join): %v", err)
		}
		defer p.Close()

		var got []string
		for {
			b, rErr := src.ReadBatch(1000)
			if b != nil && b.Len() > 0 {
				res, aErr := p.Apply(b)
				if aErr != nil {
					t.Fatalf("join Apply: %v", aErr)
				}
				for i := 0; i < res.Len(); i++ {
					m := res.RowMap(i)
					got = append(got, fmt.Sprintf("%v|%v|%v", m["orderId"], m["name"], m["tier"]))
				}
			}
			if rErr == io.EOF {
				break
			}
			if rErr != nil {
				t.Fatalf("ReadBatch: %v", rErr)
			}
		}
		sort.Strings(got)
		return got
	}

	inner := run("inner")
	wantInner := []string{"1|Alice|gold", "2|Bob|silver", "4|Alice|gold"} // order 3 (C9) dropped
	if !eqStrings(inner, wantInner) {
		t.Errorf("inner join = %v, want %v", inner, wantInner)
	}

	left := run("left")
	wantLeft := []string{"1|Alice|gold", "2|Bob|silver", "3|<nil>|<nil>", "4|Alice|gold"} // order 3 kept, null right
	if !eqStrings(left, wantLeft) {
		t.Errorf("left join = %v, want %v", left, wantLeft)
	}
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

const joinWindowOrdersCSV = `orderId,customerId,ts,amount
1,C1,2026-07-24T10:00:05Z,100
2,C2,2026-07-24T10:00:35Z,50
3,C1,2026-07-24T10:00:50Z,75
4,C2,2026-07-24T10:01:10Z,20
`

// TestJoin_Then_Window proves a *windowed keyed join*: join the order stream to
// the customer reference table (adding tier), then window + aggregate the joined
// stream by tier — the composition the design calls a "windowed keyed join".
func TestJoin_Then_Window(t *testing.T) {
	rightPath, cleanupR := writeTempCSV(t, joinCustomersCSV)
	defer cleanupR()
	leftPath, cleanupL := writeTempCSV(t, joinWindowOrdersCSV)
	defer cleanupL()

	src, err := source.New("csv", map[string]interface{}{"path": leftPath, "hasHeader": true})
	if err != nil {
		t.Fatalf("source.New(csv): %v", err)
	}
	defer src.Close()

	p, err := transform.Build([]map[string]interface{}{
		{
			"type": "join", "leftKey": "customerId", "rightKey": "id", "joinType": "inner",
			"right":        map[string]interface{}{"type": "csv", "path": rightPath, "hasHeader": true},
			"rightColumns": []interface{}{"tier"},
		},
		{
			"type":      "window",
			"eventTime": map[string]interface{}{"column": "ts", "format": "RFC3339"},
			"window":    map[string]interface{}{"kind": "tumbling", "sizeMs": float64(60000)},
			"groupBy":   []interface{}{"tier"},
			"aggregations": []interface{}{
				map[string]interface{}{"as": "total", "fn": "sum", "column": "amount"},
				map[string]interface{}{"as": "n", "fn": "count"},
			},
		},
	})
	if err != nil {
		t.Fatalf("transform.Build(join+window): %v", err)
	}
	defer p.Close()

	var got []string
	collect := func(b *record.RecordBatch) {
		if b == nil {
			return
		}
		for i := 0; i < b.Len(); i++ {
			m := b.RowMap(i)
			total, _ := toF(m["total"])
			n, _ := toI(m["n"])
			got = append(got, fmt.Sprintf("%v|%.0f|%d", m["tier"], total, n))
		}
	}
	for {
		b, rErr := src.ReadBatch(1000)
		if b != nil && b.Len() > 0 {
			res, aErr := p.Apply(b)
			if aErr != nil {
				t.Fatalf("join+window Apply: %v", aErr)
			}
			collect(res)
		}
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			t.Fatalf("ReadBatch: %v", rErr)
		}
	}
	winOp, ok := p.Stages()[1].(window.WindowedOperator)
	if !ok {
		t.Fatalf("stage 1 is not a WindowedOperator")
	}
	flushed, _ := winOp.Flush()
	collect(flushed)

	sort.Strings(got)
	want := []string{"gold|175|2", "silver|20|1", "silver|50|1"} // gold=C1(100+75); silver=C2 10:00(50),10:01(20)
	if !eqStrings(got, want) {
		t.Errorf("windowed keyed join = %v, want %v", got, want)
	}
}

// TestJoin_EmptyRightKeepsSchema: with rightColumns configured, an empty right
// reference table must still add those (null) columns — the output schema must
// not depend on whether the reference has rows (important for a left join).
func TestJoin_EmptyRightKeepsSchema(t *testing.T) {
	rightPath, cleanupR := writeTempCSV(t, "id,name,tier\n") // header only, no rows
	defer cleanupR()
	leftPath, cleanupL := writeTempCSV(t, joinOrdersCSV)
	defer cleanupL()

	src, err := source.New("csv", map[string]interface{}{"path": leftPath, "hasHeader": true})
	if err != nil {
		t.Fatalf("source.New(csv): %v", err)
	}
	defer src.Close()

	p, err := transform.Build([]map[string]interface{}{
		{
			"type": "join", "leftKey": "customerId", "rightKey": "id", "joinType": "left",
			"right":        map[string]interface{}{"type": "csv", "path": rightPath, "hasHeader": true},
			"rightColumns": []interface{}{"name", "tier"},
		},
	})
	if err != nil {
		t.Fatalf("transform.Build(join): %v", err)
	}
	defer p.Close()

	rows, withNameCol := 0, 0
	for {
		b, rErr := src.ReadBatch(1000)
		if b != nil && b.Len() > 0 {
			res, aErr := p.Apply(b)
			if aErr != nil {
				t.Fatalf("join Apply: %v", aErr)
			}
			for i := 0; i < res.Len(); i++ {
				rows++
				if _, ok := res.RowMap(i)["name"]; ok {
					withNameCol++
				}
			}
		}
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			t.Fatalf("ReadBatch: %v", rErr)
		}
	}
	if rows != 4 {
		t.Errorf("left join with empty right: %d rows, want 4", rows)
	}
	if withNameCol != 4 {
		t.Errorf("configured right column 'name' missing from %d/%d output rows", rows-withNameCol, rows)
	}
}
