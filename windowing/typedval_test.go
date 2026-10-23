package windowing

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

func TestTypedValueRoundTrip(t *testing.T) {
	d, _ := record.ParseDecimal("123456789012345678.25")
	vals := []interface{}{
		nil, "s", `with "$int" text`, int64(9007199254740993), int(-7), int32(3), uint64(1 << 63),
		1.5, true, d, time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC),
		map[string]interface{}{"a": 1.0}, []interface{}{"x", 2.0},
	}
	b, err := json.Marshal(wrapVals(vals))
	if err != nil {
		t.Fatal(err)
	}
	var back []typedValue
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if got := unwrapVals(back); !reflect.DeepEqual(got, vals) {
		t.Fatalf("round trip changed values/types:\n got  %#v\n want %#v", got, vals)
	}
}

// Snapshots written before typed encoding hold plain JSON scalars; they must
// still restore (numbers as float64, as before).
func TestRestoreLegacySnapshot(t *testing.T) {
	legacy := `{"version":1,"maxEventTime":150,"hasEvent":true,"watermark":0,"keys":[{"key":["a",7],"windows":[{"start":100,"end":200,"accs":[{"n":2},{"v":42,"t":true}]}]}]}`
	o, _ := New(Spec{Kind: Tumbling, SizeMs: 100}, []AggSpec{{As: "n", Fn: "count"}, {As: "f", Fn: "first", Column: "v"}}, 0)
	if err := o.Restore([]byte(legacy)); err != nil {
		t.Fatal(err)
	}
	res := o.Flush()
	if len(res) != 1 || res[0].Key[1] != 7.0 || res[0].Aggs["f"] != 42.0 || res[0].Aggs["n"] != int64(2) {
		t.Fatalf("legacy restore = %#v", res)
	}
}

// Integer keys and first/last values must keep their Go type (and full
// precision) whether a key stayed resident, was spilled, or was restored.
func TestSpillAndRestorePreserveTypes(t *testing.T) {
	big := int64(9007199254740993) // 2^53+1: not representable as float64
	aggs := []AggSpec{{As: "f", Fn: "first", Column: "v"}}
	o, _ := New(Spec{Kind: Tumbling, SizeMs: 100}, aggs, 0)
	if err := o.UseSpillableState(t.TempDir(), 1); err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	o.Add([]interface{}{big}, 10, map[string]interface{}{"v": int64(5)})
	o.Add([]interface{}{big + 1}, 10, map[string]interface{}{"v": int64(6)}) // spills key big
	snap, err := o.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	r, _ := New(Spec{Kind: Tumbling, SizeMs: 100}, aggs, 0)
	if err := r.Restore(snap); err != nil {
		t.Fatal(err)
	}
	for name, res := range map[string][]Result{"spilled": o.Flush(), "restored": r.Flush()} {
		if len(res) != 2 {
			t.Fatalf("%s: want 2 distinct keys, got %d: %v", name, len(res), res)
		}
		for _, x := range res {
			if _, ok := x.Key[0].(int64); !ok {
				t.Errorf("%s: key type %T, want int64", name, x.Key[0])
			}
			if _, ok := x.Aggs["f"].(int64); !ok {
				t.Errorf("%s: first() type %T, want int64", name, x.Aggs["f"])
			}
		}
	}
}
