package windowing

import (
	"bytes"
	"encoding/json"
	"strconv"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// typedValue carries an arbitrary cell value through JSON (spill files and
// checkpoint snapshots) without losing its Go type. Plain JSON would decode
// every number as float64, so an int64 group-by key or first/last value came
// back as float64 after a spill or restore — changing output types and, above
// 2^53, collapsing distinct integer keys onto one. Integers, Decimals and
// times are written as a small tagged object; every other value is written as
// plain JSON, so snapshots written before this encoding still decode exactly
// as they used to.
type typedValue struct{ v interface{} }

type taggedJSON struct {
	Int  *string `json:"$int,omitempty"`  // decimal digits of a signed integer
	Kind string  `json:"$k,omitempty"`    // Go integer kind: int, int32 (default int64)
	Uint *string `json:"$uint,omitempty"` // decimal digits of a uint64
	Dec  *string `json:"$dec,omitempty"`  // record.Decimal string form
	Time *string `json:"$time,omitempty"` // RFC 3339 with nanoseconds
}

func (t typedValue) MarshalJSON() ([]byte, error) {
	str := func(s string) *string { return &s }
	switch v := t.v.(type) {
	case int64:
		return json.Marshal(taggedJSON{Int: str(strconv.FormatInt(v, 10))})
	case int:
		return json.Marshal(taggedJSON{Int: str(strconv.Itoa(v)), Kind: "int"})
	case int32:
		return json.Marshal(taggedJSON{Int: str(strconv.FormatInt(int64(v), 10)), Kind: "int32"})
	case uint64:
		return json.Marshal(taggedJSON{Uint: str(strconv.FormatUint(v, 10))})
	case record.Decimal:
		return json.Marshal(taggedJSON{Dec: str(v.String())})
	case time.Time:
		return json.Marshal(taggedJSON{Time: str(v.Format(time.RFC3339Nano))})
	}
	return json.Marshal(t.v)
}

func (t *typedValue) UnmarshalJSON(data []byte) error {
	if bytes.Contains(data, []byte(`"$`)) {
		var tag taggedJSON
		if err := json.Unmarshal(data, &tag); err == nil {
			switch {
			case tag.Int != nil:
				n, err := strconv.ParseInt(*tag.Int, 10, 64)
				if err != nil {
					return err
				}
				switch tag.Kind {
				case "int":
					t.v = int(n)
				case "int32":
					t.v = int32(n)
				default:
					t.v = n
				}
				return nil
			case tag.Uint != nil:
				n, err := strconv.ParseUint(*tag.Uint, 10, 64)
				if err != nil {
					return err
				}
				t.v = n
				return nil
			case tag.Dec != nil:
				if d, ok := record.ParseDecimal(*tag.Dec); ok {
					t.v = d
					return nil
				}
			case tag.Time != nil:
				ts, err := time.Parse(time.RFC3339Nano, *tag.Time)
				if err != nil {
					return err
				}
				t.v = ts
				return nil
			}
		}
	}
	return json.Unmarshal(data, &t.v)
}

func wrapVals(vals []interface{}) []typedValue {
	out := make([]typedValue, len(vals))
	for i, v := range vals {
		out[i] = typedValue{v}
	}
	return out
}

func unwrapVals(vals []typedValue) []interface{} {
	out := make([]interface{}, len(vals))
	for i, v := range vals {
		out[i] = v.v
	}
	return out
}

func wrapVal(v interface{}) *typedValue {
	if v == nil {
		return nil
	}
	return &typedValue{v}
}

func unwrapVal(t *typedValue) interface{} {
	if t == nil {
		return nil
	}
	return t.v
}
