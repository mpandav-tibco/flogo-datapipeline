package record

import (
	"fmt"
	"testing"
	"time"
)

func TestFormatValueMatchesSprintf(t *testing.T) {
	d, _ := ParseDecimal("-12.340")
	vals := []interface{}{
		nil, "", "x y", int64(-42), int64(0), 7, int32(-3), int8(5), uint64(9), true, false,
		1.5, 1e21, 1e20, 3.0, float32(0.1), d, []byte("ab"), []interface{}{1, "a"},
		map[string]interface{}{"k": 1}, time.Unix(0, 0).UTC(), struct{ A int }{1},
	}
	for _, v := range vals {
		if got, want := FormatValue(v), fmt.Sprintf("%v", v); got != want {
			t.Errorf("FormatValue(%#v) = %q, want %q", v, got, want)
		}
	}
}
