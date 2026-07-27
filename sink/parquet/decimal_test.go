package parquet

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// TestDecimalToParquetBytes covers the fixed-len encoding and the out-of-range
// guard: in-range values encode to exactly 16 bytes; a value beyond signed
// 128-bit (past DECIMAL(38,10)) encodes to nil so the sink writes null instead
// of a sign-flipped / truncated (corrupt) value.
func TestDecimalToParquetBytes(t *testing.T) {
	mk := func(s string) record.Decimal {
		d, ok := record.ParseDecimal(s)
		if !ok {
			t.Fatalf("ParseDecimal(%q) failed", s)
		}
		return d
	}
	for _, s := range []string{"0", "123.45", "-9999999999999999999.99", "0.0000000001"} {
		if b := decimalToParquetBytes(mk(s)); len(b) != parquetDecimalBytes {
			t.Errorf("decimalToParquetBytes(%q) len = %d; want %d", s, len(b), parquetDecimalBytes)
		}
	}
	if b := decimalToParquetBytes(mk("1234567890123456789012345678901234567890")); b != nil {
		t.Errorf("out-of-range decimal should encode to nil, got %d bytes", len(b))
	}
}
