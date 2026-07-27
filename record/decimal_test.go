package record

import (
	"encoding/json"
	"testing"
)

func TestParseDecimalRoundTrip(t *testing.T) {
	// Parsing then String() must reproduce the authored text exactly, including
	// scale (trailing zeros) and sign — the property that makes NUMERIC values
	// round-trip through the pipeline losslessly.
	cases := []struct{ in, want string }{
		{"123.40", "123.40"},
		{"0.05", "0.05"},
		{"-0.05", "-0.05"},
		{"5", "5"},
		{"5.", "5"},
		{".5", "0.5"},
		{"+5", "5"},
		{"0", "0"},
		{"-0", "0"},
		{"00123.4500", "123.4500"}, // integer leading zeros normalize away; fractional (scale) zeros are kept
		{"1000000000000000000000.0001", "1000000000000000000000.0001"},
	}
	for _, c := range cases {
		d, ok := ParseDecimal(c.in)
		if !ok {
			t.Errorf("ParseDecimal(%q) failed", c.in)
			continue
		}
		if got := d.String(); got != c.want {
			t.Errorf("ParseDecimal(%q).String() = %q; want %q", c.in, got, c.want)
		}
	}
}

func TestParseDecimalRejects(t *testing.T) {
	for _, in := range []string{"", "  ", "1.5e3", "abc", "1.2.3", "1,000", "0x10", "+", "-", "."} {
		if d, ok := ParseDecimal(in); ok {
			t.Errorf("ParseDecimal(%q) should fail, got %q", in, d.String())
		}
	}
}

func TestDecimalCmp(t *testing.T) {
	mk := func(s string) Decimal { d, _ := ParseDecimal(s); return d }
	cases := []struct {
		a, b string
		want int
	}{
		{"1.5", "1.50", 0}, // scale-independent equality
		{"1.5", "1.6", -1},
		{"1.6", "1.5", 1},
		{"-0.05", "0", -1},
		{"100", "9", 1}, // numeric, not lexical ("100" < "9" as strings)
		{"0", "0.00", 0},
		{"-1", "-1.000", 0},
	}
	for _, c := range cases {
		if got := mk(c.a).Cmp(mk(c.b)); got != c.want {
			t.Errorf("(%s).Cmp(%s) = %d; want %d", c.a, c.b, got, c.want)
		}
	}
	// Zero value compares as 0.
	if (Decimal{}).Cmp(mk("0")) != 0 {
		t.Error("zero-value Decimal should equal 0")
	}
}

func TestDecimalExactVsFloat(t *testing.T) {
	// The whole point: values that float64 cannot represent stay exact.
	d, _ := ParseDecimal("0.1")
	if d.String() != "0.1" {
		t.Errorf("0.1 exact: got %q", d.String())
	}
	// A 20-significant-digit value survives; float64 has ~15-17.
	big, _ := ParseDecimal("12345678901234567.89")
	if big.String() != "12345678901234567.89" {
		t.Errorf("high-precision decimal drifted: got %q", big.String())
	}
}

func TestToDecimal(t *testing.T) {
	if d, ok := ToDecimal(int64(5)); !ok || d.String() != "5" {
		t.Errorf("ToDecimal(int64 5) = %q,%v", d.String(), ok)
	}
	if d, ok := ToDecimal("123.40"); !ok || d.String() != "123.40" {
		t.Errorf("ToDecimal(string) = %q,%v", d.String(), ok)
	}
	src, _ := ParseDecimal("7.25")
	if d, ok := ToDecimal(src); !ok || d.Cmp(src) != 0 {
		t.Errorf("ToDecimal(Decimal) passthrough failed")
	}
	if d, ok := ToDecimal(0.1); !ok || d.String() != "0.1" {
		t.Errorf("ToDecimal(float64 0.1) = %q,%v; want shortest-exact 0.1", d.String(), ok)
	}
	if _, ok := ToDecimal([]byte("x")); ok {
		t.Error("ToDecimal(non-numeric) should fail")
	}
}

func TestDecimalMarshalJSONAndValue(t *testing.T) {
	d, _ := ParseDecimal("123.40")
	b, err := json.Marshal(d)
	if err != nil || string(b) != "123.40" {
		t.Errorf("MarshalJSON = %q,%v; want unquoted 123.40", string(b), err)
	}
	// Embedded in a struct/map it stays a JSON number.
	b2, _ := json.Marshal(map[string]interface{}{"amt": d})
	if string(b2) != `{"amt":123.40}` {
		t.Errorf("map marshal = %s; want number", string(b2))
	}
	v, err := d.Value()
	if err != nil || v.(string) != "123.40" {
		t.Errorf("driver.Value = %v,%v; want \"123.40\"", v, err)
	}
}
