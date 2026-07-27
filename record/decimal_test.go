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
	for _, in := range []string{"", "  ", "abc", "1.2.3", "1,000", "0x10", "+", "-", ".", "1.5e", "e5", "1e", "1.5e3.5"} {
		if d, ok := ParseDecimal(in); ok {
			t.Errorf("ParseDecimal(%q) should fail, got %q", in, d.String())
		}
	}
	// Pathologically large exponents are rejected rather than allocating a huge
	// coefficient.
	if d, ok := ParseDecimal("1e2000000000"); ok {
		t.Errorf("ParseDecimal(oversized exponent) should fail, got %q", d.String())
	}
}

func TestParseDecimalScientific(t *testing.T) {
	// Scientific notation is folded into the exact coefficient/scale so it still
	// round-trips losslessly (databases and CSVs sometimes emit NUMERIC this way).
	cases := []struct{ in, want string }{
		{"1.5e3", "1500"},
		{"1.5E3", "1500"},
		{"1.5e+3", "1500"},
		{"1.5e-2", "0.015"},
		{"1e10", "10000000000"},
		{"-2.5e2", "-250"},
		{"2.5e0", "2.5"},
		{"10e-1", "1.0"},
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

func TestDecimalArithmetic(t *testing.T) {
	mk := func(s string) Decimal { d, _ := ParseDecimal(s); return d }
	// Add/Sub/Mul are exact: result scale follows fixed-point rules and no binary
	// rounding creeps in (0.1+0.2 == 0.3 exactly, unlike float64).
	cases := []struct {
		op   string
		a, b string
		want string
	}{
		{"add", "0.1", "0.2", "0.3"},
		{"add", "123.40", "0.005", "123.405"}, // scale = max(2,3)
		{"add", "-1.5", "1.5", "0.0"},
		{"sub", "1.00", "0.99", "0.01"},
		{"sub", "0", "5.25", "-5.25"},
		{"mul", "1.5", "1.5", "2.25"},  // scale = 1+1
		{"mul", "123.40", "0", "0.00"}, // scale = 2+0
		{"mul", "-2", "3.5", "-7.0"},
	}
	for _, c := range cases {
		var got Decimal
		switch c.op {
		case "add":
			got = mk(c.a).Add(mk(c.b))
		case "sub":
			got = mk(c.a).Sub(mk(c.b))
		case "mul":
			got = mk(c.a).Mul(mk(c.b))
		}
		if got.String() != c.want {
			t.Errorf("%s(%s,%s) = %q; want %q", c.op, c.a, c.b, got.String(), c.want)
		}
	}
	if got := mk("-5.25").Neg().String(); got != "5.25" {
		t.Errorf("Neg = %q", got)
	}
	if got := mk("-5.25").Abs().String(); got != "5.25" {
		t.Errorf("Abs = %q", got)
	}
	if got := mk("5.25").Scale(); got != 2 {
		t.Errorf("Scale = %d; want 2", got)
	}
}

func TestDecimalRound(t *testing.T) {
	mk := func(s string) Decimal { d, _ := ParseDecimal(s); return d }
	cases := []struct {
		in     string
		places int32
		want   string
	}{
		{"123.456", 2, "123.46"}, // half away from zero (6 → up)
		{"123.454", 2, "123.45"},
		{"123.455", 2, "123.46"}, // exactly half → away from zero
		{"-1.005", 2, "-1.01"},   // negatives round away from zero
		{"1.5", 0, "2"},
		{"2.5", 0, "3"},
		{"123.4", 5, "123.4"}, // places >= scale is a no-op
	}
	for _, c := range cases {
		if got := mk(c.in).Round(c.places).String(); got != c.want {
			t.Errorf("Round(%s,%d) = %q; want %q", c.in, c.places, got, c.want)
		}
	}
}

func TestDecimalDivRound(t *testing.T) {
	mk := func(s string) Decimal { d, _ := ParseDecimal(s); return d }
	cases := []struct {
		a, b  string
		scale int32
		want  string
	}{
		{"100.00", "3", 10, "33.3333333333"},
		{"1.00", "8", 3, "0.125"},
		{"2.00", "3", 2, "0.67"},   // 0.666… rounds up
		{"-2.00", "3", 2, "-0.67"}, // away from zero
		{"7", "2", 2, "3.50"},
	}
	for _, c := range cases {
		got, ok := mk(c.a).DivRound(mk(c.b), c.scale)
		if !ok || got.String() != c.want {
			t.Errorf("DivRound(%s,%s,%d) = %q,%v; want %q", c.a, c.b, c.scale, got.String(), ok, c.want)
		}
	}
	if _, ok := mk("1").DivRound(mk("0"), 2); ok {
		t.Error("DivRound by zero should return ok=false")
	}
}
