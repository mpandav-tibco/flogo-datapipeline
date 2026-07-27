package mapper

import (
	"strings"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// callFn invokes a registered builtin by name with the given args. It fails the
// test if the function isn't registered so a palette/runtime mismatch is caught.
func callFn(t *testing.T, name string, args ...interface{}) interface{} {
	t.Helper()
	fn, ok := builtins[strings.ToLower(name)]
	if !ok {
		t.Fatalf("builtin %q not registered", name)
	}
	return fn(args)
}

func TestEtl_String(t *testing.T) {
	if got := callFn(t, "string.split", "a,b,c", ",", 1); got != "b" {
		t.Errorf("split index = %v, want b", got)
	}
	if got := callFn(t, "string.join", "-", "a", "b", "c"); got != "a-b-c" {
		t.Errorf("join = %v, want a-b-c", got)
	}
	if got := callFn(t, "string.title", "hello world"); got != "Hello World" {
		t.Errorf("title = %v", got)
	}
	if got := callFn(t, "string.reverse", "abc"); got != "cba" {
		t.Errorf("reverse = %v", got)
	}
	if got := callFn(t, "string.repeat", "ab", 3); got != "ababab" {
		t.Errorf("repeat = %v", got)
	}
	if got := callFn(t, "string.equalsIgnoreCase", "AbC", "abc"); got != true {
		t.Errorf("equalsIgnoreCase = %v", got)
	}
	if got := callFn(t, "string.substringBefore", "key=value", "="); got != "key" {
		t.Errorf("substringBefore = %v", got)
	}
	if got := callFn(t, "string.substringAfter", "key=value", "="); got != "value" {
		t.Errorf("substringAfter = %v", got)
	}
	if got := callFn(t, "string.trimPrefix", "img_001", "img_"); got != "001" {
		t.Errorf("trimPrefix = %v", got)
	}
}

func TestEtl_Regex(t *testing.T) {
	if got := callFn(t, "regex.match", "abc123", `\d+`); got != true {
		t.Errorf("regex.match = %v", got)
	}
	if got := callFn(t, "regex.replace", "a1b2", `\d`, "#"); got != "a#b#" {
		t.Errorf("regex.replace = %v", got)
	}
	if got := callFn(t, "regex.extract", "id=42;", `id=(\d+)`, 1); got != "42" {
		t.Errorf("regex.extract = %v", got)
	}
}

func TestEtl_Math(t *testing.T) {
	if got := callFn(t, "math.pow", 2.0, 10.0); got != 1024.0 {
		t.Errorf("pow = %v", got)
	}
	if got := callFn(t, "math.sqrt", 144.0); got != 12.0 {
		t.Errorf("sqrt = %v", got)
	}
	if got := callFn(t, "math.sign", -5.0); got != -1 {
		t.Errorf("sign = %v", got)
	}
	if got := callFn(t, "math.trunc", 3.9); got != 3.0 {
		t.Errorf("trunc = %v", got)
	}
}

func TestEtl_DecimalArithmetic(t *testing.T) {
	// Numeric builtins stay EXACT when a decimal operand is present: the result is
	// a record.Decimal with no float rounding (0.1+0.2 == 0.3 exactly), not float64.
	dec := func(s string) record.Decimal {
		d, ok := record.ParseDecimal(s)
		if !ok {
			t.Fatalf("ParseDecimal(%q) failed", s)
		}
		return d
	}
	wantDec := func(name string, got interface{}, want string) {
		t.Helper()
		d, ok := got.(record.Decimal)
		if !ok {
			t.Fatalf("%s = %T (%v); want record.Decimal", name, got, got)
		}
		if d.String() != want {
			t.Errorf("%s = %q; want %q", name, d.String(), want)
		}
	}
	wantDec("multiply", callFn(t, "multiply", dec("1.5"), dec("2")), "3.0")
	wantDec("number.add", callFn(t, "number.add", dec("0.1"), dec("0.2")), "0.3")
	wantDec("number.subtract", callFn(t, "number.subtract", dec("1.00"), dec("0.99")), "0.01")
	wantDec("divide", callFn(t, "divide", dec("100.00"), dec("4")), "25.0000000000")
	wantDec("number.round", callFn(t, "number.round", dec("123.456"), 2), "123.46")
	// Decimal + numeric literal (the parser delivers literals as float64) stays exact.
	wantDec("add-literal", callFn(t, "number.add", dec("100.00"), 5.0), "105.00")
	// Non-decimal operands keep the float64 path unchanged (no silent promotion).
	if got := callFn(t, "number.add", 2.0, 3.0); got != 5.0 {
		t.Errorf("number.add(float,float) = %v (%T); want 5.0 float64", got, got)
	}
	// clamp is a range gate (float comparison), still returns the float value.
	if got := callFn(t, "clamp", dec("5.5"), dec("0"), dec("10")); got != 5.5 {
		t.Errorf("clamp(decimal) = %v, want 5.5", got)
	}
}

func TestEtl_Datetime(t *testing.T) {
	if got := callFn(t, "datetime.addDays", "2024-01-01", 30); got != "2024-01-31T00:00:00Z" {
		t.Errorf("addDays = %v", got)
	}
	if got := callFn(t, "datetime.diffDays", "2024-01-31", "2024-01-01"); got != 30 {
		t.Errorf("diffDays = %v", got)
	}
	if got := callFn(t, "datetime.hour", "2024-01-01T13:30:00Z"); got != 13 {
		t.Errorf("hour = %v", got)
	}
	if got := callFn(t, "datetime.dayOfWeek", "2024-01-01"); got != "Monday" {
		t.Errorf("dayOfWeek = %v", got)
	}
	if got := callFn(t, "datetime.epoch", "1970-01-01T00:00:00Z"); got != int64(0) {
		t.Errorf("epoch = %v", got)
	}
	if got := callFn(t, "datetime.fromEpoch", 0.0); got != "1970-01-01T00:00:00Z" {
		t.Errorf("fromEpoch = %v", got)
	}
}

func TestEtl_HashEncoding(t *testing.T) {
	if got := callFn(t, "hash.md5", "abc"); got != "900150983cd24fb0d6963f7d28e17f72" {
		t.Errorf("md5 = %v", got)
	}
	if got := callFn(t, "hash.sha256", "abc"); got != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Errorf("sha256 = %v", got)
	}
	if got := callFn(t, "encoding.base64Encode", "hi"); got != "aGk=" {
		t.Errorf("base64Encode = %v", got)
	}
	if got := callFn(t, "encoding.base64Decode", "aGk="); got != "hi" {
		t.Errorf("base64Decode = %v", got)
	}
	u, _ := callFn(t, "util.uuid").(string)
	if len(u) != 36 || strings.Count(u, "-") != 4 {
		t.Errorf("uuid = %q", u)
	}
	if got := callFn(t, "util.mask", "4111111111111234", 4); got != "************1234" {
		t.Errorf("mask = %v", got)
	}
}

func TestEtl_Logic(t *testing.T) {
	if got := callFn(t, "nvl", nil, "fallback"); got != "fallback" {
		t.Errorf("nvl = %v", got)
	}
	if got := callFn(t, "nullif", "x", "x"); got != nil {
		t.Errorf("nullif equal = %v, want nil", got)
	}
	if got := callFn(t, "between", 5.0, 1.0, 10.0); got != true {
		t.Errorf("between = %v", got)
	}
	if got := callFn(t, "decode", "B", "A", "Apple", "B", "Banana", "none"); got != "Banana" {
		t.Errorf("decode = %v", got)
	}
	if got := callFn(t, "decode", "Z", "A", "Apple", "none"); got != "none" {
		t.Errorf("decode default = %v", got)
	}
	if got := callFn(t, "not", false); got != true {
		t.Errorf("not = %v", got)
	}
}
