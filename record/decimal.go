package record

import (
	"database/sql/driver"
	"math/big"
	"strconv"
	"strings"
)

// Decimal is an exact base-10 fixed-point number: value = coef × 10^-scale,
// backed by math/big.Int. It exists so columns typed TypeDecimal keep full
// precision — monetary, quantity and regulatory values collapse and drift when
// carried as float64 (e.g. a summed invoice total). Decimal preserves the value
// exactly (no binary rounding) and remembers the authored scale (fractional
// digit count) so a value round-trips as written: "123.40" stays "123.40".
//
// The zero value is a valid decimal 0. Decimal is immutable; every operation
// returns a new Decimal.
type Decimal struct {
	coef  *big.Int // unscaled coefficient (nil == 0)
	scale int32    // number of fractional digits; value = coef / 10^scale
}

// ParseDecimal parses a plain base-10 numeric string ("[+-]?digits[.digits]")
// into an exact Decimal, preserving its scale. It deliberately rejects
// scientific notation and other non-plain forms (returning ok=false) so callers
// can fall back to their existing float/string handling rather than silently
// changing representation.
func ParseDecimal(s string) (Decimal, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Decimal{}, false
	}
	neg := false
	switch s[0] {
	case '+':
		s = s[1:]
	case '-':
		neg = true
		s = s[1:]
	}
	if s == "" {
		return Decimal{}, false
	}
	intPart, fracPart := s, ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		intPart, fracPart = s[:i], s[i+1:]
	}
	digits := intPart + fracPart
	if digits == "" || !isAllDigits(digits) {
		return Decimal{}, false
	}
	coef, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return Decimal{}, false
	}
	if neg && coef.Sign() != 0 {
		coef.Neg(coef)
	}
	return Decimal{coef: coef, scale: int32(len(fracPart))}, true
}

// ToDecimal converts a batch value to an exact Decimal. Decimal, string and
// integer inputs are exact; a float64/float32 is converted via its shortest
// exact decimal string (so no precision is added or lost beyond what the float
// already carried). Returns ok=false for values that are not numeric.
func ToDecimal(v interface{}) (Decimal, bool) {
	switch n := v.(type) {
	case Decimal:
		return n, true
	case string:
		return ParseDecimal(n)
	case int:
		return Decimal{coef: big.NewInt(int64(n))}, true
	case int32:
		return Decimal{coef: big.NewInt(int64(n))}, true
	case int64:
		return Decimal{coef: big.NewInt(n)}, true
	case float64:
		return ParseDecimal(strconv.FormatFloat(n, 'f', -1, 64))
	case float32:
		return ParseDecimal(strconv.FormatFloat(float64(n), 'f', -1, 64))
	}
	return Decimal{}, false
}

// NewDecimal builds the exact value coef × 10^-scale (scale >= 0). Useful for
// implied-decimal fields where an integer coefficient carries an out-of-band
// scale.
func NewDecimal(coef int64, scale int32) Decimal {
	return Decimal{coef: big.NewInt(coef), scale: scale}
}

func isAllDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// IsZero reports whether the decimal is exactly zero.
func (d Decimal) IsZero() bool { return d.coef == nil || d.coef.Sign() == 0 }

// Sign returns -1, 0 or +1.
func (d Decimal) Sign() int {
	if d.coef == nil {
		return 0
	}
	return d.coef.Sign()
}

// String renders the exact value with its scale, e.g. "123.40" or "-0.05".
func (d Decimal) String() string {
	if d.coef == nil {
		return "0"
	}
	neg := d.coef.Sign() < 0
	abs := new(big.Int).Abs(d.coef).String()
	var out string
	if d.scale <= 0 {
		out = abs
	} else {
		sc := int(d.scale)
		if len(abs) <= sc {
			abs = strings.Repeat("0", sc-len(abs)+1) + abs // ensure a leading integer digit
		}
		out = abs[:len(abs)-sc] + "." + abs[len(abs)-sc:]
	}
	if neg {
		out = "-" + out
	}
	return out
}

// Float64 returns the value as a float64. This is lossy for values that exceed
// float64 precision and is intended only for boundaries that require a float
// (comparison in expression transforms); the exact value is preserved for
// pass-through and typed sinks.
func (d Decimal) Float64() float64 {
	if d.coef == nil {
		return 0
	}
	f := new(big.Float).SetInt(d.coef)
	if d.scale != 0 {
		pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d.scale)), nil)
		f.Quo(f, new(big.Float).SetInt(pow))
	}
	r, _ := f.Float64()
	return r
}

// Cmp compares d and o exactly, returning -1, 0 or +1. Scales are aligned so
// "1.5" and "1.50" compare equal.
func (d Decimal) Cmp(o Decimal) int {
	a, b := d.coef, o.coef
	if a == nil {
		a = big.NewInt(0)
	}
	if b == nil {
		b = big.NewInt(0)
	}
	if d.scale == o.scale {
		return a.Cmp(b)
	}
	// Scale the lower-scale coefficient up so both are at the same scale.
	if d.scale < o.scale {
		a = new(big.Int).Mul(a, pow10(o.scale-d.scale))
	} else {
		b = new(big.Int).Mul(b, pow10(d.scale-o.scale))
	}
	return a.Cmp(b)
}

func pow10(n int32) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

// Value implements driver.Valuer so a Decimal binds to a SQL NUMERIC/DECIMAL
// parameter as its exact textual form (drivers parse numeric strings natively),
// never as a lossy float64.
func (d Decimal) Value() (driver.Value, error) { return d.String(), nil }

// MarshalJSON emits the exact value as a JSON number (unquoted), so JSON/NDJSON
// sinks preserve precision in the serialized text.
func (d Decimal) MarshalJSON() ([]byte, error) { return []byte(d.String()), nil }

var _ driver.Valuer = Decimal{}
