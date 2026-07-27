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
	// Optional scientific exponent: mantissa[eE][+-]?digits. The exponent shifts
	// the scale; a resulting negative scale is folded into the coefficient.
	exp := 0
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		mant, expStr := s[:i], s[i+1:]
		if mant == "" || expStr == "" {
			return Decimal{}, false
		}
		e, err := strconv.Atoi(expStr)
		if err != nil {
			return Decimal{}, false
		}
		// Bound the exponent so pathological input (e.g. "1e2000000000") cannot
		// allocate a giant coefficient.
		if e < -1000 || e > 1000 {
			return Decimal{}, false
		}
		exp = e
		s = mant
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
	scale := int32(len(fracPart)) - int32(exp)
	if scale < 0 {
		// A negative scale is an integer with trailing zeros: fold it into coef.
		coef.Mul(coef, pow10(-scale))
		scale = 0
	}
	return Decimal{coef: coef, scale: scale}, true
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
// sinks preserve precision in the serialized text. Note: JSON has no exact
// decimal type, so a downstream consumer that re-parses that number into a
// float64 — including this pipeline's own jsonlines source when the column is
// untyped — will re-round it. For lossless interchange, target a typed sink
// (JDBC NUMERIC / a typed decimal column) rather than round-tripping via JSON.
func (d Decimal) MarshalJSON() ([]byte, error) { return []byte(d.String()), nil }

// Scale returns the number of fractional digits the decimal carries.
func (d Decimal) Scale() int32 { return d.scale }

// NewDecimalFromBig builds the exact value coef × 10^-scale from a big.Int
// coefficient (copied so the caller may reuse its own). Useful for decoders that
// assemble an unscaled integer wider than int64 (e.g. a 128-bit Parquet DECIMAL).
func NewDecimalFromBig(coef *big.Int, scale int32) Decimal {
	if coef == nil {
		return Decimal{scale: scale}
	}
	return Decimal{coef: new(big.Int).Set(coef), scale: scale}
}

// bigCoef returns the coefficient as a non-nil big.Int (a fresh zero when nil).
func (d Decimal) bigCoef() *big.Int {
	if d.coef == nil {
		return new(big.Int)
	}
	return d.coef
}

// alignedCoefs scales the two coefficients up to a common scale (the larger of
// the two) and returns them together with that scale.
func alignedCoefs(a, b Decimal) (*big.Int, *big.Int, int32) {
	ac, bc := a.bigCoef(), b.bigCoef()
	switch {
	case a.scale == b.scale:
		return ac, bc, a.scale
	case a.scale < b.scale:
		return new(big.Int).Mul(ac, pow10(b.scale-a.scale)), bc, b.scale
	default:
		return ac, new(big.Int).Mul(bc, pow10(a.scale-b.scale)), a.scale
	}
}

// Add returns the exact sum d + o (no rounding); the result scale is the larger
// of the two operand scales.
func (d Decimal) Add(o Decimal) Decimal {
	ac, bc, s := alignedCoefs(d, o)
	return Decimal{coef: new(big.Int).Add(ac, bc), scale: s}
}

// Sub returns the exact difference d - o (no rounding).
func (d Decimal) Sub(o Decimal) Decimal {
	ac, bc, s := alignedCoefs(d, o)
	return Decimal{coef: new(big.Int).Sub(ac, bc), scale: s}
}

// Mul returns the exact product d × o (no rounding); the result scale is the sum
// of the operand scales.
func (d Decimal) Mul(o Decimal) Decimal {
	return Decimal{coef: new(big.Int).Mul(d.bigCoef(), o.bigCoef()), scale: d.scale + o.scale}
}

// Neg returns -d.
func (d Decimal) Neg() Decimal {
	return Decimal{coef: new(big.Int).Neg(d.bigCoef()), scale: d.scale}
}

// Abs returns |d|.
func (d Decimal) Abs() Decimal {
	return Decimal{coef: new(big.Int).Abs(d.bigCoef()), scale: d.scale}
}

// Round returns d rounded to `places` fractional digits using round-half-away-
// from-zero. Rounding to a scale >= the current scale leaves the value unchanged.
func (d Decimal) Round(places int32) Decimal {
	if places < 0 {
		places = 0
	}
	if places >= d.scale {
		return d
	}
	return Decimal{coef: roundedDiv(d.bigCoef(), pow10(d.scale-places)), scale: places}
}

// DivRound returns d / o rounded to `scale` fractional digits (round-half-away-
// from-zero). ok is false when o is zero. Unlike Add/Sub/Mul it is not exact — a
// quotient such as 1/3 has no finite decimal — so the caller chooses the scale.
func (d Decimal) DivRound(o Decimal, scale int32) (Decimal, bool) {
	if o.Sign() == 0 {
		return Decimal{}, false
	}
	if scale < 0 {
		scale = 0
	}
	// value = (d.coef / 10^d.scale) / (o.coef / 10^o.scale)
	//       = (d.coef × 10^(o.scale+scale)) / (o.coef × 10^d.scale), then round.
	num := new(big.Int).Mul(d.bigCoef(), pow10(o.scale+scale))
	den := new(big.Int).Mul(o.bigCoef(), pow10(d.scale))
	return Decimal{coef: roundedDiv(num, den), scale: scale}, true
}

// roundedDiv returns num/den rounded half-away-from-zero. den must be non-zero.
func roundedDiv(num, den *big.Int) *big.Int {
	q := new(big.Int)
	r := new(big.Int)
	q.QuoRem(num, den, r) // q truncates toward zero; r carries the sign of num
	if r.Sign() == 0 {
		return q
	}
	// Round away from zero when 2·|r| >= |den|.
	r2 := new(big.Int).Abs(r)
	r2.Lsh(r2, 1)
	if r2.Cmp(new(big.Int).Abs(den)) >= 0 {
		if (num.Sign() < 0) == (den.Sign() < 0) {
			q.Add(q, big.NewInt(1))
		} else {
			q.Sub(q, big.NewInt(1))
		}
	}
	return q
}

var _ driver.Valuer = Decimal{}
