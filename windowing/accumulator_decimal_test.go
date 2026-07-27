package windowing

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

func dec(t *testing.T, s string) record.Decimal {
	t.Helper()
	d, ok := record.ParseDecimal(s)
	if !ok {
		t.Fatalf("ParseDecimal(%q) failed", s)
	}
	return d
}

// TestAccumulator_DecimalSumExact proves windowed sum stays exact for decimals
// whose total exceeds float64's ~15-17 significant digits.
func TestAccumulator_DecimalSumExact(t *testing.T) {
	acc, _ := newAccumulator("sum")
	acc.Add(dec(t, "12345678901234567.89"))
	acc.Add(dec(t, "0.11"))
	got, ok := acc.Result().(record.Decimal)
	if !ok {
		t.Fatalf("sum Result = %T; want record.Decimal", acc.Result())
	}
	if got.String() != "12345678901234568.00" {
		t.Errorf("decimal sum = %q; want 12345678901234568.00", got.String())
	}
}

func TestAccumulator_DecimalAvg(t *testing.T) {
	acc, _ := newAccumulator("avg")
	acc.Add(dec(t, "1.00"))
	acc.Add(dec(t, "2.00"))
	acc.Add(dec(t, "2.00"))
	// sum 5.00 / 3, rounded to 10 fractional digits (round-half-away-from-zero).
	got := acc.Result().(record.Decimal)
	if got.String() != "1.6666666667" {
		t.Errorf("decimal avg = %q; want 1.6666666667", got.String())
	}
}

func TestAccumulator_DecimalMinMax(t *testing.T) {
	mn, _ := newAccumulator("min")
	mx, _ := newAccumulator("max")
	for _, s := range []string{"99.99", "100.50", "9.9", "100.5"} {
		mn.Add(dec(t, s))
		mx.Add(dec(t, s))
	}
	// Numeric ordering, not lexical.
	if got := mn.Result().(record.Decimal); got.String() != "9.9" {
		t.Errorf("decimal min = %q; want 9.9", got.String())
	}
	if got := mx.Result().(record.Decimal); got.String() != "100.50" {
		t.Errorf("decimal max = %q; want 100.50", got.String())
	}
}

func TestAccumulator_DecimalSnapshotRoundTrip(t *testing.T) {
	acc, _ := newAccumulator("sum")
	acc.Add(dec(t, "123.45"))
	acc.Add(dec(t, "0.55"))
	restored, _ := newAccumulator("sum")
	restored.load(acc.snap()) // exact string survives the JSON-shaped state
	got := restored.Result().(record.Decimal)
	if got.String() != "124.00" {
		t.Errorf("restored decimal sum = %q; want 124.00", got.String())
	}
}

func TestAccumulator_DecimalMerge(t *testing.T) {
	a, _ := newAccumulator("sum")
	b, _ := newAccumulator("sum")
	a.Add(dec(t, "10.25"))
	b.Add(dec(t, "5.75"))
	a.Merge(b)
	got := a.Result().(record.Decimal)
	if got.String() != "16.00" {
		t.Errorf("merged decimal sum = %q; want 16.00", got.String())
	}
}

// TestAccumulator_FloatPathUnchanged confirms non-decimal columns keep returning
// float64 (no behaviour change for existing pipelines).
func TestAccumulator_FloatPathUnchanged(t *testing.T) {
	acc, _ := newAccumulator("sum")
	acc.Add(int64(2))
	acc.Add(3.5)
	if got := acc.Result(); got != 5.5 {
		t.Errorf("float sum = %v (%T); want 5.5 float64", got, got)
	}
}
