package cfgutil

import (
	"encoding/json"
	"testing"
)

func TestInt(t *testing.T) {
	var fromJSON map[string]interface{}
	if err := json.Unmarshal([]byte(`{"a":3,"b":2.5,"c":"7","d":-1}`), &fromJSON); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		cfg  map[string]interface{}
		key  string
		want int
		ok   bool
	}{
		{fromJSON, "a", 3, true},
		{fromJSON, "b", 0, false},
		{fromJSON, "c", 0, false},
		{fromJSON, "missing", 0, false},
		{map[string]interface{}{"x": 5}, "x", 5, true},
		{map[string]interface{}{"x": int64(6)}, "x", 6, true},
		{map[string]interface{}{"x": json.Number("8")}, "x", 8, true},
	}
	for _, c := range cases {
		if got, ok := Int(c.cfg, c.key); got != c.want || ok != c.ok {
			t.Errorf("Int(%v,%q) = %d,%v want %d,%v", c.cfg, c.key, got, ok, c.want, c.ok)
		}
	}
	if PositiveInt(fromJSON, "d", 9) != 9 || PositiveInt(fromJSON, "a", 9) != 3 {
		t.Error("PositiveInt default/override mismatch")
	}
}
