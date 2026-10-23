// Package cfgutil reads typed options from the untyped config maps handed to
// source/sink/transform factories.
package cfgutil

import (
	"encoding/json"
	"math"
)

// Int returns cfg[key] as an int. Pipeline definitions are JSON-decoded, so
// numbers normally arrive as float64; Go callers (tests, the SDK) pass ints.
// Both are accepted, as are int64 and json.Number. Non-integral or
// non-numeric values report ok=false.
func Int(cfg map[string]interface{}, key string) (int, bool) {
	switch v := cfg[key].(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case int32:
		return int(v), true
	case float64:
		if v == math.Trunc(v) && !math.IsInf(v, 0) {
			return int(v), true
		}
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return int(n), true
		}
	}
	return 0, false
}

// PositiveInt returns Int(cfg, key) when it is > 0, otherwise def.
func PositiveInt(cfg map[string]interface{}, key string, def int) int {
	if n, ok := Int(cfg, key); ok && n > 0 {
		return n
	}
	return def
}
