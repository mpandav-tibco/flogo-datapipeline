package record

import (
	"fmt"
	"strconv"
)

// FormatValue renders v exactly as fmt's "%v" verb would. Strings, integers and
// bools — the bulk of cell values used as join/lookup keys and in string
// comparisons — take allocation-light fast paths; everything else falls back
// to fmt so the output is always identical to fmt.Sprintf("%v", v).
func FormatValue(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case int64:
		return strconv.FormatInt(t, 10)
	case int:
		return strconv.Itoa(t)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case bool:
		return strconv.FormatBool(t)
	case nil:
		return "<nil>"
	}
	return fmt.Sprintf("%v", v)
}
