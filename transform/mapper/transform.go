// Package mapper registers a field-mapping transform that reshapes RecordBatches.
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/transform/mapper"
//
// Config:
//
//	{
//	  "type": "map",
//	  "fields": {
//	    "outputField":   "=$inputField",
//	    "amountUsd":     "=divide($.amount, 100)",
//	    "fullName":      "=concat($.firstName, ' ', $.lastName)",
//	    "constant":      "STATIC_VALUE",
//	    "passthrough":   "=$passthrough"
//	  }
//	}
//
// Expression syntax:
//   - Starts with "=" → dynamic expression
//   - "=$fieldName"   → copy field from input row
//   - "=funcName(args...)" → built-in function call (concat, divide, upper, lower, trim)
//   - No "=" prefix  → static string constant
package mapper

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	mathrand "math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func init() {
	transform.Register("map", newMapperTransform)
	transform.Register("mapper", newMapperTransform)
}

type mapperTransform struct {
	fieldExprs []fieldExpr
	outCols    []record.Column
	// typesResolved is set once the output column types have been inferred from
	// the first batch's actual values, so downstream sinks (Parquet, JDBC) get
	// concrete types instead of a stringified TypeAny schema.
	typesResolved bool
}

type fieldExpr struct {
	outName string
	eval    func(row map[string]interface{}) interface{}
}

func newMapperTransform(cfg map[string]interface{}) (transform.Transform, error) {
	fieldsRaw, ok := cfg["fields"]
	if !ok {
		return nil, fmt.Errorf("map transform: 'fields' is required")
	}
	fieldsMap, ok := fieldsRaw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("map transform: 'fields' must be an object")
	}

	// Collect keys and establish a deterministic iteration order.
	// By default, keys are sorted lexicographically to guarantee stable column
	// order across pipeline restarts (Go map iteration is non-deterministic).
	// If the config supplies a "fieldOrder" array, that explicit order is used
	// instead so operators can control the physical column sequence in output
	// artifacts (Parquet schema, SQL INSERT list, etc.).
	keys := make([]string, 0, len(fieldsMap))
	for k := range fieldsMap {
		keys = append(keys, k)
	}
	if ordered, ok := cfg["fieldOrder"].([]interface{}); ok {
		// fieldOrder must be a permutation of the fields keys: same count, same
		// names, no duplicates. A length-only check would let a typo silently map to
		// an empty expression — dropping the intended field and emitting a blank
		// column — corrupting the output schema with no error.
		if len(ordered) != len(fieldsMap) {
			return nil, fmt.Errorf("map transform: 'fieldOrder' has %d entries but 'fields' has %d", len(ordered), len(fieldsMap))
		}
		collected := make([]string, 0, len(ordered))
		seen := make(map[string]bool, len(ordered))
		for _, v := range ordered {
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("map transform: 'fieldOrder' must be an array of strings; element %v is not a string", v)
			}
			if _, isField := fieldsMap[s]; !isField {
				return nil, fmt.Errorf("map transform: 'fieldOrder' entry %q is not a key in 'fields'", s)
			}
			if seen[s] {
				return nil, fmt.Errorf("map transform: 'fieldOrder' has duplicate entry %q", s)
			}
			seen[s] = true
			collected = append(collected, s)
		}
		keys = collected
	} else {
		sort.Strings(keys)
	}

	var exprs []fieldExpr
	var outCols []record.Column
	for _, outName := range keys {
		exprRaw := fieldsMap[outName]
		exprStr, _ := exprRaw.(string)
		eval, err := compileExpr(exprStr)
		if err != nil {
			return nil, fmt.Errorf("map transform: field %q: %w", outName, err)
		}
		exprs = append(exprs, fieldExpr{outName: outName, eval: eval})
		outCols = append(outCols, record.Column{Name: outName, Type: record.TypeAny})
	}
	return &mapperTransform{fieldExprs: exprs, outCols: outCols}, nil
}

func (m *mapperTransform) Name() string { return "map" }

func (m *mapperTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	out := record.Get()
	out.Rows = make([][]interface{}, 0, in.Len())

	for i := range in.Rows {
		row := in.RowMap(i)
		outRow := make([]interface{}, len(m.fieldExprs))
		for j, fe := range m.fieldExprs {
			outRow[j] = fe.eval(row)
		}
		out.Rows = append(out.Rows, outRow)
	}

	// Infer concrete output column types from the first batch's values, so a
	// downstream typed sink (Parquet/JDBC) doesn't stringify everything. Columns
	// whose values are all nil stay TypeAny (string-compatible).
	if !m.typesResolved {
		m.resolveTypes(out.Rows)
		m.typesResolved = true
	}
	out.Columns = m.outCols
	return out, nil
}

// resolveTypes refines TypeAny output columns to concrete pipeline types by
// scanning the first non-nil value produced for each column.
func (m *mapperTransform) resolveTypes(rows [][]interface{}) {
	for j := range m.outCols {
		if m.outCols[j].Type != record.TypeAny {
			continue
		}
		for _, r := range rows {
			if j >= len(r) || r[j] == nil {
				continue
			}
			if t, ok := inferColumnType(r[j]); ok {
				m.outCols[j].Type = t
			}
			break
		}
	}
}

// inferColumnType maps a Go value's dynamic type to a pipeline column type.
// The bool return is false when the value doesn't map to a concrete type.
func inferColumnType(v interface{}) (record.ColumnType, bool) {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return record.TypeInt, true
	case float32, float64:
		return record.TypeFloat, true
	case bool:
		return record.TypeBool, true
	case time.Time:
		return record.TypeDate, true
	case string:
		return record.TypeString, true
	default:
		return record.TypeAny, false
	}
}

func (m *mapperTransform) Close() error { return nil }

// compileExpr compiles a field-value expression into an evaluator function.
// A value with no leading "=" is a static string constant; otherwise everything
// after the "=" is compiled as an expression (field ref, function call, literal).
func compileExpr(expr string) (func(row map[string]interface{}) interface{}, error) {
	if !strings.HasPrefix(expr, "=") {
		// Static string constant.
		return func(_ map[string]interface{}) interface{} { return expr }, nil
	}
	return compileInner(strings.TrimPrefix(expr, "="))
}

// compileInner compiles an expression (the part after a leading "="). It is also
// used to compile function-call arguments, which are always expressions —
// `$field`, a numeric/quoted literal, or a nested `func(...)`. Arguments may be
// written with or without a leading "=" (both `upper($product)` and
// `upper(=$product)` resolve $product to the row value), so we tolerate an
// optional "=" prefix here.
func compileInner(inner string) (func(row map[string]interface{}) interface{}, error) {
	inner = strings.TrimSpace(inner)
	inner = strings.TrimPrefix(inner, "=")
	inner = strings.TrimSpace(inner)

	// Field reference: $fieldName or $.fieldName. A dotted path
	// ($.properties.amount) traverses nested maps so JSON/XML records can be
	// flattened; a bare name is a fast direct lookup.
	if strings.HasPrefix(inner, "$") {
		fieldName := strings.TrimPrefix(strings.TrimPrefix(inner, "$"), ".")
		if !strings.Contains(fieldName, ".") {
			return func(row map[string]interface{}) interface{} {
				return row[fieldName]
			}, nil
		}
		segments := strings.Split(fieldName, ".")
		return func(row map[string]interface{}) interface{} {
			return resolvePath(row, segments)
		}, nil
	}

	// Function call: funcName(arg1, arg2, ...)
	parenIdx := strings.Index(inner, "(")
	if parenIdx > 0 && strings.HasSuffix(inner, ")") {
		funcName := strings.ToLower(inner[:parenIdx])
		argsStr := inner[parenIdx+1 : len(inner)-1]
		args, err := splitArgs(argsStr)
		if err != nil {
			return nil, fmt.Errorf("bad function call %q: %w", inner, err)
		}
		argEvals := make([]func(map[string]interface{}) interface{}, len(args))
		for i, arg := range args {
			arg = strings.TrimSpace(arg)
			// Function arguments are value expressions: a leading "=" is
			// implicit inside a call, so a bare $field resolves to the row
			// value, numbers and 'quoted' text become literals, and nested
			// func(...) calls compile recursively. Both =upper(=$x) and
			// =upper($x) therefore work. If the arg isn't a recognizable
			// expression (a bareword), fall back to a static string constant.
			toCompile := arg
			if !strings.HasPrefix(arg, "=") {
				toCompile = "=" + arg
			}
			eval, err := compileExpr(toCompile)
			if err != nil {
				lit := arg
				eval = func(_ map[string]interface{}) interface{} { return lit }
			}
			argEvals[i] = eval
		}
		fn, ok := builtins[funcName]
		if !ok {
			return nil, fmt.Errorf("unknown function: %q", funcName)
		}
		return func(row map[string]interface{}) interface{} {
			resolved := make([]interface{}, len(argEvals))
			for i, ae := range argEvals {
				resolved[i] = ae(row)
			}
			return fn(resolved)
		}, nil
	}

	// Numeric literal without quotes.
	if f, err := strconv.ParseFloat(inner, 64); err == nil {
		return func(_ map[string]interface{}) interface{} { return f }, nil
	}

	// Quoted string literal.
	if strings.HasPrefix(inner, "'") && strings.HasSuffix(inner, "'") {
		s := inner[1 : len(inner)-1]
		return func(_ map[string]interface{}) interface{} { return s }, nil
	}

	return nil, fmt.Errorf("unrecognized expression: %q", inner)
}

// resolvePath traverses a dotted path (e.g. properties.amount) through nested
// map[string]interface{} values and returns the leaf value, or nil if any
// segment is missing or not a map.
func resolvePath(row map[string]interface{}, segments []string) interface{} {
	var cur interface{} = row
	for _, seg := range segments {
		m, ok := cur.(map[string]interface{})
		if !ok {
			return nil
		}
		cur, ok = m[seg]
		if !ok {
			return nil
		}
	}
	return cur
}

// splitArgs splits a comma-separated argument list respecting parentheses and quotes.
func splitArgs(s string) ([]string, error) {
	var args []string
	depth := 0
	inQuote := false
	var cur strings.Builder
	for _, r := range s {
		switch {
		case r == '\'' && !inQuote:
			inQuote = true
			cur.WriteRune(r)
		case r == '\'' && inQuote:
			inQuote = false
			cur.WriteRune(r)
		case r == '(' && !inQuote:
			depth++
			cur.WriteRune(r)
		case r == ')' && !inQuote:
			depth--
			cur.WriteRune(r)
		case r == ',' && depth == 0 && !inQuote:
			args = append(args, cur.String())
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		args = append(args, cur.String())
	}
	return args, nil
}

// builtins maps function names to their implementations.
var builtins = map[string]func([]interface{}) interface{}{
	"concat": func(args []interface{}) interface{} {
		var sb strings.Builder
		for _, a := range args {
			if a != nil {
				sb.WriteString(fmt.Sprintf("%v", a))
			}
		}
		return sb.String()
	},
	"divide": func(args []interface{}) interface{} {
		if len(args) < 2 {
			return nil
		}
		a, aOk := toFloat64(args[0])
		b, bOk := toFloat64(args[1])
		if !aOk || !bOk || b == 0 {
			return nil
		}
		return a / b
	},
	"multiply": func(args []interface{}) interface{} {
		if len(args) < 2 {
			return nil
		}
		a, aOk := toFloat64(args[0])
		b, bOk := toFloat64(args[1])
		if !aOk || !bOk {
			return nil
		}
		return a * b
	},
	"upper": func(args []interface{}) interface{} {
		if len(args) < 1 || args[0] == nil {
			return nil
		}
		return strings.ToUpper(fmt.Sprintf("%v", args[0]))
	},
	"lower": func(args []interface{}) interface{} {
		if len(args) < 1 || args[0] == nil {
			return nil
		}
		return strings.ToLower(fmt.Sprintf("%v", args[0]))
	},
	"trim": func(args []interface{}) interface{} {
		if len(args) < 1 || args[0] == nil {
			return nil
		}
		return strings.TrimSpace(fmt.Sprintf("%v", args[0]))
	},
	"scientific": func(args []interface{}) interface{} {
		// Signed 6-digit scientific notation, e.g. +1.332668E+04. Common in
		// instrument/report layouts. Value formatting belongs in the transform
		// stage, not the sink — so this is a first-class mapper builtin.
		if len(args) < 1 {
			return nil
		}
		f, ok := toFloat64(args[0])
		if !ok {
			return nil
		}
		return fmt.Sprintf("%+.6E", f)
	},
	"lpad": func(args []interface{}) interface{} { return padArg(args, true) },
	"rpad": func(args []interface{}) interface{} { return padArg(args, false) },
	"clamp": func(args []interface{}) interface{} {
		// clamp(v, lo, hi) → v when lo<=v<=hi, else nil (route/drop invalids).
		if len(args) < 3 {
			return nil
		}
		v, ok1 := toFloat64(args[0])
		lo, ok2 := toFloat64(args[1])
		hi, ok3 := toFloat64(args[2])
		if !ok1 || !ok2 || !ok3 || v < lo || v > hi {
			return nil
		}
		return v
	},
	"coalesce": func(args []interface{}) interface{} {
		for _, a := range args {
			if a != nil {
				return a
			}
		}
		return nil
	},
}

// padArg pads args[0] (stringified) to width args[1] with spaces; left=true
// left-justifies within the field (right-pad → text left), matching fixed-width
// report layouts. Padding is a formatting op, reusable across any pipeline.
func padArg(args []interface{}, leftJustify bool) interface{} {
	if len(args) < 2 {
		return nil
	}
	s := fmt.Sprintf("%v", args[0])
	w, ok := toFloat64(args[1])
	if !ok {
		return s
	}
	width := int(w)
	if len(s) >= width {
		return s
	}
	fill := strings.Repeat(" ", width-len(s))
	if leftJustify {
		return s + fill
	}
	return fill + s
}

func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case string:
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// --- extended (namespaced) function library ---
//
// The legacy flat builtins above (concat, upper, divide, …) remain registered
// for backward compatibility with existing pipelines. registerExtendedBuiltins
// adds a Flogo-style namespaced library (string.*, number.*, math.*, coerce.*,
// datetime.*, plus logic helpers) so the Studio function palette and the runtime
// stay in lock-step — every function the designer offers actually evaluates.
// Keys are stored lowercased because compileExpr lowercases the function name
// before lookup, so "string.toUpper" resolves via key "string.toupper".

func init() { registerExtendedBuiltins() }

func argStr(args []interface{}, i int) string {
	if i >= len(args) || args[i] == nil {
		return ""
	}
	return fmt.Sprintf("%v", args[i])
}

func argFloat(args []interface{}, i int) (float64, bool) {
	if i >= len(args) {
		return 0, false
	}
	return toFloat64(args[i])
}

func argInt(args []interface{}, i int) (int, bool) {
	f, ok := argFloat(args, i)
	return int(f), ok
}

func truthy(v interface{}) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case int:
		return x != 0
	case int64:
		return x != 0
	case float32:
		return x != 0
	case float64:
		return x != 0
	}
	return true
}

func padStr(s string, args []interface{}, left bool) string {
	w, ok := argInt(args, 1)
	if !ok {
		return s
	}
	ch := " "
	if c := argStr(args, 2); c != "" {
		ch = string([]rune(c)[0:1])
	}
	for len([]rune(s)) < w {
		if left {
			s = ch + s
		} else {
			s = s + ch
		}
	}
	return s
}

func parseTimeLoose(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	layouts := []string{
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
		"01/02/2006",
		"2006/01/02",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func registerExtendedBuiltins() {
	reg := func(names []string, fn func([]interface{}) interface{}) {
		for _, n := range names {
			builtins[strings.ToLower(n)] = fn
		}
	}

	// ── string.* ──
	reg([]string{"string.concat"}, builtins["concat"])
	reg([]string{"string.trim"}, builtins["trim"])
	reg([]string{"string.toUpper"}, builtins["upper"])
	reg([]string{"string.toLower"}, builtins["lower"])
	reg([]string{"string.length"}, func(a []interface{}) interface{} {
		return len([]rune(argStr(a, 0)))
	})
	reg([]string{"string.substring"}, func(a []interface{}) interface{} {
		r := []rune(argStr(a, 0))
		start, _ := argInt(a, 1)
		end := len(r)
		if e, ok := argInt(a, 2); ok {
			end = e
		}
		if start < 0 {
			start = 0
		}
		if end > len(r) {
			end = len(r)
		}
		if start > end {
			return ""
		}
		return string(r[start:end])
	})
	reg([]string{"string.replace"}, func(a []interface{}) interface{} {
		return strings.ReplaceAll(argStr(a, 0), argStr(a, 1), argStr(a, 2))
	})
	reg([]string{"string.indexOf"}, func(a []interface{}) interface{} {
		return strings.Index(argStr(a, 0), argStr(a, 1))
	})
	reg([]string{"string.startsWith"}, func(a []interface{}) interface{} {
		return strings.HasPrefix(argStr(a, 0), argStr(a, 1))
	})
	reg([]string{"string.endsWith"}, func(a []interface{}) interface{} {
		return strings.HasSuffix(argStr(a, 0), argStr(a, 1))
	})
	reg([]string{"string.contains"}, func(a []interface{}) interface{} {
		return strings.Contains(argStr(a, 0), argStr(a, 1))
	})
	reg([]string{"string.padLeft"}, func(a []interface{}) interface{} {
		return padStr(argStr(a, 0), a, true)
	})
	reg([]string{"string.padRight"}, func(a []interface{}) interface{} {
		return padStr(argStr(a, 0), a, false)
	})

	// ── number.* / math.* ──
	reg([]string{"number.parseInt", "integer.parseInt"}, func(a []interface{}) interface{} {
		f, ok := argFloat(a, 0)
		if !ok {
			return nil
		}
		return int(f)
	})
	reg([]string{"number.parseFloat"}, func(a []interface{}) interface{} {
		f, ok := argFloat(a, 0)
		if !ok {
			return nil
		}
		return f
	})
	reg([]string{"number.round", "math.round"}, func(a []interface{}) interface{} {
		f, ok := argFloat(a, 0)
		if !ok {
			return nil
		}
		dec, _ := argInt(a, 1)
		p := math.Pow(10, float64(dec))
		return math.Round(f*p) / p
	})
	reg([]string{"number.floor", "math.floor"}, func(a []interface{}) interface{} {
		f, ok := argFloat(a, 0)
		if !ok {
			return nil
		}
		return math.Floor(f)
	})
	reg([]string{"number.ceil", "math.ceil"}, func(a []interface{}) interface{} {
		f, ok := argFloat(a, 0)
		if !ok {
			return nil
		}
		return math.Ceil(f)
	})
	reg([]string{"number.abs", "math.abs"}, func(a []interface{}) interface{} {
		f, ok := argFloat(a, 0)
		if !ok {
			return nil
		}
		return math.Abs(f)
	})
	reg([]string{"number.min", "math.min"}, func(a []interface{}) interface{} {
		x, ok1 := argFloat(a, 0)
		y, ok2 := argFloat(a, 1)
		if !ok1 || !ok2 {
			return nil
		}
		return math.Min(x, y)
	})
	reg([]string{"number.max", "math.max"}, func(a []interface{}) interface{} {
		x, ok1 := argFloat(a, 0)
		y, ok2 := argFloat(a, 1)
		if !ok1 || !ok2 {
			return nil
		}
		return math.Max(x, y)
	})
	reg([]string{"number.mod", "math.mod"}, func(a []interface{}) interface{} {
		x, ok1 := argFloat(a, 0)
		y, ok2 := argFloat(a, 1)
		if !ok1 || !ok2 || y == 0 {
			return nil
		}
		return math.Mod(x, y)
	})
	reg([]string{"number.add"}, func(a []interface{}) interface{} {
		x, ok1 := argFloat(a, 0)
		y, ok2 := argFloat(a, 1)
		if !ok1 || !ok2 {
			return nil
		}
		return x + y
	})
	reg([]string{"number.subtract"}, func(a []interface{}) interface{} {
		x, ok1 := argFloat(a, 0)
		y, ok2 := argFloat(a, 1)
		if !ok1 || !ok2 {
			return nil
		}
		return x - y
	})
	reg([]string{"number.divide"}, builtins["divide"])
	reg([]string{"number.multiply"}, builtins["multiply"])

	// ── coerce.* ──
	reg([]string{"coerce.toInt"}, func(a []interface{}) interface{} {
		f, ok := argFloat(a, 0)
		if !ok {
			return nil
		}
		return int(f)
	})
	reg([]string{"coerce.toFloat"}, func(a []interface{}) interface{} {
		f, ok := argFloat(a, 0)
		if !ok {
			return nil
		}
		return f
	})
	reg([]string{"coerce.toString"}, func(a []interface{}) interface{} {
		return argStr(a, 0)
	})
	reg([]string{"coerce.toBool"}, func(a []interface{}) interface{} {
		if len(a) > 0 {
			if b, ok := a[0].(bool); ok {
				return b
			}
		}
		s := strings.ToLower(strings.TrimSpace(argStr(a, 0)))
		return s == "true" || s == "1" || s == "yes" || s == "y"
	})

	// ── datetime.* ──
	reg([]string{"datetime.now"}, func(a []interface{}) interface{} {
		return time.Now().UTC().Format(time.RFC3339)
	})
	reg([]string{"datetime.today"}, func(a []interface{}) interface{} {
		return time.Now().UTC().Format("2006-01-02")
	})
	reg([]string{"datetime.format"}, func(a []interface{}) interface{} {
		t, ok := parseTimeLoose(argStr(a, 0))
		if !ok {
			return argStr(a, 0)
		}
		layout := argStr(a, 1)
		if layout == "" {
			layout = time.RFC3339
		}
		return t.Format(layout)
	})
	reg([]string{"datetime.year"}, func(a []interface{}) interface{} {
		if t, ok := parseTimeLoose(argStr(a, 0)); ok {
			return t.Year()
		}
		return nil
	})
	reg([]string{"datetime.month"}, func(a []interface{}) interface{} {
		if t, ok := parseTimeLoose(argStr(a, 0)); ok {
			return int(t.Month())
		}
		return nil
	})
	reg([]string{"datetime.day"}, func(a []interface{}) interface{} {
		if t, ok := parseTimeLoose(argStr(a, 0)); ok {
			return t.Day()
		}
		return nil
	})
	reg([]string{"datetime.addDays"}, func(a []interface{}) interface{} {
		t, ok := parseTimeLoose(argStr(a, 0))
		if !ok {
			return nil
		}
		n, _ := argInt(a, 1)
		return t.AddDate(0, 0, n).Format(time.RFC3339)
	})

	// ── logic / conditional ──
	reg([]string{"iif"}, func(a []interface{}) interface{} {
		if len(a) < 3 {
			return nil
		}
		if truthy(a[0]) {
			return a[1]
		}
		return a[2]
	})
	reg([]string{"isNull"}, func(a []interface{}) interface{} {
		return len(a) == 0 || a[0] == nil
	})
	reg([]string{"isEmpty"}, func(a []interface{}) interface{} {
		return argStr(a, 0) == ""
	})
	reg([]string{"equals"}, func(a []interface{}) interface{} {
		return argStr(a, 0) == argStr(a, 1)
	})
	reg([]string{"notEquals"}, func(a []interface{}) interface{} {
		return argStr(a, 0) != argStr(a, 1)
	})
}

// --- comprehensive ETL function library ---
//
// registerEtlBuiltins rounds out the namespaced library with the functions ETL
// designers reach for most: extended string ops (split/regex/case), math
// (pow/sqrt/sign), rich datetime arithmetic (add/diff/epoch/parts), surrogate-
// key & masking helpers (hash.*, encoding.*, util.uuid), and null/conditional
// logic (nvl/nullif/between/decode). Every entry mirrors a Flogo OOTB function
// family (string.*, number.*, math.*, datetime.*, coerce.*) so the Studio
// palette and the runtime stay in lock-step. Names are registered lowercased
// because compileExpr lowercases before lookup.
func init() { registerEtlBuiltins() }

// regexCache memoizes compiled patterns; row-level transforms call these per
// record, so compiling once per distinct pattern avoids a hot-path recompile.
var (
	regexCache   = map[string]*regexp.Regexp{}
	regexCacheMu sync.RWMutex
)

func cachedRegex(pattern string) *regexp.Regexp {
	regexCacheMu.RLock()
	re := regexCache[pattern]
	regexCacheMu.RUnlock()
	if re != nil {
		return re
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return nil
	}
	regexCacheMu.Lock()
	regexCache[pattern] = compiled
	regexCacheMu.Unlock()
	return compiled
}

func registerEtlBuiltins() {
	reg := func(names []string, fn func([]interface{}) interface{}) {
		for _, n := range names {
			builtins[strings.ToLower(n)] = fn
		}
	}

	// ── string.* (extended) ──
	reg([]string{"string.split"}, func(a []interface{}) interface{} {
		// split(s, sep [, index]) → the whole []string, or the nth token when an
		// index is given (a very common ETL "take the 3rd CSV field" pattern).
		parts := strings.Split(argStr(a, 0), argStr(a, 1))
		if idx, ok := argInt(a, 2); ok {
			if idx < 0 || idx >= len(parts) {
				return nil
			}
			return parts[idx]
		}
		out := make([]interface{}, len(parts))
		for i, p := range parts {
			out[i] = p
		}
		return out
	})
	reg([]string{"string.join"}, func(a []interface{}) interface{} {
		// join(sep, v1, v2, …) — join the trailing values with the first arg.
		if len(a) == 0 {
			return ""
		}
		sep := argStr(a, 0)
		parts := make([]string, 0, len(a)-1)
		for i := 1; i < len(a); i++ {
			parts = append(parts, argStr(a, i))
		}
		return strings.Join(parts, sep)
	})
	reg([]string{"string.title", "string.capitalize"}, func(a []interface{}) interface{} {
		return strings.Title(strings.ToLower(argStr(a, 0)))
	})
	reg([]string{"string.reverse"}, func(a []interface{}) interface{} {
		r := []rune(argStr(a, 0))
		for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
			r[i], r[j] = r[j], r[i]
		}
		return string(r)
	})
	reg([]string{"string.repeat"}, func(a []interface{}) interface{} {
		n, _ := argInt(a, 1)
		if n < 0 {
			n = 0
		}
		return strings.Repeat(argStr(a, 0), n)
	})
	reg([]string{"string.lastIndexOf"}, func(a []interface{}) interface{} {
		return strings.LastIndex(argStr(a, 0), argStr(a, 1))
	})
	reg([]string{"string.equalsIgnoreCase"}, func(a []interface{}) interface{} {
		return strings.EqualFold(argStr(a, 0), argStr(a, 1))
	})
	reg([]string{"string.count"}, func(a []interface{}) interface{} {
		return strings.Count(argStr(a, 0), argStr(a, 1))
	})
	reg([]string{"string.trimLeft"}, func(a []interface{}) interface{} {
		cut := argStr(a, 1)
		if cut == "" {
			return strings.TrimLeft(argStr(a, 0), " \t\n\r")
		}
		return strings.TrimLeft(argStr(a, 0), cut)
	})
	reg([]string{"string.trimRight"}, func(a []interface{}) interface{} {
		cut := argStr(a, 1)
		if cut == "" {
			return strings.TrimRight(argStr(a, 0), " \t\n\r")
		}
		return strings.TrimRight(argStr(a, 0), cut)
	})
	reg([]string{"string.trimPrefix"}, func(a []interface{}) interface{} {
		return strings.TrimPrefix(argStr(a, 0), argStr(a, 1))
	})
	reg([]string{"string.trimSuffix"}, func(a []interface{}) interface{} {
		return strings.TrimSuffix(argStr(a, 0), argStr(a, 1))
	})
	reg([]string{"string.substringBefore"}, func(a []interface{}) interface{} {
		s, sep := argStr(a, 0), argStr(a, 1)
		if i := strings.Index(s, sep); i >= 0 {
			return s[:i]
		}
		return s
	})
	reg([]string{"string.substringAfter"}, func(a []interface{}) interface{} {
		s, sep := argStr(a, 0), argStr(a, 1)
		if i := strings.Index(s, sep); i >= 0 {
			return s[i+len(sep):]
		}
		return ""
	})

	// ── regex.* ──
	reg([]string{"regex.match", "string.match"}, func(a []interface{}) interface{} {
		re := cachedRegex(argStr(a, 1))
		if re == nil {
			return false
		}
		return re.MatchString(argStr(a, 0))
	})
	reg([]string{"regex.replace", "string.replaceRegex"}, func(a []interface{}) interface{} {
		re := cachedRegex(argStr(a, 1))
		if re == nil {
			return argStr(a, 0)
		}
		return re.ReplaceAllString(argStr(a, 0), argStr(a, 2))
	})
	reg([]string{"regex.extract"}, func(a []interface{}) interface{} {
		// extract(s, pattern [, group]) → the matched group (0 = whole match).
		re := cachedRegex(argStr(a, 1))
		if re == nil {
			return nil
		}
		m := re.FindStringSubmatch(argStr(a, 0))
		if m == nil {
			return nil
		}
		grp, _ := argInt(a, 2)
		if grp < 0 || grp >= len(m) {
			return nil
		}
		return m[grp]
	})

	// ── math.* (extended) ──
	reg([]string{"math.pow", "number.pow"}, func(a []interface{}) interface{} {
		x, ok1 := argFloat(a, 0)
		y, ok2 := argFloat(a, 1)
		if !ok1 || !ok2 {
			return nil
		}
		return math.Pow(x, y)
	})
	reg([]string{"math.sqrt", "number.sqrt"}, func(a []interface{}) interface{} {
		x, ok := argFloat(a, 0)
		if !ok || x < 0 {
			return nil
		}
		return math.Sqrt(x)
	})
	reg([]string{"math.sign", "number.sign"}, func(a []interface{}) interface{} {
		x, ok := argFloat(a, 0)
		if !ok {
			return nil
		}
		switch {
		case x > 0:
			return 1
		case x < 0:
			return -1
		default:
			return 0
		}
	})
	reg([]string{"math.trunc", "number.trunc"}, func(a []interface{}) interface{} {
		x, ok := argFloat(a, 0)
		if !ok {
			return nil
		}
		return math.Trunc(x)
	})
	reg([]string{"math.log", "number.log"}, func(a []interface{}) interface{} {
		x, ok := argFloat(a, 0)
		if !ok || x <= 0 {
			return nil
		}
		return math.Log(x)
	})
	reg([]string{"math.log10"}, func(a []interface{}) interface{} {
		x, ok := argFloat(a, 0)
		if !ok || x <= 0 {
			return nil
		}
		return math.Log10(x)
	})
	reg([]string{"math.exp"}, func(a []interface{}) interface{} {
		x, ok := argFloat(a, 0)
		if !ok {
			return nil
		}
		return math.Exp(x)
	})
	reg([]string{"number.random", "math.random"}, func(a []interface{}) interface{} {
		if limit, ok := argInt(a, 0); ok && limit > 0 {
			return mathrand.Intn(limit)
		}
		return mathrand.Float64()
	})

	// ── datetime.* (extended) ──
	reg([]string{"datetime.addHours"}, func(a []interface{}) interface{} {
		t, ok := parseTimeLoose(argStr(a, 0))
		if !ok {
			return nil
		}
		n, _ := argInt(a, 1)
		return t.Add(time.Duration(n) * time.Hour).Format(time.RFC3339)
	})
	reg([]string{"datetime.addMinutes"}, func(a []interface{}) interface{} {
		t, ok := parseTimeLoose(argStr(a, 0))
		if !ok {
			return nil
		}
		n, _ := argInt(a, 1)
		return t.Add(time.Duration(n) * time.Minute).Format(time.RFC3339)
	})
	reg([]string{"datetime.addSeconds"}, func(a []interface{}) interface{} {
		t, ok := parseTimeLoose(argStr(a, 0))
		if !ok {
			return nil
		}
		n, _ := argInt(a, 1)
		return t.Add(time.Duration(n) * time.Second).Format(time.RFC3339)
	})
	reg([]string{"datetime.diffDays"}, func(a []interface{}) interface{} {
		t1, ok1 := parseTimeLoose(argStr(a, 0))
		t2, ok2 := parseTimeLoose(argStr(a, 1))
		if !ok1 || !ok2 {
			return nil
		}
		return int(t1.Sub(t2).Hours() / 24)
	})
	reg([]string{"datetime.diffSeconds"}, func(a []interface{}) interface{} {
		t1, ok1 := parseTimeLoose(argStr(a, 0))
		t2, ok2 := parseTimeLoose(argStr(a, 1))
		if !ok1 || !ok2 {
			return nil
		}
		return int(t1.Sub(t2).Seconds())
	})
	reg([]string{"datetime.hour"}, func(a []interface{}) interface{} {
		if t, ok := parseTimeLoose(argStr(a, 0)); ok {
			return t.Hour()
		}
		return nil
	})
	reg([]string{"datetime.minute"}, func(a []interface{}) interface{} {
		if t, ok := parseTimeLoose(argStr(a, 0)); ok {
			return t.Minute()
		}
		return nil
	})
	reg([]string{"datetime.second"}, func(a []interface{}) interface{} {
		if t, ok := parseTimeLoose(argStr(a, 0)); ok {
			return t.Second()
		}
		return nil
	})
	reg([]string{"datetime.dayOfWeek", "datetime.weekday"}, func(a []interface{}) interface{} {
		if t, ok := parseTimeLoose(argStr(a, 0)); ok {
			return t.Weekday().String()
		}
		return nil
	})
	reg([]string{"datetime.epoch", "datetime.unix", "datetime.toEpoch"}, func(a []interface{}) interface{} {
		if t, ok := parseTimeLoose(argStr(a, 0)); ok {
			return t.Unix()
		}
		return nil
	})
	reg([]string{"datetime.fromEpoch"}, func(a []interface{}) interface{} {
		sec, ok := argFloat(a, 0)
		if !ok {
			return nil
		}
		return time.Unix(int64(sec), 0).UTC().Format(time.RFC3339)
	})
	reg([]string{"datetime.parse"}, func(a []interface{}) interface{} {
		// parse(s [, layout]) → normalize to RFC3339. With a Go layout, parse
		// exactly; otherwise best-effort across common layouts.
		s := argStr(a, 0)
		if layout := argStr(a, 1); layout != "" {
			if t, err := time.Parse(layout, s); err == nil {
				return t.UTC().Format(time.RFC3339)
			}
			return nil
		}
		if t, ok := parseTimeLoose(s); ok {
			return t.UTC().Format(time.RFC3339)
		}
		return nil
	})

	// ── hash.* / encoding.* — surrogate keys, masking, dedup fingerprints ──
	reg([]string{"hash.md5"}, func(a []interface{}) interface{} {
		sum := md5.Sum([]byte(concatArgs(a)))
		return hex.EncodeToString(sum[:])
	})
	reg([]string{"hash.sha1"}, func(a []interface{}) interface{} {
		sum := sha1.Sum([]byte(concatArgs(a)))
		return hex.EncodeToString(sum[:])
	})
	reg([]string{"hash.sha256"}, func(a []interface{}) interface{} {
		sum := sha256.Sum256([]byte(concatArgs(a)))
		return hex.EncodeToString(sum[:])
	})
	reg([]string{"encoding.base64Encode", "encoding.encodeBase64"}, func(a []interface{}) interface{} {
		return base64.StdEncoding.EncodeToString([]byte(argStr(a, 0)))
	})
	reg([]string{"encoding.base64Decode", "encoding.decodeBase64"}, func(a []interface{}) interface{} {
		b, err := base64.StdEncoding.DecodeString(argStr(a, 0))
		if err != nil {
			return nil
		}
		return string(b)
	})
	reg([]string{"encoding.hexEncode"}, func(a []interface{}) interface{} {
		return hex.EncodeToString([]byte(argStr(a, 0)))
	})
	reg([]string{"util.uuid", "uuid"}, func(a []interface{}) interface{} {
		return newUUIDv4()
	})
	reg([]string{"util.mask"}, func(a []interface{}) interface{} {
		// mask(s [, keepLast]) → replace all but the last N chars with '*'.
		s := []rune(argStr(a, 0))
		keep, _ := argInt(a, 1)
		if keep < 0 {
			keep = 0
		}
		if keep >= len(s) {
			return string(s)
		}
		for i := 0; i < len(s)-keep; i++ {
			s[i] = '*'
		}
		return string(s)
	})

	// ── logic / null handling (SQL-style) ──
	reg([]string{"nvl", "ifNull"}, func(a []interface{}) interface{} {
		if len(a) > 0 && a[0] != nil && argStr(a, 0) != "" {
			return a[0]
		}
		if len(a) > 1 {
			return a[1]
		}
		return nil
	})
	reg([]string{"nullif"}, func(a []interface{}) interface{} {
		if len(a) >= 2 && argStr(a, 0) == argStr(a, 1) {
			return nil
		}
		if len(a) > 0 {
			return a[0]
		}
		return nil
	})
	reg([]string{"between"}, func(a []interface{}) interface{} {
		v, ok0 := argFloat(a, 0)
		lo, ok1 := argFloat(a, 1)
		hi, ok2 := argFloat(a, 2)
		if !ok0 || !ok1 || !ok2 {
			return false
		}
		return v >= lo && v <= hi
	})
	reg([]string{"greaterThan"}, func(a []interface{}) interface{} {
		x, ok1 := argFloat(a, 0)
		y, ok2 := argFloat(a, 1)
		return ok1 && ok2 && x > y
	})
	reg([]string{"lessThan"}, func(a []interface{}) interface{} {
		x, ok1 := argFloat(a, 0)
		y, ok2 := argFloat(a, 1)
		return ok1 && ok2 && x < y
	})
	reg([]string{"and"}, func(a []interface{}) interface{} {
		for _, v := range a {
			if !truthy(v) {
				return false
			}
		}
		return len(a) > 0
	})
	reg([]string{"or"}, func(a []interface{}) interface{} {
		for _, v := range a {
			if truthy(v) {
				return true
			}
		}
		return false
	})
	reg([]string{"not"}, func(a []interface{}) interface{} {
		return len(a) > 0 && !truthy(a[0])
	})
	reg([]string{"decode"}, func(a []interface{}) interface{} {
		// decode(expr, s1,r1, s2,r2, … [, default]) — Oracle-style value map.
		if len(a) < 3 {
			return nil
		}
		key := argStr(a, 0)
		i := 1
		for ; i+1 < len(a); i += 2 {
			if argStr(a, i) == key {
				return a[i+1]
			}
		}
		if i < len(a) { // trailing default
			return a[i]
		}
		return nil
	})
}

// concatArgs stringifies and joins all args — used by the hash.* helpers so a
// composite surrogate key can be built from several columns in one call.
func concatArgs(args []interface{}) string {
	var sb strings.Builder
	for _, a := range args {
		if a != nil {
			sb.WriteString(fmt.Sprintf("%v", a))
		}
	}
	return sb.String()
}

// newUUIDv4 returns a random RFC-4122 v4 UUID without pulling in a dependency.
func newUUIDv4() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
