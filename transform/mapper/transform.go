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
	"fmt"
	"sort"
	"strconv"
	"strings"
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
	if ordered, ok := cfg["fieldOrder"].([]interface{}); ok && len(ordered) == len(keys) {
		keys = keys[:0]
		for _, v := range ordered {
			if s, ok := v.(string); ok {
				keys = append(keys, s)
			}
		}
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
			eval, err := compileInner(strings.TrimSpace(arg))
			if err != nil {
				return nil, fmt.Errorf("arg %d of %s: %w", i, funcName, err)
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
	"coalesce": func(args []interface{}) interface{} {
		for _, a := range args {
			if a != nil {
				return a
			}
		}
		return nil
	},
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
