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
	out.Columns = m.outCols
	out.Rows = make([][]interface{}, 0, in.Len())

	for i := range in.Rows {
		row := in.RowMap(i)
		outRow := make([]interface{}, len(m.fieldExprs))
		for j, fe := range m.fieldExprs {
			outRow[j] = fe.eval(row)
		}
		out.Rows = append(out.Rows, outRow)
	}
	return out, nil
}

func (m *mapperTransform) Close() error { return nil }

// compileExpr compiles an expression string into an evaluator function.
func compileExpr(expr string) (func(row map[string]interface{}) interface{}, error) {
	if !strings.HasPrefix(expr, "=") {
		// Static string constant.
		return func(_ map[string]interface{}) interface{} { return expr }, nil
	}

	inner := strings.TrimPrefix(expr, "=")

	// Field reference: =$fieldName or =$.fieldName
	if strings.HasPrefix(inner, "$") {
		fieldName := strings.TrimPrefix(strings.TrimPrefix(inner, "$"), ".")
		return func(row map[string]interface{}) interface{} {
			return row[fieldName]
		}, nil
	}

	// Function call: =funcName(arg1, arg2, ...)
	parenIdx := strings.Index(inner, "(")
	if parenIdx > 0 && strings.HasSuffix(inner, ")") {
		funcName := strings.ToLower(inner[:parenIdx])
		argsStr := inner[parenIdx+1 : len(inner)-1]
		args, err := splitArgs(argsStr)
		if err != nil {
			return nil, fmt.Errorf("bad function call %q: %w", expr, err)
		}
		argEvals := make([]func(map[string]interface{}) interface{}, len(args))
		for i, arg := range args {
			arg = strings.TrimSpace(arg)
			eval, err := compileExpr(arg)
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

	return nil, fmt.Errorf("unrecognized expression: %q", expr)
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
