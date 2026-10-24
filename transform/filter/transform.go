// Package filter registers a row-filtering transform.
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/transform/filter"
//
// Config:
//
//	{ "type": "filter", "expr": "amount > 0 AND status != 'VOID'" }
package filter

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func init() {
	transform.Register("filter", func(cfg map[string]interface{}) (transform.Transform, error) {
		expr, _ := cfg["expr"].(string)
		if expr == "" {
			return nil, fmt.Errorf("filter transform: 'expr' is required")
		}
		cond, err := parseCondition(expr)
		if err != nil {
			return nil, fmt.Errorf("filter transform: parse expr %q: %w", expr, err)
		}
		return &filterTransform{expr: expr, cond: cond}, nil
	})
}

type filterTransform struct {
	expr string
	cond condition
	// Position-bound form of cond for the most recent schema; rebuilt only when
	// the column layout changes (see record.SameColumnNames).
	boundCols []record.Column
	bound     boundCond
}

func (f *filterTransform) Name() string { return "filter" }

func (f *filterTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	if f.bound == nil || !record.SameColumnNames(f.boundCols, in.Columns) {
		f.bound, f.boundCols = f.cond.bind(in.Columns), in.Columns
	}
	out := in.Clone(in.Len())
	out.Columns = in.Columns
	for i, row := range in.Rows {
		if f.bound.evalRow(row) {
			out.Rows = append(out.Rows, in.Rows[i])
		}
	}
	if len(out.Rows) == 0 {
		record.Put(out)
		return nil, nil
	}
	return out, nil
}

func (f *filterTransform) Close() error { return nil }

// Predicate is a compiled boolean row expression using the filter grammar.
// It lets other transforms (e.g. router) reuse the exact same predicate language
// as the filter transform's "expr" without duplicating the parser.
type Predicate struct{ cond condition }

// Compile parses a filter expression (same grammar as the filter transform's
// "expr": field op literal joined by AND/OR, with IN / LIKE / IS [NOT] NULL)
// into a reusable Predicate.
func Compile(expr string) (Predicate, error) {
	c, err := parseCondition(expr)
	if err != nil {
		return Predicate{}, err
	}
	return Predicate{cond: c}, nil
}

// Eval reports whether the row satisfies the predicate. A zero-value Predicate
// (no compiled condition) evaluates to false.
func (p Predicate) Eval(row map[string]interface{}) bool {
	return p.cond != nil && p.cond.eval(row)
}

// BoundPredicate is a Predicate resolved against one column layout, so rows are
// evaluated straight from their []interface{} slice without building a map.
type BoundPredicate struct{ cond boundCond }

// Bind resolves the predicate's field references to positions in cols.
func (p Predicate) Bind(cols []record.Column) BoundPredicate {
	if p.cond == nil {
		return BoundPredicate{}
	}
	return BoundPredicate{cond: p.cond.bind(cols)}
}

// EvalRow reports whether row (laid out per the bound columns) satisfies the
// predicate; it agrees with Eval on the equivalent row map.
func (b BoundPredicate) EvalRow(row []interface{}) bool {
	return b.cond != nil && b.cond.evalRow(row)
}

// --- expression parser ---
// Supports: field op literal [AND|OR field op literal ...]
// op: > >= < <= = != IS NULL IS NOT NULL
// literal: 'string' | number | true | false | null

type condition interface {
	eval(row map[string]interface{}) bool
	bind(cols []record.Column) boundCond
}

// boundCond evaluates a condition against a positional row.
type boundCond interface {
	evalRow(row []interface{}) bool
}

type andCond struct{ parts []condition }
type orCond struct{ parts []condition }
type simpleCond struct {
	field string
	op    string
	lit   literal
}
type notNullCond struct{ field string }
type isNullCond struct{ field string }

// literal is a right-hand operand with its numeric and string forms computed
// once at parse time instead of on every row.
type literal struct {
	val   interface{} // nil means NULL
	num   float64
	isNum bool
	str   string
}

func newLiteral(v interface{}) literal {
	l := literal{val: v}
	if v != nil {
		l.num, l.isNum = toFloat(v)
		l.str = fmt.Sprintf("%v", v)
	}
	return l
}

// cell returns row[idx] and whether the column exists in the row — the
// positional equivalent of a map lookup's (value, ok).
func cell(row []interface{}, idx int) (interface{}, bool) {
	if idx < 0 || idx >= len(row) {
		return nil, false
	}
	return row[idx], true
}

func fieldIndex(cols []record.Column, field string) int {
	return record.IndexOf(cols, field)[0]
}

func (c andCond) eval(row map[string]interface{}) bool {
	for _, p := range c.parts {
		if !p.eval(row) {
			return false
		}
	}
	return true
}

func (c orCond) eval(row map[string]interface{}) bool {
	for _, p := range c.parts {
		if p.eval(row) {
			return true
		}
	}
	return false
}

func (c notNullCond) eval(row map[string]interface{}) bool {
	v, ok := row[c.field]
	return ok && v != nil
}

func (c isNullCond) eval(row map[string]interface{}) bool {
	v, ok := row[c.field]
	return !ok || v == nil
}

func (c simpleCond) eval(row map[string]interface{}) bool {
	return compareLit(row[c.field], c.op, &c.lit)
}

type boundAnd []boundCond
type boundOr []boundCond
type boundNotNull int
type boundIsNull int
type boundSimple struct {
	idx int
	op  string
	lit literal
}

func bindAll(parts []condition, cols []record.Column) []boundCond {
	out := make([]boundCond, len(parts))
	for i, p := range parts {
		out[i] = p.bind(cols)
	}
	return out
}

func (c andCond) bind(cols []record.Column) boundCond { return boundAnd(bindAll(c.parts, cols)) }
func (c orCond) bind(cols []record.Column) boundCond  { return boundOr(bindAll(c.parts, cols)) }
func (c notNullCond) bind(cols []record.Column) boundCond {
	return boundNotNull(fieldIndex(cols, c.field))
}
func (c isNullCond) bind(cols []record.Column) boundCond {
	return boundIsNull(fieldIndex(cols, c.field))
}
func (c simpleCond) bind(cols []record.Column) boundCond {
	return &boundSimple{idx: fieldIndex(cols, c.field), op: c.op, lit: c.lit}
}

func (c boundAnd) evalRow(row []interface{}) bool {
	for _, p := range c {
		if !p.evalRow(row) {
			return false
		}
	}
	return true
}

func (c boundOr) evalRow(row []interface{}) bool {
	for _, p := range c {
		if p.evalRow(row) {
			return true
		}
	}
	return false
}

func (c boundNotNull) evalRow(row []interface{}) bool {
	v, ok := cell(row, int(c))
	return ok && v != nil
}

func (c boundIsNull) evalRow(row []interface{}) bool {
	v, ok := cell(row, int(c))
	return !ok || v == nil
}

func (c *boundSimple) evalRow(row []interface{}) bool {
	v, _ := cell(row, c.idx)
	return compareLit(v, c.op, &c.lit)
}

func compareLit(left interface{}, op string, right *literal) bool {
	if right.val == nil {
		switch op {
		case "=", "IS":
			return left == nil
		case "!=", "IS NOT":
			return left != nil
		}
		return false
	}
	// A NULL left operand never satisfies ordering or equality comparisons
	// against a non-NULL right operand (SQL three-valued logic).
	if left == nil {
		return false
	}
	// Numeric comparison.
	if right.isNum {
		if lf, ok := toFloat(left); ok {
			return compareFloat(lf, op, right.num)
		}
	}
	ls, rs := record.FormatValue(left), right.str
	switch op {
	case "=":
		return ls == rs
	case "!=":
		return ls != rs
	case ">":
		return ls > rs
	case ">=":
		return ls >= rs
	case "<":
		return ls < rs
	case "<=":
		return ls <= rs
	}
	return false
}

func compareFloat(lf float64, op string, rf float64) bool {
	switch op {
	case "=":
		return lf == rf
	case "!=":
		return lf != rf
	case ">":
		return lf > rf
	case ">=":
		return lf >= rf
	case "<":
		return lf < rf
	case "<=":
		return lf <= rf
	}
	return false
}

func toFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case record.Decimal:
		return n.Float64(), true
	case string:
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// parseCondition parses simple boolean expressions.
// Grammar (simplified):
//
//	expr    = term (AND term)*  (OR is supported only at top level for now)
//	term    = field IS NULL | field IS NOT NULL | field op literal
//	op      = > | >= | < | <= | = | !=
//	literal = 'str' | number | true | false | null
func parseCondition(expr string) (condition, error) {
	tokens := tokenize(expr)
	parts, err := parseOr(tokens)
	if err != nil {
		return nil, err
	}
	return parts, nil
}

func parseOr(tokens []string) (condition, error) {
	// Split on top-level OR.
	var orParts [][]string
	cur := []string{}
	for _, t := range tokens {
		if strings.EqualFold(t, "OR") {
			orParts = append(orParts, cur)
			cur = []string{}
		} else {
			cur = append(cur, t)
		}
	}
	orParts = append(orParts, cur)

	if len(orParts) == 1 {
		return parseAnd(orParts[0])
	}
	conds := make([]condition, 0, len(orParts))
	for _, part := range orParts {
		c, err := parseAnd(part)
		if err != nil {
			return nil, err
		}
		conds = append(conds, c)
	}
	return orCond{conds}, nil
}

func parseAnd(tokens []string) (condition, error) {
	// Split on AND.
	var andParts [][]string
	cur := []string{}
	for _, t := range tokens {
		if strings.EqualFold(t, "AND") {
			andParts = append(andParts, cur)
			cur = []string{}
		} else {
			cur = append(cur, t)
		}
	}
	andParts = append(andParts, cur)

	if len(andParts) == 1 {
		return parseTerm(andParts[0])
	}
	conds := make([]condition, 0, len(andParts))
	for _, part := range andParts {
		c, err := parseTerm(part)
		if err != nil {
			return nil, err
		}
		conds = append(conds, c)
	}
	return andCond{conds}, nil
}

func parseTerm(tokens []string) (condition, error) {
	if len(tokens) < 2 {
		return nil, fmt.Errorf("incomplete condition: %v", tokens)
	}
	field := tokens[0]

	// IS NULL / IS NOT NULL
	if strings.EqualFold(tokens[1], "IS") {
		if len(tokens) == 3 && strings.EqualFold(tokens[2], "NULL") {
			return isNullCond{field}, nil
		}
		if len(tokens) == 4 && strings.EqualFold(tokens[2], "NOT") && strings.EqualFold(tokens[3], "NULL") {
			return notNullCond{field}, nil
		}
		return nil, fmt.Errorf("expected IS NULL or IS NOT NULL, got: %v", tokens)
	}

	if len(tokens) < 3 {
		return nil, fmt.Errorf("incomplete condition: %v", tokens)
	}
	op := tokens[1]
	litStr := strings.Join(tokens[2:], " ")
	lit, err := parseLiteral(litStr)
	if err != nil {
		return nil, fmt.Errorf("bad literal %q: %w", litStr, err)
	}
	return simpleCond{field: field, op: op, lit: newLiteral(lit)}, nil
}

func parseLiteral(s string) (interface{}, error) {
	s = strings.TrimSpace(s)
	if strings.EqualFold(s, "null") {
		return nil, nil
	}
	if strings.EqualFold(s, "true") {
		return true, nil
	}
	if strings.EqualFold(s, "false") {
		return false, nil
	}
	if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") {
		return s[1 : len(s)-1], nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, nil
	}
	return nil, fmt.Errorf("unrecognized literal: %q", s)
}

// tokenize splits an expression into tokens, preserving quoted strings.
func tokenize(expr string) []string {
	var tokens []string
	var cur strings.Builder
	inQuote := false

	for _, r := range expr {
		switch {
		case r == '\'' && !inQuote:
			inQuote = true
			cur.WriteRune(r)
		case r == '\'' && inQuote:
			inQuote = false
			cur.WriteRune(r)
		case (unicode.IsSpace(r) || r == ',') && !inQuote:
			if cur.Len() > 0 {
				tokens = append(tokens, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		tokens = append(tokens, cur.String())
	}
	return tokens
}
