package rest

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
)

// paginationConfig holds the parsed "pagination" block of the source config.
// See the package doc for the meaning of each pagination type.
type paginationConfig struct {
	Type string // "none" | "page" | "offset" | "cursor"

	PageParam     string
	PageSizeParam string
	StartPage     int
	PageSize      int

	OffsetParam string
	LimitParam  string
	StartOffset int

	CursorParam    string
	NextCursorPath string
	NextURLPath    string

	// MaxPages caps the number of pages fetched across ALL pagination types
	// (including "cursor") as a safety valve against a misbehaving or
	// misconfigured endpoint that never signals completion. 0 = unlimited.
	MaxPages int
}

// paginationState is the per-source, mutable cursor advanced after each page.
type paginationState struct {
	Page         int
	Offset       int
	Cursor       string // set once a cursor-mode response has provided one
	NextURL      string // set once a nextUrlPath-mode response has provided one
	PagesFetched int
}

// parsePaginationConfig parses cfg["pagination"] (nil means "no pagination").
func parsePaginationConfig(raw interface{}) (paginationConfig, error) {
	pc := paginationConfig{
		Type:          "none",
		PageParam:     "page",
		PageSizeParam: "limit",
		StartPage:     1,
		PageSize:      100,
		OffsetParam:   "offset",
		LimitParam:    "limit",
		CursorParam:   "cursor",
	}
	if raw == nil {
		return pc, nil
	}
	m, ok := raw.(map[string]interface{})
	if !ok {
		return pc, fmt.Errorf("'pagination' must be a JSON object")
	}

	if t, ok := m["type"].(string); ok && t != "" {
		pc.Type = t
	}
	if v, ok := m["pageParam"].(string); ok && v != "" {
		pc.PageParam = v
	}
	if v, ok := m["pageSizeParam"].(string); ok && v != "" {
		pc.PageSizeParam = v
	}
	if v, ok := intFromMap(m, "startPage"); ok {
		pc.StartPage = v
	}
	if v, ok := intFromMap(m, "pageSize"); ok {
		pc.PageSize = v
	}
	if v, ok := m["offsetParam"].(string); ok && v != "" {
		pc.OffsetParam = v
	}
	if v, ok := m["limitParam"].(string); ok && v != "" {
		pc.LimitParam = v
	}
	if v, ok := intFromMap(m, "startOffset"); ok {
		pc.StartOffset = v
	}
	if v, ok := m["cursorParam"].(string); ok && v != "" {
		pc.CursorParam = v
	}
	if v, ok := m["nextCursorPath"].(string); ok {
		pc.NextCursorPath = v
	}
	if v, ok := m["nextUrlPath"].(string); ok {
		pc.NextURLPath = v
	}
	if v, ok := intFromMap(m, "maxPages"); ok {
		pc.MaxPages = v
	}

	switch pc.Type {
	case "none", "page", "offset":
	case "cursor":
		if pc.NextCursorPath == "" && pc.NextURLPath == "" {
			return pc, fmt.Errorf("pagination type 'cursor' requires 'nextCursorPath' or 'nextUrlPath'")
		}
	default:
		return pc, fmt.Errorf("unknown pagination type %q (want none|page|offset|cursor)", pc.Type)
	}
	if pc.PageSize <= 0 {
		pc.PageSize = 100
	}
	return pc, nil
}

func intFromMap(m map[string]interface{}, key string) (int, bool) {
	switch v := m[key].(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64: // JSON numbers decode as float64
		return int(v), true
	}
	return 0, false
}

func (pc paginationConfig) initialState() paginationState {
	return paginationState{Page: pc.StartPage, Offset: pc.StartOffset}
}

// buildRequestURL constructs the URL for the next request given the current
// pagination state. For "cursor" mode with a next-URL already captured, that
// URL is used verbatim (it is assumed to be a complete, ready-to-fetch link —
// e.g. a GitHub-style pagination link — so static query params are not
// re-applied to avoid conflicting with what the API already encoded there).
func (pc paginationConfig) buildRequestURL(base string, staticQuery map[string]string, st paginationState) (string, error) {
	if pc.Type == "cursor" && pc.NextURLPath != "" && st.NextURL != "" {
		return st.NextURL, nil
	}

	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	q := u.Query()
	for k, v := range staticQuery {
		q.Set(k, v)
	}
	switch pc.Type {
	case "page":
		q.Set(pc.PageParam, strconv.Itoa(st.Page))
		q.Set(pc.PageSizeParam, strconv.Itoa(pc.PageSize))
	case "offset":
		q.Set(pc.OffsetParam, strconv.Itoa(st.Offset))
		q.Set(pc.LimitParam, strconv.Itoa(pc.PageSize))
	case "cursor":
		if st.Cursor != "" {
			q.Set(pc.CursorParam, st.Cursor)
		}
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// advance updates st for the NEXT fetch based on the just-fetched page
// (parsed is the whole decoded response body; recsInPage is the row count
// extracted from dataPath) and reports whether there is a next page to fetch.
func (pc paginationConfig) advance(st *paginationState, parsed interface{}, recsInPage int) bool {
	st.PagesFetched++
	if pc.MaxPages > 0 && st.PagesFetched >= pc.MaxPages {
		return false
	}

	switch pc.Type {
	case "none":
		return false
	case "page":
		if recsInPage < pc.PageSize {
			return false
		}
		st.Page++
		return true
	case "offset":
		if recsInPage < pc.PageSize {
			return false
		}
		st.Offset += pc.PageSize
		return true
	case "cursor":
		if pc.NextURLPath != "" {
			v, err := walkPath(parsed, pc.NextURLPath)
			if err != nil {
				return false
			}
			s, ok := v.(string)
			if !ok || s == "" {
				return false
			}
			st.NextURL = s
			return true
		}
		v, err := walkPath(parsed, pc.NextCursorPath)
		if err != nil {
			return false
		}
		cursor := formatCursorValue(v)
		if cursor == "" {
			return false
		}
		st.Cursor = cursor
		return true
	default:
		return false
	}
}

// formatCursorValue renders a cursor extracted from JSON (string, number, or
// absent) as the string sent on the query parameter. Whole-number floats
// (JSON's only numeric type) are rendered without a decimal point/exponent so
// e.g. an integer offset-style cursor round-trips as "1500", not "1.5e+03".
func formatCursorValue(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		if x == math.Trunc(x) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case nil:
		return ""
	default:
		return fmt.Sprint(x)
	}
}
