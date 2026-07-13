package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// handleIntrospectTables handles POST /v1/introspect/tables.
// Body: { "driver": "postgres", "dsn": "..." }
// Returns { "tables": ["public.orders", "customers", ...] }.
func (s *Server) handleIntrospectTables(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	var body struct {
		Driver string `json:"driver"`
		DSN    string `json:"dsn"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error())
		return
	}
	if body.Driver == "" || body.DSN == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "driver and dsn are required")
		return
	}
	tables, err := introspectTables(body.Driver, body.DSN)
	if err != nil {
		writeError(w, http.StatusBadGateway, "introspect_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"tables": sortedTableNames(tables)})
}

// handleIntrospectColumns handles POST /v1/introspect/columns.
// Body: { "driver": "postgres", "dsn": "...", "table": "public.orders" }
// Returns { "columns": [{ "name": "id", "type": "int", "dbType": "integer", "nullable": false }, ...] }.
func (s *Server) handleIntrospectColumns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	var body struct {
		Driver string `json:"driver"`
		DSN    string `json:"dsn"`
		Table  string `json:"table"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error())
		return
	}
	if body.Driver == "" || body.DSN == "" || body.Table == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "driver, dsn and table are required")
		return
	}
	cols, err := introspectColumns(body.Driver, body.DSN, body.Table)
	if err != nil {
		writeError(w, http.StatusBadGateway, "introspect_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"columns": cols})
}

// TableInfo is a discovered database table.
type TableInfo struct {
	Name   string `json:"name"`
	Schema string `json:"schema,omitempty"`
}

// ColumnInfo is a discovered table column mapped to a pipeline column type.
type ColumnInfo struct {
	Name string `json:"name"`
	// Type is the pipeline column type (string|int|float|bool|date|decimal).
	Type string `json:"type"`
	// DBType is the raw database type (for display / debugging).
	DBType   string `json:"dbType,omitempty"`
	Nullable bool   `json:"nullable,omitempty"`
}

// introspectTables lists user tables for a driver + DSN. Supports postgres,
// mysql, and sqlite. Uses the standard database/sql drivers registered in main.
func introspectTables(driver, dsn string) ([]TableInfo, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	var query string
	switch driver {
	case "postgres", "pgx":
		query = `SELECT table_schema, table_name FROM information_schema.tables
		         WHERE table_type='BASE TABLE'
		           AND table_schema NOT IN ('pg_catalog','information_schema')
		         ORDER BY table_schema, table_name`
	case "mysql":
		query = `SELECT table_schema, table_name FROM information_schema.tables
		         WHERE table_type='BASE TABLE'
		           AND table_schema NOT IN ('mysql','sys','performance_schema','information_schema')
		         ORDER BY table_schema, table_name`
	case "sqlite", "sqlite3":
		query = `SELECT '' AS table_schema, name AS table_name FROM sqlite_master
		         WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
	default:
		return nil, fmt.Errorf("table introspection not supported for driver %q", driver)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()

	var out []TableInfo
	for rows.Next() {
		var schema, name string
		if err := rows.Scan(&schema, &name); err != nil {
			return nil, err
		}
		out = append(out, TableInfo{Name: name, Schema: schema})
	}
	return out, rows.Err()
}

// introspectColumns returns the column schema of a table, mapping DB types to
// pipeline column types. table may be "schema.name" or just "name".
func introspectColumns(driver, dsn, table string) ([]ColumnInfo, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	schema, name := splitTable(table)

	switch driver {
	case "postgres", "pgx", "mysql":
		q := `SELECT column_name, data_type, is_nullable FROM information_schema.columns
		      WHERE table_name = $1`
		args := []interface{}{name}
		if driver == "mysql" {
			q = strings.ReplaceAll(q, "$1", "?")
		}
		if schema != "" {
			if driver == "mysql" {
				q += " AND table_schema = ?"
			} else {
				q += " AND table_schema = $2"
			}
			args = append(args, schema)
		}
		q += " ORDER BY ordinal_position"
		rows, err := db.Query(q, args...)
		if err != nil {
			return nil, fmt.Errorf("describe table: %w", err)
		}
		defer rows.Close()
		var out []ColumnInfo
		for rows.Next() {
			var col, dbType, nullable string
			if err := rows.Scan(&col, &dbType, &nullable); err != nil {
				return nil, err
			}
			out = append(out, ColumnInfo{Name: col, Type: mapDBType(dbType), DBType: dbType, Nullable: nullable == "YES"})
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("table %q not found or has no columns", table)
		}
		return out, nil
	case "sqlite", "sqlite3":
		rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", quoteIdent(name)))
		if err != nil {
			return nil, fmt.Errorf("describe table: %w", err)
		}
		defer rows.Close()
		var out []ColumnInfo
		for rows.Next() {
			var cid int
			var col, dbType string
			var notnull, pk int
			var dflt sql.NullString
			if err := rows.Scan(&cid, &col, &dbType, &notnull, &dflt, &pk); err != nil {
				return nil, err
			}
			out = append(out, ColumnInfo{Name: col, Type: mapDBType(dbType), DBType: dbType, Nullable: notnull == 0})
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("table %q not found or has no columns", table)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("column introspection not supported for driver %q", driver)
	}
}

func splitTable(table string) (schema, name string) {
	if i := strings.LastIndex(table, "."); i >= 0 {
		return table[:i], table[i+1:]
	}
	return "", table
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// mapDBType maps a database column type to a pipeline column type.
func mapDBType(dbType string) string {
	t := strings.ToLower(strings.TrimSpace(dbType))
	// Strip any size/precision, e.g. "varchar(255)" -> "varchar".
	if i := strings.IndexAny(t, "( "); i >= 0 {
		t = t[:i]
	}
	switch {
	case strings.Contains(t, "int"): // int, integer, bigint, smallint, tinyint
		return "int"
	case t == "numeric" || t == "decimal" || t == "money":
		return "decimal"
	case strings.Contains(t, "float") || strings.Contains(t, "double") || t == "real":
		return "float"
	case strings.Contains(t, "bool"):
		return "bool"
	case strings.Contains(t, "date") || strings.Contains(t, "time"): // date, timestamp, datetime, timestamptz
		return "date"
	default:
		return "string"
	}
}

// sortedTableNames returns "schema.name" (or just "name" for the public/default
// schema) for a set of tables, sorted for stable display.
func sortedTableNames(tables []TableInfo) []string {
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		if t.Schema == "" || t.Schema == "public" || t.Schema == "main" {
			names = append(names, t.Name)
		} else {
			names = append(names, t.Schema+"."+t.Name)
		}
	}
	sort.Strings(names)
	return names
}
