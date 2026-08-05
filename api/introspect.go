package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/internal/sqlutil"
	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
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
	if err := json.NewDecoder(limitBody(w, r)).Decode(&body); err != nil {
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
	if err := json.NewDecoder(limitBody(w, r)).Decode(&body); err != nil {
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

// handleIntrospectSourceSchema handles POST /v1/introspect/source-schema.
// Body: { "type": "parquet"|"csv"|"jsonlines"|"xml"|..., "config": { ...source config... } }
// Opens the source and returns its column schema (mapped to pipeline types) —
// self-describing for Parquet (footer), header-derived for CSV/TSV, sampled for
// JSON-lines. Side-effect-free: reads at most one small batch, then closes.
func (s *Server) handleIntrospectSourceSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	var body struct {
		Type   string                 `json:"type"`
		Config map[string]interface{} `json:"config"`
	}
	if err := json.NewDecoder(limitBody(w, r)).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error())
		return
	}
	if body.Type == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "source type is required")
		return
	}
	cols, err := introspectSourceSchema(body.Type, body.Config)
	if err != nil {
		writeError(w, http.StatusBadGateway, "introspect_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"columns": cols})
}

// handleIntrospectObjects handles POST /v1/introspect/objects.
// Body: { "provider": "s3"|"gcs"|"adls", "config": { bucket, prefix, ... } }
// Lists objects in the bucket under an optional prefix so users can browse and
// pick a file/prefix instead of hand-typing keys.
func (s *Server) handleIntrospectObjects(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	var body struct {
		Provider string                 `json:"provider"`
		Config   map[string]interface{} `json:"config"`
	}
	if err := json.NewDecoder(limitBody(w, r)).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error())
		return
	}
	if body.Provider == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "provider is required")
		return
	}
	objs, err := introspectObjects(body.Provider, body.Config)
	if err != nil {
		writeError(w, http.StatusBadGateway, "introspect_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"objects": objs})
}

// introspectSourceSchema opens a source and returns its column schema. It reads
// at most one small batch to force schema resolution for formats that determine
// columns lazily (CSV header, JSON-lines sampling), then closes.
func introspectSourceSchema(typ string, cfg map[string]interface{}) ([]ColumnInfo, error) {
	if cfg == nil {
		cfg = map[string]interface{}{}
	}
	src, err := source.New(typ, cfg)
	if err != nil {
		return nil, fmt.Errorf("open source: %w", err)
	}
	defer src.Close()

	schema := src.Schema()
	if len(schema) == 0 {
		// Force lazy schema resolution by reading a tiny batch.
		batch, rerr := src.ReadBatch(1)
		if rerr != nil && rerr != io.EOF {
			return nil, fmt.Errorf("read sample: %w", rerr)
		}
		if batch != nil {
			if len(batch.Columns) > 0 {
				schema = batch.Columns
			}
			record.Put(batch)
		}
		if len(schema) == 0 {
			schema = src.Schema()
		}
	}
	if len(schema) == 0 {
		return nil, fmt.Errorf("source schema is empty — the source may need explicit columns")
	}
	out := make([]ColumnInfo, len(schema))
	for i, c := range schema {
		out[i] = ColumnInfo{Name: c.Name, Type: string(c.Type), DBType: string(c.Type)}
	}
	return out, nil
}

// ObjectInfo is a discovered object-store object.
type ObjectInfo struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
}

// introspectObjects lists objects in an object store under an optional prefix.
func introspectObjects(provider string, cfg map[string]interface{}) ([]ObjectInfo, error) {
	if cfg == nil {
		cfg = map[string]interface{}{}
	}
	store, err := objstore.New(provider, cfg)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	prefix, _ := cfg["prefix"].(string)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	objs, err := store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}
	// Cap the response so a huge bucket doesn't overwhelm the UI.
	const maxObjects = 500
	out := make([]ObjectInfo, 0, len(objs))
	for _, o := range objs {
		out = append(out, ObjectInfo{Key: o.Key, Size: o.Size})
		if len(out) >= maxObjects {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
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
// mysql, SQL Server, and Oracle. Uses the standard database/sql drivers registered in main.
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
	case "sqlserver", "mssql":
		query = `SELECT table_schema, table_name FROM information_schema.tables
		         WHERE table_type='BASE TABLE'
		           AND table_schema NOT IN ('sys','INFORMATION_SCHEMA')
		         ORDER BY table_schema, table_name`
	case "oracle":
		// USER_TABLES lists the connected schema's own tables — the useful default
		// for a DI connection. USER is Oracle's current-schema pseudo-column.
		query = `SELECT USER AS table_schema, table_name FROM user_tables ORDER BY table_name`
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
	case "postgres", "pgx", "mysql", "sqlserver", "mssql":
		// All expose ANSI information_schema.columns; only the bind placeholder
		// differs (handled by sqlutil.Placeholder).
		q := `SELECT column_name, data_type, is_nullable FROM information_schema.columns
		      WHERE table_name = ` + sqlutil.Placeholder(1, driver)
		args := []interface{}{name}
		if schema != "" {
			q += " AND table_schema = " + sqlutil.Placeholder(2, driver)
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
	case "oracle":
		// Oracle stores unquoted identifiers upper-cased and exposes column
		// metadata via ALL_TAB_COLUMNS; NULLABLE is 'Y'/'N'.
		q := `SELECT column_name, data_type, nullable FROM all_tab_columns
		      WHERE UPPER(table_name) = UPPER(:1)`
		args := []interface{}{name}
		if schema != "" {
			q += " AND UPPER(owner) = UPPER(:2)"
			args = append(args, schema)
		}
		q += " ORDER BY column_id"
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
			out = append(out, ColumnInfo{Name: col, Type: mapDBType(dbType), DBType: dbType, Nullable: nullable == "Y"})
		}
		if err := rows.Err(); err != nil {
			return nil, err
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
	case t == "numeric" || t == "decimal" || t == "money" || strings.HasPrefix(t, "number"):
		return "decimal" // Oracle NUMBER, SQL Server MONEY, ANSI NUMERIC/DECIMAL
	case strings.Contains(t, "float") || strings.Contains(t, "double") || t == "real":
		return "float" // includes Oracle BINARY_FLOAT/BINARY_DOUBLE
	case strings.Contains(t, "bool") || t == "bit":
		return "bool" // SQL Server BIT
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
