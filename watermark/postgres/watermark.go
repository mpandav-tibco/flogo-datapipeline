// Package postgres registers a PostgreSQL-backed watermark store.
// Marks are persisted in a flogo_pipeline_watermarks table that is created
// automatically on first use.  Save() uses an upsert (INSERT ... ON CONFLICT)
// so concurrent writes from multiple instances are safe.
//
// Schema created on first use:
//
//	CREATE TABLE IF NOT EXISTS flogo_pipeline_watermarks (
//	    pipeline_id  TEXT PRIMARY KEY,
//	    column_name  TEXT NOT NULL,
//	    value        TEXT NOT NULL,
//	    value_type   TEXT NOT NULL,
//	    saved_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
//	);
//
// Config keys:
//
//	dsn  string  PostgreSQL connection string (libpq format or URL) — required
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/watermark/postgres"
package postgres

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver

	"github.com/mpandav-tibco/flogo-datapipeline/watermark"
)

func init() {
	watermark.Register("postgres", newPostgresStore)
}

const createTableSQL = `
CREATE TABLE IF NOT EXISTS flogo_pipeline_watermarks (
    pipeline_id  TEXT PRIMARY KEY,
    column_name  TEXT NOT NULL,
    value        TEXT NOT NULL,
    value_type   TEXT NOT NULL,
    saved_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`

const upsertSQL = `
INSERT INTO flogo_pipeline_watermarks (pipeline_id, column_name, value, value_type, saved_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (pipeline_id) DO UPDATE SET
    column_name = EXCLUDED.column_name,
    value       = EXCLUDED.value,
    value_type  = EXCLUDED.value_type,
    saved_at    = EXCLUDED.saved_at`

const loadSQL = `
SELECT column_name, value, value_type, saved_at
FROM flogo_pipeline_watermarks
WHERE pipeline_id = $1`

type postgresStore struct {
	db *sql.DB
}

func newPostgresStore(cfg map[string]interface{}) (watermark.Store, error) {
	dsn, _ := cfg["dsn"].(string)
	if dsn == "" {
		return nil, fmt.Errorf("postgres watermark: 'dsn' is required")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres watermark: open db: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("postgres watermark: ping db: %w", err)
	}

	// Modest pool — this store is called infrequently (once per checkpoint interval).
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)

	// Ensure the table exists before returning the store.
	if _, err := db.Exec(createTableSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("postgres watermark: create table: %w", err)
	}

	return &postgresStore{db: db}, nil
}

func (p *postgresStore) Load(pipelineID string) (*watermark.Mark, error) {
	var (
		colName   string
		value     string
		valueType string
		savedAt   time.Time
	)
	err := p.db.QueryRow(loadSQL, pipelineID).Scan(&colName, &value, &valueType, &savedAt)
	if err == sql.ErrNoRows {
		return nil, nil // no mark — first run, full-table load
	}
	if err != nil {
		return nil, fmt.Errorf("postgres watermark: load %q: %w", pipelineID, err)
	}
	return &watermark.Mark{
		Column:    colName,
		Value:     value,
		ValueType: valueType,
		SavedAt:   savedAt.UTC(),
	}, nil
}

func (p *postgresStore) Save(pipelineID string, m *watermark.Mark) error {
	savedAt := m.SavedAt
	if savedAt.IsZero() {
		savedAt = time.Now().UTC()
	}
	_, err := p.db.Exec(upsertSQL, pipelineID, m.Column, m.Value, m.ValueType, savedAt)
	if err != nil {
		return fmt.Errorf("postgres watermark: save %q: %w", pipelineID, err)
	}
	return nil
}

func (p *postgresStore) Close() error {
	return p.db.Close()
}
