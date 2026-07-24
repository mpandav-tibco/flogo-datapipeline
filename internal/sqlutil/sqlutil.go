// Package sqlutil provides shared SQL helpers for the jdbc source and sink packages.
// Centralising quoteIdent, Placeholder, LimitClause, and type mapping here avoids
// duplication and ensures consistent behaviour across drivers.
package sqlutil

import (
	"fmt"
	"strings"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// QuoteIdent quotes a SQL identifier for the given driver.
// Postgres and SQLite use double-quotes; MySQL uses back-ticks.
func QuoteIdent(name, driver string) string {
	switch driver {
	case "mysql":
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	default: // postgres, sqlite3
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
}

// Placeholder returns the positional placeholder for argument n (1-based).
// MySQL and SQLite use "?"; Postgres uses "$N".
func Placeholder(n int, driver string) string {
	switch driver {
	case "mysql", "sqlite3":
		return "?"
	default: // postgres
		return fmt.Sprintf("$%d", n)
	}
}

// QuoteLiteral renders s as a safely-escaped, single-quoted SQL string literal
// for the given driver. Prefer bind parameters over this; it exists for the few
// places that must embed a value in a reusable query string (e.g. the jdbc
// source's high-water-mark predicate and keyset bound).
//
// All drivers double embedded single quotes ('→''). MySQL additionally treats a
// backslash as an escape character inside string literals (unless the server has
// NO_BACKSLASH_ESCAPES set), so backslashes are doubled for MySQL — without this
// a trailing backslash would escape the closing quote (a SQL-injection / broken-
// query vector) and interior "\x" sequences would silently drop the backslash
// (a correctness bug). Standard-conforming drivers (postgres, sqlite3) treat the
// backslash literally, so doubling it there would corrupt the value.
func QuoteLiteral(s, driver string) string {
	switch driver {
	case "mysql":
		s = strings.ReplaceAll(s, "\\", "\\\\")
		s = strings.ReplaceAll(s, "'", "''")
		return "'" + s + "'"
	default: // postgres, sqlite3 — standard-conforming string literals
		return "'" + strings.ReplaceAll(s, "'", "''") + "'"
	}
}

// LimitClause returns a LIMIT/OFFSET SQL fragment for the given driver.
// When limit is 0 the clause contains only OFFSET (useful for schema inference).
// When both are 0, an empty string is returned.
func LimitClause(driver string, limit, offset int64) string {
	var sb strings.Builder
	if limit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", limit))
	}
	if offset > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", offset))
	}
	return sb.String()
}

// SQLTypeToRecord maps a driver-specific DatabaseTypeName (as returned by
// sql.ColumnType.DatabaseTypeName) to the nearest record.ColumnType.
// Unknown types fall back to record.TypeString so the pipeline never panics.
func SQLTypeToRecord(dbTypeName string) record.ColumnType {
	// Normalise: uppercase, strip parenthetical sizes like "VARCHAR(255)".
	name := strings.ToUpper(dbTypeName)
	if idx := strings.Index(name, "("); idx != -1 {
		name = name[:idx]
	}
	name = strings.TrimSpace(name)

	switch name {
	// Integer families — Postgres (INT2/INT4/INT8), MySQL (TINYINT … BIGINT), SQLite (INTEGER).
	case "INT", "INT2", "INT4", "INT8", "INTEGER",
		"TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT",
		"YEAR":
		return record.TypeInt

	// Floating-point families.
	case "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "REAL":
		return record.TypeFloat

	// Exact numeric — map to Decimal so scale information is preserved downstream.
	case "NUMERIC", "DECIMAL", "MONEY", "SMALLMONEY":
		return record.TypeDecimal

	// Boolean — Postgres BOOL/BOOLEAN; MySQL TINYINT(1) typically reads back as
	// "TINYINT" and is handled above; explicit BOOLEAN/BIT variants are here.
	case "BOOL", "BOOLEAN", "BIT":
		return record.TypeBool

	// Date / time families — all map to TypeDate; the pipeline uses time.Time values.
	case "DATE", "TIME", "TIMETZ",
		"TIMESTAMP", "TIMESTAMPTZ", "DATETIME",
		"TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITHOUT TIME ZONE":
		return record.TypeDate

	// Everything else: text, blobs, JSON, arrays, UUIDs — TypeString covers them all.
	default:
		return record.TypeString
	}
}
