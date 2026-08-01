// Package storage provides SQLite-backed persistence for EventHub.
// The SQLite driver is registered via blank import below.
package storage

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite" // pure-Go SQLite driver (no CGO)
)

// NewDB opens (or creates) the SQLite database at the given path and applies
// the embedded schema. The returned *sql.DB is safe for concurrent use.
//
// PRAGMAs (foreign_keys, journal_mode) are applied via DSN _pragma so that
// every connection from the pool inherits them automatically.
func NewDB(dbPath string) (*sql.DB, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)", dbPath)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %q: %w", dsn, err)
	}

	// Apply schema (CREATE TABLE IF NOT EXISTS — idempotent).
	if _, err := db.Exec(SchemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	return db, nil
}
