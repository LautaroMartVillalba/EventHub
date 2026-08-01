package storage

import _ "embed"

// SchemaSQL contains the embedded SQLite schema for EventHub.
// It is applied at startup to create tables and indexes if they don't exist.
//
//go:embed schema.sql
var SchemaSQL string
