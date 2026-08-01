// Package storage provides SQLite-backed persistence for EventHub.
package storage

import "errors"

// Sentinel errors returned by Repository methods.
var (
	// ErrNotFound is returned when a requested record does not exist.
	ErrNotFound = errors.New("record not found")

	// ErrConflict is returned when an insert violates an idempotency key.
	ErrConflict = errors.New("idempotency key already exists")
)
