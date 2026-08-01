// Package idempotency provides deterministic idempotency key generation,
// context injection, and an HTTP middleware for extracting idempotency keys
// from incoming requests.
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
)

// ---------------------------------------------------------------------------
// GenerateKey
// ---------------------------------------------------------------------------

// GenerateKey returns a deterministic idempotency key derived from the event
// type and its JSON payload. The key is the first 16 bytes (32 hex chars) of
// SHA256(eventType + ":" + payload).
func GenerateKey(eventType, payload string) string {
	hash := sha256.Sum256([]byte(eventType + ":" + payload))
	return hex.EncodeToString(hash[:16])
}

// ---------------------------------------------------------------------------
// Context helpers
// ---------------------------------------------------------------------------

// contextKey is a private, unexported type used to store the idempotency key
// in a context.Context. Using a custom type prevents collisions with other
// context values defined in different packages.
type contextKey string

// ctxKey is the sole key used to store/retrieve the idempotency key.
const ctxKey contextKey = "idempotency_key"

// WithContext returns a new context with the given idempotency key stored
// inside it.
func WithContext(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, ctxKey, key)
}

// FromContext extracts the idempotency key previously stored in the context
// by WithContext. The boolean is false when no key is present.
func FromContext(ctx context.Context) (string, bool) {
	key, ok := ctx.Value(ctxKey).(string)
	return key, ok
}
