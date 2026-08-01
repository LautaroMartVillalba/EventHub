package domain

import "errors"

// Sentinel errors for domain-level validation.
var (
	ErrInvalidEventStatus   = errors.New("invalid event status")
	ErrInvalidProcessStatus = errors.New("invalid process status")
	ErrEmptyEventType       = errors.New("event type must not be empty")
	ErrEmptyIdempotencyKey  = errors.New("idempotency key must not be empty")
)
