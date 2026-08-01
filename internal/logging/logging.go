// Package logging provides structured JSON logging via log/slog with context
// injection helpers and level parsing utilities.
package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Logger construction
// ---------------------------------------------------------------------------

// New creates a new slog.Logger that writes JSON to os.Stdout. Timestamps are
// emitted in UTC with RFC3339Nano precision for maximum granularity.
func New(level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && len(groups) == 0 {
				if t, ok := a.Value.Any().(time.Time); ok {
					return slog.String(slog.TimeKey, t.UTC().Format(time.RFC3339Nano))
				}
			}
			return a
		},
	}))
}

// NewDefault is a convenience function that returns a logger at Info level.
func NewDefault() *slog.Logger {
	return New(slog.LevelInfo)
}

// ---------------------------------------------------------------------------
// Level parsing
// ---------------------------------------------------------------------------

// ParseLevel converts a case-insensitive level string to slog.Level.
// Supported values: "debug", "info", "warn" / "warning", "error".
// Unknown values default to slog.LevelInfo.
func ParseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// ---------------------------------------------------------------------------
// Context helpers
// ---------------------------------------------------------------------------

// contextKey is a private type used to store the logger in a context.Context.
type contextKey string

// loggerCtxKey is the sole key for storing/retrieving a *slog.Logger.
const loggerCtxKey contextKey = "logger"

// WithContext returns a new context that carries the given logger.
func WithContext(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerCtxKey, logger)
}

// FromContext returns the logger stored in the context, or slog.Default() if
// no logger was previously stored via WithContext.
func FromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(loggerCtxKey).(*slog.Logger); ok {
		return logger
	}
	return slog.Default()
}
