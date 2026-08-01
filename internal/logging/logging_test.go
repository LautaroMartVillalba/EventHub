package logging

import (
	"context"
	"log/slog"
	"testing"
)

// ---------------------------------------------------------------------------
// ParseLevel
// ---------------------------------------------------------------------------

func TestParseLevel_Debug(t *testing.T) {
	if got := ParseLevel("debug"); got != slog.LevelDebug {
		t.Fatalf("expected LevelDebug, got %v", got)
	}
}

func TestParseLevel_Info(t *testing.T) {
	if got := ParseLevel("info"); got != slog.LevelInfo {
		t.Fatalf("expected LevelInfo, got %v", got)
	}
}

func TestParseLevel_Warn(t *testing.T) {
	if got := ParseLevel("warn"); got != slog.LevelWarn {
		t.Fatalf("expected LevelWarn, got %v", got)
	}
}

func TestParseLevel_Warning(t *testing.T) {
	if got := ParseLevel("warning"); got != slog.LevelWarn {
		t.Fatalf("expected LevelWarn, got %v", got)
	}
}

func TestParseLevel_Error(t *testing.T) {
	if got := ParseLevel("error"); got != slog.LevelError {
		t.Fatalf("expected LevelError, got %v", got)
	}
}

func TestParseLevel_CaseInsensitive(t *testing.T) {
	if got := ParseLevel("DEBUG"); got != slog.LevelDebug {
		t.Fatalf("expected LevelDebug, got %v", got)
	}
	if got := ParseLevel("Info"); got != slog.LevelInfo {
		t.Fatalf("expected LevelInfo, got %v", got)
	}
}

func TestParseLevel_UnknownDefaultsToInfo(t *testing.T) {
	if got := ParseLevel("trace"); got != slog.LevelInfo {
		t.Fatalf("expected LevelInfo, got %v", got)
	}
	if got := ParseLevel(""); got != slog.LevelInfo {
		t.Fatalf("expected LevelInfo, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// New / NewDefault
// ---------------------------------------------------------------------------

func TestNew_ReturnsNonNil(t *testing.T) {
	logger := New(slog.LevelDebug)
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestNewDefault_ReturnsNonNil(t *testing.T) {
	logger := NewDefault()
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

// ---------------------------------------------------------------------------
// Context helpers
// ---------------------------------------------------------------------------

func TestWithContext_ThenFromContext(t *testing.T) {
	ctx := context.Background()
	logger := New(slog.LevelInfo)
	ctx = WithContext(ctx, logger)

	got := FromContext(ctx)
	if got != logger {
		t.Fatal("FromContext did not return the same logger instance")
	}
}

func TestFromContext_Empty(t *testing.T) {
	got := FromContext(context.Background())
	if got != slog.Default() {
		t.Fatal("expected slog.Default() on bare context")
	}
}

func TestFromContext_WrongType(t *testing.T) {
	ctx := context.WithValue(context.Background(), loggerCtxKey, "not-a-logger")
	got := FromContext(ctx)
	if got != slog.Default() {
		t.Fatal("expected slog.Default() when value is not *slog.Logger")
	}
}
