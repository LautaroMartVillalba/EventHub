package idempotency

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// GenerateKey
// ---------------------------------------------------------------------------

func TestGenerateKey_Deterministic(t *testing.T) {
	k1 := GenerateKey("order.created", `{"id":42}`)
	k2 := GenerateKey("order.created", `{"id":42}`)
	if k1 != k2 {
		t.Fatalf("expected deterministic keys, got %q vs %q", k1, k2)
	}
}

func TestGenerateKey_Length(t *testing.T) {
	key := GenerateKey("user.signup", `{"email":"a@b.com"}`)
	if len(key) != 32 {
		t.Fatalf("expected 32 hex chars, got %d: %q", len(key), key)
	}
}

func TestGenerateKey_DifferentInputs(t *testing.T) {
	k1 := GenerateKey("a", `{}`)
	k2 := GenerateKey("b", `{}`)
	if k1 == k2 {
		t.Fatal("expected different keys for different event types")
	}
}

func TestGenerateKey_EmptyPayload(t *testing.T) {
	key := GenerateKey("ping", "")
	if len(key) != 32 {
		t.Fatalf("expected 32 hex chars, got %d: %q", len(key), key)
	}
}

// ---------------------------------------------------------------------------
// Context helpers
// ---------------------------------------------------------------------------

func TestWithContext_ThenFromContext_Found(t *testing.T) {
	ctx := context.Background()
	key := "my-idem-key"
	ctx = WithContext(ctx, key)

	got, ok := FromContext(ctx)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got != key {
		t.Fatalf("expected %q, got %q", key, got)
	}
}

func TestFromContext_NotFound(t *testing.T) {
	_, ok := FromContext(context.Background())
	if ok {
		t.Fatal("expected ok=false on bare context")
	}
}

func TestFromContext_WrongType(t *testing.T) {
	// Store a non-string value under the same key to simulate misuse.
	ctx := context.WithValue(context.Background(), ctxKey, 42)
	_, ok := FromContext(ctx)
	if ok {
		t.Fatal("expected ok=false when value is not a string")
	}
}

// ---------------------------------------------------------------------------
// Middleware (HTTP)
// ---------------------------------------------------------------------------

// testHandler is a helper that records the idempotency key (if any) and echoes
// the request body back so we can verify body restoration.
type testHandler struct {
	t      *testing.T
	gotKey string
	gotOK  bool
	body   []byte
}

func (h *testHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.gotKey, h.gotOK = FromContext(r.Context())
	var err error
	h.body, err = io.ReadAll(r.Body)
	if err != nil {
		h.t.Fatalf("failed to read body in test handler: %v", err)
	}
	_ = r.Body.Close()
	w.WriteHeader(http.StatusOK)
}

func TestMiddleware_HeaderTakesPriority(t *testing.T) {
	h := &testHandler{t: t}
	handler := Middleware(h)
	body := `{"idempotency_key": "body-key", "type": "test", "payload": "data"}`

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", "header-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !h.gotOK {
		t.Fatal("expected key in context")
	}
	if h.gotKey != "header-key" {
		t.Fatalf("expected header key %q, got %q", "header-key", h.gotKey)
	}
}

func TestMiddleware_BodyIdempotencyKey(t *testing.T) {
	h := &testHandler{t: t}
	handler := Middleware(h)
	body := `{"idempotency_key": "body-key", "type": "test", "payload": "data"}`

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !h.gotOK {
		t.Fatal("expected key in context")
	}
	if h.gotKey != "body-key" {
		t.Fatalf("expected body key %q, got %q", "body-key", h.gotKey)
	}
}

func TestMiddleware_GenerateKey(t *testing.T) {
	h := &testHandler{t: t}
	handler := Middleware(h)
	body := `{"type": "test.event", "payload": {"id":42}}`

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !h.gotOK {
		t.Fatal("expected key in context")
	}
	expected := GenerateKey("test.event", `{"id":42}`)
	if h.gotKey != expected {
		t.Fatalf("expected generated key %q, got %q", expected, h.gotKey)
	}
}

func TestMiddleware_InvalidJSON(t *testing.T) {
	h := &testHandler{t: t}
	handler := Middleware(h)
	body := `not-valid-json`

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if h.gotOK {
		t.Fatal("expected no idempotency key for invalid JSON body")
	}
}

func TestMiddleware_EmptyBody(t *testing.T) {
	h := &testHandler{t: t}
	handler := Middleware(h)

	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if h.gotOK {
		t.Fatal("expected no idempotency key for empty body")
	}
}

func TestMiddleware_HeaderAndBody(t *testing.T) {
	// Verify header priority when both header and body supply a key.
	h := &testHandler{t: t}
	handler := Middleware(h)
	body := `{"idempotency_key": "body-key"}`

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", "header-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !h.gotOK {
		t.Fatal("expected key in context")
	}
	if h.gotKey != "header-key" {
		t.Fatalf("expected header key %q, got %q", "header-key", h.gotKey)
	}
}

func TestMiddleware_NoKeyNoBody(t *testing.T) {
	h := &testHandler{t: t}
	handler := Middleware(h)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if h.gotOK {
		t.Fatal("expected no idempotency key")
	}
}

func TestMiddleware_LargeBody(t *testing.T) {
	handler := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("downstream handler should NOT be called when body exceeds limit")
	}))

	// Build a body that is strictly larger than maxBodySize.
	buf := make([]byte, maxBodySize+1)
	for i := range buf {
		buf[i] = 'x'
	}
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(buf))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rec.Code)
	}
}

func TestMiddleware_BodyRestored(t *testing.T) {
	// Verify that the downstream handler can read the original body after
	// the middleware buffers it.
	h := &testHandler{t: t}
	handler := Middleware(h)
	body := `{"type": "test", "payload": {"msg": "hello"}}`

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if string(h.body) != body {
		t.Fatalf("expected body %q, got %q", body, string(h.body))
	}
}
