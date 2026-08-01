package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"eventhub/internal/domain"
	"eventhub/internal/ratelimit"
	"eventhub/internal/storage"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// setupTestServer creates a fully wired Server backed by a real in-memory
// SQLite database and a running ingest batcher (POST /events persists
// synchronously through it). The caller receives the Server, Repository, and
// *sql.DB so tests can pre-seed data via the repository when needed. The
// batcher is drained and the database closed when the test finishes.
func setupTestServer(t *testing.T) (*Server, *storage.Repository) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := storage.NewDB(dbPath)
	require.NoError(t, err)

	repo := storage.NewRepository(db)
	logger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	// The batcher must be running for the synchronous POST /events tests to
	// observe the 201/409 outcomes. Small capacity is enough for tests.
	batcher := ratelimit.New(10, repo, logger)
	batcher.Start(context.Background())

	srv := NewServer(repo, logger, batcher)

	t.Cleanup(func() {
		// Drain any pending events before closing the database underneath.
		batcher.Shutdown()
		db.Close()
	})
	return srv, repo
}

// serve executes a request against the server handler and returns the
// httptest.ResponseRecorder for assertions.
func serve(srv *Server, method, path string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

// mustMarshal is a shorthand for json.Marshal; it panics on error (only for
// test constants that are known to be valid).
func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// ===========================================================================
// POST /events
// ===========================================================================

func TestCreateEvent_ValidBody_Returns201(t *testing.T) {
	srv, _ := setupTestServer(t)

	body := []byte(`{"type":"purchase_completed","payload":{"order_id":"abc"}}`)
	rec := serve(srv, http.MethodPost, "/events", body, nil)

	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("X-Request-ID"))

	var resp createEventResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.ID)
	assert.Equal(t, domain.StatusPending, resp.Status)
}

func TestCreateEvent_SameBodyTwice_Returns409(t *testing.T) {
	srv, _ := setupTestServer(t)

	body := []byte(`{"type":"purchase_completed","payload":{"order_id":"abc"}}`)

	// First request
	rec1 := serve(srv, http.MethodPost, "/events", body, nil)
	assert.Equal(t, http.StatusCreated, rec1.Code)

	// Second request — same body yields same deterministic idempotency key
	rec2 := serve(srv, http.MethodPost, "/events", body, nil)
	assert.Equal(t, http.StatusConflict, rec2.Code)

	var errResp errorResponse
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &errResp))
	assert.Contains(t, errResp.Error, "idempotency key already exists")
}

func TestCreateEvent_ExplicitIdempotencyKeyInBody_Duplicate_Returns409(t *testing.T) {
	srv, _ := setupTestServer(t)

	body := []byte(`{"type":"purchase_completed","payload":{"order_id":"abc"},"idempotency_key":"my-key"}`)

	rec1 := serve(srv, http.MethodPost, "/events", body, nil)
	assert.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := serve(srv, http.MethodPost, "/events", body, nil)
	assert.Equal(t, http.StatusConflict, rec2.Code)

	var errResp errorResponse
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &errResp))
	assert.Equal(t, "idempotency key already exists", errResp.Error)
}

func TestCreateEvent_EmptyBody_Returns400(t *testing.T) {
	srv, _ := setupTestServer(t)

	rec := serve(srv, http.MethodPost, "/events", []byte{}, nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "request body is required", errResp.Error)
}

func TestCreateEvent_InvalidJSON_Returns400(t *testing.T) {
	srv, _ := setupTestServer(t)

	rec := serve(srv, http.MethodPost, "/events", []byte(`{"type":`), nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "invalid JSON body", errResp.Error)
}

func TestCreateEvent_MissingType_Returns400(t *testing.T) {
	srv, _ := setupTestServer(t)

	rec := serve(srv, http.MethodPost, "/events", []byte(`{"type":"","payload":{"foo":"bar"}}`), nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, `field "type" must not be empty`, errResp.Error)
}

func TestCreateEvent_MissingPayload_Returns400(t *testing.T) {
	srv, _ := setupTestServer(t)

	rec := serve(srv, http.MethodPost, "/events", []byte(`{"type":"x"}`), nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, `field "payload" is required`, errResp.Error)
}

func TestCreateEvent_InvalidPayloadJSON_Returns400(t *testing.T) {
	// The body {"type":"x","payload":{"foo":}} is invalid JSON at the top
	// level, so the handler returns "invalid JSON body" (400) before reaching
	// the payload-specific validation.
	srv, _ := setupTestServer(t)

	rec := serve(srv, http.MethodPost, "/events", []byte(`{"type":"x","payload":{"foo":}}`), nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "invalid JSON body", errResp.Error)
}

func TestCreateEvent_IdempotencyKeyHeader_SuccessThenConflict(t *testing.T) {
	srv, _ := setupTestServer(t)

	body := []byte(`{"type":"purchase_completed","payload":{"order_id":"abc"}}`)
	headers := map[string]string{"Idempotency-Key": "key-fija-1"}

	// First request
	rec1 := serve(srv, http.MethodPost, "/events", body, headers)
	assert.Equal(t, http.StatusCreated, rec1.Code)
	assert.NotEmpty(t, rec1.Header().Get("X-Request-ID"))

	// Second request with same header
	rec2 := serve(srv, http.MethodPost, "/events", body, headers)
	assert.Equal(t, http.StatusConflict, rec2.Code)

	var errResp errorResponse
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &errResp))
	assert.Equal(t, "idempotency key already exists", errResp.Error)
}

func TestCreateEvent_ContentTypeJSON_Returns201(t *testing.T) {
	// Verify that the Content-Type header on the request is set correctly.
	srv, _ := setupTestServer(t)

	body := []byte(`{"type":"purchase_completed","payload":{"order_id":"abc"}}`)
	rec := serve(srv, http.MethodPost, "/events", body, nil)

	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
}

func TestCreateEvent_ShutdownBatcher_Returns503(t *testing.T) {
	// A batcher that has been shut down rejects new submissions; the handler
	// must surface that as 503 Service Unavailable, not 500.
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := storage.NewDB(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	repo := storage.NewRepository(db)
	logger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	batcher := ratelimit.New(10, repo, logger)
	batcher.Start(context.Background())
	batcher.Shutdown()

	srv := NewServer(repo, logger, batcher)
	rec := serve(srv, http.MethodPost, "/events", []byte(`{"type":"x","payload":{"a":1}}`), nil)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	var errResp errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "service is shutting down", errResp.Error)
}

// ===========================================================================
// GET /events
// ===========================================================================

func TestListEvents_NoEvents_ReturnsEmptyArray(t *testing.T) {
	srv, _ := setupTestServer(t)

	rec := serve(srv, http.MethodGet, "/events", nil, nil)

	assert.Equal(t, http.StatusOK, rec.Code)
	// Must be exactly "[]" — not null.
	assert.Equal(t, "[]", strings.TrimSpace(rec.Body.String()))
}

func TestListEvents_WithEvents_ReturnsArray(t *testing.T) {
	srv, _ := setupTestServer(t)

	// Create two events.
	body := []byte(`{"type":"order_created","payload":{"order_id":"1"}}`)
	rec1 := serve(srv, http.MethodPost, "/events", body, nil)
	assert.Equal(t, http.StatusCreated, rec1.Code)

	body2 := []byte(`{"type":"payment_done","payload":{"payment_id":"2"}}`)
	rec2 := serve(srv, http.MethodPost, "/events", body2, nil)
	assert.Equal(t, http.StatusCreated, rec2.Code)

	rec := serve(srv, http.MethodGet, "/events", nil, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var events []eventResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &events))
	assert.Len(t, events, 2)

	for _, ev := range events {
		assert.NotEmpty(t, ev.ID)
		assert.NotEmpty(t, ev.Type)
		assert.NotEmpty(t, ev.Status)
		// processes must be a non-nil empty slice → serialises as []
		assert.NotNil(t, ev.Processes)
		assert.Len(t, ev.Processes, 0)
	}
}

func TestListEvents_StatusFilterPending_ReturnsOnlyPending(t *testing.T) {
	srv, repo := setupTestServer(t)

	// Insert a pending event.
	body := []byte(`{"type":"order_created","payload":{"order_id":"1"}}`)
	rec := serve(srv, http.MethodPost, "/events", body, nil)
	assert.Equal(t, http.StatusCreated, rec.Code)

	// Insert a completed event directly.
	completedEvent := domain.NewEvent("done", `{"x":1}`, "ik-done", domain.StatusCompleted, nil)
	err := repo.InsertEvent(context.Background(), *completedEvent)
	require.NoError(t, err)

	// List with ?status=pending
	req := httptest.NewRequest(http.MethodGet, "/events?status=pending", nil)
	req.Header.Set("Content-Type", "application/json")
	recFiltered := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recFiltered, req)

	assert.Equal(t, http.StatusOK, recFiltered.Code)

	var events []eventResponse
	require.NoError(t, json.Unmarshal(recFiltered.Body.Bytes(), &events))
	assert.Len(t, events, 1)
	assert.Equal(t, domain.StatusPending, events[0].Status)
}

func TestListEvents_InvalidStatusFilter_Returns400(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/events?status=nonexistent", nil)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Contains(t, errResp.Error, "invalid status")
}

func TestListEvents_StatusFilterCompleted_ReturnsEmptyArray(t *testing.T) {
	srv, _ := setupTestServer(t)

	// Create a pending event — no completed events exist.
	body := []byte(`{"type":"order_created","payload":{"order_id":"1"}}`)
	rec1 := serve(srv, http.MethodPost, "/events", body, nil)
	assert.Equal(t, http.StatusCreated, rec1.Code)

	req := httptest.NewRequest(http.MethodGet, "/events?status=completed", nil)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	// Must be exactly "[]" (empty array), never null.
	assert.Equal(t, "[]", strings.TrimSpace(rec.Body.String()))
}

// ===========================================================================
// GET /events/{eventID}
// ===========================================================================

func TestGetEvent_Existing_Returns200(t *testing.T) {
	srv, _ := setupTestServer(t)

	body := []byte(`{"type":"purchase_completed","payload":{"order_id":"abc"}}`)
	createRec := serve(srv, http.MethodPost, "/events", body, nil)
	assert.Equal(t, http.StatusCreated, createRec.Code)

	var created createEventResponse
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	rec := serve(srv, http.MethodGet, "/events/"+created.ID, nil, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var ev eventResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ev))
	assert.Equal(t, created.ID, ev.ID)
	assert.Equal(t, "purchase_completed", ev.Type)
	assert.Equal(t, domain.StatusPending, ev.Status)
	assert.NotNil(t, ev.Processes) // always an array, never null
}

func TestGetEvent_NonExistentUUID_Returns404(t *testing.T) {
	srv, _ := setupTestServer(t)

	rec := serve(srv, http.MethodGet, "/events/00000000-0000-0000-0000-000000000000", nil, nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)

	var errResp errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Contains(t, errResp.Error, "not found")
}

func TestGetEvent_MalformedID_Returns404(t *testing.T) {
	srv, _ := setupTestServer(t)

	rec := serve(srv, http.MethodGet, "/events/not-a-uuid", nil, nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)

	var errResp errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Contains(t, errResp.Error, "not found")
}

// ===========================================================================
// POST /admin/events/{eventID}/requeue
// ===========================================================================

func TestRequeueEvent_DeadEvent_Returns204(t *testing.T) {
	srv, repo := setupTestServer(t)

	// Create an event.
	body := []byte(`{"type":"order_created","payload":{"order_id":"1"}}`)
	createRec := serve(srv, http.MethodPost, "/events", body, nil)
	assert.Equal(t, http.StatusCreated, createRec.Code)

	var created createEventResponse
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	// Move it to dead letter via repository.
	err := repo.MoveEventToDeadLetter(context.Background(), created.ID, "test requeue")
	require.NoError(t, err)

	// Requeue the dead event.
	rec := serve(srv, http.MethodPost, "/admin/events/"+created.ID+"/requeue", nil, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Empty(t, rec.Body.String())
}

func TestRequeueEvent_PendingEvent_Returns409(t *testing.T) {
	srv, _ := setupTestServer(t)

	// Create a pending event.
	body := []byte(`{"type":"order_created","payload":{"order_id":"1"}}`)
	createRec := serve(srv, http.MethodPost, "/events", body, nil)
	assert.Equal(t, http.StatusCreated, createRec.Code)

	var created createEventResponse
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	// Try to requeue a non-dead (pending) event.
	rec := serve(srv, http.MethodPost, "/admin/events/"+created.ID+"/requeue", nil, nil)
	assert.Equal(t, http.StatusConflict, rec.Code)

	var errResp errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Contains(t, errResp.Error, "only dead events can be requeued")
}

func TestRequeueEvent_NonExistent_Returns404(t *testing.T) {
	srv, _ := setupTestServer(t)

	rec := serve(srv, http.MethodPost, "/admin/events/00000000-0000-0000-0000-000000000000/requeue", nil, nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)

	var errResp errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Contains(t, errResp.Error, "not found")
}

// ===========================================================================
// Middleware: X-Request-ID
// ===========================================================================

func TestRequestIDMiddleware_ReflectedInResponse(t *testing.T) {
	srv, _ := setupTestServer(t)

	// Hit the health-like endpoint: GET /events (any route works).
	rec := serve(srv, http.MethodGet, "/events", nil, nil)

	assert.NotEmpty(t, rec.Header().Get("X-Request-ID"))
}

func TestRequestIDMiddleware_HonoursIncomingHeader(t *testing.T) {
	srv, _ := setupTestServer(t)

	rec := serve(srv, http.MethodGet, "/events", nil, map[string]string{"X-Request-ID": "my-custom-id"})

	assert.Equal(t, "my-custom-id", rec.Header().Get("X-Request-ID"))
}

// ===========================================================================
// Middleware: recovery
// ===========================================================================

func TestRecoveryMiddleware_Returns500(t *testing.T) {
	// Build a minimal server with just the recovery middleware applied to a
	// panicking handler. No real router or database is needed.
	logger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	srv := &Server{logger: logger}

	panicHandler := srv.recovery(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	}))

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	rec := httptest.NewRecorder()
	panicHandler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	var errResp errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "internal server error", errResp.Error)
}

// ===========================================================================
// Response helpers (writeJSON / writeError)
// ===========================================================================

func TestWriteJSON_WritesJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusOK, map[string]string{"hello": "world"})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assert.JSONEq(t, `{"hello":"world"}`, rec.Body.String())
}

func TestWriteError_WritesErrorJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	writeError(rec, http.StatusNotFound, "item not found")

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.JSONEq(t, `{"error":"item not found"}`, rec.Body.String())
}

// ===========================================================================
// response type constructors
// ===========================================================================

func TestNewEventResponse_ProcessesNeverNil(t *testing.T) {
	ev := domain.NewEvent("test", `{"k":"v"}`, "ik-1", domain.StatusPending, nil)
	resp := newEventResponse(*ev)

	assert.NotNil(t, resp.Processes)
	assert.Len(t, resp.Processes, 0)
	// Verify it serialises as "[]" not "null"
	jsonBytes, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.True(t, strings.Contains(string(jsonBytes), `"processes":[]`))
}

func TestNewProcessResponse_AllFields(t *testing.T) {
	proc := domain.Process{
		ID:          "proc-1",
		EventID:     "ev-1",
		ProcessName: "email",
		Status:      domain.ProcessStatusPending,
		Attempts:    0,
		ErrorMsg:    "",
	}
	resp := newProcessResponse(proc)

	assert.Equal(t, "proc-1", resp.ID)
	assert.Equal(t, "ev-1", resp.EventID)
	assert.Equal(t, "email", resp.ProcessName)
	assert.Equal(t, domain.ProcessStatusPending, resp.Status)
	assert.Equal(t, 0, resp.Attempts)
	// NextRetryAt should be nil
	assert.Nil(t, resp.NextRetryAt)
}

// ===========================================================================
// createEvent: io.ReadAll error path
// ===========================================================================

// errorReader is an io.Reader that always returns an error.
type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("simulated read error")
}

func TestCreateEvent_ReadBodyError_Returns400(t *testing.T) {
	srv, _ := setupTestServer(t)

	// Call createEvent directly (bypassing the idempotency middleware which
	// would consume the body before the handler sees it). The test file lives
	// in package httpapi so we have access to the unexported method.
	req := httptest.NewRequest(http.MethodPost, "/events", errorReader{})
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.createEvent(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "failed to read request body", errResp.Error)
}

// ===========================================================================
// newEventResponse with processes
// ===========================================================================

func TestNewEventResponse_WithProcesses(t *testing.T) {
	proc := domain.NewProcess("ev-id", "email-notification")
	ev := domain.NewEvent("order.created", `{"order_id":"123"}`, "ik-1", domain.StatusPending, []domain.Process{*proc})
	resp := newEventResponse(*ev)

	assert.NotNil(t, resp.Processes)
	assert.Len(t, resp.Processes, 1)
	assert.Equal(t, proc.ID, resp.Processes[0].ID)
	assert.Equal(t, "email-notification", resp.Processes[0].ProcessName)

	// Verify JSON serialisation: processes must be an array, not null.
	jsonBytes, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.Contains(t, string(jsonBytes), `"processes":[{`)
}

// ===========================================================================
// isEventStatusValid
// ===========================================================================

func TestIsEventStatusValid(t *testing.T) {
	assert.True(t, isEventStatusValid(domain.StatusPending))
	assert.True(t, isEventStatusValid(domain.StatusProcessing))
	assert.True(t, isEventStatusValid(domain.StatusCompleted))
	assert.True(t, isEventStatusValid(domain.StatusPartialFailed))
	assert.True(t, isEventStatusValid(domain.StatusDead))
	assert.False(t, isEventStatusValid("invalid"))
	assert.False(t, isEventStatusValid(""))
}
