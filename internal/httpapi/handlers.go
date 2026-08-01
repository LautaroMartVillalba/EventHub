package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"eventhub/internal/domain"
	"eventhub/internal/idempotency"
	"eventhub/internal/logging"
	"eventhub/internal/ratelimit"
	"eventhub/internal/storage"
)

// createEventRequest is the JSON body accepted by POST /events. The
// idempotency_key field does not appear here because the idempotency
// middleware already extracts it from the body and stores it in the context.
type createEventRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// createEvent handles POST /events. It validates the body, resolves an
// idempotency key (context key, or a deterministic hash of type+payload) and
// enqueues the event as pending through the ingest batcher.
//
// The event is submitted to the batcher instead of being inserted directly:
// when the batcher's queue is full the handler blocks (natural backpressure,
// never a 429), and the batcher persists the event asynchronously. The
// handler answers 201 only after the batcher confirms the event was
// persisted; the outcome (nil, ErrConflict, or a database error) is
// propagated back through the batcher's result channel.
func (server *Server) createEvent(w http.ResponseWriter, r *http.Request) {
	logger := logging.FromContext(r.Context())
	requestID := requestIDFromContext(r.Context())

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Debug("validation failed",
			"reason", "failed to read request body",
			"request_id", requestID,
		)
		writeError(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	if len(bodyBytes) == 0 {
		logger.Debug("validation failed",
			"reason", "request body is required",
			"request_id", requestID,
		)
		writeError(w, http.StatusBadRequest, "request body is required")
		return
	}

	var request createEventRequest
	if err := json.Unmarshal(bodyBytes, &request); err != nil {
		// The parser detail (e.g. *json.SyntaxError) is Go-internal and
		// useless to the client, so the response stays generic; the detail is
		// logged at Debug level for diagnosis.
		logger.Debug("validation failed",
			"reason", "invalid JSON body",
			"error", err,
			"request_id", requestID,
		)
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if strings.TrimSpace(request.Type) == "" {
		logger.Debug("validation failed",
			"reason", "field type must not be empty",
			"request_id", requestID,
		)
		writeError(w, http.StatusBadRequest, "field \"type\" must not be empty")
		return
	}
	if len(request.Payload) == 0 {
		logger.Debug("validation failed",
			"reason", "field payload is required",
			"request_id", requestID,
		)
		writeError(w, http.StatusBadRequest, "field \"payload\" is required")
		return
	}
	if !json.Valid(request.Payload) {
		logger.Debug("validation failed",
			"reason", "field payload must be valid JSON",
			"request_id", requestID,
		)
		writeError(w, http.StatusBadRequest, "field \"payload\" must be valid JSON")
		return
	}

	// Resolve the idempotency key. The idempotency middleware stores it in
	// the context (header > body idempotency_key > generated hash). When no
	// key is present derive one deterministically from type + payload so the
	// request stays idempotent.
	key, ok := idempotency.FromContext(r.Context())
	if !ok {
		key = idempotency.GenerateKey(request.Type, string(request.Payload))
	}

	// The event is built with no processes (nil) exactly as before. The
	// batcher persists it via InsertEvent, which writes the event row and any
	// embedded processes inside a single transaction. POST /events submits
	// events without processes, so only the event row is written. See
	// ratelimit.Batcher.persistEvent.
	event := domain.NewEvent(request.Type, string(request.Payload), key, domain.StatusPending, nil)
	if err := server.batcher.Submit(r.Context(), *event); err != nil {
		if errors.Is(err, storage.ErrConflict) {
			writeError(w, http.StatusConflict, "idempotency key already exists")
			return
		}
		if errors.Is(err, ratelimit.ErrClosed) {
			// The batcher is shutting down and rejected the event before it
			// was accepted; the client can retry against a healthy node.
			logger.Warn("rejected event: ingest batcher is shutting down",
				"request_id", requestID,
				"event_type", request.Type,
			)
			writeError(w, http.StatusServiceUnavailable, "service is shutting down")
			return
		}
		logger.Error("failed to persist event",
			"error", err,
			"request_id", requestID,
			"event_type", request.Type,
		)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	writeJSON(w, http.StatusCreated, createEventResponse{
		ID:     event.ID,
		Status: event.Status,
	})
}

// listEvents handles GET /events. An optional ?status= query parameter
// filters by event status; without it every event is returned. The response
// is always a JSON array, never null.
func (server *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	logger := logging.FromContext(r.Context())
	requestID := requestIDFromContext(r.Context())

	statusFilter := r.URL.Query().Get("status")

	var events []domain.Event
	var err error
	if statusFilter == "" {
		events, err = server.repository.FetchAll(r.Context())
	} else {
		eventStatus := domain.EventStatus(statusFilter)
		if !isEventStatusValid(eventStatus) {
			logger.Debug("validation failed",
				"reason", "invalid status filter",
				"status_filter", statusFilter,
				"request_id", requestID,
			)
			writeError(w, http.StatusBadRequest,
				fmt.Sprintf("invalid status %q; must be one of: pending, processing, completed, partial_failed, dead", statusFilter))
			return
		}
		events, err = server.repository.FetchByStatus(r.Context(), eventStatus)
	}
	if err != nil {
		logger.Error("failed to list events",
			"error", err,
			"request_id", requestID,
			"status_filter", statusFilter,
		)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	responses := make([]eventResponse, 0, len(events))
	for _, event := range events {
		responses = append(responses, newEventResponse(event))
	}
	writeJSON(w, http.StatusOK, responses)
}

// getEvent handles GET /events/{eventID}.
func (server *Server) getEvent(w http.ResponseWriter, r *http.Request) {
	logger := logging.FromContext(r.Context())
	requestID := requestIDFromContext(r.Context())
	eventID := r.PathValue("eventID")

	event, err := server.repository.FetchByID(r.Context(), eventID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, fmt.Sprintf("event %q not found", eventID))
			return
		}
		logger.Error("failed to fetch event",
			"error", err,
			"request_id", requestID,
			"event_id", eventID,
		)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	writeJSON(w, http.StatusOK, newEventResponse(event))
}

// requeueEvent handles POST /admin/events/{eventID}/requeue. Only dead
// events can be requeued; the event is reset to pending with zero attempts.
// A successful requeue returns 204 No Content.
//
// The handler uses a fetch-then-act pattern: it reads the event to validate
// its status and then calls RequeueEvent. This leaves a small TOCTOU window
// in which two concurrent requeue requests can both observe status "dead"
// and both call RequeueEvent. That is safe by design: RequeueEvent is
// idempotent and runs inside a single transaction, so the second invocation
// simply resets an already-pending event to pending again. The only harmful
// interleaving — the event disappearing between the fetch and the requeue —
// is handled by the ErrNotFound branch below (404).
func (server *Server) requeueEvent(w http.ResponseWriter, r *http.Request) {
	logger := logging.FromContext(r.Context())
	requestID := requestIDFromContext(r.Context())
	eventID := r.PathValue("eventID")

	event, err := server.repository.FetchByID(r.Context(), eventID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, fmt.Sprintf("event %q not found", eventID))
			return
		}
		logger.Error("failed to fetch event for requeue",
			"error", err,
			"request_id", requestID,
			"event_id", eventID,
		)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	if event.Status != domain.StatusDead {
		writeError(w, http.StatusConflict,
			fmt.Sprintf("event %q has status %q; only dead events can be requeued", eventID, event.Status))
		return
	}

	if err := server.repository.RequeueEvent(r.Context(), eventID); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// The event disappeared between the fetch and the requeue (race).
			writeError(w, http.StatusNotFound, fmt.Sprintf("event %q not found", eventID))
			return
		}
		logger.Error("failed to requeue event",
			"error", err,
			"request_id", requestID,
			"event_id", eventID,
		)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
