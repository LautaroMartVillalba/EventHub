package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"eventhub/internal/domain"
)

// errorResponse is the JSON body returned for every non-2xx response.
type errorResponse struct {
	Error string `json:"error"`
}

// createEventResponse is the JSON body returned by a successful POST /events.
type createEventResponse struct {
	ID     string             `json:"id"`
	Status domain.EventStatus `json:"status"`
}

// processResponse is the JSON wire representation of a domain.Process. It
// exists so the API can expose explicit json tags without modifying the
// domain types.
type processResponse struct {
	ID          string               `json:"id"`
	EventID     string               `json:"event_id"`
	ProcessName string               `json:"process_name"`
	Status      domain.ProcessStatus `json:"status"`
	Attempts    int                  `json:"attempts"`
	NextRetryAt *time.Time           `json:"next_retry_at"`
	ErrorMsg    string               `json:"error_msg"`
	CreatedAt   time.Time            `json:"created_at"`
	UpdatedAt   time.Time            `json:"updated_at"`
}

// eventResponse is the JSON wire representation of a domain.Event with its
// processes embedded.
type eventResponse struct {
	ID             string             `json:"id"`
	Type           string             `json:"type"`
	Payload        json.RawMessage    `json:"payload"`
	IdempotencyKey string             `json:"idempotency_key"`
	Status         domain.EventStatus `json:"status"`
	Attempts       int                `json:"attempts"`
	NextRetryAt    *time.Time         `json:"next_retry_at"`
	CreatedAt      time.Time          `json:"created_at"`
	UpdatedAt      time.Time          `json:"updated_at"`
	Processes      []processResponse  `json:"processes"`
}

// newProcessResponse converts a domain.Process into its JSON representation.
func newProcessResponse(process domain.Process) processResponse {
	return processResponse{
		ID:          process.ID,
		EventID:     process.EventID,
		ProcessName: process.ProcessName,
		Status:      process.Status,
		Attempts:    process.Attempts,
		NextRetryAt: process.NextRetryAt,
		ErrorMsg:    process.ErrorMsg,
		CreatedAt:   process.CreatedAt,
		UpdatedAt:   process.UpdatedAt,
	}
}

// newEventResponse converts a domain.Event into its JSON representation.
// The processes slice is normalised to a non-nil empty slice so the payload
// field always serialises as an array, never as null.
func newEventResponse(event domain.Event) eventResponse {
	processes := make([]processResponse, 0, len(event.Processes))
	for _, process := range event.Processes {
		processes = append(processes, newProcessResponse(process))
	}
	return eventResponse{
		ID:             event.ID,
		Type:           event.Type,
		Payload:        json.RawMessage(event.Payload),
		IdempotencyKey: event.IdempotencyKey,
		Status:         event.Status,
		Attempts:       event.Attempts,
		NextRetryAt:    event.NextRetryAt,
		CreatedAt:      event.CreatedAt,
		UpdatedAt:      event.UpdatedAt,
		Processes:      processes,
	}
}

// validEventStatuses is the set of event statuses accepted by the API. It
// mirrors the CHECK constraint on the events table.
var validEventStatuses = map[domain.EventStatus]struct{}{
	domain.StatusPending:       {},
	domain.StatusProcessing:    {},
	domain.StatusCompleted:     {},
	domain.StatusPartialFailed: {},
	domain.StatusDead:          {},
}

// isEventStatusValid reports whether the given status is one of the five
// accepted event statuses.
func isEventStatusValid(status domain.EventStatus) bool {
	_, ok := validEventStatuses[status]
	return ok
}

// writeJSON marshals payload and writes it with the given status code and the
// application/json content type. The body is marshalled to a buffer first so
// a marshal failure can still produce a well-formed 500 response.
func writeJSON(w http.ResponseWriter, status int, payload any) {
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal server error"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(bodyBytes)
}

// writeError writes a JSON error body with the given status code using the
// standard {"error": "message"} shape.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
