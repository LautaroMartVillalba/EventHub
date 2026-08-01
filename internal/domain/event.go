// Package domain defines the core business types for EventHub.
//
// Event and Process are the two main entities. Events flow through named
// processes; each process tracks its own status and retry schedule.
package domain

import (
	"time"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// Event status constants
// ---------------------------------------------------------------------------

type EventStatus string

const (
	StatusPending       EventStatus = "pending"
	StatusProcessing    EventStatus = "processing"
	StatusCompleted     EventStatus = "completed"
	StatusPartialFailed EventStatus = "partial_failed"
	StatusDead          EventStatus = "dead"
)

// ---------------------------------------------------------------------------
// Process status constants
// ---------------------------------------------------------------------------

type ProcessStatus string

const (
	ProcessStatusPending    ProcessStatus = "pending"
	ProcessStatusProcessing ProcessStatus = "processing"
	ProcessStatusCompleted  ProcessStatus = "completed"
	ProcessStatusFailed     ProcessStatus = "failed"
	ProcessStatusDead       ProcessStatus = "dead"
)

// ---------------------------------------------------------------------------
// Event
// ---------------------------------------------------------------------------

// Event represents a unit of work flowing through the EventHub pipeline.
// Each event carries a JSON payload, an idempotency key, and a list of
// processes that must be executed.
type Event struct {
	ID             string
	Type           string
	Payload        string // JSON string
	IdempotencyKey string
	Status         EventStatus
	Attempts       int
	NextRetryAt    *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Processes      []Process
}

// NewEvent creates a new Event with a generated UUID, the provided properties,
// and timestamps set to the current UTC time. If processes is non-nil every
// process receives the new event ID.
func NewEvent(eventType, payload, idempotencyKey string, status EventStatus, processes []Process) *Event {
	now := time.Now().UTC()
	event := &Event{
		ID:             uuid.New().String(),
		Type:           eventType,
		Payload:        payload,
		IdempotencyKey: idempotencyKey,
		Status:         status,
		Attempts:       0,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if processes != nil {
		// Copy so we don't mutate the caller's slice.
		event.Processes = make([]Process, len(processes))
		copy(event.Processes, processes)
		for i := range event.Processes {
			event.Processes[i].EventID = event.ID
		}
	}
	return event
}

// ---------------------------------------------------------------------------
// Process
// ---------------------------------------------------------------------------

// Process represents a single processing step for an event. Multiple
// processes can belong to the same event; each tracks its own status,
// retry count, and error information.
type Process struct {
	ID          string
	EventID     string
	ProcessName string
	Status      ProcessStatus
	Attempts    int
	NextRetryAt *time.Time
	ErrorMsg    string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// NewProcess creates a new Process with a generated UUID, the given event
// and process name, default status "pending", and timestamps set to the
// current UTC time.
func NewProcess(eventID, processName string) *Process {
	now := time.Now().UTC()
	return &Process{
		ID:          uuid.New().String(),
		EventID:     eventID,
		ProcessName: processName,
		Status:      ProcessStatusPending,
		Attempts:    0,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}
