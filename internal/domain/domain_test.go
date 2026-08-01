package domain

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// TestNewEvent
// =============================================================================

func TestNewEvent(t *testing.T) {
	processes := []Process{
		*NewProcess("", "validate"),
		*NewProcess("", "notify"),
	}

	event := NewEvent("order.created", `{"orderId":123}`, "idem-key-001", StatusPending, processes)

	// ID must be a valid UUID
	_, err := uuid.Parse(event.ID)
	require.NoError(t, err, "event ID must be a valid UUID")

	// Basic field checks
	assert.Equal(t, "order.created", event.Type)
	assert.Equal(t, `{"orderId":123}`, event.Payload)
	assert.Equal(t, "idem-key-001", event.IdempotencyKey)
	assert.Equal(t, StatusPending, event.Status)
	assert.Equal(t, 0, event.Attempts)
	assert.False(t, event.CreatedAt.IsZero(), "CreatedAt must not be zero")
	assert.False(t, event.UpdatedAt.IsZero(), "UpdatedAt must not be zero")
	assert.Equal(t, event.CreatedAt, event.UpdatedAt, "CreatedAt and UpdatedAt should be equal at creation")

	// Processes must be populated and have the correct EventID
	require.Len(t, event.Processes, 2)
	for i, process := range event.Processes {
		assert.Equal(t, event.ID, process.EventID, "process %d EventID must match the event ID", i)
		assert.NotEmpty(t, process.ID, "process %d must have a generated UUID", i)
	}

	assert.Equal(t, "validate", event.Processes[0].ProcessName)
	assert.Equal(t, "notify", event.Processes[1].ProcessName)

	// Verify the original processes slice is not mutated (copied)
	assert.Empty(t, processes[0].EventID, "original process EventID must not be mutated")
}

// =============================================================================
// TestNewEvent_NilProcesses
// =============================================================================

func TestNewEvent_NilProcesses(t *testing.T) {
	event := NewEvent("order.created", `{}`, "idem-key-002", StatusPending, nil)

	require.NotNil(t, event, "NewEvent must never return nil")
	assert.NotEmpty(t, event.ID)
	assert.Equal(t, "order.created", event.Type)
	assert.Nil(t, event.Processes, "Processes should be nil when passed nil")
}

// =============================================================================
// TestNewEvent_DefaultStatus
// =============================================================================

func TestNewEvent_DefaultStatus(t *testing.T) {
	// Verify all statuses are accepted
	for _, status := range []EventStatus{
		StatusPending,
		StatusProcessing,
		StatusCompleted,
		StatusPartialFailed,
		StatusDead,
	} {
		t.Run(string(status), func(t *testing.T) {
			event := NewEvent("test", `{}`, "idem-"+string(status), status, nil)
			require.NotNil(t, event)
			assert.Equal(t, status, event.Status)
		})
	}
}

// =============================================================================
// TestNewProcess
// =============================================================================

func TestNewProcess(t *testing.T) {
	process := NewProcess("evt-12345", "validate-order")

	// ID must be a valid UUID
	_, err := uuid.Parse(process.ID)
	require.NoError(t, err, "process ID must be a valid UUID")

	assert.Equal(t, "evt-12345", process.EventID)
	assert.Equal(t, "validate-order", process.ProcessName)
	assert.Equal(t, ProcessStatusPending, process.Status, "default status must be pending")
	assert.Equal(t, 0, process.Attempts)
	assert.Empty(t, process.ErrorMsg)
	assert.Nil(t, process.NextRetryAt)
	assert.False(t, process.CreatedAt.IsZero(), "CreatedAt must not be zero")
	assert.False(t, process.UpdatedAt.IsZero(), "UpdatedAt must not be zero")
	assert.Equal(t, process.CreatedAt, process.UpdatedAt, "CreatedAt and UpdatedAt should be equal")
}

// =============================================================================
// TestNewProcess_EmptyEventID
// =============================================================================

func TestNewProcess_EmptyEventID(t *testing.T) {
	// Should not panic with empty eventID
	process := NewProcess("", "some-process")
	require.NotNil(t, process)
	assert.Empty(t, process.EventID)
	assert.Equal(t, "some-process", process.ProcessName)
}

// =============================================================================
// TestEventStatus_Constants
// =============================================================================

func TestEventStatus_Constants(t *testing.T) {
	tests := []struct {
		name     string
		constant EventStatus
		expected string
	}{
		{"StatusPending", StatusPending, "pending"},
		{"StatusProcessing", StatusProcessing, "processing"},
		{"StatusCompleted", StatusCompleted, "completed"},
		{"StatusPartialFailed", StatusPartialFailed, "partial_failed"},
		{"StatusDead", StatusDead, "dead"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, string(tt.constant),
				"%s should have value %q", tt.name, tt.expected)
		})
	}

	// Verify EventStatus is a string type
	var es EventStatus = "pending"
	assert.Equal(t, "pending", string(es))
}

// =============================================================================
// TestProcessStatus_Constants
// =============================================================================

func TestProcessStatus_Constants(t *testing.T) {
	tests := []struct {
		name     string
		constant ProcessStatus
		expected string
	}{
		{"ProcessStatusPending", ProcessStatusPending, "pending"},
		{"ProcessStatusProcessing", ProcessStatusProcessing, "processing"},
		{"ProcessStatusCompleted", ProcessStatusCompleted, "completed"},
		{"ProcessStatusFailed", ProcessStatusFailed, "failed"},
		{"ProcessStatusDead", ProcessStatusDead, "dead"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, string(tt.constant),
				"%s should have value %q", tt.name, tt.expected)
		})
	}

	// Verify ProcessStatus is a string type
	var ps ProcessStatus = "pending"
	assert.Equal(t, "pending", string(ps))
}

// =============================================================================
// TestEventStruct
// =============================================================================

func TestEventStruct(t *testing.T) {
	e := Event{
		ID:             "evt-1",
		Type:           "order.created",
		Payload:        `{"data":"test"}`,
		IdempotencyKey: "key-1",
		Status:         StatusPending,
		Attempts:       3,
		Processes:      []Process{},
	}

	assert.Equal(t, "evt-1", e.ID)
	assert.Equal(t, "order.created", e.Type)
	assert.Equal(t, `{"data":"test"}`, e.Payload)
	assert.Equal(t, "key-1", e.IdempotencyKey)
	assert.Equal(t, StatusPending, e.Status)
	assert.Equal(t, 3, e.Attempts)
	assert.NotNil(t, e.Processes)
}

// =============================================================================
// TestProcessStruct
// =============================================================================

func TestProcessStruct(t *testing.T) {
	p := Process{
		ID:          "proc-1",
		EventID:     "evt-1",
		ProcessName: "validate",
		Status:      ProcessStatusCompleted,
		Attempts:    2,
		ErrorMsg:    "timeout",
	}

	assert.Equal(t, "proc-1", p.ID)
	assert.Equal(t, "evt-1", p.EventID)
	assert.Equal(t, "validate", p.ProcessName)
	assert.Equal(t, ProcessStatusCompleted, p.Status)
	assert.Equal(t, 2, p.Attempts)
	assert.Equal(t, "timeout", p.ErrorMsg)
}

// =============================================================================
// TestDomainErrors
// =============================================================================

func TestDomainErrors(t *testing.T) {
	// Verify sentinel errors exist and are non-nil
	assert.NotNil(t, ErrInvalidEventStatus)
	assert.NotNil(t, ErrInvalidProcessStatus)
	assert.NotNil(t, ErrEmptyEventType)
	assert.NotNil(t, ErrEmptyIdempotencyKey)

	// Verify error messages
	assert.Equal(t, "invalid event status", ErrInvalidEventStatus.Error())
	assert.Equal(t, "invalid process status", ErrInvalidProcessStatus.Error())
	assert.Equal(t, "event type must not be empty", ErrEmptyEventType.Error())
	assert.Equal(t, "idempotency key must not be empty", ErrEmptyIdempotencyKey.Error())
}

// =============================================================================
// TestNewEvent_ProcessesAreIndependentCopies
// =============================================================================

func TestNewEvent_ProcessesAreIndependentCopies(t *testing.T) {
	original := []Process{
		*NewProcess("", "step1"),
		*NewProcess("", "step2"),
	}

	event := NewEvent("test", `{}`, "idem-003", StatusPending, original)

	// Modify the returned processes
	event.Processes[0].Status = ProcessStatusCompleted

	// Original must be unchanged
	assert.Equal(t, ProcessStatusPending, original[0].Status,
		"modifying event processes must not affect the original slice")
}
