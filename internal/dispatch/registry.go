// Package dispatch routes events to the processing steps registered for
// their event type.
//
// The Registry is populated at startup (T17) with the four real event types
// and their processes; the level-3 fan-out (workers.FanOut, T11) reads it
// concurrently from the worker pool goroutines while handling events, so
// reads and writes are protected with a RWMutex.
package dispatch

import (
	"context"
	"sync"

	"eventhub/internal/domain"
)

// Process is a named processing step registered for an event type. Name must
// match the ProcessName column of the domain.Process rows persisted for the
// event, so the fan-out can resolve database rows to their handler.
type Process struct {
	// Name identifies the process; it must equal the ProcessName of the
	// domain.Process rows persisted for the event.
	Name string
	// Fn executes the process for an event. A non-nil error is treated as
	// a process failure and retried according to the fan-out's policy.
	//
	// The returned error text is persisted verbatim in the error_msg column
	// of event_processes (plain text in SQLite and exposed through the API),
	// so it must not contain sensitive data (PII, tokens, secrets). The same
	// applies to panics: the recovered value is embedded in the persisted
	// error message, so Fn must also avoid panicking with sensitive values
	// (or the process must sanitize them before rethrowing).
	Fn func(ctx context.Context, event domain.Event) error
}

// Registry maps event types to the processes registered for them. It is
// written once at startup (Register) and read concurrently by the level-3
// fan-out goroutines (GetProcesses).
type Registry struct {
	processes map[string][]Process

	// processesMu guards the map: Register takes the write lock, every
	// GetProcesses takes the read lock so the fan-out goroutines can read
	// in parallel.
	processesMu sync.RWMutex
}

// NewRegistry returns an empty registry ready to be populated with Register.
func NewRegistry() *Registry {
	return &Registry{
		processes: make(map[string][]Process),
	}
}

// Register binds the given processes to the event type, replacing any
// previous registration for that type. It is intended to run once at
// startup, before the worker pool starts consuming events; the write lock
// makes concurrent Register calls safe. The processes slice is copied so the
// caller's slice cannot be mutated to corrupt the registry afterwards.
func (registry *Registry) Register(eventType string, processes []Process) {
	registry.processesMu.Lock()
	defer registry.processesMu.Unlock()
	copied := make([]Process, len(processes))
	copy(copied, processes)
	registry.processes[eventType] = copied
}

// GetProcesses returns the processes registered for the event type, or nil
// if none are. The returned slice is a defensive copy: the caller can
// reorder or mutate it without affecting the registry.
func (registry *Registry) GetProcesses(eventType string) []Process {
	registry.processesMu.RLock()
	defer registry.processesMu.RUnlock()
	registered, ok := registry.processes[eventType]
	if !ok {
		return nil
	}
	processes := make([]Process, len(registered))
	copy(processes, registered)
	return processes
}
