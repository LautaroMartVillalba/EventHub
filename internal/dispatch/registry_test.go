package dispatch

import (
	"context"
	"sync"
	"testing"

	"eventhub/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dummyProcessFn is a no-op handler for use in registry tests.
func dummyProcessFn(ctx context.Context, event domain.Event) error {
	return nil
}

// TestRegistry_RegisterAndGetProcesses verifies that registered processes
// are returned by GetProcesses.
func TestRegistry_RegisterAndGetProcesses(t *testing.T) {
	reg := NewRegistry()
	processes := []Process{
		{Name: "send-email", Fn: dummyProcessFn},
		{Name: "update-db", Fn: dummyProcessFn},
	}
	reg.Register("order.created", processes)

	got := reg.GetProcesses("order.created")
	require.Len(t, got, 2)
	assert.Equal(t, "send-email", got[0].Name)
	assert.Equal(t, "update-db", got[1].Name)
}

// TestRegistry_GetProcesses_UnregisteredType returns nil for a type that was
// never registered.
func TestRegistry_GetProcesses_UnregisteredType(t *testing.T) {
	reg := NewRegistry()
	got := reg.GetProcesses("nonexistent")
	assert.Nil(t, got)
}

// TestRegistry_GetProcesses_DefensiveCopy verifies that mutating the slice
// returned by GetProcesses does NOT affect the registry's internal state.
func TestRegistry_GetProcesses_DefensiveCopy(t *testing.T) {
	reg := NewRegistry()
	processes := []Process{
		{Name: "send-email", Fn: dummyProcessFn},
	}
	reg.Register("order.created", processes)

	// Get a copy and mutate it.
	got := reg.GetProcesses("order.created")
	require.Len(t, got, 1)
	got[0] = Process{Name: "corrupted", Fn: nil}

	// Second GetProcesses must still return the original.
	got2 := reg.GetProcesses("order.created")
	require.Len(t, got2, 1)
	assert.Equal(t, "send-email", got2[0].Name)
}

// TestRegistry_Register_ReplacesPrevious verifies that registering the same
// event type twice replaces the previous registration.
func TestRegistry_Register_ReplacesPrevious(t *testing.T) {
	reg := NewRegistry()
	reg.Register("order.created", []Process{
		{Name: "first", Fn: dummyProcessFn},
	})
	reg.Register("order.created", []Process{
		{Name: "second", Fn: dummyProcessFn},
	})

	got := reg.GetProcesses("order.created")
	require.Len(t, got, 1)
	assert.Equal(t, "second", got[0].Name)
}

// TestRegistry_ConcurrentAccess runs Register and GetProcesses from multiple
// goroutines to verify no data races with -race.
func TestRegistry_ConcurrentAccess(t *testing.T) {
	reg := NewRegistry()
	processes := []Process{
		{Name: "send-email", Fn: dummyProcessFn},
		{Name: "update-db", Fn: dummyProcessFn},
	}

	var wg sync.WaitGroup
	eventTypes := []string{"order.created", "user.registered", "payment.completed", "email.sent"}

	// Register each type from its own goroutine.
	for _, et := range eventTypes {
		wg.Add(1)
		go func(eventType string) {
			defer wg.Done()
			reg.Register(eventType, processes)
		}(et)
	}

	// Concurrently read from multiple goroutines.
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, et := range eventTypes {
				_ = reg.GetProcesses(et)
			}
			_ = reg.GetProcesses("nonexistent")
		}()
	}

	wg.Wait()

	// After all registrations, each type must have two processes.
	for _, et := range eventTypes {
		got := reg.GetProcesses(et)
		require.Len(t, got, 2, "event type %s should have 2 processes", et)
	}
}
