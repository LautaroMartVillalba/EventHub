package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"eventhub/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Helpers
// =============================================================================

// setupRepo creates a temporary SQLite database and returns a Repository.
func setupRepo(t *testing.T) *Repository {
	t.Helper()
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	require.NoError(t, err, "NewDB must succeed")
	t.Cleanup(func() { db.Close() })
	return NewRepository(db)
}

// helperEvent creates a domain.Event ready for insertion.
func helperEvent(eventType, payload, idempotencyKey string, status domain.EventStatus, processes []domain.Process) domain.Event {
	event := domain.NewEvent(eventType, payload, idempotencyKey, status, processes)
	return *event
}

// helperProcess creates a domain.Process ready for insertion.
func helperProcess(eventID, processName string) domain.Process {
	process := domain.NewProcess(eventID, processName)
	return *process
}

// =============================================================================
// TestInsertEvent_Success
// =============================================================================

func TestInsertEvent_Success(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	procs := []domain.Process{
		helperProcess("", "validate"),
		helperProcess("", "notify"),
	}
	event := helperEvent("order.created", `{"orderId":1}`, "idem-001", domain.StatusPending, procs)

	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err, "InsertEvent must succeed")

	// Fetch and verify
	fetched, err := repo.FetchByID(ctx, event.ID)
	require.NoError(t, err, "FetchByID must succeed")

	assert.Equal(t, event.ID, fetched.ID)
	assert.Equal(t, "order.created", fetched.Type)
	assert.Equal(t, `{"orderId":1}`, fetched.Payload)
	assert.Equal(t, "idem-001", fetched.IdempotencyKey)
	assert.Equal(t, domain.StatusPending, fetched.Status)
	assert.Equal(t, 0, fetched.Attempts)

	// Verify processes were stored
	require.Len(t, fetched.Processes, 2)
	assert.Equal(t, event.ID, fetched.Processes[0].EventID)
	assert.Equal(t, event.ID, fetched.Processes[1].EventID)
	assert.Equal(t, "validate", fetched.Processes[0].ProcessName)
	assert.Equal(t, "notify", fetched.Processes[1].ProcessName)
	assert.Equal(t, domain.ProcessStatusPending, fetched.Processes[0].Status)
	assert.Equal(t, domain.ProcessStatusPending, fetched.Processes[1].Status)
}

// =============================================================================
// TestInsertEvent_Conflict
// =============================================================================

func TestInsertEvent_Conflict(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	event1 := helperEvent("order.created", `{}`, "idem-dup", domain.StatusPending, nil)
	err := repo.InsertEvent(ctx, event1)
	require.NoError(t, err, "first insert must succeed")

	// Insert again with same idempotency_key
	event2 := helperEvent("order.created", `{}`, "idem-dup", domain.StatusPending, nil)
	err = repo.InsertEvent(ctx, event2)
	require.Error(t, err, "second insert with same key must fail")
	assert.True(t, errors.Is(err, ErrConflict), "error must be ErrConflict, got: %v", err)
}

// =============================================================================
// TestInsertEvent_EmptyProcesses
// =============================================================================

func TestInsertEvent_EmptyProcesses(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	event := helperEvent("test", `{}`, "idem-emptyprocs", domain.StatusPending, nil)
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err, "InsertEvent with nil processes must succeed")

	fetched, err := repo.FetchByID(ctx, event.ID)
	require.NoError(t, err)
	assert.Empty(t, fetched.Processes, "no processes should be stored")
	assert.Equal(t, domain.StatusPending, fetched.Status)
}

// =============================================================================
// TestInsertEvent_WithZeroLengthProcessesSlice
// =============================================================================

func TestInsertEvent_WithZeroLengthProcessesSlice(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	event := helperEvent("test", `{}`, "idem-zero", domain.StatusPending, []domain.Process{})
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err, "InsertEvent with empty processes slice must succeed")

	fetched, err := repo.FetchByID(ctx, event.ID)
	require.NoError(t, err)
	assert.Empty(t, fetched.Processes)
}

// =============================================================================
// TestFetchByID_NotFound
// =============================================================================

func TestFetchByID_NotFound(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	_, err := repo.FetchByID(ctx, "nonexistent-id")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound),
		"error must wrap ErrNotFound, got: %v", err)
}

// =============================================================================
// TestFetchByID_WithProcesses
// =============================================================================

func TestFetchByID_WithProcesses(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	procs := []domain.Process{
		helperProcess("", "step-1"),
		helperProcess("", "step-2"),
		helperProcess("", "step-3"),
	}
	event := helperEvent("multi.step", `{"data":"x"}`, "idem-fetchprocs", domain.StatusPending, procs)

	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	fetched, err := repo.FetchByID(ctx, event.ID)
	require.NoError(t, err)

	require.Len(t, fetched.Processes, 3)

	// Verify each process
	for i, p := range fetched.Processes {
		assert.NotEmpty(t, p.ID, "process %d must have an ID", i)
		assert.Equal(t, event.ID, p.EventID, "process %d EventID mismatch", i)
		assert.Equal(t, fmt.Sprintf("step-%d", i+1), p.ProcessName)
		assert.Equal(t, domain.ProcessStatusPending, p.Status)
		assert.Equal(t, 0, p.Attempts)
	}
}

// =============================================================================
// TestFetchReadyEvents
// =============================================================================

func TestFetchReadyEvents(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	// Insert a pending event with a pending process → should be ready
	procPending := helperProcess("", "ready-proc")
	eventReady := helperEvent("ready", `{}`, "idem-ready", domain.StatusPending, []domain.Process{procPending})
	err := repo.InsertEvent(ctx, eventReady)
	require.NoError(t, err)

	// Insert a partial_failed event with a pending process → should be ready
	procPartial := helperProcess("", "partial-proc")
	eventPartial := helperEvent("partial", `{}`, "idem-partial", domain.StatusPartialFailed, []domain.Process{procPartial})
	err = repo.InsertEvent(ctx, eventPartial)
	require.NoError(t, err)

	// Fetch ready events
	events, err := repo.FetchReadyEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, events, 2, "both pending and partial_failed events should be ready")

	// Verify processes are loaded
	for _, e := range events {
		assert.NotEmpty(t, e.Processes, "event %s should have processes loaded", e.ID)
	}
}

// =============================================================================
// TestFetchReadyEvents_NotReady
// =============================================================================

func TestFetchReadyEvents_NotReady(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	// Processing event → not ready
	proc1 := helperProcess("", "p1")
	eventProcessing := helperEvent("proc", `{}`, "idem-processing", domain.StatusProcessing, []domain.Process{proc1})
	err := repo.InsertEvent(ctx, eventProcessing)
	require.NoError(t, err)

	// Completed event → not ready
	proc2 := helperProcess("", "p2")
	eventCompleted := helperEvent("done", `{}`, "idem-completed", domain.StatusCompleted, []domain.Process{proc2})
	err = repo.InsertEvent(ctx, eventCompleted)
	require.NoError(t, err)

	// Dead event → not ready
	proc3 := helperProcess("", "p3")
	eventDead := helperEvent("dead", `{}`, "idem-dead", domain.StatusDead, []domain.Process{proc3})
	err = repo.InsertEvent(ctx, eventDead)
	require.NoError(t, err)

	events, err := repo.FetchReadyEvents(ctx, 10)
	require.NoError(t, err)
	assert.Empty(t, events, "processing, completed, and dead events must not be ready")
}

// =============================================================================
// TestFetchReadyEvents_AllProcessesCompleted
// =============================================================================

func TestFetchReadyEvents_AllProcessesCompleted(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	// Insert a pending event where all processes are already completed
	proc := domain.Process{
		ID:          domain.NewProcess("", "done-proc").ID,
		EventID:     "", // will be set by NewEvent
		ProcessName: "done-proc",
		Status:      domain.ProcessStatusCompleted,
		Attempts:    1,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}
	event := helperEvent("done", `{}`, "idem-alldone", domain.StatusPending, []domain.Process{proc})
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	events, err := repo.FetchReadyEvents(ctx, 10)
	require.NoError(t, err)
	assert.Empty(t, events, "event with all completed processes must not be ready")
}

// =============================================================================
// TestFetchReadyEvents_NextRetryInFuture
// =============================================================================

func TestFetchReadyEvents_NextRetryInFuture(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	futureTime := time.Now().UTC().Add(1 * time.Hour)

	// Insert a pending event with a process whose next_retry_at is in the future
	proc := helperProcess("", "future-proc")
	proc.Status = domain.ProcessStatusFailed
	proc.NextRetryAt = &futureTime
	event := helperEvent("future", `{}`, "idem-future", domain.StatusPending, []domain.Process{proc})
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	events, err := repo.FetchReadyEvents(ctx, 10)
	require.NoError(t, err)
	assert.Empty(t, events, "event with process next_retry in the future must not be ready")
}

// =============================================================================
// TestFetchReadyEvents_Limit
// =============================================================================

func TestFetchReadyEvents_Limit(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	// Insert 5 ready events
	for i := 0; i < 5; i++ {
		proc := helperProcess("", "proc")
		event := helperEvent("test", `{}`, fmt.Sprintf("idem-limit-%d", i), domain.StatusPending, []domain.Process{proc})
		err := repo.InsertEvent(ctx, event)
		require.NoError(t, err)
	}

	events, err := repo.FetchReadyEvents(ctx, 2)
	require.NoError(t, err)
	assert.Len(t, events, 2, "limit must be respected")
}

// =============================================================================
// TestFetchByStatus
// =============================================================================

func TestFetchByStatus(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	// Insert events with different statuses
	for i := 0; i < 3; i++ {
		event := helperEvent("test", `{}`, fmt.Sprintf("idem-pending-%d", i), domain.StatusPending, nil)
		err := repo.InsertEvent(ctx, event)
		require.NoError(t, err)
	}
	// Insert a completed event
	eventCompleted := helperEvent("test", `{}`, "idem-completed-1", domain.StatusCompleted, nil)
	err := repo.InsertEvent(ctx, eventCompleted)
	require.NoError(t, err)

	// Fetch pending events
	pending, err := repo.FetchByStatus(ctx, domain.StatusPending)
	require.NoError(t, err)
	assert.Len(t, pending, 3, "should find 3 pending events")

	// Fetch completed events
	completed, err := repo.FetchByStatus(ctx, domain.StatusCompleted)
	require.NoError(t, err)
	assert.Len(t, completed, 1, "should find 1 completed event")

	// Fetch dead events (none)
	dead, err := repo.FetchByStatus(ctx, domain.StatusDead)
	require.NoError(t, err)
	assert.Empty(t, dead, "should find 0 dead events")
}

// =============================================================================
// TestInsertProcesses
// =============================================================================

func TestInsertProcesses(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	// Insert an event without processes
	event := helperEvent("test", `{}`, "idem-addprocs", domain.StatusPending, nil)
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	// Now add processes
	newProcs := []domain.Process{
		helperProcess(event.ID, "new-proc-1"),
		helperProcess(event.ID, "new-proc-2"),
	}
	err = repo.InsertProcesses(ctx, event.ID, newProcs)
	require.NoError(t, err, "InsertProcesses must succeed")

	// Verify via FetchByID
	fetched, err := repo.FetchByID(ctx, event.ID)
	require.NoError(t, err)
	require.Len(t, fetched.Processes, 2)
	assert.Equal(t, "new-proc-1", fetched.Processes[0].ProcessName)
	assert.Equal(t, "new-proc-2", fetched.Processes[1].ProcessName)
}

// =============================================================================
// TestFetchProcessesForRetry
// =============================================================================

func TestFetchProcessesForRetry(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	// Create processes with different statuses
	procs := []domain.Process{
		helperProcess("", "pending-proc"),   // pending → should appear
		helperProcess("", "failed-proc"),    // pending by default
		helperProcess("", "completed-proc"), // will be set to completed
	}

	event := helperEvent("test", `{}`, "idem-retry", domain.StatusPending, procs)
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	// Mark the third process as completed
	err = repo.UpdateProcessStatus(ctx, event.Processes[2].ID, domain.ProcessStatusCompleted, 1, nil, "")
	require.NoError(t, err)

	// Fetch processes for retry — only non-completed should appear
	retryProcs, err := repo.FetchProcessesForRetry(ctx, event.ID)
	require.NoError(t, err)
	assert.Len(t, retryProcs, 2, "only non-completed processes should be returned")

	for _, p := range retryProcs {
		assert.NotEqual(t, domain.ProcessStatusCompleted, p.Status,
			"completed process must not appear in retry results")
	}
}

// =============================================================================
// TestFetchProcessesForRetry_Empty
// =============================================================================

func TestFetchProcessesForRetry_Empty(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	procs, err := repo.FetchProcessesForRetry(ctx, "nonexistent-event")
	require.NoError(t, err)
	assert.Empty(t, procs, "non-existent event should return empty slice")
}

// =============================================================================
// TestFetchProcessesForRetry_AllCompleted
// =============================================================================

func TestFetchProcessesForRetry_AllCompleted(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	procs := []domain.Process{
		helperProcess("", "p1"),
		helperProcess("", "p2"),
	}
	event := helperEvent("test", `{}`, "idem-allcompleted", domain.StatusPending, procs)
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	// Mark both as completed
	for _, p := range event.Processes {
		err = repo.UpdateProcessStatus(ctx, p.ID, domain.ProcessStatusCompleted, 1, nil, "")
		require.NoError(t, err)
	}

	retryProcs, err := repo.FetchProcessesForRetry(ctx, event.ID)
	require.NoError(t, err)
	assert.Empty(t, retryProcs, "all completed should return empty slice")
}

// =============================================================================
// TestUpdateEventStatus
// =============================================================================

func TestUpdateEventStatus(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	event := helperEvent("test", `{}`, "idem-updatestatus", domain.StatusPending, nil)
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	// Update to processing
	err = repo.UpdateEventStatus(ctx, event.ID, domain.StatusProcessing)
	require.NoError(t, err, "UpdateEventStatus must succeed")

	fetched, err := repo.FetchByID(ctx, event.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusProcessing, fetched.Status)
}

// =============================================================================
// TestUpdateEventStatus_NotFound
// =============================================================================

func TestUpdateEventStatus_NotFound(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	err := repo.UpdateEventStatus(ctx, "nonexistent", domain.StatusProcessing)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound),
		"error must wrap ErrNotFound, got: %v", err)
}

// =============================================================================
// TestUpdateEventStatus_MultipleStatuses
// =============================================================================

func TestUpdateEventStatus_MultipleStatuses(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	event := helperEvent("test", `{}`, "idem-multistatus", domain.StatusPending, nil)
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	statuses := []domain.EventStatus{
		domain.StatusProcessing,
		domain.StatusPartialFailed,
		domain.StatusCompleted,
	}

	for _, status := range statuses {
		err = repo.UpdateEventStatus(ctx, event.ID, status)
		require.NoError(t, err, "UpdateEventStatus to %q must succeed", status)

		fetched, err := repo.FetchByID(ctx, event.ID)
		require.NoError(t, err)
		assert.Equal(t, status, fetched.Status, "status should be %q", status)
	}
}

// =============================================================================
// TestUpdateProcessStatus
// =============================================================================

func TestUpdateProcessStatus(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	proc := helperProcess("", "test-proc")
	event := helperEvent("test", `{}`, "idem-procupdate", domain.StatusPending, []domain.Process{proc})
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	processID := event.Processes[0].ID
	nextRetry := time.Now().UTC().Add(5 * time.Minute)

	// Update process
	err = repo.UpdateProcessStatus(ctx, processID, domain.ProcessStatusFailed, 3, &nextRetry, "connection timeout")
	require.NoError(t, err, "UpdateProcessStatus must succeed")

	// Verify
	fetched, err := repo.FetchByID(ctx, event.ID)
	require.NoError(t, err)
	require.Len(t, fetched.Processes, 1)

	p := fetched.Processes[0]
	assert.Equal(t, domain.ProcessStatusFailed, p.Status)
	assert.Equal(t, 3, p.Attempts)
	assert.Equal(t, "connection timeout", p.ErrorMsg)
	require.NotNil(t, p.NextRetryAt)
	assert.WithinDuration(t, nextRetry, *p.NextRetryAt, 2*time.Second,
		"NextRetryAt should match within 2 seconds")
}

// =============================================================================
// TestUpdateProcessStatus_NilNextRetry
// =============================================================================

func TestUpdateProcessStatus_NilNextRetry(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	proc := helperProcess("", "nil-retry")
	event := helperEvent("test", `{}`, "idem-nilretry", domain.StatusPending, []domain.Process{proc})
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	processID := event.Processes[0].ID

	// Update with nil NextRetryAt
	err = repo.UpdateProcessStatus(ctx, processID, domain.ProcessStatusFailed, 1, nil, "some error")
	require.NoError(t, err)

	fetched, err := repo.FetchByID(ctx, event.ID)
	require.NoError(t, err)
	p := fetched.Processes[0]
	assert.Equal(t, domain.ProcessStatusFailed, p.Status)
	assert.Equal(t, 1, p.Attempts)
	assert.Equal(t, "some error", p.ErrorMsg)
	assert.Nil(t, p.NextRetryAt, "NextRetryAt should be nil")
}

// =============================================================================
// TestUpdateProcessStatus_EmptyErrorMsg
// =============================================================================

func TestUpdateProcessStatus_EmptyErrorMsg(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	proc := helperProcess("", "empty-err")
	event := helperEvent("test", `{}`, "idem-emptyerr", domain.StatusPending, []domain.Process{proc})
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	processID := event.Processes[0].ID

	err = repo.UpdateProcessStatus(ctx, processID, domain.ProcessStatusCompleted, 1, nil, "")
	require.NoError(t, err)

	fetched, err := repo.FetchByID(ctx, event.ID)
	require.NoError(t, err)
	p := fetched.Processes[0]
	assert.Equal(t, domain.ProcessStatusCompleted, p.Status)
	assert.Empty(t, p.ErrorMsg, "ErrorMsg should be empty")
}

// =============================================================================
// TestUpdateProcessStatus_NotFound
// =============================================================================

func TestUpdateProcessStatus_NotFound(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	err := repo.UpdateProcessStatus(ctx, "nonexistent-proc", domain.ProcessStatusCompleted, 1, nil, "")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound),
		"error must wrap ErrNotFound, got: %v", err)
}

// =============================================================================
// TestUpdateProcessStatus_ZeroAttempts
// =============================================================================

func TestUpdateProcessStatus_ZeroAttempts(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	proc := helperProcess("", "zero-att")
	event := helperEvent("test", `{}`, "idem-zeroatt", domain.StatusPending, []domain.Process{proc})
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	processID := event.Processes[0].ID

	// Reset attempts to zero
	err = repo.UpdateProcessStatus(ctx, processID, domain.ProcessStatusPending, 0, nil, "")
	require.NoError(t, err)

	fetched, err := repo.FetchByID(ctx, event.ID)
	require.NoError(t, err)
	assert.Equal(t, 0, fetched.Processes[0].Attempts)
	assert.Equal(t, domain.ProcessStatusPending, fetched.Processes[0].Status)
}

// =============================================================================
// TestMoveEventToDeadLetter
// =============================================================================

func TestMoveEventToDeadLetter(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	procs := []domain.Process{
		helperProcess("", "step-1"),
	}
	event := helperEvent("dead.test", `{"data":"important"}`, "idem-dlq", domain.StatusPending, procs)
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	reason := "max retries exceeded for step-1"
	err = repo.MoveEventToDeadLetter(ctx, event.ID, reason)
	require.NoError(t, err, "MoveEventToDeadLetter must succeed")

	// Event status should now be dead
	fetched, err := repo.FetchByID(ctx, event.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusDead, fetched.Status,
		"event status must be dead after moving to dead letter")
}

// =============================================================================
// TestMoveEventToDeadLetter_NotFound
// =============================================================================

func TestMoveEventToDeadLetter_NotFound(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	err := repo.MoveEventToDeadLetter(ctx, "nonexistent-event", "reason")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound),
		"error must wrap ErrNotFound, got: %v", err)
}

// =============================================================================
// TestMoveEventToDeadLetter_DeadLetterPersisted
// =============================================================================

func TestMoveEventToDeadLetter_DeadLetterPersisted(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	event := helperEvent("dlq.test", `{"orderId":999}`, "idem-dlq2", domain.StatusPending, nil)
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	err = repo.MoveEventToDeadLetter(ctx, event.ID, "test reason")
	require.NoError(t, err)

	// Verify the dead_letter entry exists by checking if event is still reachable as dead
	fetched, err := repo.FetchByID(ctx, event.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusDead, fetched.Status)

	// Also query the dead_letter table directly via db
	var count int
	err = repo.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dead_letter WHERE event_id = ?", event.ID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "dead_letter entry must exist")

	var dlReason string
	err = repo.db.QueryRowContext(ctx, "SELECT reason FROM dead_letter WHERE event_id = ?", event.ID).Scan(&dlReason)
	require.NoError(t, err)
	assert.Equal(t, "test reason", dlReason)
}

// =============================================================================
// TestRequeueEvent
// =============================================================================

func TestRequeueEvent(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	// Insert event with multiple processes
	procs := []domain.Process{
		helperProcess("", "proc-a"),
		helperProcess("", "proc-b"),
		helperProcess("", "proc-c"),
	}
	event := helperEvent("requeue.test", `{}`, "idem-requeue", domain.StatusProcessing, procs)
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	// Mark some processes as completed, some as failed
	err = repo.UpdateProcessStatus(ctx, event.Processes[0].ID, domain.ProcessStatusCompleted, 2, nil, "")
	require.NoError(t, err)
	err = repo.UpdateProcessStatus(ctx, event.Processes[1].ID, domain.ProcessStatusFailed, 5, nil, "timeout")
	require.NoError(t, err)
	// Leave proc-c as pending

	// Also update event status to partial_failed to simulate a real scenario
	err = repo.UpdateEventStatus(ctx, event.ID, domain.StatusPartialFailed)
	require.NoError(t, err)

	// Requeue
	err = repo.RequeueEvent(ctx, event.ID)
	require.NoError(t, err, "RequeueEvent must succeed")

	// Verify event
	fetched, err := repo.FetchByID(ctx, event.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusPending, fetched.Status, "event must be reset to pending")
	assert.Equal(t, 0, fetched.Attempts, "attempts must be reset to 0")
	assert.Nil(t, fetched.NextRetryAt, "NextRetryAt must be nil")

	// Completed process must remain completed
	assert.Equal(t, domain.ProcessStatusCompleted, fetched.Processes[0].Status,
		"completed process must stay completed")
	assert.Equal(t, 2, fetched.Processes[0].Attempts,
		"completed process attempts must stay unchanged")

	// Failed process must be reset to pending
	assert.Equal(t, domain.ProcessStatusPending, fetched.Processes[1].Status,
		"failed process must be reset to pending")
	assert.Equal(t, 0, fetched.Processes[1].Attempts,
		"failed process attempts must be reset to 0")
	assert.Empty(t, fetched.Processes[1].ErrorMsg,
		"failed process ErrorMsg must be cleared")
	assert.Nil(t, fetched.Processes[1].NextRetryAt,
		"failed process NextRetryAt must be nil")

	// Pending process must stay pending but attempts reset
	assert.Equal(t, domain.ProcessStatusPending, fetched.Processes[2].Status,
		"pending process must stay pending")
	assert.Equal(t, 0, fetched.Processes[2].Attempts,
		"pending process attempts must be reset to 0")
}

// =============================================================================
// TestRequeueEvent_NotFound
// =============================================================================

func TestRequeueEvent_NotFound(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	err := repo.RequeueEvent(ctx, "nonexistent-event")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound),
		"error must wrap ErrNotFound, got: %v", err)
}

// =============================================================================
// TestRequeueEvent_OnlyPendingProcesses
// =============================================================================

func TestRequeueEvent_OnlyPendingProcesses(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	procs := []domain.Process{
		helperProcess("", "proc-only-pending"),
	}
	event := helperEvent("test", `{}`, "idem-reqpending", domain.StatusPending, procs)
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	err = repo.RequeueEvent(ctx, event.ID)
	require.NoError(t, err)

	fetched, err := repo.FetchByID(ctx, event.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusPending, fetched.Status)
	assert.Equal(t, 0, fetched.Attempts)
	assert.Equal(t, domain.ProcessStatusPending, fetched.Processes[0].Status)
}

// =============================================================================
// TestConcurrentWrites
// =============================================================================

func TestConcurrentWrites(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	// SQLite allows only one writer at a time. Serialize writes through the connection
	// pool so concurrent goroutines don't hit SQLITE_BUSY.
	repo.db.SetMaxOpenConns(1)

	const numGoroutines = 10

	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			event := helperEvent(
				"concurrent",
				`{}`,
				fmt.Sprintf("idem-concurrent-%d", index),
				domain.StatusPending,
				nil,
			)
			errCh <- repo.InsertEvent(ctx, event)
		}(i)
	}

	wg.Wait()
	close(errCh)

	// All inserts must succeed
	var failures []error
	for e := range errCh {
		if e != nil {
			failures = append(failures, e)
		}
	}
	assert.Empty(t, failures, "concurrent inserts must all succeed, failures: %v", failures)

	// Verify all were inserted
	events, err := repo.FetchByStatus(ctx, domain.StatusPending)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(events), numGoroutines,
		"all concurrently inserted events must exist")
}

// =============================================================================
// TestInsertEvent_AllStatuses
// =============================================================================

func TestInsertEvent_AllStatuses(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	statuses := []domain.EventStatus{
		domain.StatusPending,
		domain.StatusProcessing,
		domain.StatusCompleted,
		domain.StatusPartialFailed,
		domain.StatusDead,
	}

	for i, status := range statuses {
		event := helperEvent("test", `{}`, fmt.Sprintf("idem-allstatus-%d", i), status, nil)
		err := repo.InsertEvent(ctx, event)
		require.NoError(t, err, "InsertEvent with status %q must succeed", status)

		fetched, err := repo.FetchByID(ctx, event.ID)
		require.NoError(t, err)
		assert.Equal(t, status, fetched.Status, "stored status must be %q", status)
	}
}

// =============================================================================
// TestInsertEvent_LargePayload
// =============================================================================

func TestInsertEvent_LargePayload(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	// Create a large JSON payload
	largePayload := `{"data":"` + string(make([]byte, 10000)) + `"}`
	// Actually let's use a valid JSON
	largePayload = `{"items":[]}`
	for i := 0; i < 100; i++ {
		largePayload = largePayload[:len(largePayload)-1] + fmt.Sprintf(`,{"id":%d,"name":"item-%d"}`, i, i) + `]}`
	}

	event := helperEvent("large", largePayload, "idem-large", domain.StatusPending, nil)
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err, "InsertEvent with large payload must succeed")

	fetched, err := repo.FetchByID(ctx, event.ID)
	require.NoError(t, err)
	assert.Equal(t, largePayload, fetched.Payload)
}

// =============================================================================
// TestInsertEvent_SpecialCharactersInPayload
// =============================================================================

func TestInsertEvent_SpecialCharactersInPayload(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	specialPayload := `{"msg":"hello\nworld\t\"quoted\"", "emoji":"🚀", "null":null, "bool":true}`
	event := helperEvent("special", specialPayload, "idem-special", domain.StatusPending, nil)
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err, "InsertEvent with special chars must succeed")

	fetched, err := repo.FetchByID(ctx, event.ID)
	require.NoError(t, err)
	assert.Equal(t, specialPayload, fetched.Payload)
}

// =============================================================================
// TestFetchReadyEvents_WithPastNextRetry
// =============================================================================

func TestFetchReadyEvents_WithPastNextRetry(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	pastTime := time.Now().UTC().Add(-1 * time.Hour)

	// Insert a pending event with a process whose next_retry_at is in the past → should be ready
	proc := helperProcess("", "past-proc")
	proc.Status = domain.ProcessStatusFailed
	proc.NextRetryAt = &pastTime
	event := helperEvent("past", `{}`, "idem-past", domain.StatusPending, []domain.Process{proc})
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	events, err := repo.FetchReadyEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, events, 1, "event with past next_retry_at must be ready")
}

// =============================================================================
// TestInsertEvent_ProcessWithErrorMsg
// =============================================================================

func TestInsertEvent_ProcessWithErrorMsg(t *testing.T) {
	repo := setupRepo(t)
	ctx := context.Background()

	// Construct a process with a non-empty ErrorMsg to cover the nullString non-empty branch.
	now := time.Now().UTC()
	proc := domain.Process{
		ID:          domain.NewProcess("", "error-proc").ID,
		EventID:     "", // will be set by NewEvent
		ProcessName: "error-proc",
		Status:      domain.ProcessStatusFailed,
		Attempts:    3,
		ErrorMsg:    "something went wrong",
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	event := helperEvent("test", `{}`, "idem-errmsg", domain.StatusPartialFailed, []domain.Process{proc})
	err := repo.InsertEvent(ctx, event)
	require.NoError(t, err)

	fetched, err := repo.FetchByID(ctx, event.ID)
	require.NoError(t, err)
	require.Len(t, fetched.Processes, 1)
	assert.Equal(t, "something went wrong", fetched.Processes[0].ErrorMsg,
		"ErrorMsg must be preserved")
	assert.Equal(t, domain.ProcessStatusFailed, fetched.Processes[0].Status)
	assert.Equal(t, 3, fetched.Processes[0].Attempts)
}

// =============================================================================
// TestStorageErrors
// =============================================================================

func TestStorageErrors(t *testing.T) {
	assert.NotNil(t, ErrNotFound)
	assert.NotNil(t, ErrConflict)
	assert.Equal(t, "record not found", ErrNotFound.Error())
	assert.Equal(t, "idempotency key already exists", ErrConflict.Error())
}
