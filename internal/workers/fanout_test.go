package workers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"eventhub/internal/dispatch"
	"eventhub/internal/domain"
	"eventhub/internal/logging"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Silent logger for fanout tests
// ---------------------------------------------------------------------------

func silentLoggerFanout() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func silentCtxFanout() context.Context {
	return logging.WithContext(context.Background(), silentLoggerFanout())
}

// ---------------------------------------------------------------------------
// Fake FanOutRepository
// ---------------------------------------------------------------------------

type updateProcessStatusCall struct {
	ProcessID   string
	Status      domain.ProcessStatus
	Attempts    int
	NextRetryAt *time.Time
	ErrorMsg    string
}

type moveToDeadLetterCall struct {
	EventID string
	Reason  string
}

type fakeFanOutRepo struct {
	mu                    sync.Mutex
	processesByEventID    map[string][]domain.Process
	updateCalls           []updateProcessStatusCall
	moveToDeadLetterCalls []moveToDeadLetterCall
	fetchError            error
}

func newFakeFanOutRepo() *fakeFanOutRepo {
	return &fakeFanOutRepo{
		processesByEventID: make(map[string][]domain.Process),
	}
}

func (f *fakeFanOutRepo) FetchProcessesForRetry(_ context.Context, eventID string) ([]domain.Process, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fetchError != nil {
		return nil, f.fetchError
	}
	return f.processesByEventID[eventID], nil
}

func (f *fakeFanOutRepo) UpdateProcessStatus(_ context.Context, processID string, status domain.ProcessStatus, attempts int, nextRetryAt *time.Time, errorMsg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateCalls = append(f.updateCalls, updateProcessStatusCall{
		ProcessID:   processID,
		Status:      status,
		Attempts:    attempts,
		NextRetryAt: nextRetryAt,
		ErrorMsg:    errorMsg,
	})
	return nil
}

func (f *fakeFanOutRepo) MoveEventToDeadLetter(_ context.Context, eventID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.moveToDeadLetterCalls = append(f.moveToDeadLetterCalls, moveToDeadLetterCall{
		EventID: eventID,
		Reason:  reason,
	})
	return nil
}

// UpdateCallsForProcess returns all UpdateProcessStatus calls for the given process ID.
func (f *fakeFanOutRepo) UpdateCallsForProcess(processID string) []updateProcessStatusCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	var result []updateProcessStatusCall
	for _, c := range f.updateCalls {
		if c.ProcessID == processID {
			result = append(result, c)
		}
	}
	return result
}

// LastUpdateCallForProcess returns the last UpdateProcessStatus call for the process.
func (f *fakeFanOutRepo) LastUpdateCallForProcess(processID string) *updateProcessStatusCall {
	calls := f.UpdateCallsForProcess(processID)
	if len(calls) == 0 {
		return nil
	}
	return &calls[len(calls)-1]
}

// MoveToDeadLetterCalls returns all MoveEventToDeadLetter calls.
func (f *fakeFanOutRepo) MoveToDeadLetterCalls() []moveToDeadLetterCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]moveToDeadLetterCall, len(f.moveToDeadLetterCalls))
	copy(out, f.moveToDeadLetterCalls)
	return out
}

// TotalUpdateCalls returns the total number of UpdateProcessStatus calls.
func (f *fakeFanOutRepo) TotalUpdateCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.updateCalls)
}

// ---------------------------------------------------------------------------
// Time assertion helper
// ---------------------------------------------------------------------------

// assertNextRetryAtApprox verifies that nextRetryAt is approximately
// beforeCall + expectedBackoff, within a 2-second tolerance.
// Uses the same formula as the production code: time.Now().UTC().Add(backoff).
func assertNextRetryAtApprox(t *testing.T, expectedBackoff time.Duration, actual *time.Time, beforeCall time.Time) {
	t.Helper()
	require.NotNil(t, actual, "nextRetryAt must not be nil")
	lowerBound := beforeCall.Add(expectedBackoff)
	upperBound := time.Now().UTC().Add(expectedBackoff)
	assert.False(t, actual.Before(lowerBound),
		"nextRetryAt %v should be >= lower bound %v (beforeCall + backoff)", actual, lowerBound)
	assert.False(t, actual.After(upperBound),
		"nextRetryAt %v should be <= upper bound %v (now + backoff)", actual, upperBound)
}

// ---------------------------------------------------------------------------
// Case a: No handlers registered for the event type → nil
// ---------------------------------------------------------------------------

func TestFanOut_Process_NoHandlersRegistered(t *testing.T) {
	reg := dispatch.NewRegistry()
	repo := newFakeFanOutRepo()
	repo.processesByEventID["ev-1"] = []domain.Process{
		{ID: "proc-1", EventID: "ev-1", ProcessName: "send-email", Status: domain.ProcessStatusPending, Attempts: 0},
	}

	// No handlers registered for "order.created"
	fanout := NewFanOut(reg, repo, 3, nil)
	ctx := silentCtxFanout()
	event := domain.Event{ID: "ev-1", Type: "order.created"}

	err := fanout.Process(ctx, event)
	assert.NoError(t, err, "must return nil when no handlers are registered")

	// FetchProcessesForRetry is never called because len(handlers)==0 short-circuits.
	// Repo.UpdateProcessStatus should never be called.
	assert.Equal(t, 0, repo.TotalUpdateCalls(), "no UpdateProcessStatus calls expected")
}

// ---------------------------------------------------------------------------
// Case b: 2 processes ready, both OK → nil, both completed
// ---------------------------------------------------------------------------

func TestFanOut_Process_TwoProcessesBothOK(t *testing.T) {
	reg := dispatch.NewRegistry()
	var mu sync.Mutex
	var invocations []string

	reg.Register("order.created", []dispatch.Process{
		{Name: "send-email", Fn: func(ctx context.Context, event domain.Event) error {
			mu.Lock()
			invocations = append(invocations, "send-email")
			mu.Unlock()
			return nil
		}},
		{Name: "update-db", Fn: func(ctx context.Context, event domain.Event) error {
			mu.Lock()
			invocations = append(invocations, "update-db")
			mu.Unlock()
			return nil
		}},
	})

	repo := newFakeFanOutRepo()
	repo.processesByEventID["ev-1"] = []domain.Process{
		{ID: "proc-1", EventID: "ev-1", ProcessName: "send-email", Status: domain.ProcessStatusPending, Attempts: 0},
		{ID: "proc-2", EventID: "ev-1", ProcessName: "update-db", Status: domain.ProcessStatusPending, Attempts: 0},
	}

	fanout := NewFanOut(reg, repo, 3, nil)
	ctx := silentCtxFanout()
	event := domain.Event{ID: "ev-1", Type: "order.created"}

	err := fanout.Process(ctx, event)
	assert.NoError(t, err, "must return nil when all processes succeed")

	// Both processes must have been updated to completed with attempts+1.
	call1 := repo.LastUpdateCallForProcess("proc-1")
	require.NotNil(t, call1)
	assert.Equal(t, domain.ProcessStatusCompleted, call1.Status)
	assert.Equal(t, 1, call1.Attempts) // was 0, now 0+1
	assert.Nil(t, call1.NextRetryAt)
	assert.Empty(t, call1.ErrorMsg)

	call2 := repo.LastUpdateCallForProcess("proc-2")
	require.NotNil(t, call2)
	assert.Equal(t, domain.ProcessStatusCompleted, call2.Status)
	assert.Equal(t, 1, call2.Attempts)
	assert.Nil(t, call2.NextRetryAt)
	assert.Empty(t, call2.ErrorMsg)

	// Both handlers must have been called.
	assert.Len(t, invocations, 2)
}

// ---------------------------------------------------------------------------
// Case c: 1 OK + 1 error (retries left) → error, failed gets nextRetryAt
// ---------------------------------------------------------------------------

func TestFanOut_Process_OneOKOneError_RetriesLeft(t *testing.T) {
	reg := dispatch.NewRegistry()
	var mu sync.Mutex
	var invocations []string

	reg.Register("order.created", []dispatch.Process{
		{Name: "send-email", Fn: func(ctx context.Context, event domain.Event) error {
			mu.Lock()
			invocations = append(invocations, "send-email")
			mu.Unlock()
			return nil
		}},
		{Name: "update-db", Fn: func(ctx context.Context, event domain.Event) error {
			mu.Lock()
			invocations = append(invocations, "update-db")
			mu.Unlock()
			return errors.New("db connection refused")
		}},
	})

	repo := newFakeFanOutRepo()
	repo.processesByEventID["ev-1"] = []domain.Process{
		{ID: "proc-1", EventID: "ev-1", ProcessName: "send-email", Status: domain.ProcessStatusPending, Attempts: 0},
		{ID: "proc-2", EventID: "ev-1", ProcessName: "update-db", Status: domain.ProcessStatusPending, Attempts: 1},
	}

	schedule := []time.Duration{2 * time.Second, 5 * time.Second, 15 * time.Second}
	fanout := NewFanOut(reg, repo, 5, schedule)
	ctx := silentCtxFanout()
	event := domain.Event{ID: "ev-1", Type: "order.created"}

	beforeCall := time.Now().UTC()
	err := fanout.Process(ctx, event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 process(es) failed")

	// proc-1: completed
	call1 := repo.LastUpdateCallForProcess("proc-1")
	require.NotNil(t, call1)
	assert.Equal(t, domain.ProcessStatusCompleted, call1.Status)
	assert.Equal(t, 1, call1.Attempts)

	// proc-2: failed with nextRetryAt (newAttempts=2, uses schedule[1]=5s)
	call2 := repo.LastUpdateCallForProcess("proc-2")
	require.NotNil(t, call2)
	assert.Equal(t, domain.ProcessStatusFailed, call2.Status)
	assert.Equal(t, 2, call2.Attempts)
	assert.NotNil(t, call2.NextRetryAt)
	assert.Equal(t, "db connection refused", call2.ErrorMsg)

	// nextRetryAt ≈ now + schedule[1] = now + 5s
	expectedBackoff := backoffFor(2, schedule)
	assert.Equal(t, 5*time.Second, expectedBackoff)
	assertNextRetryAtApprox(t, expectedBackoff, call2.NextRetryAt, beforeCall)
}

// ---------------------------------------------------------------------------
// Case d: Process reaches maxAttempts → dead, MoveEventToDeadLetter, ErrEventDead
// ---------------------------------------------------------------------------

func TestFanOut_Process_MaxAttemptsReached_Dead(t *testing.T) {
	reg := dispatch.NewRegistry()
	reg.Register("order.created", []dispatch.Process{
		{Name: "failing-handler", Fn: func(ctx context.Context, event domain.Event) error {
			return errors.New("persistent failure")
		}},
	})

	repo := newFakeFanOutRepo()
	// Attempts=4, maxAttempts=5 → newAttempts=5 >= 5 → dead
	repo.processesByEventID["ev-1"] = []domain.Process{
		{ID: "proc-1", EventID: "ev-1", ProcessName: "failing-handler", Status: domain.ProcessStatusFailed, Attempts: 4, NextRetryAt: nil},
	}

	fanout := NewFanOut(reg, repo, 5, nil)
	ctx := silentCtxFanout()
	event := domain.Event{ID: "ev-1", Type: "order.created"}

	err := fanout.Process(ctx, event)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrEventDead), "must return ErrEventDead")
	assert.Contains(t, err.Error(), "moved to dead letter")

	// UpdateProcessStatus(dead, 5, nil, errorMsg)
	call := repo.LastUpdateCallForProcess("proc-1")
	require.NotNil(t, call)
	assert.Equal(t, domain.ProcessStatusDead, call.Status)
	assert.Equal(t, 5, call.Attempts)
	assert.Nil(t, call.NextRetryAt)
	assert.Equal(t, "persistent failure", call.ErrorMsg)

	// MoveEventToDeadLetter must have been called.
	dlCalls := repo.MoveToDeadLetterCalls()
	require.Len(t, dlCalls, 1)
	assert.Equal(t, "ev-1", dlCalls[0].EventID)
	assert.Contains(t, dlCalls[0].Reason, "failing-handler")
	assert.Contains(t, dlCalls[0].Reason, "exceeded max attempts")
	assert.Contains(t, dlCalls[0].Reason, "5")
}

// ---------------------------------------------------------------------------
// Case e: Process with NextRetryAt future → skipped (not executed)
// ---------------------------------------------------------------------------

func TestFanOut_Process_NextRetryAtFuture_Skipped(t *testing.T) {
	reg := dispatch.NewRegistry()
	reg.Register("order.created", []dispatch.Process{
		{Name: "send-email", Fn: func(ctx context.Context, event domain.Event) error {
			t.Error("handler should NOT be called for skipped process")
			return nil
		}},
	})

	repo := newFakeFanOutRepo()
	future := time.Now().UTC().Add(1 * time.Hour)
	repo.processesByEventID["ev-1"] = []domain.Process{
		{ID: "proc-1", EventID: "ev-1", ProcessName: "send-email", Status: domain.ProcessStatusFailed, Attempts: 1, NextRetryAt: &future},
	}

	fanout := NewFanOut(reg, repo, 3, nil)
	ctx := silentCtxFanout()
	event := domain.Event{ID: "ev-1", Type: "order.created"}

	err := fanout.Process(ctx, event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 process(es) still in backoff")

	// UpdateProcessStatus must NOT have been called for proc-1.
	assert.Nil(t, repo.LastUpdateCallForProcess("proc-1"), "no update expected for skipped process")
}

// ---------------------------------------------------------------------------
// Case f: NextRetryAt == nil → process executes (ready)
// ---------------------------------------------------------------------------

func TestFanOut_Process_NextRetryAtNil_Executes(t *testing.T) {
	reg := dispatch.NewRegistry()
	var handlerCalled atomic.Bool
	reg.Register("order.created", []dispatch.Process{
		{Name: "send-email", Fn: func(ctx context.Context, event domain.Event) error {
			handlerCalled.Store(true)
			return nil
		}},
	})

	repo := newFakeFanOutRepo()
	repo.processesByEventID["ev-1"] = []domain.Process{
		{ID: "proc-1", EventID: "ev-1", ProcessName: "send-email", Status: domain.ProcessStatusFailed, Attempts: 0, NextRetryAt: nil},
	}

	fanout := NewFanOut(reg, repo, 3, nil)
	ctx := silentCtxFanout()
	event := domain.Event{ID: "ev-1", Type: "order.created"}

	err := fanout.Process(ctx, event)
	assert.NoError(t, err)
	assert.True(t, handlerCalled.Load(), "handler must have been called")

	call := repo.LastUpdateCallForProcess("proc-1")
	require.NotNil(t, call)
	assert.Equal(t, domain.ProcessStatusCompleted, call.Status)
	assert.Equal(t, 1, call.Attempts)
}

// ---------------------------------------------------------------------------
// Case g: NextRetryAt in the past → process executes
// ---------------------------------------------------------------------------

func TestFanOut_Process_NextRetryAtPast_Executes(t *testing.T) {
	reg := dispatch.NewRegistry()
	var handlerCalled atomic.Bool
	reg.Register("order.created", []dispatch.Process{
		{Name: "send-email", Fn: func(ctx context.Context, event domain.Event) error {
			handlerCalled.Store(true)
			return nil
		}},
	})

	repo := newFakeFanOutRepo()
	past := time.Now().UTC().Add(-1 * time.Hour)
	repo.processesByEventID["ev-1"] = []domain.Process{
		{ID: "proc-1", EventID: "ev-1", ProcessName: "send-email", Status: domain.ProcessStatusFailed, Attempts: 1, NextRetryAt: &past},
	}

	fanout := NewFanOut(reg, repo, 3, nil)
	ctx := silentCtxFanout()
	event := domain.Event{ID: "ev-1", Type: "order.created"}

	err := fanout.Process(ctx, event)
	assert.NoError(t, err)
	assert.True(t, handlerCalled.Load())

	call := repo.LastUpdateCallForProcess("proc-1")
	require.NotNil(t, call)
	assert.Equal(t, domain.ProcessStatusCompleted, call.Status)
	assert.Equal(t, 2, call.Attempts) // was 1, now 1+1=2
}

// ---------------------------------------------------------------------------
// Case h: No handler registered for ProcessName → failure with backoff
// ---------------------------------------------------------------------------

func TestFanOut_Process_NoHandlerForProcessName(t *testing.T) {
	reg := dispatch.NewRegistry()
	// Only "send-email" is registered; "update-db" is not.
	reg.Register("order.created", []dispatch.Process{
		{Name: "send-email", Fn: func(ctx context.Context, event domain.Event) error {
			return nil
		}},
	})

	repo := newFakeFanOutRepo()
	repo.processesByEventID["ev-1"] = []domain.Process{
		{ID: "proc-1", EventID: "ev-1", ProcessName: "send-email", Status: domain.ProcessStatusPending, Attempts: 0},
		{ID: "proc-2", EventID: "ev-1", ProcessName: "update-db", Status: domain.ProcessStatusPending, Attempts: 0},
	}

	schedule := []time.Duration{2 * time.Second}
	fanout := NewFanOut(reg, repo, 3, schedule)
	ctx := silentCtxFanout()
	event := domain.Event{ID: "ev-1", Type: "order.created"}

	beforeCall := time.Now().UTC()
	err := fanout.Process(ctx, event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 process(es) failed")

	// proc-1 (send-email): completed (handler exists)
	call1 := repo.LastUpdateCallForProcess("proc-1")
	require.NotNil(t, call1)
	assert.Equal(t, domain.ProcessStatusCompleted, call1.Status)

	// proc-2 (update-db): failed, no handler registered
	call2 := repo.LastUpdateCallForProcess("proc-2")
	require.NotNil(t, call2)
	assert.Equal(t, domain.ProcessStatusFailed, call2.Status)
	assert.Equal(t, 1, call2.Attempts)
	assert.Contains(t, call2.ErrorMsg, "no handler registered for process update-db")

	// nextRetryAt must be set (backoff applied)
	expectedBackoff := backoffFor(1, schedule)
	assertNextRetryAtApprox(t, expectedBackoff, call2.NextRetryAt, beforeCall)
}

// ---------------------------------------------------------------------------
// Case i: Panic in Fn → recover → failure (no panic propagation)
// ---------------------------------------------------------------------------

func TestFanOut_Process_PanicInFn_Recovered(t *testing.T) {
	reg := dispatch.NewRegistry()
	reg.Register("order.created", []dispatch.Process{
		{Name: "panicking-handler", Fn: func(ctx context.Context, event domain.Event) error {
			panic("unexpected nil pointer")
		}},
	})

	repo := newFakeFanOutRepo()
	repo.processesByEventID["ev-1"] = []domain.Process{
		{ID: "proc-1", EventID: "ev-1", ProcessName: "panicking-handler", Status: domain.ProcessStatusPending, Attempts: 0},
	}

	schedule := []time.Duration{2 * time.Second}
	fanout := NewFanOut(reg, repo, 3, schedule)
	ctx := silentCtxFanout()
	event := domain.Event{ID: "ev-1", Type: "order.created"}

	beforeCall := time.Now().UTC()
	err := fanout.Process(ctx, event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 process(es) failed")

	// Process did not propagate the panic — it must return an error, not panic.
	call := repo.LastUpdateCallForProcess("proc-1")
	require.NotNil(t, call)
	assert.Equal(t, domain.ProcessStatusFailed, call.Status)
	assert.Equal(t, 1, call.Attempts)
	assert.Contains(t, call.ErrorMsg, "panic in process panicking-handler")
	assert.Contains(t, call.ErrorMsg, "unexpected nil pointer")
	// Stack trace should be truncated to 512 bytes
	assert.LessOrEqual(t, len(call.ErrorMsg), 1024, "error message should have bounded stack trace")

	// Backoff must be applied.
	assertNextRetryAtApprox(t, backoffFor(1, schedule), call.NextRetryAt, beforeCall)
}

// ---------------------------------------------------------------------------
// Case j: maxAttempts=1, Fn fails → dead immediate
// ---------------------------------------------------------------------------

func TestFanOut_Process_MaxAttemptsOne_FailsImmediately(t *testing.T) {
	reg := dispatch.NewRegistry()
	reg.Register("order.created", []dispatch.Process{
		{Name: "failing-handler", Fn: func(ctx context.Context, event domain.Event) error {
			return errors.New("first and last attempt failed")
		}},
	})

	repo := newFakeFanOutRepo()
	// Attempts=0, maxAttempts=1 → newAttempts=1 >= 1 → dead
	repo.processesByEventID["ev-1"] = []domain.Process{
		{ID: "proc-1", EventID: "ev-1", ProcessName: "failing-handler", Status: domain.ProcessStatusPending, Attempts: 0},
	}

	fanout := NewFanOut(reg, repo, 1, nil)
	ctx := silentCtxFanout()
	event := domain.Event{ID: "ev-1", Type: "order.created"}

	err := fanout.Process(ctx, event)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrEventDead))

	call := repo.LastUpdateCallForProcess("proc-1")
	require.NotNil(t, call)
	assert.Equal(t, domain.ProcessStatusDead, call.Status)
	assert.Equal(t, 1, call.Attempts)
	assert.Equal(t, "first and last attempt failed", call.ErrorMsg)

	dlCalls := repo.MoveToDeadLetterCalls()
	require.Len(t, dlCalls, 1)
}

// ---------------------------------------------------------------------------
// Case k: backoffFor unit tests
// ---------------------------------------------------------------------------

func TestBackoffFor_Schedule(t *testing.T) {
	schedule := []time.Duration{2 * time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second, 60 * time.Second}

	// Attempt 1 → schedule[0] (first entry)
	assert.Equal(t, 2*time.Second, backoffFor(1, schedule))

	// Attempt 2 → schedule[1]
	assert.Equal(t, 5*time.Second, backoffFor(2, schedule))

	// Attempt len(schedule) → last entry
	assert.Equal(t, 60*time.Second, backoffFor(5, schedule))

	// Attempt > len(schedule) → last entry (clamped)
	assert.Equal(t, 60*time.Second, backoffFor(6, schedule))
	assert.Equal(t, 60*time.Second, backoffFor(100, schedule))
}

func TestBackoffFor_EmptySchedule_Fallback(t *testing.T) {
	// Empty schedule (nil or empty slice) → fallbackBackoff = 1s
	assert.Equal(t, 1*time.Second, backoffFor(1, nil))
	assert.Equal(t, 1*time.Second, backoffFor(1, []time.Duration{}))
	assert.Equal(t, 1*time.Second, backoffFor(5, []time.Duration{}))
}

// ---------------------------------------------------------------------------
// Case l: NewFanOut panics on invalid arguments
// ---------------------------------------------------------------------------

func TestNewFanOut_Panics_RegistryNil(t *testing.T) {
	repo := newFakeFanOutRepo()
	assert.PanicsWithValue(t, "workers: registry must not be nil", func() {
		NewFanOut(nil, repo, 3, nil)
	})
}

func TestNewFanOut_Panics_RepoNil(t *testing.T) {
	reg := dispatch.NewRegistry()
	assert.PanicsWithValue(t, "workers: repo must not be nil", func() {
		NewFanOut(reg, nil, 3, nil)
	})
}

func TestNewFanOut_Panics_MaxAttemptsZero(t *testing.T) {
	reg := dispatch.NewRegistry()
	repo := newFakeFanOutRepo()
	assert.PanicsWithValue(t, "workers: maxAttempts must be at least 1", func() {
		NewFanOut(reg, repo, 0, nil)
	})
}

func TestNewFanOut_Success(t *testing.T) {
	reg := dispatch.NewRegistry()
	repo := newFakeFanOutRepo()
	schedule := []time.Duration{2 * time.Second}
	fanout := NewFanOut(reg, repo, 5, schedule)
	assert.NotNil(t, fanout)
}

// ---------------------------------------------------------------------------
// Case m: Empty pending (FetchProcessesForRetry → []) → nil (healing)
// ---------------------------------------------------------------------------

func TestFanOut_Process_EmptyPending_ReturnsNil(t *testing.T) {
	reg := dispatch.NewRegistry()
	reg.Register("order.created", []dispatch.Process{
		{Name: "send-email", Fn: func(ctx context.Context, event domain.Event) error {
			t.Error("handler should not be called, there are no pending processes")
			return nil
		}},
	})

	repo := newFakeFanOutRepo()
	// No processes configured for this event ID → empty slice.
	fanout := NewFanOut(reg, repo, 3, nil)
	ctx := silentCtxFanout()
	event := domain.Event{ID: "ev-1", Type: "order.created"}

	err := fanout.Process(ctx, event)
	assert.NoError(t, err, "must return nil when no pending processes (healing)")

	assert.Equal(t, 0, repo.TotalUpdateCalls(), "no UpdateProcessStatus calls expected")
}

// ---------------------------------------------------------------------------
// Case m2: FetchProcessesForRetry repo error → propagates error
// ---------------------------------------------------------------------------

func TestFanOut_Process_RepoFetchError_Propagates(t *testing.T) {
	reg := dispatch.NewRegistry()
	reg.Register("order.created", []dispatch.Process{
		{Name: "send-email", Fn: func(ctx context.Context, event domain.Event) error {
			return nil
		}},
	})

	repo := newFakeFanOutRepo()
	repo.fetchError = errors.New("database connection lost")
	fanout := NewFanOut(reg, repo, 3, nil)
	ctx := silentCtxFanout()
	event := domain.Event{ID: "ev-1", Type: "order.created"}

	err := fanout.Process(ctx, event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database connection lost")
}

// ---------------------------------------------------------------------------
// Case n: Concurrency — multiple events processed concurrently, no data races
// ---------------------------------------------------------------------------

func TestFanOut_Process_ConcurrentEvents_NoRace(t *testing.T) {
	reg := dispatch.NewRegistry()
	var handlerCalls atomic.Int64
	var mu sync.Mutex
	var invocationOrder []string

	reg.Register("order.created", []dispatch.Process{
		{Name: "send-email", Fn: func(ctx context.Context, event domain.Event) error {
			handlerCalls.Add(1)
			mu.Lock()
			invocationOrder = append(invocationOrder, "send-email")
			mu.Unlock()
			return nil
		}},
		{Name: "update-db", Fn: func(ctx context.Context, event domain.Event) error {
			handlerCalls.Add(1)
			mu.Lock()
			invocationOrder = append(invocationOrder, "update-db")
			mu.Unlock()
			return nil
		}},
	})

	schedule := []time.Duration{2 * time.Second}

	// Properly set up the repo's processesByEventID BEFORE creating the FanOut
	eventCount := 30
	repo := newFakeFanOutRepo()
	for i := 0; i < eventCount; i++ {
		eventID := fmt.Sprintf("ev-%d", i+1)
		repo.processesByEventID[eventID] = []domain.Process{
			{ID: fmt.Sprintf("proc-a-%d", i+1), EventID: eventID, ProcessName: "send-email", Status: domain.ProcessStatusPending, Attempts: 0},
			{ID: fmt.Sprintf("proc-b-%d", i+1), EventID: eventID, ProcessName: "update-db", Status: domain.ProcessStatusPending, Attempts: 0},
		}
	}
	fanout2 := NewFanOut(reg, repo, 5, schedule)

	ctx := silentCtxFanout()

	var wg sync.WaitGroup
	for i := 0; i < eventCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			eventID := fmt.Sprintf("ev-%d", idx+1)
			event := domain.Event{ID: eventID, Type: "order.created"}
			_ = fanout2.Process(ctx, event)
		}(i)
	}

	wg.Wait()

	// Each event has 2 processes, both succeed → 2 * eventCount handler calls.
	assert.Equal(t, int64(2*eventCount), handlerCalls.Load(),
		"every process handler must be invoked exactly once per event")
}

// ---------------------------------------------------------------------------
// Case o: Event with dead process + skipped (backoff) → dead-letter wins
// ---------------------------------------------------------------------------

func TestFanOut_Process_DeadAndSkipped_DeadLetterWins(t *testing.T) {
	reg := dispatch.NewRegistry()
	reg.Register("order.created", []dispatch.Process{
		{Name: "failing-handler", Fn: func(ctx context.Context, event domain.Event) error {
			return errors.New("fatal")
		}},
		{Name: "backoff-handler", Fn: func(ctx context.Context, event domain.Event) error {
			t.Error("backoff handler should NOT execute (it's skipped)")
			return nil
		}},
	})

	repo := newFakeFanOutRepo()
	// proc-1: will be dead (attempts=4, max=5, fails → newAttempts=5 >= 5 → dead)
	// proc-2: in backoff (NextRetryAt future)
	future := time.Now().UTC().Add(1 * time.Hour)
	repo.processesByEventID["ev-1"] = []domain.Process{
		{ID: "proc-1", EventID: "ev-1", ProcessName: "failing-handler", Status: domain.ProcessStatusFailed, Attempts: 4, NextRetryAt: nil},
		{ID: "proc-2", EventID: "ev-1", ProcessName: "backoff-handler", Status: domain.ProcessStatusFailed, Attempts: 1, NextRetryAt: &future},
	}

	fanout := NewFanOut(reg, repo, 5, nil)
	ctx := silentCtxFanout()
	event := domain.Event{ID: "ev-1", Type: "order.created"}

	err := fanout.Process(ctx, event)
	require.Error(t, err)
	// anyDead wins over skipped — must be ErrEventDead, NOT the skipped error.
	assert.True(t, errors.Is(err, ErrEventDead), "anyDead must take priority over skipped")
	assert.NotContains(t, err.Error(), "still in backoff", "skipped error should not be returned when dead")

	// MoveEventToDeadLetter must have been called.
	dlCalls := repo.MoveToDeadLetterCalls()
	require.Len(t, dlCalls, 1)
	assert.Equal(t, "ev-1", dlCalls[0].EventID)

	// proc-1: should be marked dead.
	call1 := repo.LastUpdateCallForProcess("proc-1")
	require.NotNil(t, call1)
	assert.Equal(t, domain.ProcessStatusDead, call1.Status)

	// proc-2: should NOT be updated (it was skipped).
	assert.Nil(t, repo.LastUpdateCallForProcess("proc-2"))
}
