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

	"eventhub/internal/domain"
	"eventhub/internal/logging"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Test helpers (shared with orchestrator_test.go but redeclared for isolation)
// ---------------------------------------------------------------------------

// silentLogger returns a slog.Logger that discards all output.
func silentLoggerPool() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// silentCtxPool returns a context.Background() with a silent logger injected.
func silentCtxPool() context.Context {
	return logging.WithContext(context.Background(), silentLoggerPool())
}

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// fakeProcessor is a deterministic EventProcessor that can be configured per
// invocation to succeed, return an error, or panic. All invocations and their
// event IDs are tracked.
type fakeProcessor struct {
	mu           sync.Mutex
	invocations  []string           // event IDs in order of processing
	processFunc  func(ctx context.Context, event domain.Event) error // if nil, always succeed
}

func (fp *fakeProcessor) Process(ctx context.Context, event domain.Event) error {
	fp.mu.Lock()
	fp.invocations = append(fp.invocations, event.ID)
	fp.mu.Unlock()
	if fp.processFunc != nil {
		return fp.processFunc(ctx, event)
	}
	return nil
}

// Invocations returns the ordered list of event IDs that were passed to Process.
func (fp *fakeProcessor) Invocations() []string {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	out := make([]string, len(fp.invocations))
	copy(out, fp.invocations)
	return out
}

// fakeStatusUpdater records transitions per event and can be configured to
// fail or panic on specific status values. Transitions are recorded before
// the per-call behaviour executes, so even failed/panicked calls are traceable.
type fakeStatusUpdater struct {
	mu           sync.Mutex
	transitions  map[string][]domain.EventStatus // eventID → ordered status calls
	updateFunc   func(ctx context.Context, eventID string, status domain.EventStatus) error // if nil, always succeed
}

func (fsu *fakeStatusUpdater) UpdateEventStatus(ctx context.Context, eventID string, status domain.EventStatus) error {
	fsu.mu.Lock()
	fsu.transitions[eventID] = append(fsu.transitions[eventID], status)
	fsu.mu.Unlock()
	if fsu.updateFunc != nil {
		return fsu.updateFunc(ctx, eventID, status)
	}
	return nil
}

// Transitions returns the ordered status transitions recorded for the given
// event ID. Returns nil if the event was never seen.
func (fsu *fakeStatusUpdater) Transitions(eventID string) []domain.EventStatus {
	fsu.mu.Lock()
	defer fsu.mu.Unlock()
	return append([]domain.EventStatus(nil), fsu.transitions[eventID]...)
}

// ---------------------------------------------------------------------------
// 1. NewPool validations — each invalid argument panics
// ---------------------------------------------------------------------------

func TestNewPool_Panics_WorkerCountZero(t *testing.T) {
	ch := make(chan domain.Event, 1)
	assert.Panics(t, func() {
		NewPool(0, ch, &fakeProcessor{}, &fakeStatusUpdater{})
	})
}

func TestNewPool_Panics_WorkerCountNegative(t *testing.T) {
	ch := make(chan domain.Event, 1)
	assert.Panics(t, func() {
		NewPool(-1, ch, &fakeProcessor{}, &fakeStatusUpdater{})
	})
}

func TestNewPool_Panics_ReadyEventsNil(t *testing.T) {
	assert.Panics(t, func() {
		NewPool(1, nil, &fakeProcessor{}, &fakeStatusUpdater{})
	})
}

func TestNewPool_Panics_ProcessorNil(t *testing.T) {
	ch := make(chan domain.Event, 1)
	assert.Panics(t, func() {
		NewPool(1, ch, nil, &fakeStatusUpdater{})
	})
}

func TestNewPool_Panics_StatusUpdaterNil(t *testing.T) {
	ch := make(chan domain.Event, 1)
	assert.Panics(t, func() {
		NewPool(1, ch, &fakeProcessor{}, nil)
	})
}

func TestNewPool_Success(t *testing.T) {
	ch := make(chan domain.Event, 5)
	proc := &fakeProcessor{}
	upd := &fakeStatusUpdater{}
	pool := NewPool(3, ch, proc, upd)

	assert.NotNil(t, pool)
	assert.Equal(t, int64(0), pool.Processed())
	assert.Equal(t, int64(0), pool.Failed())
}

// ---------------------------------------------------------------------------
// 2. Start launches N workers — verify with exactly N events processed
// ---------------------------------------------------------------------------

func TestPoolStart_LaunchesNWorkers_ThreeWorkersThreeEvents(t *testing.T) {
	ch := make(chan domain.Event, 3)
	proc := &fakeProcessor{}
	upd := &fakeStatusUpdater{transitions: make(map[string][]domain.EventStatus)}

	pool := NewPool(3, ch, proc, upd)
	ctx := silentCtxPool()
	pool.Start(ctx)

	// Send 3 events — each worker picks up one (eventually all are processed)
	for i := 0; i < 3; i++ {
		ch <- domain.Event{ID: fmt.Sprintf("ev-%d", i+1), Type: "test"}
	}
	pool.Shutdown()

	assert.Equal(t, int64(3), pool.Processed())
	assert.Equal(t, int64(0), pool.Failed())
}

// ---------------------------------------------------------------------------
// 3. Successful processing — complete happy path
// ---------------------------------------------------------------------------

func TestPool_SuccessfulProcessing(t *testing.T) {
	ch := make(chan domain.Event, 1)
	proc := &fakeProcessor{}
	upd := &fakeStatusUpdater{transitions: make(map[string][]domain.EventStatus)}

	pool := NewPool(1, ch, proc, upd)
	ctx := silentCtxPool()
	pool.Start(ctx)

	ch <- domain.Event{ID: "ev-1", Type: "test"}
	pool.Shutdown()

	assert.Equal(t, int64(1), pool.Processed())
	assert.Equal(t, int64(0), pool.Failed())

	// Transitions must be [processing, completed]
	transitions := upd.Transitions("ev-1")
	require.Len(t, transitions, 2)
	assert.Equal(t, domain.StatusProcessing, transitions[0])
	assert.Equal(t, domain.StatusCompleted, transitions[1])
}

// ---------------------------------------------------------------------------
// 4. Processing error — processor returns error
// ---------------------------------------------------------------------------

func TestPool_ProcessingError(t *testing.T) {
	ch := make(chan domain.Event, 1)
	proc := &fakeProcessor{
		processFunc: func(ctx context.Context, event domain.Event) error {
			return errors.New("simulated processing error")
		},
	}
	upd := &fakeStatusUpdater{transitions: make(map[string][]domain.EventStatus)}

	pool := NewPool(1, ch, proc, upd)
	ctx := silentCtxPool()
	pool.Start(ctx)

	ch <- domain.Event{ID: "ev-1", Type: "test"}
	pool.Shutdown()

	assert.Equal(t, int64(1), pool.Failed())
	assert.Equal(t, int64(0), pool.Processed())

	// Transitions: processing → partial_failed
	transitions := upd.Transitions("ev-1")
	require.Len(t, transitions, 2)
	assert.Equal(t, domain.StatusProcessing, transitions[0])
	assert.Equal(t, domain.StatusPartialFailed, transitions[1])
}

// ---------------------------------------------------------------------------
// 5. Panic in Process — processor panics, worker recovers and continues
// ---------------------------------------------------------------------------

func TestPool_PanicInProcess_WorkerSurvives(t *testing.T) {
	ch := make(chan domain.Event, 1)
	var panicOnce sync.Once

	proc := &fakeProcessor{
		processFunc: func(ctx context.Context, event domain.Event) error {
			panicOnce.Do(func() {
				panic("simulated processor panic")
			})
			return nil
		},
	}
	upd := &fakeStatusUpdater{transitions: make(map[string][]domain.EventStatus)}

	pool := NewPool(1, ch, proc, upd)
	ctx := silentCtxPool()
	pool.Start(ctx)

	// First event: triggers panic
	ch <- domain.Event{ID: "ev-panic", Type: "test"}

	// Wait for the panic to be caught and counted
	require.Eventually(t, func() bool { return pool.Failed() == 1 }, 2*time.Second, 5*time.Millisecond,
		"failed counter must reach 1 after processor panic")

	// Verify Processed is still 0 (the panicked event is not counted as processed)
	assert.Equal(t, int64(0), pool.Processed(), "processed must be 0 because the panicked event didn't complete")

	// Second event: succeeds — proves worker is still alive
	ch <- domain.Event{ID: "ev-ok", Type: "test"}

	require.Eventually(t, func() bool { return pool.Processed() == 1 }, 2*time.Second, 5*time.Millisecond,
		"processed counter must reach 1 after successful second event")

	assert.Equal(t, int64(1), pool.Failed(), "failed must still be 1 (only the first event failed)")
	assert.Equal(t, int64(1), pool.Processed(), "processed must be 1 (the second event succeeded)")

	// Verify transitions for the panicked event
	panicTransitions := upd.Transitions("ev-panic")
	assert.Equal(t, domain.StatusProcessing, panicTransitions[0],
		"first transition for panicked event must be processing")
	assert.Equal(t, domain.StatusPartialFailed, panicTransitions[len(panicTransitions)-1],
		"last transition for panicked event must be partial_failed (from defer)")

	pool.Shutdown()
}

// ---------------------------------------------------------------------------
// 6. Panic in UpdateEventStatus(completed) — defer catches, event is failed
// ---------------------------------------------------------------------------

func TestPool_PanicInUpdateEventStatusCompleted(t *testing.T) {
	ch := make(chan domain.Event, 1)
	proc := &fakeProcessor{} // always succeeds
	upd := &fakeStatusUpdater{
		transitions: make(map[string][]domain.EventStatus),
		updateFunc: func(ctx context.Context, eventID string, status domain.EventStatus) error {
			if status == domain.StatusCompleted {
				panic("simulated panic on completed update")
			}
			return nil
		},
	}

	pool := NewPool(1, ch, proc, upd)
	ctx := silentCtxPool()
	pool.Start(ctx)

	ch <- domain.Event{ID: "ev-1", Type: "test"}
	pool.Shutdown()

	// Invariant: a single attempt NEVER counts the same event as both processed and failed
	assert.Equal(t, int64(1), pool.Failed(), "failed must be 1 (completed update panicked)")
	assert.Equal(t, int64(0), pool.Processed(), "processed must be 0 (completed update never persisted)")

	// Transitions: processing was recorded, completed attempted (and panicked),
	// partial_failed recorded by the defer
	transitions := upd.Transitions("ev-1")
	require.GreaterOrEqual(t, len(transitions), 2)
	assert.Equal(t, domain.StatusProcessing, transitions[0])
	assert.Equal(t, domain.StatusPartialFailed, transitions[len(transitions)-1])
}

// ---------------------------------------------------------------------------
// 7. Error in UpdateEventStatus(completed) — event not counted as processed or failed
// ---------------------------------------------------------------------------

func TestPool_ErrorInUpdateEventStatusCompleted(t *testing.T) {
	ch := make(chan domain.Event, 1)
	proc := &fakeProcessor{} // always succeeds
	upd := &fakeStatusUpdater{
		transitions: make(map[string][]domain.EventStatus),
		updateFunc: func(ctx context.Context, eventID string, status domain.EventStatus) error {
			if status == domain.StatusCompleted {
				return errors.New("simulated db error on completed")
			}
			return nil
		},
	}

	pool := NewPool(1, ch, proc, upd)
	ctx := silentCtxPool()
	pool.Start(ctx)

	ch <- domain.Event{ID: "ev-1", Type: "test"}
	pool.Shutdown()

	// The event was processed successfully but the completed update failed →
	// processed is NOT incremented (the event will be redelivered later).
	// The event is NOT counted as failed either (it was a persistence issue, not a processing error).
	assert.Equal(t, int64(0), pool.Processed(), "processed must be 0 — completed update failed")
	assert.Equal(t, int64(0), pool.Failed(), "failed must be 0 — processor didn't error, only persistence did")
}

// ---------------------------------------------------------------------------
// 8. Error in UpdateEventStatus(processing) — event is still processed (at-least-once)
// ---------------------------------------------------------------------------

func TestPool_ErrorInUpdateEventStatusProcessing_ProcessesAnyway(t *testing.T) {
	ch := make(chan domain.Event, 1)
	proc := &fakeProcessor{} // always succeeds
	upd := &fakeStatusUpdater{
		transitions: make(map[string][]domain.EventStatus),
		updateFunc: func(ctx context.Context, eventID string, status domain.EventStatus) error {
			if status == domain.StatusProcessing {
				return errors.New("simulated db error on processing marker")
			}
			return nil
		},
	}

	pool := NewPool(1, ch, proc, upd)
	ctx := silentCtxPool()
	pool.Start(ctx)

	ch <- domain.Event{ID: "ev-1", Type: "test"}
	pool.Shutdown()

	// At-least-once delivery: processing marker failure doesn't block processing.
	// The event was already handed off; the final completed update fixes the record.
	assert.Equal(t, int64(1), pool.Processed(), "processed must be 1 — event is processed despite processing marker error")
	assert.Equal(t, int64(0), pool.Failed(), "failed must be 0 — processor succeeded")

	// Transitions: processing was called (and errored), completed was called and succeeded
	transitions := upd.Transitions("ev-1")
	require.Len(t, transitions, 2)
	assert.Equal(t, domain.StatusProcessing, transitions[0])
	assert.Equal(t, domain.StatusCompleted, transitions[1])
}

// ---------------------------------------------------------------------------
// 9. Shutdown drains the buffer — all buffered events are processed
// ---------------------------------------------------------------------------

func TestPool_ShutdownDrainsBuffer(t *testing.T) {
	ch := make(chan domain.Event, 10)
	// Fill buffer with 5 events BEFORE starting the pool
	for i := 0; i < 5; i++ {
		ch <- domain.Event{ID: fmt.Sprintf("ev-%d", i+1), Type: "test"}
	}

	proc := &fakeProcessor{}
	upd := &fakeStatusUpdater{transitions: make(map[string][]domain.EventStatus)}

	pool := NewPool(1, ch, proc, upd)
	ctx := silentCtxPool()
	pool.Start(ctx)

	// Shutdown immediately — the worker must drain the buffered events
	pool.Shutdown()

	assert.Equal(t, int64(5), pool.Processed(), "all 5 buffered events must be processed")
	assert.Equal(t, int64(0), pool.Failed())

	// Verify channel is closed
	_, ok := <-ch
	assert.False(t, ok, "channel must be closed after Shutdown")
}

// ---------------------------------------------------------------------------
// 10. Shutdown before Start — no panic, no hang, channel closed
// ---------------------------------------------------------------------------

func TestPool_ShutdownBeforeStart(t *testing.T) {
	ch := make(chan domain.Event, 5)
	proc := &fakeProcessor{}
	upd := &fakeStatusUpdater{}

	pool := NewPool(1, ch, proc, upd)

	// Shutdown must not panic and must return quickly
	shutdownDone := make(chan struct{})
	go func() {
		pool.Shutdown()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		// OK — Shutdown returned
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown() before Start hung")
	}

	// Channel must be closed
	_, ok := <-ch
	assert.False(t, ok, "channel must be closed after Shutdown, even without Start")

	// Counters stay at zero
	assert.Equal(t, int64(0), pool.Processed())
	assert.Equal(t, int64(0), pool.Failed())
}

// ---------------------------------------------------------------------------
// 11. Start idempotent — calling Start twice launches workers only once
// ---------------------------------------------------------------------------

func TestPool_StartIdempotent(t *testing.T) {
	ch := make(chan domain.Event, 1)
	var processCount atomic.Int32

	proc := &fakeProcessor{
		processFunc: func(ctx context.Context, event domain.Event) error {
			processCount.Add(1)
			return nil
		},
	}
	upd := &fakeStatusUpdater{transitions: make(map[string][]domain.EventStatus)}

	pool := NewPool(1, ch, proc, upd)
	ctx := silentCtxPool()

	pool.Start(ctx)
	pool.Start(ctx) // second call — must be no-op

	ch <- domain.Event{ID: "ev-1", Type: "test"}
	pool.Shutdown()

	// If double Start launched 2 goroutines, both could try to process the event,
	// but only one can receive from the channel. The event must be processed exactly once.
	assert.Equal(t, int32(1), processCount.Load(), "event must be processed exactly once")
	assert.Equal(t, int64(1), pool.Processed())
	assert.Equal(t, int64(0), pool.Failed())
}

// ---------------------------------------------------------------------------
// 12. Shutdown idempotent — calling Shutdown twice does not panic (no double close)
// ---------------------------------------------------------------------------

func TestPool_ShutdownIdempotent(t *testing.T) {
	ch := make(chan domain.Event, 1)
	proc := &fakeProcessor{}
	upd := &fakeStatusUpdater{transitions: make(map[string][]domain.EventStatus)}

	pool := NewPool(1, ch, proc, upd)
	ctx := silentCtxPool()
	pool.Start(ctx)

	ch <- domain.Event{ID: "ev-1", Type: "test"}
	pool.Shutdown()  // first — works, closes channel
	pool.Shutdown()  // second — must be no-op (no double close panic)

	assert.Equal(t, int64(1), pool.Processed())

	// Verify channel is still in a closed (safe) state
	_, ok := <-ch
	assert.False(t, ok, "channel must be closed after first Shutdown")
}

// ---------------------------------------------------------------------------
// 13. LoggingProcessor — Process() returns nil
// ---------------------------------------------------------------------------

func TestLoggingProcessor_ProcessReturnsNil(t *testing.T) {
	ctx := silentCtxPool()
	lp := &LoggingProcessor{}

	event := domain.Event{
		ID:        "ev-lp",
		Type:      "test.logging",
		Payload:   `{"data":1}`,
		Processes: []domain.Process{{ProcessName: "send-email"}, {ProcessName: "update-db"}},
	}

	err := lp.Process(ctx, event)
	assert.NoError(t, err, "LoggingProcessor.Process must always return nil")
}

// ---------------------------------------------------------------------------
// 14. Shutdown waits for slow worker — Shutdown blocks until in-flight event completes
// ---------------------------------------------------------------------------

func TestPool_ShutdownWaitsForSlowWorker(t *testing.T) {
	ch := make(chan domain.Event, 1)
	processingStarted := make(chan struct{})
	blockProcessing := make(chan struct{})
	processingFinished := make(chan struct{})

	proc := &fakeProcessor{
		processFunc: func(ctx context.Context, event domain.Event) error {
			close(processingStarted)
			<-blockProcessing // block until we unblock
			close(processingFinished)
			return nil
		},
	}
	upd := &fakeStatusUpdater{transitions: make(map[string][]domain.EventStatus)}

	pool := NewPool(1, ch, proc, upd)
	ctx := silentCtxPool()
	pool.Start(ctx)

	// Send event and wait for worker to pick it up
	ch <- domain.Event{ID: "ev-1", Type: "slow"}

	select {
	case <-processingStarted:
		// Worker is now processing the event
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not start processing in time")
	}

	// Call Shutdown in a goroutine — it must block
	shutdownReturned := make(chan struct{})
	go func() {
		pool.Shutdown()
		close(shutdownReturned)
	}()

	// Verify Shutdown has NOT returned yet (it's waiting for the worker)
	select {
	case <-shutdownReturned:
		t.Fatal("Shutdown returned before worker finished processing")
	case <-time.After(200 * time.Millisecond):
		// Good — Shutdown is still blocked waiting for the in-flight event
	}

	// Unblock the worker
	close(blockProcessing)

	// Wait for processing to actually finish
	select {
	case <-processingFinished:
		// Worker finished
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not finish after unblock")
	}

	// Now Shutdown must return
	select {
	case <-shutdownReturned:
		// Good
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return after worker finished")
	}

	assert.Equal(t, int64(1), pool.Processed())
	assert.Equal(t, int64(0), pool.Failed())
}

// ---------------------------------------------------------------------------
// 15. Panic in UpdateEventStatus(processing) — defer catches, event is failed
//     (different from error case: panic means the processing marker is never
//     "acknowledged" — the defer treats it as a full failure)
// ---------------------------------------------------------------------------

func TestPool_PanicInUpdateEventStatusProcessing(t *testing.T) {
	ch := make(chan domain.Event, 1)
	proc := &fakeProcessor{} // would succeed if reached
	upd := &fakeStatusUpdater{
		transitions: make(map[string][]domain.EventStatus),
		updateFunc: func(ctx context.Context, eventID string, status domain.EventStatus) error {
			if status == domain.StatusProcessing {
				panic("simulated panic on processing marker")
			}
			return nil
		},
	}

	pool := NewPool(1, ch, proc, upd)
	ctx := silentCtxPool()
	pool.Start(ctx)

	ch <- domain.Event{ID: "ev-1", Type: "test"}
	pool.Shutdown()

	// The panic in UpdateEventStatus(processing) is caught by the per-item defer.
	// The event is counted as failed (Failed+1, partial_failed best-effort).
	assert.Equal(t, int64(1), pool.Failed(), "failed must be 1 — processing marker panicked")
	assert.Equal(t, int64(0), pool.Processed(), "processed must be 0 — event was never fully processed")

	transitions := upd.Transitions("ev-1")
	require.GreaterOrEqual(t, len(transitions), 1)
	// First (and possibly only) transition: processing was attempted
	assert.Equal(t, domain.StatusProcessing, transitions[0])
	// The defer should have recorded partial_failed
	assert.Equal(t, domain.StatusPartialFailed, transitions[len(transitions)-1],
		"last transition must be partial_failed from the defer")
}

// ---------------------------------------------------------------------------
// 16. Multievent scenario with mixed outcomes — exercises multiple code paths
// ---------------------------------------------------------------------------

func TestPool_MixedOutcomes(t *testing.T) {
	ch := make(chan domain.Event, 3)
	// Processor: first event succeeds, second errors, third succeeds
	var callCount atomic.Int32
	proc := &fakeProcessor{
		processFunc: func(ctx context.Context, event domain.Event) error {
			count := callCount.Add(1)
			if count == 2 {
				return errors.New("error on second event")
			}
			return nil
		},
	}
	upd := &fakeStatusUpdater{transitions: make(map[string][]domain.EventStatus)}

	pool := NewPool(1, ch, proc, upd)
	ctx := silentCtxPool()
	pool.Start(ctx)

	ch <- domain.Event{ID: "ev-ok1", Type: "test"}
	ch <- domain.Event{ID: "ev-err", Type: "test"}
	ch <- domain.Event{ID: "ev-ok2", Type: "test"}
	pool.Shutdown()

	assert.Equal(t, int64(2), pool.Processed(), "two events should succeed")
	assert.Equal(t, int64(1), pool.Failed(), "one event should fail")

	// Verify specific transitions
	ok1Transitions := upd.Transitions("ev-ok1")
	require.Len(t, ok1Transitions, 2)
	assert.Equal(t, domain.StatusProcessing, ok1Transitions[0])
	assert.Equal(t, domain.StatusCompleted, ok1Transitions[1])

	errTransitions := upd.Transitions("ev-err")
	require.Len(t, errTransitions, 2)
	assert.Equal(t, domain.StatusProcessing, errTransitions[0])
	assert.Equal(t, domain.StatusPartialFailed, errTransitions[1])

	ok2Transitions := upd.Transitions("ev-ok2")
	require.Len(t, ok2Transitions, 2)
	assert.Equal(t, domain.StatusProcessing, ok2Transitions[0])
	assert.Equal(t, domain.StatusCompleted, ok2Transitions[1])
}

// ---------------------------------------------------------------------------
// 17. Graceful shutdown with multiple workers — all events in buffer processed
// ---------------------------------------------------------------------------

func TestPool_MultipleWorkersGracefulShutdown(t *testing.T) {
	// 3 workers, 9 events — distribute the work
	ch := make(chan domain.Event, 9)
	for i := 0; i < 9; i++ {
		ch <- domain.Event{ID: fmt.Sprintf("ev-%d", i+1), Type: "test"}
	}

	proc := &fakeProcessor{}
	upd := &fakeStatusUpdater{transitions: make(map[string][]domain.EventStatus)}

	pool := NewPool(3, ch, proc, upd)
	ctx := silentCtxPool()
	pool.Start(ctx)
	pool.Shutdown()

	assert.Equal(t, int64(9), pool.Processed())
	assert.Equal(t, int64(0), pool.Failed())
}

// ---------------------------------------------------------------------------
// 18. Per-goroutine recovery in runWorker — when the per-item defer itself
//     panics (e.g. UpdateEventStatus(partial_failed) panics during recovery),
//     the goroutine-level recovery in runWorker catches it so waitGroup is
//     balanced and Shutdown never hangs.
// ---------------------------------------------------------------------------

func TestPool_PerGoroutineRecovery_WorkerDoesNotHangShutdown(t *testing.T) {
	ch := make(chan domain.Event, 2)
	// Processor panics → per-item defer tries UpdateEventStatus(partial_failed) →
	// updater panics on partial_failed too → the per-item defer's panic propagates
	// → caught by runWorker's per-goroutine recovery
	proc := &fakeProcessor{
		processFunc: func(ctx context.Context, event domain.Event) error {
			panic("processor panic")
		},
	}
	upd := &fakeStatusUpdater{
		transitions: make(map[string][]domain.EventStatus),
		updateFunc: func(ctx context.Context, eventID string, status domain.EventStatus) error {
			if status == domain.StatusPartialFailed {
				panic("updater panics even on partial_failed — per-item recovery cascades")
			}
			return nil
		},
	}

	pool := NewPool(1, ch, proc, upd)
	ctx := silentCtxPool()
	pool.Start(ctx)

	// Send the event that triggers the double-panic chain
	ch <- domain.Event{ID: "ev-boom", Type: "test"}

	// Shutdown must return — runWorker's recovery must balance waitGroup
	// so that Shutdown doesn't hang forever
	shutdownDone := make(chan struct{})
	go func() {
		pool.Shutdown()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		// Shutdown returned — the goroutine-level recovery worked
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown hung — per-goroutine recovery failed to balance waitGroup")
	}

	// The panicked event should be counted as failed (by the per-item defer before it cascaded)
	// Note: per-item defer tried failed+1 before UpdateEventStatus, so it should be counted
	assert.Equal(t, int64(1), pool.Failed(), "event must be counted as failed")
	assert.Equal(t, int64(0), pool.Processed())
}

// ---------------------------------------------------------------------------
// 19. Defer best-effort partial_failed returns error — when the per-item
//     recovery's UpdateEventStatus(partial_failed) call itself returns an
//     error, the error is logged and the function exits gracefully.
//     Covers pool.go lines 193-198.
// ---------------------------------------------------------------------------

func TestPool_PanicInProcess_DeferPartialFailedErrors(t *testing.T) {
	ch := make(chan domain.Event, 1)
	proc := &fakeProcessor{
		processFunc: func(ctx context.Context, event domain.Event) error {
			panic("processor panic")
		},
	}
	// Updater returns an error (not panic) specifically on partial_failed
	upd := &fakeStatusUpdater{
		transitions: make(map[string][]domain.EventStatus),
		updateFunc: func(ctx context.Context, eventID string, status domain.EventStatus) error {
			if status == domain.StatusPartialFailed {
				return errors.New("db unavailable for partial_failed")
			}
			return nil
		},
	}

	pool := NewPool(1, ch, proc, upd)
	ctx := silentCtxPool()
	pool.Start(ctx)

	ch <- domain.Event{ID: "ev-1", Type: "test"}
	pool.Shutdown()

	// Event is still counted as failed (the defer increments failed before
	// attempting the best-effort status update)
	assert.Equal(t, int64(1), pool.Failed())
	assert.Equal(t, int64(0), pool.Processed())

	// Transitions: processing was recorded, partial_failed was attempted (and errored)
	transitions := upd.Transitions("ev-1")
	require.GreaterOrEqual(t, len(transitions), 2)
	assert.Equal(t, domain.StatusProcessing, transitions[0])
	assert.Equal(t, domain.StatusPartialFailed, transitions[len(transitions)-1])
}

// ---------------------------------------------------------------------------
// 20. Processing error → partial_failed update also errors — the best-effort
//     status persistence after a processor error itself fails. The event is
//     still counted as failed. Covers pool.go lines 223-228.
// ---------------------------------------------------------------------------

func TestPool_ProcessingError_PartialFailedUpdateErrors(t *testing.T) {
	ch := make(chan domain.Event, 1)
	proc := &fakeProcessor{
		processFunc: func(ctx context.Context, event domain.Event) error {
			return errors.New("processing failed")
		},
	}
	// Updater returns error on partial_failed (best-effort also fails)
	upd := &fakeStatusUpdater{
		transitions: make(map[string][]domain.EventStatus),
		updateFunc: func(ctx context.Context, eventID string, status domain.EventStatus) error {
			if status == domain.StatusPartialFailed {
				return errors.New("db unavailable for partial_failed")
			}
			return nil
		},
	}

	pool := NewPool(1, ch, proc, upd)
	ctx := silentCtxPool()
	pool.Start(ctx)

	ch <- domain.Event{ID: "ev-1", Type: "test"}
	pool.Shutdown()

	// Event is counted as failed regardless of the partial_failed update outcome
	assert.Equal(t, int64(1), pool.Failed())
	assert.Equal(t, int64(0), pool.Processed())

	// Transitions: processing was recorded, partial_failed was attempted (and errored)
	transitions := upd.Transitions("ev-1")
	require.Len(t, transitions, 2)
	assert.Equal(t, domain.StatusProcessing, transitions[0])
	assert.Equal(t, domain.StatusPartialFailed, transitions[1])
}

// ---------------------------------------------------------------------------
// 21. Processor returns ErrEventDead (T11 FanOut dead-letter) →
//     pool does NOT persist partial_failed, leaves dead status untouched.
//     Failed() = 1, Processed() = 0, transitions = [processing] only.
// ---------------------------------------------------------------------------

func TestPool_ProcessorReturnsErrEventDead(t *testing.T) {
	ch := make(chan domain.Event, 1)
	proc := &fakeProcessor{
		processFunc: func(ctx context.Context, event domain.Event) error {
			return ErrEventDead
		},
	}
	upd := &fakeStatusUpdater{transitions: make(map[string][]domain.EventStatus)}

	pool := NewPool(1, ch, proc, upd)
	ctx := silentCtxPool()
	pool.Start(ctx)

	ch <- domain.Event{ID: "ev-dead", Type: "test"}
	pool.Shutdown()

	// The event is counted as failed (ErrEventDead is an error).
	assert.Equal(t, int64(1), pool.Failed(), "failed must be 1 — processor returned ErrEventDead")
	assert.Equal(t, int64(0), pool.Processed(), "processed must be 0 — event was not completed")

	// Transitions: only [processing] — no partial_failed (dead status is left untouched).
	transitions := upd.Transitions("ev-dead")
	require.Len(t, transitions, 1,
		"only processing must be recorded; partial_failed must NOT be recorded for ErrEventDead")
	assert.Equal(t, domain.StatusProcessing, transitions[0])
}
