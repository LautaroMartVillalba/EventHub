// ---------------------------------------------------------------------------
// Level-3 fan-out processor (T11)
// ---------------------------------------------------------------------------
package workers

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"eventhub/internal/dispatch"
	"eventhub/internal/domain"
	"eventhub/internal/logging"
	"eventhub/internal/retry"
)

// maxPanicStackBytes caps the stack trace embedded in a process failure's
// error message, so the persisted error_msg stays bounded while keeping the
// frames that panicked.
const maxPanicStackBytes = 512

// FanOutRepository is the minimal persistence contract the FanOut depends
// on. *storage.Repository satisfies it via FetchProcessesForRetry,
// UpdateProcessStatus and MoveEventToDeadLetter; tests inject a fake
// implementation for deterministic behaviour (same seam pattern as the
// Pool's EventStatusUpdater).
type FanOutRepository interface {
	// FetchProcessesForRetry returns the event's processes whose status is
	// neither completed nor dead — pending, or failed with an elapsed
	// next_retry_at.
	FetchProcessesForRetry(ctx context.Context, eventID string) ([]domain.Process, error)
	// UpdateProcessStatus persists the given status for a process.
	UpdateProcessStatus(ctx context.Context, processID string, status domain.ProcessStatus, attempts int, nextRetryAt *time.Time, errorMsg string) error
	// MoveEventToDeadLetter persists a dead-letter snapshot of the event
	// and marks the event as dead.
	MoveEventToDeadLetter(ctx context.Context, eventID, reason string) error
}

// FanOut is the level-3 processor (T11) that replaces the LoggingProcessor
// of T10: it dispatches each retryable process of an event to the handler
// registered for its name, one goroutine per process, and persists the
// per-process status transitions.
//
// It satisfies workers.EventProcessor through Process, so the Pool consumes
// it without knowing about the registry or the repository. Every field is
// set once by NewFanOut and never mutated afterwards.
type FanOut struct {
	registry   *dispatch.Registry
	repo       FanOutRepository
	calculator *retry.Calculator
}

// NewFanOut returns a FanOut that dispatches the retryable processes of an
// event to the handlers registered in registry, persisting status
// transitions through repo.
//
// registry, repo and calculator must be non-nil; otherwise NewFanOut panics.
// The calculator encapsulates the retry policy — the maximum attempt count
// and the backoff schedule — so the FanOut itself neither reads nor parses
// configuration: the caller builds the policy with retry.NewCalculator from
// config.MaxAttempts (env MAX_ATTEMPTS, default 5) and
// config.BackoffSchedule (env BACKOFF_SCHEDULE, default "2s,5s,15s,30s,60s").
func NewFanOut(registry *dispatch.Registry, repo FanOutRepository, calculator *retry.Calculator) *FanOut {
	if registry == nil {
		panic("workers: registry must not be nil")
	}
	if repo == nil {
		panic("workers: repo must not be nil")
	}
	if calculator == nil {
		panic("workers: calculator must not be nil")
	}
	return &FanOut{
		registry:   registry,
		repo:       repo,
		calculator: calculator,
	}
}

// Process implements workers.EventProcessor (level-3 fan-out). It runs every
// process of the event that is ready for a retry — pending, or failed with an
// elapsed next_retry_at — each on its own goroutine, and persists the
// per-process status transitions. Processes still in backoff (a future
// next_retry_at) are skipped without running and only counted. The aggregated
// outcome decides the event's final status, which the Pool persists:
//
//   - nil: every process completed, so the Pool marks the event completed.
//   - a plain error: one or more processes failed but still have retries
//     left, or one or more processes were skipped because they are still in
//     backoff; the Pool marks the event partial_failed and the Orchestrator
//     re-polls it once the earliest next_retry_at has elapsed.
//   - ErrEventDead: at least one process exceeded maxAttempts and the event
//     was already moved to the dead-letter queue; the Pool leaves the dead
//     status untouched.
//
// The ready/backoff split happens in the FanOut rather than in
// FetchProcessesForRetry on purpose: the fan-out must learn that processes
// remained in backoff so it can report the event as partial_failed. If the
// query excluded them, a multi-process event could be marked completed while
// pending processes would never run again.
func (fanout *FanOut) Process(ctx context.Context, event domain.Event) error {
	handlers := fanout.registry.GetProcesses(event.Type)
	if len(handlers) == 0 {
		logging.FromContext(ctx).Warn("fanout: no processes registered for event type",
			"event_id", event.ID,
			"event_type", event.Type,
		)
		return nil
	}

	// Index the registered handlers by name so each process row resolves to
	// its handler in O(1). dbProcess (a domain.Process database row) and
	// handler (a dispatch.Process callback) are deliberately named apart to
	// keep the two kinds of "process" distinct.
	registryByName := make(map[string]dispatch.Process, len(handlers))
	for _, handler := range handlers {
		registryByName[handler.Name] = handler
	}

	// Fetching only the processes that still need a retry gives the fan-out
	// partial idempotency: a redelivered event only re-runs the processes
	// that did not complete instead of replaying the whole event.
	pending, err := fanout.repo.FetchProcessesForRetry(ctx, event.ID)
	if err != nil {
		return err
	}

	var waitGroup sync.WaitGroup
	result := &fanOutResult{}

	// FetchProcessesForRetry returns every non-completed process regardless
	// of its next_retry_at, so the backoff filter is applied here in memory:
	// the fan-out must count the processes that remain in backoff to report
	// the event as partial_failed (see the method doc comment). Only the
	// ready processes run on goroutines.
	now := time.Now().UTC()
	ready := make([]domain.Process, 0, len(pending))
	for _, dbProcess := range pending {
		if dbProcess.NextRetryAt != nil && dbProcess.NextRetryAt.After(now) {
			result.recordSkipped()
			continue
		}
		ready = append(ready, dbProcess)
	}

	for _, dbProcess := range ready {
		waitGroup.Add(1)
		go func(process domain.Process) {
			defer waitGroup.Done()
			fanout.runProcess(ctx, event, process, registryByName, result)
		}(dbProcess)
	}

	waitGroup.Wait()

	switch {
	case result.anyDead:
		// At least one process exhausted its retries. Move the whole event
		// to the dead-letter queue: MoveEventToDeadLetter persists a JSON
		// snapshot of the event (with every process in its final state) and
		// marks the event dead, which the requeue API and the DLQ rely on.
		reason := fmt.Sprintf("process %s exceeded max attempts (%d)", result.deadProcess, result.deadAttempts)
		if err := fanout.repo.MoveEventToDeadLetter(ctx, event.ID, reason); err != nil {
			return err
		}
		logging.FromContext(ctx).Error("fanout: event moved to dead letter",
			"event_id", event.ID,
			"event_type", event.Type,
			"reason", reason,
		)
		return ErrEventDead
	case result.anyFailed:
		return fmt.Errorf("fanout: %d process(es) failed for event %s", result.failedCount, event.ID)
	case result.skipped > 0:
		// At least one process is still in backoff. Returning an error makes
		// the Pool persist the event as partial_failed instead of completed,
		// so the Orchestrator re-polls it once the earliest next_retry_at
		// has elapsed.
		return fmt.Errorf("fanout: %d process(es) still in backoff for event %s", result.skipped, event.ID)
	default:
		return nil
	}
}

// runProcess executes one process row of the event on its own goroutine and
// persists the outcome. The per-process panic recovery runs before the
// caller's waitGroup.Done, so a panicking handler can neither crash the
// fan-out nor leave the WaitGroup unbalanced (same pattern as the Pool's
// processEvent).
func (fanout *FanOut) runProcess(ctx context.Context, event domain.Event, dbProcess domain.Process, registryByName map[string]dispatch.Process, result *fanOutResult) {
	logger := logging.FromContext(ctx)

	defer func() {
		if recovered := recover(); recovered != nil {
			stack := debug.Stack()
			errorMsg := fmt.Sprintf("panic in process %s: %v\n%s", dbProcess.ProcessName, recovered, briefStack(stack))
			logger.Error("fanout: panic in process recovered",
				"event_id", event.ID,
				"process_id", dbProcess.ID,
				"process_name", dbProcess.ProcessName,
				"stack", string(stack),
			)
			fanout.handleProcessFailure(ctx, event, dbProcess, errorMsg, result)
		}
	}()

	handler, ok := registryByName[dbProcess.ProcessName]
	if !ok {
		// A process row whose name has no registered handler is a
		// misconfiguration: fail the process with a descriptive error so
		// the retry machinery surfaces it and eventually dead-letters it.
		errorMsg := fmt.Sprintf("no handler registered for process %s", dbProcess.ProcessName)
		logger.Error("fanout: process failed",
			"event_id", event.ID,
			"process_id", dbProcess.ID,
			"process_name", dbProcess.ProcessName,
			"error", errorMsg,
		)
		fanout.handleProcessFailure(ctx, event, dbProcess, errorMsg, result)
		return
	}

	if err := handler.Fn(ctx, event); err != nil {
		logger.Error("fanout: process failed",
			"event_id", event.ID,
			"process_id", dbProcess.ID,
			"process_name", dbProcess.ProcessName,
			"error", err,
		)
		fanout.handleProcessFailure(ctx, event, dbProcess, err.Error(), result)
		return
	}

	// Success: persist the completed status with the attempt count that
	// actually ran (previous attempts plus this one).
	if err := fanout.repo.UpdateProcessStatus(ctx, dbProcess.ID, domain.ProcessStatusCompleted, dbProcess.Attempts+1, nil, ""); err != nil {
		logger.Error("fanout: failed to mark process as completed",
			"event_id", event.ID,
			"process_id", dbProcess.ID,
			"process_name", dbProcess.ProcessName,
			"error", err,
		)
		// The process ran but its completed status could not be persisted,
		// so the database still shows it as retryable. Treat the outcome as
		// a failure: the event is not marked completed and a later
		// redelivery re-runs the process (at-least-once).
		fanout.handleProcessFailure(ctx, event, dbProcess, err.Error(), result)
		return
	}
}

// handleProcessFailure records one failed attempt of a process and schedules
// its retry: if the calculator reports the attempt is not retryable (the
// attempt count has reached maxAttempts) the process is marked dead (and the
// event will be dead-lettered); otherwise it is marked failed with the next
// retry time computed from the calculator's backoff schedule. The outcome
// flags are written through result's mutex.
func (fanout *FanOut) handleProcessFailure(ctx context.Context, event domain.Event, dbProcess domain.Process, errorMsg string, result *fanOutResult) {
	newAttempts := dbProcess.Attempts + 1

	if !fanout.calculator.ShouldRetry(newAttempts) {
		if err := fanout.repo.UpdateProcessStatus(ctx, dbProcess.ID, domain.ProcessStatusDead, newAttempts, nil, errorMsg); err != nil {
			logging.FromContext(ctx).Error("fanout: failed to mark process as dead",
				"event_id", event.ID,
				"process_id", dbProcess.ID,
				"process_name", dbProcess.ProcessName,
				"error", err,
			)
		}
		result.recordDead(dbProcess.ProcessName, newAttempts)
		return
	}

	nextRetryAt, _ := fanout.calculator.NextRetry(newAttempts)
	if err := fanout.repo.UpdateProcessStatus(ctx, dbProcess.ID, domain.ProcessStatusFailed, newAttempts, &nextRetryAt, errorMsg); err != nil {
		logging.FromContext(ctx).Error("fanout: failed to mark process as failed",
			"event_id", event.ID,
			"process_id", dbProcess.ID,
			"process_name", dbProcess.ProcessName,
			"error", err,
		)
	}
	result.recordFailure()
}

// fanOutResult aggregates the per-process outcomes written by the fan-out
// goroutines. Every write goes through mutex; Process reads the fields only
// after waitGroup.Wait has returned, by which time every writer has exited,
// so reads need no lock.
type fanOutResult struct {
	mutex        sync.Mutex
	anyFailed    bool
	failedCount  int
	anyDead      bool
	deadProcess  string
	deadAttempts int
	skipped      int
}

// recordFailure marks one process that failed but still has retries left.
func (result *fanOutResult) recordFailure() {
	result.mutex.Lock()
	defer result.mutex.Unlock()
	result.anyFailed = true
	result.failedCount++
}

// recordDead marks one process that exceeded maxAttempts. The first dead
// process is kept as the source of the dead-letter reason.
func (result *fanOutResult) recordDead(processName string, attempts int) {
	result.mutex.Lock()
	defer result.mutex.Unlock()
	if !result.anyDead {
		result.deadProcess = processName
		result.deadAttempts = attempts
	}
	result.anyDead = true
}

// recordSkipped counts one process that was left in backoff (a future
// next_retry_at) and therefore did not run in this pass.
func (result *fanOutResult) recordSkipped() {
	result.mutex.Lock()
	defer result.mutex.Unlock()
	result.skipped++
}

// briefStack returns the first maxPanicStackBytes bytes of a stack trace.
func briefStack(stack []byte) []byte {
	if len(stack) > maxPanicStackBytes {
		return stack[:maxPanicStackBytes]
	}
	return stack
}
