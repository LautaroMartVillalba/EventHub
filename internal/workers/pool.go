// ---------------------------------------------------------------------------
// Level-2 worker pool (T10)
// ---------------------------------------------------------------------------
package workers

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"eventhub/internal/domain"
	"eventhub/internal/logging"
)

// EventProcessor executes the processes of a single event. T10 ships the
// LoggingProcessor default, which only logs the event; T11 replaces it with
// the real fan-out processor that dispatches each process of the event.
// Keeping this seam lets the pool stay unchanged when the processor evolves.
type EventProcessor interface {
	// Process runs the event and returns nil on success or an error that
	// marks the event as partially failed. A processor that already moved
	// the event to the dead-letter queue returns ErrEventDead; the pool
	// leaves the event's dead status untouched.
	Process(ctx context.Context, event domain.Event) error
}

// ErrEventDead is returned by an EventProcessor when the event has already
// been moved to the dead-letter queue (T11 FanOut). The pool recognises it
// and does not overwrite the event's dead status with partial_failed.
var ErrEventDead = errors.New("workers: event moved to dead letter")

// EventStatusUpdater is the minimal persistence contract the Pool depends on
// to record event status transitions. *storage.Repository satisfies it via
// UpdateEventStatus; tests inject a fake implementation for deterministic
// behaviour (same seam pattern as the Orchestrator's EventFetcher).
type EventStatusUpdater interface {
	// UpdateEventStatus persists the given status for the event.
	UpdateEventStatus(ctx context.Context, eventID string, status domain.EventStatus) error
}

// Pool is the level-2 consumer side of the EventHub pipeline: it spawns
// workerCount goroutines that drain the Orchestrator's ReadyEvents channel
// and execute each event through an injectable EventProcessor.
//
// Lifecycle: call NewPool, then Start once with a lifecycle context, and
// finally Shutdown once (idempotent). Start launches the worker goroutines;
// Shutdown closes ReadyEvents and waits until every worker has drained the
// remaining events and exited. Shutdown before Start is a no-op; Start and
// Shutdown must not be called concurrently (same contract as the
// Orchestrator).
//
// Channel contract: the Pool never sends to ReadyEvents — it only closes it
// during Shutdown (Regla 1 of the workers conventions: closing the channel
// is the consumer's responsibility, never the producer's). Closing makes the
// workers' `for event := range` loops drain the buffered events and exit
// naturally, so no event already handed off is lost. In the T18 wiring
// Orchestrator.Shutdown must run before Pool.Shutdown: the Orchestrator's
// poll loop stops sending before the pool closes the channel, so no send on
// a closed channel can ever happen.
//
// Failure semantics: events are processed at-least-once. The worker marks an
// event as processing, invokes the processor, and persists the final status
// — completed on success, partial_failed on error or panic. If the initial
// "processing" update fails the event is still processed (it was already
// delivered); the final update fixes the record. If the final update fails,
// the event is left in a stale status in the database and a later poll
// rediscovers it, so it is either retried or dead-lettered by the
// repository's retry logic. A panicking processor or status update never
// kills the pool: the panic is recovered per event, the event is treated as
// failed, and the worker moves on to the next one.
type Pool struct {
	workerCount   int
	readyEvents   chan domain.Event
	processor     EventProcessor
	statusUpdater EventStatusUpdater

	// startOnce and shutdownOnce make Start and Shutdown idempotent.
	startOnce    sync.Once
	shutdownOnce sync.Once

	// waitGroup tracks the worker goroutines so Shutdown can wait for all
	// of them to drain the channel and exit.
	waitGroup sync.WaitGroup

	// Counters (atomics) exposed for metrics and tests.
	processed atomic.Int64 // events whose Process succeeded and completed was persisted
	failed    atomic.Int64 // events whose Process errored or panicked, or whose completed update panicked
}

// NewPool returns a Pool that runs workerCount goroutines consuming
// readyEvents, executing each event through processor and recording status
// transitions through statusUpdater.
//
// workerCount must be at least 1, and readyEvents, processor and
// statusUpdater must be non-nil; otherwise NewPool panics. The channel is
// typically the Orchestrator's ReadyEvents, and workerCount comes from
// config.WorkersLevel2Count (env WORKERS_LEVEL2_COUNT, default 5) — the
// Pool itself does not read configuration, it receives the value as an
// argument.
func NewPool(workerCount int, readyEvents chan domain.Event, processor EventProcessor, statusUpdater EventStatusUpdater) *Pool {
	if workerCount < 1 {
		panic("workers: workerCount must be at least 1")
	}
	if readyEvents == nil {
		panic("workers: readyEvents channel must not be nil")
	}
	if processor == nil {
		panic("workers: processor must not be nil")
	}
	if statusUpdater == nil {
		panic("workers: statusUpdater must not be nil")
	}
	return &Pool{
		workerCount:   workerCount,
		readyEvents:   readyEvents,
		processor:     processor,
		statusUpdater: statusUpdater,
	}
}

// Start launches the worker goroutines. It must be called once; repeated
// calls are no-ops.
//
// The context is used to resolve the logger and to carry cancellation into
// the status updater and processor calls. The Pool does not derive its own
// cancellable context and keeps no cancel function in the struct, so there
// is no data race between Start and Shutdown (the documented Orchestrator
// race does not apply here). A cancelled parent cannot stop a worker
// mid-event: it only aborts in-flight repository calls, which return their
// errors to be logged by the worker.
func (pool *Pool) Start(ctx context.Context) {
	pool.startOnce.Do(func() {
		for workerIndex := 0; workerIndex < pool.workerCount; workerIndex++ {
			pool.waitGroup.Add(1)
			go pool.runWorker(ctx)
		}
	})
}

// Shutdown stops the pool: it closes ReadyEvents and waits until every
// worker has drained the remaining events and exited. It is idempotent:
// calling it more than once is a no-op. If Start was never called, Shutdown
// closes the channel and returns immediately.
//
// Closing the channel is the pool's right and duty as the consumer (Regla 1
// of the workers conventions): workers ranging over it drain the buffered
// events and exit naturally, so events already handed off are still
// processed. Shutdown does not cancel in-flight events — it simply stops
// accepting new ones and waits.
func (pool *Pool) Shutdown() {
	pool.shutdownOnce.Do(func() {
		close(pool.readyEvents)
		pool.waitGroup.Wait()
	})
}

// runWorker is one of the goroutines launched by Start. It consumes events
// from readyEvents until the channel is closed and drained, processing each
// one. The panic recovery runs BEFORE waitGroup.Done so an unexpected panic
// can never leave the waitGroup unbalanced, or Shutdown would hang forever
// on waitGroup.Wait() (same pattern as the Orchestrator's run).
func (pool *Pool) runWorker(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			logging.FromContext(ctx).Error("worker panic recovered", "panic", r, "stack", string(debug.Stack()))
		}
		pool.waitGroup.Done()
	}()

	for event := range pool.readyEvents {
		pool.processEvent(ctx, event)
	}
}

// processEvent executes a single event: it marks it as processing, invokes
// the processor, and persists the final status (completed on success,
// partial_failed on failure or panic). The per-item recover keeps a
// panicking processor or status update from killing the whole pool: the
// event is treated as failed and the worker loop continues with the next
// event.
//
// The logger is resolved from the context once per event and reused for
// that event's operations; it is never cached in the Pool struct (Regla 4
// of the workers conventions).
func (pool *Pool) processEvent(ctx context.Context, event domain.Event) {
	// Level (a): per-item panic recovery, registered before the logger is
	// resolved so even a panic while resolving the logger is contained.
	// The event is counted as failed and partial_failed is persisted
	// best-effort while the status updater is still usable.
	defer func() {
		if r := recover(); r != nil {
			logger := logging.FromContext(ctx)
			logger.Error("worker panic while processing event",
				"panic", r,
				"stack", string(debug.Stack()),
				"event_id", event.ID,
			)
			pool.failed.Add(1)
			if err := pool.statusUpdater.UpdateEventStatus(ctx, event.ID, domain.StatusPartialFailed); err != nil {
				logger.Error("worker failed to record partial failure after panic",
					"event_id", event.ID,
					"error", err,
				)
			}
		}
	}()

	logger := logging.FromContext(ctx)

	// At-least-once delivery: if the "processing" marker cannot be
	// persisted, the event was already handed off to this worker — log the
	// failure and process it anyway; the final status update below fixes
	// the record.
	if err := pool.statusUpdater.UpdateEventStatus(ctx, event.ID, domain.StatusProcessing); err != nil {
		logger.Error("worker failed to mark event as processing",
			"event_id", event.ID,
			"event_type", event.Type,
			"error", err,
		)
	}

	if err := pool.processor.Process(ctx, event); err != nil {
		logger.Error("worker failed to process event",
			"event_id", event.ID,
			"event_type", event.Type,
			"error", err,
		)
		pool.failed.Add(1)
		if errors.Is(err, ErrEventDead) {
			// The processor (T11 FanOut) already moved the event to the
			// dead-letter queue and recorded status 'dead'; do not overwrite
			// it with partial_failed (the dead status is required by the
			// requeue API and the DLQ snapshot).
			return
		}
		if updateErr := pool.statusUpdater.UpdateEventStatus(ctx, event.ID, domain.StatusPartialFailed); updateErr != nil {
			logger.Error("worker failed to record partial failure",
				"event_id", event.ID,
				"error", updateErr,
			)
		}
		return
	}

	// An event only counts as processed once its final "completed" status
	// has been persisted. If the update fails, the event is left stale in
	// the database and a later poll redelivers it (at-least-once); if it
	// panics, the per-item recover above counts the event as failed and
	// persists partial_failed best-effort. In both cases processed is not
	// incremented, so a single attempt can never count the same event as
	// both processed and failed.
	if updateErr := pool.statusUpdater.UpdateEventStatus(ctx, event.ID, domain.StatusCompleted); updateErr != nil {
		logger.Error("worker failed to mark event as completed",
			"event_id", event.ID,
			"error", updateErr,
		)
		return
	}
	pool.processed.Add(1)
}

// ---------------------------------------------------------------------------
// Default processor (T10)
// ---------------------------------------------------------------------------

// LoggingProcessor is the level-2 default processor of T10: it does not
// execute any process, it only logs the event with structured fields. T11
// replaces it with the real fan-out processor that dispatches each process
// of the event; the Pool is agnostic to which processor is injected.
type LoggingProcessor struct{}

// Process logs the event with its identifier, type and process count and
// returns nil, so the event is considered successfully processed. The
// pointer receiver follows the house convention of the Orchestrator and the
// Batcher; *LoggingProcessor still satisfies EventProcessor.
func (*LoggingProcessor) Process(ctx context.Context, event domain.Event) error {
	logging.FromContext(ctx).Info("worker processing event",
		"event_id", event.ID,
		"event_type", event.Type,
		"process_count", len(event.Processes),
	)
	return nil
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

// Processed returns the number of events whose Process call succeeded AND
// whose completed status was successfully persisted since the Pool was
// created. An event whose final completed update fails (or panics) is not
// counted here: it stays stale (or partial_failed) in the database and a
// later poll redelivers it. Under at-least-once semantics the same event
// can therefore be counted more than once across redeliveries, whenever a
// redelivered attempt eventually persists completed.
func (pool *Pool) Processed() int64 {
	return pool.processed.Load()
}

// Failed returns the number of events that were not completed since the
// Pool was created: events whose Process call returned an error or
// panicked, or whose final completed status update panicked. These events
// are persisted as partial_failed (best-effort in the panic cases) so the
// repository's retry logic can redeliver them.
func (pool *Pool) Failed() int64 {
	return pool.failed.Load()
}
