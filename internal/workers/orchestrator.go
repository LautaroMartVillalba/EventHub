// Package workers implements the background execution pipeline of EventHub:
// the poll orchestrator that discovers ready events and the worker pool that
// executes them. The package provides both components.
//
// Orchestrator (T09) is the producer side of the pipeline. On a fixed poll
// interval (config.OrchestratorPoll, default 5s) it asks the repository for
// events whose status makes them ready for processing — pending or
// partial_failed with at least one process past its retry window — and
// forwards them on a buffered channel, ReadyEvents. It never blocks on the
// channel: if the buffer is full, the event is skipped for this tick and
// remains pending in the database, so the next poll picks it up again. This
// gives the pipeline two properties:
//
//   - Poll-based discovery: new or retryable events are picked up within at
//     most one poll interval of becoming ready.
//   - Non-blocking handoff: a slow worker pool cannot stall the poll loop; it
//     only causes events to be rediscovered on the next tick.
//
// Pool (T10) is the consumer side: workerCount goroutines drain ReadyEvents,
// mark each event as processing, execute it through an injectable
// EventProcessor and persist the final status (completed or partial_failed).
// T11 replaces the default LoggingProcessor with the real fan-out processor
// without touching the pool.
//
// The Orchestrator is context-cancellable. Start derives a cancellable
// context from the lifecycle context it receives, and Shutdown cancels it and
// waits for the poll goroutine to exit. ReadyEvents is never closed by the
// Orchestrator: closing it is the consumer's (worker pool) responsibility, so
// the channel contract survives the orchestrator's lifecycle.
package workers

import (
	"context"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"eventhub/internal/domain"
	"eventhub/internal/logging"
)

// EventFetcher is the minimal persistence contract the Orchestrator depends
// on: return up to limit events whose status makes them ready for the
// level-2 workers (pending or partial_failed with processes ready for
// retry). *storage.Repository satisfies it via FetchReadyEvents; tests in
// this package inject a fake implementation for deterministic behaviour.
type EventFetcher interface {
	FetchReadyEvents(ctx context.Context, limit int) ([]domain.Event, error)
}

// Orchestrator polls the repository for ready events and forwards them on a
// buffered channel consumed by the level-2 worker pool.
//
// Lifecycle: call New, then Start once with a lifecycle context, and finally
// Shutdown once (idempotent) to stop the poll loop. Start performs an
// immediate first poll and then polls on every tick of pollInterval.
//
// Contract: Shutdown never closes ReadyEvents — the channel belongs to the
// consumers (the worker pool), which drain it as part of their own shutdown.
// Shutdown only cancels the poll loop and waits for it to exit, so a
// consumer that keeps reading the channel is unaffected. Start and Shutdown
// are not intended to run concurrently.
type Orchestrator struct {
	pollInterval time.Duration
	batchSize    int
	fetcher      EventFetcher

	// ReadyEvents is the buffered channel the Orchestrator publishes ready
	// events to. Consumers (the level-2 workers, T10/T11) read from it. The
	// Orchestrator never closes it, so the channel stays usable after
	// Shutdown until the consumers drain it themselves.
	ReadyEvents chan domain.Event

	// startOnce and shutdownOnce make Start and Shutdown idempotent.
	startOnce    sync.Once
	shutdownOnce sync.Once

	// waitGroup tracks the poll goroutine so Shutdown can wait for it.
	waitGroup sync.WaitGroup

	// cancel cancels the poll context derived by Start; Shutdown calls it.
	// It is written inside startOnce and read inside shutdownOnce; callers
	// must not run Start and Shutdown concurrently (see the Orchestrator
	// contract).
	cancel context.CancelFunc

	// Counters (atomics) exposed for metrics and tests.
	fetched atomic.Int64 // events returned by FetchReadyEvents
	sent    atomic.Int64 // events forwarded to ReadyEvents
	dropped atomic.Int64 // events skipped because ReadyEvents was full
}

// New returns an Orchestrator that polls the repository through fetcher every
// pollInterval, fetching up to batchSize events per poll and forwarding them
// on a buffered channel of capacity channelCapacity.
//
// pollInterval must be greater than zero, batchSize and channelCapacity must
// be at least 1, and fetcher must be non-nil; otherwise New panics.
// Configuration supplies the default values via config.OrchestratorPoll
// (env ORCHESTRATOR_POLL, default 5s), config.WorkersLevel2Count
// (env WORKERS_LEVEL2_COUNT, default 5) and double that for the channel
// capacity.
func New(pollInterval time.Duration, batchSize int, channelCapacity int, fetcher EventFetcher) *Orchestrator {
	if pollInterval <= 0 {
		panic("workers: pollInterval must be greater than zero")
	}
	if batchSize < 1 {
		panic("workers: batchSize must be at least 1")
	}
	if channelCapacity < 1 {
		panic("workers: channelCapacity must be at least 1")
	}
	if fetcher == nil {
		panic("workers: fetcher must not be nil")
	}
	return &Orchestrator{
		pollInterval: pollInterval,
		batchSize:    batchSize,
		fetcher:      fetcher,
		ReadyEvents:  make(chan domain.Event, channelCapacity),
	}
}

// Start launches the poll goroutine. It must be called once before the
// Orchestrator can feed events; repeated calls are no-ops.
//
// The poll context is derived from the given context with context.WithCancel:
// cancelling the parent cascades into the poll loop, and Shutdown cancels the
// same derived context. The first poll runs immediately; subsequent polls run
// on every tick of pollInterval.
func (orchestrator *Orchestrator) Start(ctx context.Context) {
	orchestrator.startOnce.Do(func() {
		pollCtx, cancel := context.WithCancel(ctx)
		orchestrator.cancel = cancel
		orchestrator.waitGroup.Add(1)
		go orchestrator.run(pollCtx)
	})
}

// Shutdown cancels the poll loop and waits until the poll goroutine has
// exited. It is idempotent: calling it more than once is a no-op. If Start
// was never called, Shutdown returns immediately.
//
// Shutdown never closes ReadyEvents; draining the channel is the consumers'
// responsibility (see the Orchestrator contract).
func (orchestrator *Orchestrator) Shutdown() {
	orchestrator.shutdownOnce.Do(func() {
		if orchestrator.cancel != nil {
			orchestrator.cancel()
		}
		orchestrator.waitGroup.Wait()
	})
}

// run is the poll goroutine launched by Start. It performs an immediate first
// poll and then polls once per tick until the context is cancelled.
//
// The panic recovery runs BEFORE waitGroup.Done so an unexpected panic can
// never leave the waitGroup unbalanced, or Shutdown would hang forever on
// waitGroup.Wait().
func (orchestrator *Orchestrator) run(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			logging.FromContext(ctx).Error("orchestrator panic recovered", "panic", r, "stack", string(debug.Stack()))
		}
		orchestrator.waitGroup.Done()
	}()

	// First poll immediately: time.NewTicker waits for the first interval,
	// but an orchestrator must probe for work as soon as it starts.
	orchestrator.poll(ctx)

	ticker := time.NewTicker(orchestrator.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			orchestrator.poll(ctx)
		}
	}
}

// poll fetches up to batchSize ready events from the repository and forwards
// them to ReadyEvents without blocking.
//
// The send is a three-way select: the event is forwarded when the channel has
// room, the poll aborts when the context is cancelled, and when the channel
// is full the event is dropped — it stays pending in the database and the
// next tick fetches it again, so nothing is lost and the ticker never stalls.
// The logger is resolved from the context on every poll, never cached in the
// Orchestrator. Fetch errors are logged and skipped; the poll loop continues.
func (orchestrator *Orchestrator) poll(ctx context.Context) {
	logger := logging.FromContext(ctx)

	events, err := orchestrator.fetcher.FetchReadyEvents(ctx, orchestrator.batchSize)
	if err != nil {
		logger.Error("orchestrator failed to fetch ready events", "error", err)
		return
	}

	fetchedCount := int64(len(events))
	orchestrator.fetched.Add(fetchedCount)

	var sentCount, droppedCount int64
	for _, event := range events {
		select {
		case orchestrator.ReadyEvents <- event:
			sentCount++
		case <-ctx.Done():
			// Shutting down mid-poll: persist what was sent so the counters
			// stay truthful, then abort. Events not yet tried stay pending
			// in the database; nobody is consuming them anyway.
			orchestrator.sent.Add(sentCount)
			orchestrator.dropped.Add(droppedCount)
			return
		default:
			// Channel full: skip for this tick. The event is not lost — it
			// is still pending in the database and the next poll will fetch
			// it again.
			droppedCount++
		}
	}
	orchestrator.sent.Add(sentCount)
	orchestrator.dropped.Add(droppedCount)

	if fetchedCount == 0 {
		// Debug level for the empty case: logging every empty poll at Info
		// would flood the log with noise.
		logger.Debug("orchestrator poll found no ready events")
		return
	}
	logger.Info("orchestrator forwarded ready events",
		"fetched_events", fetchedCount,
		"sent_events", sentCount,
		"dropped_events", droppedCount,
	)
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

// Fetched returns the number of events returned by FetchReadyEvents since
// the Orchestrator was created. Events from a failed fetch do not count.
func (orchestrator *Orchestrator) Fetched() int64 {
	return orchestrator.fetched.Load()
}

// Sent returns the number of events forwarded to ReadyEvents since the
// Orchestrator was created.
func (orchestrator *Orchestrator) Sent() int64 {
	return orchestrator.sent.Load()
}

// Dropped returns the number of events skipped since the Orchestrator was
// created because ReadyEvents was full at poll time. Dropped events are not
// lost: they remain pending in the database and the next poll fetches them
// again.
func (orchestrator *Orchestrator) Dropped() int64 {
	return orchestrator.dropped.Load()
}
