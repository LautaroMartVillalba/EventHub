// Package ratelimit provides the ingest batcher that decouples the HTTP
// handlers from the persistence layer.
//
// The Batcher is a bounded FIFO queue (a buffered channel). POST /events
// submits events through Batcher.Submit instead of writing to the database
// directly, which gives the ingest path two properties:
//
//   - Natural backpressure: when the queue is full, Submit blocks the HTTP
//     handler until the consumer frees a slot. The API never answers 429;
//     clients slow down naturally instead of being rejected.
//   - Asynchronous persistence: a single consumer goroutine drains the queue
//     and writes each event to the repository, so the HTTP layer never
//     touches the database directly.
//
// Go channels are FIFO per sender, so events submitted from the same sender
// are persisted in submission order.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"eventhub/internal/domain"
	"eventhub/internal/storage"
)

// drainTimeout bounds how long a drain may take after the lifecycle context
// passed to Start has been cancelled, or when Shutdown has to persist the
// queue itself. It is deliberately generous: drains only run at shutdown,
// when the process is ending anyway and the remaining work must finish.
const drainTimeout = 30 * time.Second

// ErrClosed is returned by Submit once the Batcher has been shut down and no
// longer accepts events. Handlers typically map it to 503 Service
// Unavailable.
var ErrClosed = errors.New("ratelimit batcher is closed")

// queueItem is a single accepted event together with a private channel the
// consumer uses to report the persistence outcome back to the waiting
// Submit caller.
//
// result is buffered with capacity 1 so the consumer never blocks delivering
// the result, even when the caller has stopped waiting (for example because
// the HTTP client disconnected while the event was queued). A buffered send
// succeeds regardless of whether the receiver still reads the channel.
type queueItem struct {
	event  domain.Event
	result chan error
}

// Batcher is a bounded FIFO queue that persists accepted events asynchronously.
//
// Lifecycle: call New, then Start once with a lifecycle context, submit events
// via Submit, and finally Shutdown once (idempotent) to drain the queue and
// stop the consumer. Start and Shutdown are not intended to run concurrently.
//
// Contract: Submit requires Start to have been called at least once — the
// consumer goroutine drains the queue. If the consumer dies, Shutdown may not
// complete; the panic recovery inside the consumer (run and handleItem) is the
// primary defense against that.
type Batcher struct {
	repository *storage.Repository
	logger     *slog.Logger

	// items is the bounded input queue. Submitters block on it when full
	// (natural backpressure); the consumer drains it.
	items chan queueItem

	// submitMu serialises the "check closed + send" pair in Submit so the
	// input channel is never closed while a sender is mid-send. Shutdown
	// takes the same lock before closing, guaranteeing that every event
	// accepted before Shutdown is enqueued and therefore drained.
	submitMu sync.Mutex

	// closed is set once Shutdown begins; Submit checks it to return ErrClosed.
	closed atomic.Bool

	// shutdownOnce makes Shutdown idempotent.
	shutdownOnce sync.Once

	// startOnce ensures Start launches the consumer goroutine at most once.
	startOnce sync.Once

	// waitGroup tracks the consumer goroutine so Shutdown can wait for it.
	waitGroup sync.WaitGroup

	// Counters (atomics) exposed for metrics and tests.
	submitted atomic.Int64 // events accepted by Submit (enqueued)
	processed atomic.Int64 // events dequeued and handled by the consumer
	failed    atomic.Int64 // events whose persistence failed (excluding ErrConflict)

	// persistFunc is the persistence seam invoked by handleItem for every
	// queued event. New wires it to persistEvent; tests in this package may
	// replace it to inject errors or panics. It is never nil in production.
	persistFunc func(ctx context.Context, event domain.Event) error
}

// New returns a Batcher that persists events through repository using a
// queue of the given capacity (batchSize).
//
// batchSize must be at least 1; repository and logger must be non-nil.
// Configuration supplies the default value via config.RateBatchSize
// (env RATE_BATCH_SIZE, default 300).
func New(batchSize int, repository *storage.Repository, logger *slog.Logger) *Batcher {
	if batchSize < 1 {
		panic("ratelimit: batchSize must be at least 1")
	}
	if repository == nil {
		panic("ratelimit: repository must not be nil")
	}
	if logger == nil {
		panic("ratelimit: logger must not be nil")
	}
	batcher := &Batcher{
		repository: repository,
		logger:     logger,
		items:      make(chan queueItem, batchSize),
	}
	batcher.persistFunc = batcher.persistEvent
	return batcher
}

// Start launches the consumer goroutine that drains the queue and persists
// events. It must be called once before submitting events; repeated calls are
// no-ops.
//
// The lifecycle context is detached from database writes via
// context.WithoutCancel: cancelling it (for example because the process is
// being killed) signals that shutdown began, but never aborts an insert that
// is already in flight — the consumer keeps draining the accepted events so
// nothing is lost. See run for the rationale.
func (batcher *Batcher) Start(lifecycleCtx context.Context) {
	batcher.startOnce.Do(func() {
		batcher.waitGroup.Add(1)
		go batcher.run(lifecycleCtx)
	})
}

// Submit enqueues an event for persistence and blocks until the consumer has
// persisted it, returning the persistence outcome: nil on success,
// storage.ErrConflict when the idempotency key already exists, or the wrapped
// database error.
//
// If the queue is full Submit blocks — that is the intended natural
// backpressure, never a 429 rejection. During backpressure (queue full) the
// blocking send does not observe ctx cancellation; the event is still accepted
// and persisted. The caller's context is only observed while waiting for the
// persistence result after the send, so a cancellation there yields a context
// error even though the event stays queued and is still persisted. After
// Shutdown, Submit returns ErrClosed.
//
// Submit requires Start to have been called at least once; the consumer
// goroutine drains the queue. If the consumer dies, Shutdown may not complete
// — recover in the consumer is the primary defense (see run and handleItem).
func (batcher *Batcher) Submit(ctx context.Context, event domain.Event) error {
	item := queueItem{
		event:  event,
		result: make(chan error, 1),
	}

	// The send is performed under submitMu so Shutdown cannot close the
	// channel underneath it. When the queue is full the send blocks here,
	// which is the desired backpressure; Shutdown waits for the lock, so an
	// event accepted just before Shutdown is always enqueued first and then
	// drained. The submitted counter is also updated under the lock so the
	// invariant submitted >= processed holds at every instant.
	batcher.submitMu.Lock()
	if batcher.closed.Load() {
		batcher.submitMu.Unlock()
		return ErrClosed
	}
	batcher.items <- item
	batcher.submitted.Add(1)
	batcher.submitMu.Unlock()

	// Wait for the consumer to persist the event and report the outcome. The
	// result channel is buffered, so the consumer never blocks even if this
	// caller stops waiting (client disconnected) while the event is queued.
	select {
	case <-ctx.Done():
		return fmt.Errorf("waiting for event %q persistence: %w", event.ID, ctx.Err())
	case persistErr := <-item.result:
		return persistErr
	}
}

// Shutdown stops accepting events and waits until every accepted event has
// been persisted. It closes the input channel, lets the consumer goroutine
// (if running) drain the queue, and if no consumer was started drains the
// queue inline so nothing accepted is lost. Shutdown is idempotent: calling
// it more than once is a no-op.
func (batcher *Batcher) Shutdown() {
	batcher.shutdownOnce.Do(func() {
		batcher.closed.Store(true)

		// Close the input channel. submitMu guarantees no Submit is mid-send,
		// so everything accepted so far is in the queue and will be drained.
		batcher.submitMu.Lock()
		close(batcher.items)
		batcher.submitMu.Unlock()

		// If Start launched the consumer, it drains the closed channel; wait
		// for it. If Start was never called, or runs concurrently, drain the
		// remaining items inline — ranging over the closed channel is safe
		// and a no-op once it is empty.
		batcher.waitGroup.Wait()
		drainCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
		defer cancel()
		for item := range batcher.items {
			batcher.handleItem(drainCtx, item)
		}
	})
}

// run is the consumer goroutine launched by Start. It drains the queue until
// the input channel is closed and empty, persisting every event one at a time.
//
// The write context is derived from lifecycleCtx with context.WithoutCancel:
// cancelling the lifecycle context (e.g. the process is being killed) must
// never abort a persistence that is already in flight. With modernc/sqlite a
// cancellation landing mid-transaction leaves the outcome indeterminate (the
// insert may have committed while tx.Commit reports a bogus error), which
// would silently lose an event that was already accepted. By detaching
// writes from cancellation, the consumer always completes what it started and
// drains the queue normally until Shutdown closes the channel.
func (batcher *Batcher) run(lifecycleCtx context.Context) {
	// Recover BEFORE Done: a panic escaping handleItem (or the loop itself)
	// must never leave the waitGroup unbalanced, or Shutdown would hang
	// forever on waitGroup.Wait().
	defer func() {
		if r := recover(); r != nil {
			batcher.logger.Error("consumer panic recovered", "panic", r)
		}
		batcher.waitGroup.Done()
	}()

	writeCtx := context.WithoutCancel(lifecycleCtx)
	for item := range batcher.items {
		batcher.handleItem(writeCtx, item)
	}
}

// handleItem persists one queued event, updates the counters, reports the
// outcome on the item's result channel and logs the result.
func (batcher *Batcher) handleItem(ctx context.Context, item queueItem) {
	// The event was dequeued and is now being handled, so it counts as
	// processed BEFORE persisting. Incrementing here — instead of after
	// persistFunc — keeps the counter consistent even when persistFunc
	// panics: a panicking event was still dequeued and handled (its outcome
	// is reported back to the Submit caller by the recover below), so it
	// must count as processed or Pending() would over-estimate the queue
	// depth forever.
	batcher.processed.Add(1)

	// Per-item recovery: a panic while persisting this event is contained so
	// the consumer keeps processing the rest of the queue and a single bad
	// event cannot take the whole ingest pipeline down. The caller of Submit
	// is unblocked with an error — the buffered result channel (cap 1)
	// cannot receive twice because the normal body below stops executing at
	// the panic point.
	defer func() {
		if r := recover(); r != nil {
			batcher.logger.Error("panic persisting event", "event_id", item.event.ID, "panic", r)
			item.result <- fmt.Errorf("internal panic: %v", r)
			batcher.failed.Add(1)
		}
	}()

	err := batcher.persistFunc(ctx, item.event)
	if err != nil && !errors.Is(err, storage.ErrConflict) {
		batcher.failed.Add(1)
	}
	item.result <- err

	switch {
	case err == nil:
		// Success; nothing to log at Info level (too noisy for the ingest
		// path). Kept as an explicit branch for clarity.
	case errors.Is(err, storage.ErrConflict):
		batcher.logger.Debug("event already exists; idempotent retry skipped",
			"event_id", item.event.ID,
			"idempotency_key", item.event.IdempotencyKey,
		)
	default:
		batcher.logger.Error("failed to persist event",
			"error", err,
			"event_id", item.event.ID,
			"event_type", item.event.Type,
		)
	}
}

// persistEvent writes one event to the repository.
//
// All persistence is delegated to InsertEvent, which writes the event row AND
// any processes embedded in event.Processes inside a single atomic
// transaction. persistEvent deliberately never calls InsertProcesses: doing so
// would re-insert the same processes and violate the UNIQUE constraint on
// event_processes.id (double-insert hazard), and splitting the write across
// two transactions could leave a partial event if the second one failed.
// Repository.InsertProcesses remains public storage API for attaching
// processes to existing events in the future, but the batcher does not use it.
func (batcher *Batcher) persistEvent(ctx context.Context, event domain.Event) error {
	if err := batcher.repository.InsertEvent(ctx, event); err != nil {
		if errors.Is(err, storage.ErrConflict) {
			return err // propagated unchanged so handlers can map it to 409
		}
		return fmt.Errorf("persist event %q: %w", event.ID, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

// Submitted returns the number of events accepted by Submit since the
// Batcher was created.
func (batcher *Batcher) Submitted() int64 {
	return batcher.submitted.Load()
}

// Processed returns the number of events dequeued and handled by the
// consumer (or by the Shutdown drain) since the Batcher was created.
func (batcher *Batcher) Processed() int64 {
	return batcher.processed.Load()
}

// Failed returns the number of events whose persistence failed with an error
// other than storage.ErrConflict (conflicts are an expected outcome, not a
// failure).
func (batcher *Batcher) Failed() int64 {
	return batcher.failed.Load()
}

// Pending returns an approximate count of accepted events still waiting in
// the queue. The two underlying counters are read independently, so under
// concurrency the value may be off by a small number. Intended for logging
// and observability, not for control-flow decisions.
func (batcher *Batcher) Pending() int64 {
	return batcher.submitted.Load() - batcher.processed.Load()
}
