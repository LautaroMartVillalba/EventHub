package ratelimit

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"eventhub/internal/domain"
	"eventhub/internal/storage"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// newTestBatcher returns a Batcher backed by a real SQLite database in a
// temporary directory, plus the repository for direct DB assertions. The
// database is closed when the test finishes.
func newTestBatcher(t *testing.T, batchSize int) (*Batcher, *storage.Repository) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "batcher.db")
	db, err := storage.NewDB(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	repo := storage.NewRepository(db)
	logger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(batchSize, repo, logger), repo
}

// testEvent builds a pending event with a deterministic idempotency key so
// payloads can be made unique per index.
func testEvent(index int) *domain.Event {
	payload := fmt.Sprintf(`{"order_id":"order-%d"}`, index)
	return domain.NewEvent("purchase_completed", payload, "batch-key-"+strconv.Itoa(index), domain.StatusPending, nil)
}

// persistedCount returns the total number of events in the database.
func persistedCount(t *testing.T, repo *storage.Repository) int {
	t.Helper()
	events, err := repo.FetchAll(context.Background())
	require.NoError(t, err)
	return len(events)
}

// ---------------------------------------------------------------------------
// lifecycle: Start / Shutdown idempotency
// ---------------------------------------------------------------------------

func TestShutdown_IsIdempotent(t *testing.T) {
	batcher, _ := newTestBatcher(t, 10)
	batcher.Start(context.Background())

	batcher.Shutdown()
	batcher.Shutdown() // must not panic
}

func TestShutdown_WithoutStart_DoesNotPanic(t *testing.T) {
	batcher, _ := newTestBatcher(t, 10)
	batcher.Shutdown()
	batcher.Shutdown()
}

func TestStart_Twice_LaunchesSingleConsumer(t *testing.T) {
	batcher, _ := newTestBatcher(t, 10)
	batcher.Start(context.Background())
	batcher.Start(context.Background()) // second call is a no-op

	event := testEvent(0)
	require.NoError(t, batcher.Submit(context.Background(), *event))
	require.Equal(t, int64(1), batcher.Processed())
}

// ---------------------------------------------------------------------------
// Submit after Shutdown
// ---------------------------------------------------------------------------

func TestSubmit_AfterShutdown_ReturnsErrClosed(t *testing.T) {
	batcher, _ := newTestBatcher(t, 10)
	batcher.Start(context.Background())
	batcher.Shutdown()

	event := testEvent(0)
	err := batcher.Submit(context.Background(), *event)
	require.ErrorIs(t, err, ErrClosed)
}

// ---------------------------------------------------------------------------
// persistence and outcome propagation
// ---------------------------------------------------------------------------

func TestSubmit_PersistsEvent_AndReturnsNil(t *testing.T) {
	batcher, repo := newTestBatcher(t, 10)
	batcher.Start(context.Background())

	event := testEvent(1)
	require.NoError(t, batcher.Submit(context.Background(), *event))

	require.Equal(t, 1, persistedCount(t, repo))
	persisted, err := repo.FetchByID(context.Background(), event.ID)
	require.NoError(t, err)
	assert.Equal(t, event.IdempotencyKey, persisted.IdempotencyKey)
	assert.Equal(t, domain.StatusPending, persisted.Status)

	// Stats reflect exactly one event.
	assert.Equal(t, int64(1), batcher.Submitted())
	assert.Equal(t, int64(1), batcher.Processed())
	assert.Equal(t, int64(0), batcher.Failed())
}

func TestSubmit_DuplicateIdempotencyKey_ReturnsErrConflict(t *testing.T) {
	batcher, _ := newTestBatcher(t, 10)
	batcher.Start(context.Background())

	// Two events with the same idempotency key (built with NewEvent so they
	// carry different IDs — the key collision is what matters).
	first := domain.NewEvent("t", `{"a":1}`, "same-key", domain.StatusPending, nil)
	second := domain.NewEvent("t", `{"a":1}`, "same-key", domain.StatusPending, nil)

	require.NoError(t, batcher.Submit(context.Background(), *first))
	err := batcher.Submit(context.Background(), *second)
	require.ErrorIs(t, err, storage.ErrConflict)

	// A conflict is processed but not counted as a failure.
	assert.Equal(t, int64(2), batcher.Processed())
	assert.Equal(t, int64(0), batcher.Failed())
}

func TestSubmit_CancelledContext_ReturnsContextError(t *testing.T) {
	batcher, repo := newTestBatcher(t, 10)
	// Deliberately NOT starting the consumer: the event stays queued and its
	// result never arrives, so cancelling the caller's context deterministically
	// aborts the wait (no race with the consumer delivering a nil result).
	ctx, cancel := context.WithCancel(context.Background())
	event := testEvent(3)

	result := make(chan error, 1)
	go func() {
		result <- batcher.Submit(ctx, *event)
	}()

	// Wait until the event is accepted, then cancel the wait.
	require.Eventually(t, func() bool { return batcher.Submitted() == 1 }, 5*time.Second, time.Millisecond)
	cancel()

	require.ErrorIs(t, <-result, context.Canceled)

	// The event was accepted before cancellation; Shutdown must still persist it.
	batcher.Shutdown()
	require.Equal(t, 1, persistedCount(t, repo))
}

// ---------------------------------------------------------------------------
// backpressure
// ---------------------------------------------------------------------------

func TestSubmit_BlocksWhenQueueFull_ThenDrainsWhenConsumerStarts(t *testing.T) {
	batcher, repo := newTestBatcher(t, 1)
	// Deliberately NOT starting the consumer: the single-slot queue fills up
	// and the next Submit must block (natural backpressure, no 429).

	firstEvent := testEvent(0)
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- batcher.Submit(context.Background(), *firstEvent)
	}()

	// Wait until the first event is enqueued (the queue is now full).
	require.Eventually(t, func() bool { return batcher.Submitted() == 1 }, 5*time.Second, time.Millisecond)

	secondEvent := testEvent(1)
	secondResult := make(chan error, 1)
	go func() {
		secondResult <- batcher.Submit(context.Background(), *secondEvent)
	}()

	// The second Submit must be blocked: the queue is full and no consumer
	// is running, so the send cannot complete.
	select {
	case err := <-secondResult:
		t.Fatalf("second Submit returned %v while the queue was full; expected it to block", err)
	case <-time.After(300 * time.Millisecond):
		// still blocked — backpressure working as intended
	}

	// Starting the consumer frees the queue: both events drain and both
	// Submits complete successfully.
	batcher.Start(context.Background())
	require.NoError(t, <-firstResult)
	require.NoError(t, <-secondResult)

	require.Equal(t, 2, persistedCount(t, repo))
}

// ---------------------------------------------------------------------------
// Shutdown drains pending events
// ---------------------------------------------------------------------------

func TestShutdown_DrainsEventsSubmittedBeforeStart(t *testing.T) {
	const eventCount = 10

	// Queue capacity equals the number of events, so every Submit enqueues
	// without blocking; Start is intentionally never called, which means
	// nothing is persisted until Shutdown drains the queue inline.
	batcher, repo := newTestBatcher(t, eventCount)

	results := make(chan error, eventCount)
	for index := 0; index < eventCount; index++ {
		go func(i int) {
			results <- batcher.Submit(context.Background(), *testEvent(i))
		}(index)
	}

	// All events must be enqueued (accepted) before Shutdown.
	require.Eventually(t, func() bool { return batcher.Submitted() == eventCount }, 5*time.Second, time.Millisecond)
	require.Equal(t, int64(0), batcher.Processed(), "nothing should be persisted before Shutdown")

	batcher.Shutdown()

	// Every Submit must have received a nil persistence result.
	for index := 0; index < eventCount; index++ {
		require.NoError(t, <-results)
	}

	// And every accepted event must be in the database — zero losses.
	require.Equal(t, eventCount, persistedCount(t, repo))
	require.Equal(t, int64(eventCount), batcher.Processed())
	require.Equal(t, int64(0), batcher.Failed())
}

func TestShutdown_DrainsEventsStillQueuedWhileConsumerRunning(t *testing.T) {
	const eventCount = 300

	batcher, repo := newTestBatcher(t, 32)
	batcher.Start(context.Background())

	results := make(chan error, eventCount)
	for index := 0; index < eventCount; index++ {
		go func(i int) {
			results <- batcher.Submit(context.Background(), *testEvent(i))
		}(index)
	}

	// Wait until every event has been accepted, then shut down immediately —
	// some events are still queued (the consumer cannot have persisted all
	// of them synchronously). Shutdown must drain whatever is pending.
	require.Eventually(t, func() bool { return batcher.Submitted() == eventCount }, 10*time.Second, time.Millisecond)
	batcher.Shutdown()

	for index := 0; index < eventCount; index++ {
		require.NoError(t, <-results)
	}

	require.Equal(t, eventCount, persistedCount(t, repo))
	require.Equal(t, int64(eventCount), batcher.Processed())
	require.Equal(t, int64(0), batcher.Failed())
}

// ---------------------------------------------------------------------------
// context cancellation before Shutdown
// ---------------------------------------------------------------------------

func TestStartContextCancelled_StillPersistsAcceptedEvents(t *testing.T) {
	const eventCount = 50

	batcher, repo := newTestBatcher(t, 16)
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	batcher.Start(lifecycleCtx)

	results := make(chan error, eventCount)
	for index := 0; index < eventCount; index++ {
		go func(i int) {
			results <- batcher.Submit(context.Background(), *testEvent(i))
		}(index)
	}
	require.Eventually(t, func() bool { return batcher.Submitted() == eventCount }, 10*time.Second, time.Millisecond)

	// Cancel the lifecycle context mid-flight, then shut down: the consumer
	// must switch to the bounded fallback context and still persist every
	// accepted event.
	cancelLifecycle()
	batcher.Shutdown()

	for index := 0; index < eventCount; index++ {
		require.NoError(t, <-results)
	}
	require.Equal(t, eventCount, persistedCount(t, repo))
	require.Equal(t, int64(eventCount), batcher.Processed())
}

// ---------------------------------------------------------------------------
// consumer panic recovery
// ---------------------------------------------------------------------------

func TestConsumerPanic_DoesNotHangShutdown(t *testing.T) {
	batcher, _ := newTestBatcher(t, 10)
	// Inject a panicking persistence via the test seam: the consumer must
	// recover per item, unblock the Submit caller with an error, keep
	// draining, and leave the waitGroup balanced so Shutdown returns.
	batcher.persistFunc = func(ctx context.Context, event domain.Event) error {
		panic("boom: injected persist panic")
	}
	batcher.Start(context.Background())

	event := testEvent(0)
	result := make(chan error, 1)
	go func() {
		result <- batcher.Submit(context.Background(), *event)
	}()

	// The caller of Submit must not wait forever: it receives the recovered
	// panic as an error instead.
	select {
	case err := <-result:
		require.ErrorContains(t, err, "internal panic")
	case <-time.After(5 * time.Second):
		t.Fatal("Submit never returned: the panic was not reported to the caller")
	}

	// Shutdown must complete: the consumer recovered the panic and still
	// called waitGroup.Done() when the channel closed.
	done := make(chan struct{})
	go func() {
		batcher.Shutdown()
		close(done)
	}()
	select {
	case <-done:
		// Shutdown returned — the waitGroup was not left hanging.
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown hung: consumer panic left the waitGroup unbalanced")
	}

	// The recovered panic is counted as a failed persistence...
	assert.Equal(t, int64(1), batcher.Failed())

	// ...and as processed (audit finding N1): the event was dequeued and
	// handled — its outcome was reported back to the caller — so Pending()
	// must not over-estimate the queue depth after a persist panic.
	assert.Equal(t, int64(1), batcher.Processed())
}

// ---------------------------------------------------------------------------
// embedded processes are persisted exactly once (H3)
// ---------------------------------------------------------------------------

func TestSubmit_EventWithProcesses_PersistsOnce(t *testing.T) {
	batcher, repo := newTestBatcher(t, 10)
	batcher.Start(context.Background())

	// An event that DOES carry embedded processes. persistEvent must write
	// them exactly once through InsertEvent: the old code path also called
	// InsertProcesses, which re-inserted the same process rows and violated
	// the PRIMARY KEY constraint on event_processes.id.
	process := domain.NewProcess("", "notify_customer")
	event := domain.NewEvent("purchase_completed", `{"order_id":"order-42"}`, "key-with-processes-1", domain.StatusPending, []domain.Process{*process})

	require.NoError(t, batcher.Submit(context.Background(), *event))

	persisted, err := repo.FetchByID(context.Background(), event.ID)
	require.NoError(t, err)
	require.Len(t, persisted.Processes, 1)
	assert.Equal(t, process.ID, persisted.Processes[0].ID)
	assert.Equal(t, "notify_customer", persisted.Processes[0].ProcessName)
	assert.Equal(t, event.ID, persisted.Processes[0].EventID)

	// Exactly one event row and no persistence failure.
	require.Equal(t, 1, persistedCount(t, repo))
	assert.Equal(t, int64(1), batcher.Processed())
	assert.Equal(t, int64(0), batcher.Failed())
}
