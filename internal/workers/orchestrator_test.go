package workers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"eventhub/internal/domain"
	"eventhub/internal/logging"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// silentLogger returns a slog.Logger that discards all output, keeping test
// output clean while still exercising the real logging path.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// silentCtx returns a context.Background() with a silent logger injected so
// that logging.FromContext returns the discard logger instead of
// slog.Default() (which writes to stderr).
func silentCtx() context.Context {
	return logging.WithContext(context.Background(), silentLogger())
}

// fakeFetcher is a deterministic EventFetcher used in every test.
//
// Fields:
//   - events: what FetchReadyEvents returns (unless firstErr is set for call 1)
//   - firstErr: if non-nil, the first call returns this error instead of events
//   - callCount: atomically incremented on every call (for idempotency/stopping tests)
//   - checkCtx: if true, FetchReadyEvents respects context cancellation
type fakeFetcher struct {
	events    []domain.Event
	firstErr  error
	callCount atomic.Int32
	checkCtx  bool
}

func (f *fakeFetcher) FetchReadyEvents(ctx context.Context, limit int) ([]domain.Event, error) {
	f.callCount.Add(1)
	if f.checkCtx {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if f.firstErr != nil && f.callCount.Load() == 1 {
		return nil, f.firstErr
	}
	if f.events == nil {
		return nil, nil
	}
	return f.events, nil
}

// Calls returns the total number of times FetchReadyEvents was invoked.
func (f *fakeFetcher) Calls() int32 {
	return f.callCount.Load()
}

// ---------------------------------------------------------------------------
// 1. TestNew_Panics — each invalid argument panics
// ---------------------------------------------------------------------------

func TestNew_Panics_PollIntervalZero(t *testing.T) {
	assert.Panics(t, func() {
		New(0, 10, 10, &fakeFetcher{events: []domain.Event{{ID: "x"}}})
	})
}

func TestNew_Panics_PollIntervalNegative(t *testing.T) {
	assert.Panics(t, func() {
		New(-1*time.Second, 10, 10, &fakeFetcher{events: []domain.Event{{ID: "x"}}})
	})
}

func TestNew_Panics_BatchSizeZero(t *testing.T) {
	assert.Panics(t, func() {
		New(5*time.Second, 0, 10, &fakeFetcher{events: []domain.Event{{ID: "x"}}})
	})
}

func TestNew_Panics_BatchSizeNegative(t *testing.T) {
	assert.Panics(t, func() {
		New(5*time.Second, -1, 10, &fakeFetcher{events: []domain.Event{{ID: "x"}}})
	})
}

func TestNew_Panics_ChannelCapacityZero(t *testing.T) {
	assert.Panics(t, func() {
		New(5*time.Second, 10, 0, &fakeFetcher{events: []domain.Event{{ID: "x"}}})
	})
}

func TestNew_Panics_ChannelCapacityNegative(t *testing.T) {
	assert.Panics(t, func() {
		New(5*time.Second, 10, -1, &fakeFetcher{events: []domain.Event{{ID: "x"}}})
	})
}

func TestNew_Panics_FetcherNil(t *testing.T) {
	assert.Panics(t, func() {
		New(5*time.Second, 10, 10, nil)
	})
}

func TestNew_Success(t *testing.T) {
	fetcher := &fakeFetcher{events: []domain.Event{{ID: "ev-1"}}}
	orchestrator := New(5*time.Second, 10, 20, fetcher)
	assert.NotNil(t, orchestrator)
	assert.NotNil(t, orchestrator.ReadyEvents)
	assert.Equal(t, 20, cap(orchestrator.ReadyEvents))

	// Verify channel capacity is correct
	dummy := domain.Event{ID: "test"}
	select {
	case orchestrator.ReadyEvents <- dummy:
		// Should succeed: capacity is 20, we only put 1
	default:
		t.Fatal("expected channel to accept 1 event with capacity 20")
	}
}

// ---------------------------------------------------------------------------
// 2. TestStart_ImmediateFirstPoll — Start with N events; first poll delivers
//    them immediately; do NOT wait for any ticker.
// ---------------------------------------------------------------------------

func TestStart_ImmediateFirstPoll_SingleEvent(t *testing.T) {
	fetcher := &fakeFetcher{events: []domain.Event{{ID: "ev-1", Type: "test"}}}
	orchestrator := New(1*time.Hour, 10, 10, fetcher) // long interval to prevent ticks
	ctx := silentCtx()

	orchestrator.Start(ctx)
	defer orchestrator.Shutdown()

	// The first poll runs immediately in a goroutine; drain the channel with
	// a timeout so we don't hang if the poll hasn't completed yet.
	select {
	case event := <-orchestrator.ReadyEvents:
		assert.Equal(t, "ev-1", event.ID)
		assert.Equal(t, "test", event.Type)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for first poll to deliver event to channel")
	}

	// Counters must reflect the exact poll.
	assert.Equal(t, int64(1), orchestrator.Fetched())
	assert.Equal(t, int64(1), orchestrator.Sent())
	assert.Equal(t, int64(0), orchestrator.Dropped())
}

func TestStart_ImmediateFirstPoll_MultipleEvents(t *testing.T) {
	events := []domain.Event{
		{ID: "ev-a"},
		{ID: "ev-b"},
		{ID: "ev-c"},
	}
	fetcher := &fakeFetcher{events: events}
	orchestrator := New(1*time.Hour, 10, 10, fetcher) // long interval
	ctx := silentCtx()

	orchestrator.Start(ctx)
	defer orchestrator.Shutdown()

	// Drain all events with a timeout per event.
	received := make(map[string]bool)
	for i := 0; i < len(events); i++ {
		select {
		case event := <-orchestrator.ReadyEvents:
			received[event.ID] = true
		case <-time.After(1 * time.Second):
			t.Fatalf("timed out after receiving %d/%d events", i, len(events))
		}
	}

	assert.Len(t, received, 3, "all 3 events must arrive with unique IDs")
	assert.True(t, received["ev-a"])
	assert.True(t, received["ev-b"])
	assert.True(t, received["ev-c"])

	assert.Equal(t, int64(3), orchestrator.Fetched())
	assert.Equal(t, int64(3), orchestrator.Sent())
	assert.Equal(t, int64(0), orchestrator.Dropped())
}

// ---------------------------------------------------------------------------
// 3. TestPoll_ForwardsEventsToChannel — exact event content arrives
// ---------------------------------------------------------------------------

func TestPoll_ForwardsEventsToChannel_ExactContent(t *testing.T) {
	now := time.Now().UTC()
	events := []domain.Event{
		{ID: "ev-abc", Type: "order.created", Payload: `{"order":1}`, IdempotencyKey: "ik-1", Status: domain.StatusPending, Attempts: 2, CreatedAt: now, UpdatedAt: now},
		{ID: "ev-xyz", Type: "user.updated", Payload: `{"user":2}`, IdempotencyKey: "ik-2", Status: domain.StatusProcessing, Attempts: 1, CreatedAt: now.Add(-1 * time.Hour), UpdatedAt: now},
	}
	fetcher := &fakeFetcher{events: events}
	orchestrator := New(1*time.Hour, 10, 10, fetcher)
	ctx := silentCtx()

	orchestrator.Start(ctx)
	defer orchestrator.Shutdown()

	var received []domain.Event
	for i := 0; i < len(events); i++ {
		select {
		case event := <-orchestrator.ReadyEvents:
			received = append(received, event)
		case <-time.After(1 * time.Second):
			t.Fatalf("timed out after receiving %d/%d events", i, len(events))
		}
	}

	// Build maps by ID to verify content regardless of order.
	byID := make(map[string]domain.Event)
	for _, ev := range received {
		byID[ev.ID] = ev
	}

	ev1 := byID["ev-abc"]
	assert.Equal(t, "order.created", ev1.Type)
	assert.Equal(t, `{"order":1}`, ev1.Payload)
	assert.Equal(t, "ik-1", ev1.IdempotencyKey)
	assert.Equal(t, domain.StatusPending, ev1.Status)
	assert.Equal(t, 2, ev1.Attempts)

	ev2 := byID["ev-xyz"]
	assert.Equal(t, "user.updated", ev2.Type)
	assert.Equal(t, `{"user":2}`, ev2.Payload)
	assert.Equal(t, "ik-2", ev2.IdempotencyKey)
	assert.Equal(t, domain.StatusProcessing, ev2.Status)
	assert.Equal(t, 1, ev2.Attempts)

	assert.Equal(t, int64(2), orchestrator.Fetched())
	assert.Equal(t, int64(2), orchestrator.Sent())
	assert.Equal(t, int64(0), orchestrator.Dropped())
}

// ---------------------------------------------------------------------------
// 4. TestFetchError_Continues — first poll errors, next tick succeeds
// ---------------------------------------------------------------------------

func TestFetchError_Continues(t *testing.T) {
	dbErr := errors.New("simulated database error")
	fetcher := &fakeFetcher{
		events:   []domain.Event{{ID: "ev-1"}},
		firstErr: dbErr,
	}
	orchestrator := New(5*time.Millisecond, 10, 10, fetcher) // short tick for retry
	ctx := silentCtx()

	orchestrator.Start(ctx)
	defer orchestrator.Shutdown()

	// Wait until the first poll has been attempted (it will error).
	require.Eventually(t, func() bool { return fetcher.Calls() >= 1 }, 500*time.Millisecond, 1*time.Millisecond)

	// After the first (error) poll, Fetched() must be 0 — errors don't count.
	assert.Equal(t, int64(0), orchestrator.Fetched(), "Fetched must be 0 after error poll")
	assert.Equal(t, int64(0), orchestrator.Sent())

	// Wait for the next tick to fire and succeed.
	require.Eventually(t, func() bool { return orchestrator.Fetched() >= 1 }, 1*time.Second, 1*time.Millisecond)

	assert.Equal(t, int64(1), orchestrator.Fetched())
	assert.Equal(t, int64(1), orchestrator.Sent())
	assert.Equal(t, int64(0), orchestrator.Dropped())

	// Verify the event actually arrived.
	select {
	case event := <-orchestrator.ReadyEvents:
		assert.Equal(t, "ev-1", event.ID)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected event on channel after successful poll")
	}
}

// ---------------------------------------------------------------------------
// 5. TestShutdown_CancelsAndWaits — Shutdown returns (doesn't hang) after
//    cancelling the poll loop.
// ---------------------------------------------------------------------------

func TestShutdown_CancelsAndWaits(t *testing.T) {
	fetcher := &fakeFetcher{events: []domain.Event{{ID: "ev-1"}}}
	orchestrator := New(1*time.Hour, 10, 10, fetcher) // long interval
	ctx := silentCtx()

	orchestrator.Start(ctx)

	// Drain the immediate first poll so the goroutine enters the ticker wait.
	select {
	case <-orchestrator.ReadyEvents:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for first poll")
	}

	recordsBeforeShutdown := orchestrator.Fetched()

	// Shutdown must return within a short time (not hang for the poll interval).
	done := make(chan struct{})
	go func() {
		orchestrator.Shutdown()
		close(done)
	}()

	select {
	case <-done:
		// Shutdown returned — the goroutine exited.
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown() hung; expected it to return quickly after cancelling context")
	}

	// After Shutdown, the counter must not grow (no more polls).
	assert.Equal(t, recordsBeforeShutdown, orchestrator.Fetched(), "Fetched must not grow after Shutdown")
}

// ---------------------------------------------------------------------------
// 6. TestShutdown_BeforeStart — Shutdown without Start returns immediately
// ---------------------------------------------------------------------------

func TestShutdown_BeforeStart_NoPanic(t *testing.T) {
	fetcher := &fakeFetcher{events: []domain.Event{{ID: "ev-1"}}}
	orchestrator := New(5*time.Second, 10, 10, fetcher)

	// Shutdown without Start must not panic and must return quickly.
	done := make(chan struct{})
	go func() {
		orchestrator.Shutdown()
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(1 * time.Second):
		t.Fatal("Shutdown() before Start did not return in time")
	}
}

// ---------------------------------------------------------------------------
// 7. TestShutdown_DoesNotCloseReadyEvents — after Shutdown the channel is
//    still open and writable.
// ---------------------------------------------------------------------------

func TestShutdown_DoesNotCloseReadyEvents(t *testing.T) {
	fetcher := &fakeFetcher{events: []domain.Event{{ID: "ev-1"}}}
	orchestrator := New(1*time.Hour, 10, 10, fetcher)
	ctx := silentCtx()

	orchestrator.Start(ctx)

	// Drain first poll.
	select {
	case <-orchestrator.ReadyEvents:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for first event")
	}

	orchestrator.Shutdown()

	// After Shutdown, the channel must still be open — sending must work.
	event := domain.Event{ID: "after-shutdown"}
	select {
	case orchestrator.ReadyEvents <- event:
		// Send succeeded → channel is not closed.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected send to ReadyEvents after Shutdown to succeed, but it blocked or channel appears closed")
	}

	// Also verify we can receive what we just sent.
	select {
	case received := <-orchestrator.ReadyEvents:
		assert.Equal(t, "after-shutdown", received.ID)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected to read back the event we just sent")
	}
}

// ---------------------------------------------------------------------------
// 8. TestShutdown_Idempotent — calling Shutdown twice does not panic or hang
// ---------------------------------------------------------------------------

func TestShutdown_Idempotent(t *testing.T) {
	fetcher := &fakeFetcher{events: []domain.Event{{ID: "ev-1"}}}
	orchestrator := New(1*time.Hour, 10, 10, fetcher)
	ctx := silentCtx()

	orchestrator.Start(ctx)
	// Drain first poll.
	select {
	case <-orchestrator.ReadyEvents:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for first event")
	}

	orchestrator.Shutdown()  // first call — works
	orchestrator.Shutdown()  // second call — must be no-op (no panic, no hang)

	// After the second Shutdown we should still be able to send to the channel.
	event := domain.Event{ID: "after-double-shutdown"}
	select {
	case orchestrator.ReadyEvents <- event:
		// OK
	default:
		t.Fatal("channel should still be open after double Shutdown")
	}
}

// ---------------------------------------------------------------------------
// 9. TestStart_Idempotent — calling Start twice is a no-op; only one poll
//    goroutine runs. Verified by checking FetchReadyEvents call count.
// ---------------------------------------------------------------------------

func TestStart_Idempotent(t *testing.T) {
	fetcher := &fakeFetcher{events: []domain.Event{{ID: "ev-1"}}}
	orchestrator := New(1*time.Hour, 10, 10, fetcher) // long interval
	ctx := silentCtx()

	orchestrator.Start(ctx)
	orchestrator.Start(ctx) // second call — must be no-op
	defer orchestrator.Shutdown()

	// Drain first poll.
	select {
	case event := <-orchestrator.ReadyEvents:
		assert.Equal(t, "ev-1", event.ID)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for first event")
	}

	// Fetched() must be exactly 1 (one poll), not 2.
	assert.Equal(t, int64(1), orchestrator.Fetched(), "second Start must not trigger an extra poll")
	assert.Equal(t, int64(1), orchestrator.Sent())

	// The fake fetcher must have been called exactly once.
	assert.Equal(t, int32(1), fetcher.Calls(), "FetchReadyEvents must be called exactly once (not duplicated by second Start)")
}

// ---------------------------------------------------------------------------
// 10. TestContextCancellation_StopsPolling — cancelling the parent context
//     makes the orchestrator stop polling.
// ---------------------------------------------------------------------------

func TestContextCancellation_StopsPolling(t *testing.T) {
	fetcher := &fakeFetcher{
		events:   []domain.Event{{ID: "ev-1"}},
		checkCtx: true, // respect context cancellation
	}
	orchestrator := New(5*time.Millisecond, 10, 10, fetcher) // short tick

	parentCtx, cancel := context.WithCancel(context.Background())
	ctx := logging.WithContext(parentCtx, silentLogger())

	orchestrator.Start(ctx)

	// Wait for the first immediate poll to complete.
	require.Eventually(t, func() bool { return orchestrator.Fetched() >= 1 }, 500*time.Millisecond, 1*time.Millisecond)

	fetchedBeforeCancel := orchestrator.Fetched()

	// Cancel the parent context.
	cancel()

	// Wait several poll intervals — Fetched must not grow after the poll
	// loop exits (allow for one in-flight poll that may complete).
	time.Sleep(50 * time.Millisecond)

	fetchedAfterCancel := orchestrator.Fetched()

	// After cancellation, no more polls should increment Fetched.
	// Accepting that an in-flight poll might have completed just as we cancelled;
	// but after 5+ intervals any growth beyond that is a failure.
	assert.LessOrEqual(t, fetchedAfterCancel, fetchedBeforeCancel+10,
		"Fetched() should stabilize after context cancellation")

	// After an additional wait, Fetched must be completely stable.
	time.Sleep(50 * time.Millisecond)
	fetchedFinal := orchestrator.Fetched()
	assert.Equal(t, fetchedAfterCancel, fetchedFinal,
		"Fetched() must not grow after context already cancelled and poll loop stopped")

	orchestrator.Shutdown()
}

// ---------------------------------------------------------------------------
// 11. TestChannelFull_DropsAndCounts — small channel capacity drops events
//     and counts them exactly.
// ---------------------------------------------------------------------------

func TestChannelFull_DropsAndCounts(t *testing.T) {
	// 3 events, channel capacity 1, NO consumer → first event fills buffer,
	// subsequent events hit default case and increment dropped.
	events := []domain.Event{
		{ID: "ev-1"},
		{ID: "ev-2"},
		{ID: "ev-3"},
	}
	fetcher := &fakeFetcher{events: events}
	orchestrator := New(1*time.Hour, 10, 1, fetcher) // capacity=1, long interval
	ctx := silentCtx()

	orchestrator.Start(ctx)
	defer orchestrator.Shutdown()

	// Wait for the immediate first poll to complete.
	require.Eventually(t, func() bool { return orchestrator.Fetched() >= 3 }, 500*time.Millisecond, 1*time.Millisecond)

	// Expected: Fetched=3 (all fetched), Sent=1 (only first fits in buffer),
	// Dropped=2 (remaining 2 hit default).
	assert.Equal(t, int64(3), orchestrator.Fetched())
	assert.Equal(t, int64(1), orchestrator.Sent(), "only 1 event fits in capacity-1 channel with no consumer")
	assert.Equal(t, int64(2), orchestrator.Dropped(), "remaining 2 events must be dropped (channel full)")

	// The event in the buffer should be "ev-1" (first one sent).
	select {
	case event := <-orchestrator.ReadyEvents:
		assert.Equal(t, "ev-1", event.ID)
	default:
		t.Fatal("expected ev-1 to be in the channel")
	}
}

// ---------------------------------------------------------------------------
// 12. TestFirstPoll_WithEmptyResult — fetcher returns empty slice, no panic,
//     counters stay at zero.
// ---------------------------------------------------------------------------

func TestFirstPoll_WithEmptyResult(t *testing.T) {
	fetcher := &fakeFetcher{events: []domain.Event{}} // empty slice, not nil
	orchestrator := New(1*time.Hour, 10, 10, fetcher)
	ctx := silentCtx()

	orchestrator.Start(ctx)
	defer orchestrator.Shutdown()

	// Give the immediate first poll time to complete (it's in a goroutine).
	require.Eventually(t, func() bool { return fetcher.Calls() >= 1 }, 500*time.Millisecond, 1*time.Millisecond)

	assert.Equal(t, int64(0), orchestrator.Fetched())
	assert.Equal(t, int64(0), orchestrator.Sent())
	assert.Equal(t, int64(0), orchestrator.Dropped())

	// Channel must be empty.
	select {
	case <-orchestrator.ReadyEvents:
		t.Fatal("expected no events on channel after empty poll")
	default:
		// OK
	}
}

// ---------------------------------------------------------------------------
// 13. TestConcurrentStartShutdown — runs with -race; known data race on
//     `cancel` field is accepted per contract (Start/Shutdown not concurrent).
// ---------------------------------------------------------------------------

func TestConcurrentStartShutdown(t *testing.T) {
	fetcher := &fakeFetcher{events: []domain.Event{{ID: "ev-1"}}}
	orchestrator := New(1*time.Hour, 10, 10, fetcher)
	ctx := silentCtx()

	// Run Start and Shutdown concurrently. The contract says they are not
	// intended to run concurrently; any data race on the `cancel` field is
	// a known and accepted limitation.
	doneStart := make(chan struct{})
	doneShutdown := make(chan struct{})

	go func() {
		orchestrator.Start(ctx)
		close(doneStart)
	}()

	go func() {
		orchestrator.Shutdown()
		close(doneShutdown)
	}()

	// Both must complete without hanging.
	select {
	case <-doneStart:
	case <-time.After(2 * time.Second):
		t.Fatal("Start() hung in concurrent scenario")
	}

	select {
	case <-doneShutdown:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown() hung in concurrent scenario")
	}

	// Final Shutdown to ensure cleanup.
	orchestrator.Shutdown()
}

// ---------------------------------------------------------------------------
// Edge case: verify Shutdown after Start that never had a first poll complete
// (e.g., context was already cancelled when Start was called).
// ---------------------------------------------------------------------------

func TestStart_ThenImmediateShutdown(t *testing.T) {
	fetcher := &fakeFetcher{events: []domain.Event{{ID: "ev-1"}}}
	orchestrator := New(1*time.Hour, 10, 10, fetcher)
	ctx := silentCtx()

	orchestrator.Start(ctx)

	// Immediately shutdown without draining the channel.
	// The run goroutine has polled (or is about to poll). Shutdown cancels
	// the context, which should cause the poll loop to exit.
	orchestrator.Shutdown()

	// Shutdown must have returned. No panic, no hang.
	// We can still receive the event from the first poll if it completed.
	// Drain without blocking (use select).
	select {
	case <-orchestrator.ReadyEvents:
		// Event arrived before shutdown — OK
	default:
		// First poll didn't complete before shutdown — also OK
	}
}

// ---------------------------------------------------------------------------
// Verify that after a poll with empty result, the counters stay at zero
// even after multiple ticks.
// ---------------------------------------------------------------------------

func TestMultipleEmptyPolls_CountersStayZero(t *testing.T) {
	fetcher := &fakeFetcher{events: []domain.Event{}} // always empty
	orchestrator := New(5*time.Millisecond, 10, 10, fetcher)
	ctx := silentCtx()

	orchestrator.Start(ctx)
	defer orchestrator.Shutdown()

	// Wait for at least 3 polls (immediate + 2 ticks)
	require.Eventually(t, func() bool { return fetcher.Calls() >= 3 }, 500*time.Millisecond, 1*time.Millisecond)

	assert.Equal(t, int64(0), orchestrator.Fetched())
	assert.Equal(t, int64(0), orchestrator.Sent())
	assert.Equal(t, int64(0), orchestrator.Dropped())
}

// ---------------------------------------------------------------------------
// Verify nil events slice from fetcher is handled (fetcher returns nil, not empty slice).
// ---------------------------------------------------------------------------

func TestFirstPoll_WithNilResult(t *testing.T) {
	// fakeFetcher with nil events returns nil from FetchReadyEvents
	fetcher := &fakeFetcher{}
	orchestrator := New(1*time.Hour, 10, 10, fetcher)
	ctx := silentCtx()

	orchestrator.Start(ctx)
	defer orchestrator.Shutdown()

	require.Eventually(t, func() bool { return fetcher.Calls() >= 1 }, 500*time.Millisecond, 1*time.Millisecond)

	// len(nil) == 0, so fetched += 0
	assert.Equal(t, int64(0), orchestrator.Fetched())
	assert.Equal(t, int64(0), orchestrator.Sent())
	assert.Equal(t, int64(0), orchestrator.Dropped())
}
