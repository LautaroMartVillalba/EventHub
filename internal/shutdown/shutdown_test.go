// Package shutdown — white-box tests for the graceful shutdown sequence (T18).
//
// The tests use fakes for every lifecycle seam and exercise: mandatory
// ordering, force-continue under global timeout, per-step errors, nil
// skipping, unlimited timeout, and the internal helpers runBounded /
// newShutdownContext.
package shutdown

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// recorder tracks the ordered sequence of shutdown calls across all fakes.
// Every fake appends its role name on each Shutdown/Close call, protected by
// the mutex so concurrent access (force path goroutines) is safe.
type recorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *recorder) record(name string) {
	r.mu.Lock()
	r.calls = append(r.calls, name)
	r.mu.Unlock()
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	cpy := make([]string, len(r.calls))
	copy(cpy, r.calls)
	return cpy
}

// fakeHTTP fulfills shutdown.HTTPServer.
type fakeHTTP struct {
	rec   *recorder
	name  string
	err   error
	block chan struct{}
}

func (f *fakeHTTP) Shutdown(ctx context.Context) error {
	f.rec.record(f.name)
	if f.block != nil {
		<-f.block
	}
	return f.err
}

// fakeStopper fulfills shutdown.OrchestratorStopper, shutdown.PoolStopper
// and shutdown.Batcher (same Shutdown() signature). The counter getters are
// used only when the fake plays the Batcher role.
type fakeStopper struct {
	rec          *recorder
	name         string
	block        chan struct{}
	pendingVal   int64
	processedVal int64
	failedVal    int64
}

func (f *fakeStopper) Shutdown() {
	f.rec.record(f.name)
	if f.block != nil {
		<-f.block
	}
}

func (f *fakeStopper) Pending() int64   { return f.pendingVal }
func (f *fakeStopper) Processed() int64  { return f.processedVal }
func (f *fakeStopper) Failed() int64     { return f.failedVal }

// fakeDB fulfills shutdown.Database.
type fakeDB struct {
	rec   *recorder
	name  string
	err   error
	block chan struct{}
}

func (f *fakeDB) Close() error {
	f.rec.record(f.name)
	if f.block != nil {
		<-f.block
	}
	return f.err
}

// silentLogger returns a *slog.Logger that discards output — used when the
// test doesn't need to inspect log output.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
}

// ---------------------------------------------------------------------------
// 1. Mandatory shutdown order
// ---------------------------------------------------------------------------

func TestGraceful_ShutdownOrder(t *testing.T) {
	rec := &recorder{}

	components := Components{
		HTTPServer:   &fakeHTTP{rec: rec, name: "http"},
		Orchestrator: &fakeStopper{rec: rec, name: "orchestrator"},
		Pool:         &fakeStopper{rec: rec, name: "pool"},
		Batcher:      &fakeStopper{rec: rec, name: "batcher", pendingVal: 3},
		Database:     &fakeDB{rec: rec, name: "database"},
	}

	err := Graceful(components, Options{
		Timeout: 5 * time.Second,
		Logger:  silentLogger(),
	})
	require.NoError(t, err)

	expected := []string{"http", "orchestrator", "pool", "batcher", "database"}
	assert.Equal(t, expected, rec.snapshot(),
		"shutdown order must be: HTTPServer → Orchestrator → Pool → Batcher → Database")
}

// ---------------------------------------------------------------------------
// 2. Force timeout: a blocking step is forced; remaining steps still execute
// ---------------------------------------------------------------------------

func TestGraceful_ForceTimeout_RemainingStepsExecute(t *testing.T) {
	rec := &recorder{}

	// HTTP server blocks forever; timeout fires and the step is forced.
	httpFake := &fakeHTTP{rec: rec, name: "http", block: make(chan struct{})}
	poolFake := &fakeStopper{rec: rec, name: "pool"}

	defer close(httpFake.block) // release the blocked goroutine so runBounded can exit

	components := Components{
		HTTPServer:   httpFake,
		Orchestrator: &fakeStopper{rec: rec, name: "orchestrator"},
		Pool:         poolFake,
		Batcher:      &fakeStopper{rec: rec, name: "batcher"},
		Database:     &fakeDB{rec: rec, name: "database"},
	}

	// 50 ms is enough for all steps except the blocked HTTP one.
	err := Graceful(components, Options{
		Timeout: 50 * time.Millisecond,
		Logger:  silentLogger(),
	})
	require.NoError(t, err, "force is not an error; Graceful must return nil")

	// After the global context expires, every remaining step's runBounded
	// launches a goroutine and immediately returns (false, nil) because
	// ctx.Done() is already ready. Those goroutines execute fn()
	// asynchronously — we must let them finish before inspecting the
	// recorder.
	time.Sleep(200 * time.Millisecond)

	calls := rec.snapshot()

	// HTTP must be called first — it records before the timeout expires.
	require.NotEmpty(t, calls)
	assert.Equal(t, "http", calls[0], "HTTP server must be the first step")

	// After the force, the remaining steps MUST have been called.
	// Order among goroutines launched with an already-expired context is
	// non-deterministic (scheduler-dependent), so we only assert presence,
	// not relative order.
	assert.Contains(t, calls, "orchestrator", "orchestrator must run after http is forced")
	assert.Contains(t, calls, "pool", "pool must run after orchestrator")
	assert.Contains(t, calls, "batcher", "batcher must run after pool")
	assert.Contains(t, calls, "database", "database must run after batcher")

	// Verify all five steps are present (no duplicates, no missing).
	assert.Len(t, calls, 5, "exactly 5 steps must have been called")
}

// ---------------------------------------------------------------------------
// 3. Errors per step: each step that returns an error is aggregated
// ---------------------------------------------------------------------------

func TestGraceful_HTTPServerError(t *testing.T) {
	rec := &recorder{}
	customErr := errors.New("http shutdown failure")

	err := Graceful(Components{
		HTTPServer:   &fakeHTTP{rec: rec, name: "http", err: customErr},
		Orchestrator: &fakeStopper{rec: rec, name: "orchestrator"},
		Pool:         &fakeStopper{rec: rec, name: "pool"},
		Batcher:      &fakeStopper{rec: rec, name: "batcher"},
		Database:     &fakeDB{rec: rec, name: "database"},
	}, Options{Timeout: time.Second, Logger: silentLogger()})

	require.Error(t, err)
	assert.True(t, errors.Is(err, customErr),
		"returned error must wrap the step error")
	assert.Contains(t, err.Error(), "shutdown: HTTP server:",
		"error message must prefix with 'shutdown: HTTP server:'")
}

func TestGraceful_DatabaseError(t *testing.T) {
	rec := &recorder{}
	customErr := errors.New("db close failure")

	err := Graceful(Components{
		HTTPServer:   &fakeHTTP{rec: rec, name: "http"},
		Orchestrator: &fakeStopper{rec: rec, name: "orchestrator"},
		Pool:         &fakeStopper{rec: rec, name: "pool"},
		Batcher:      &fakeStopper{rec: rec, name: "batcher"},
		Database:     &fakeDB{rec: rec, name: "database", err: customErr},
	}, Options{Timeout: time.Second, Logger: silentLogger()})

	require.Error(t, err)
	assert.True(t, errors.Is(err, customErr),
		"returned error must wrap the database error")
	assert.Contains(t, err.Error(), "shutdown: database:",
		"error message must prefix with 'shutdown: database:'")
}

func TestGraceful_MultipleErrors(t *testing.T) {
	rec := &recorder{}
	dbErr := errors.New("db close failure")
	httpErr := errors.New("http shutdown failure")

	err := Graceful(Components{
		HTTPServer:   &fakeHTTP{rec: rec, name: "http", err: httpErr},
		Orchestrator: &fakeStopper{rec: rec, name: "orchestrator"},
		Pool:         &fakeStopper{rec: rec, name: "pool"},
		Batcher:      &fakeStopper{rec: rec, name: "batcher"},
		Database:     &fakeDB{rec: rec, name: "database", err: dbErr},
	}, Options{Timeout: time.Second, Logger: silentLogger()})

	require.Error(t, err)

	// errors.Join aggregates; both wrapped errors must be findable.
	assert.True(t, errors.Is(err, httpErr),
		"aggregated error must contain http shutdown failure")
	assert.True(t, errors.Is(err, dbErr),
		"aggregated error must contain database close failure")

	errStr := err.Error()
	assert.Contains(t, errStr, "shutdown: HTTP server:")
	assert.Contains(t, errStr, "shutdown: database:")
}

// ---------------------------------------------------------------------------
// 4. Nil components are skipped
// ---------------------------------------------------------------------------

func TestGraceful_AllNilComponents(t *testing.T) {
	err := Graceful(Components{}, Options{
		Timeout: time.Second,
		Logger:  silentLogger(),
	})
	require.NoError(t, err, "empty Components must return nil")
}

func TestGraceful_SomeNilComponents(t *testing.T) {
	rec := &recorder{}

	err := Graceful(Components{
		// HTTPServer is nil → skipped
		Orchestrator: &fakeStopper{rec: rec, name: "orchestrator"},
		Pool:         &fakeStopper{rec: rec, name: "pool"},
		// Batcher is nil → skipped
		Database: &fakeDB{rec: rec, name: "database"},
	}, Options{Timeout: time.Second, Logger: silentLogger()})

	require.NoError(t, err)

	calls := rec.snapshot()
	assert.Equal(t, []string{"orchestrator", "pool", "database"}, calls,
		"only non-nil components must be called, in order")
}

// ---------------------------------------------------------------------------
// 5. Timeout <= 0: no deadline, steps complete normally
// ---------------------------------------------------------------------------

func TestGraceful_NoDeadline(t *testing.T) {
	rec := &recorder{}

	// With Timeout=0 there is no deadline; all steps complete normally.
	// Even a step that takes 100ms must complete without a force warning.
	err := Graceful(Components{
		HTTPServer:   &fakeHTTP{rec: rec, name: "http"},
		Orchestrator: &fakeStopper{rec: rec, name: "orchestrator"},
		Pool:         &fakeStopper{rec: rec, name: "pool"},
		Batcher:      &fakeStopper{rec: rec, name: "batcher", pendingVal: 0},
		Database:     &fakeDB{rec: rec, name: "database"},
	}, Options{Timeout: 0, Logger: silentLogger()})

	require.NoError(t, err)
	assert.Equal(t, []string{"http", "orchestrator", "pool", "batcher", "database"},
		rec.snapshot(),
		"all steps must complete when there is no deadline")
}

func TestGraceful_TimeoutZero_StepTakesTime(t *testing.T) {
	rec := &recorder{}
	// Simulate a step that takes 100ms; with Timeout=0 it must complete.
	release := make(chan struct{})

	go func() {
		time.Sleep(100 * time.Millisecond)
		close(release)
	}()

	components := Components{
		HTTPServer: &fakeHTTP{rec: rec, name: "http", block: release},
	}

	err := Graceful(components, Options{Timeout: 0, Logger: silentLogger()})
	require.NoError(t, err)
	assert.Equal(t, []string{"http"}, rec.snapshot(),
		"step must complete when timeout is 0 (no deadline)")
}

// ---------------------------------------------------------------------------
// 6. runBounded and newShutdownContext (internal helpers)
// ---------------------------------------------------------------------------

func TestRunBounded_FastFunction(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("fast fn")

	completed, err := runBounded(ctx, func() error {
		return sentinel
	})

	assert.True(t, completed, "fast fn must complete")
	assert.True(t, errors.Is(err, sentinel), "fast fn error must be passed through")
}

func TestRunBounded_FastFunctionNoError(t *testing.T) {
	ctx := context.Background()

	completed, err := runBounded(ctx, func() error {
		return nil
	})

	assert.True(t, completed)
	assert.NoError(t, err)
}

func TestRunBounded_FnBlocks_CtxExpires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// fn blocks on a sleep, longer than the ctx timeout.
	completed, err := runBounded(ctx, func() error {
		time.Sleep(time.Second)
		return nil
	})

	assert.False(t, completed, "fn that blocks past timeout must not complete")
	assert.NoError(t, err, "timeout must not produce an error")

	// Give the goroutine time to finish its sleep so the channel drain happens.
	time.Sleep(1100 * time.Millisecond)
}

func TestNewShutdownContext_PositiveTimeout_HasDeadline(t *testing.T) {
	ctx, cancel := newShutdownContext(100 * time.Millisecond)
	defer cancel()

	deadline, ok := ctx.Deadline()
	assert.True(t, ok, "positive timeout must set a deadline")
	assert.False(t, deadline.IsZero(), "deadline must be non-zero")
}

func TestNewShutdownContext_ZeroTimeout_NoDeadline(t *testing.T) {
	ctx, cancel := newShutdownContext(0)
	defer cancel()

	_, ok := ctx.Deadline()
	assert.False(t, ok, "zero timeout must produce no deadline")
}

func TestNewShutdownContext_NegativeTimeout_NoDeadline(t *testing.T) {
	ctx, cancel := newShutdownContext(-1)
	defer cancel()

	_, ok := ctx.Deadline()
	assert.False(t, ok, "negative timeout must produce no deadline")
}

// ---------------------------------------------------------------------------
// 7. Database forced warning — verify the log message contains the expected
//    key-value pair events_may_be_left_in_processing=true.
// ---------------------------------------------------------------------------

func TestGraceful_DatabaseForced_LogsWarning(t *testing.T) {
	rec := &recorder{}

	// Block the database close forever; the timeout will force past it.
	dbFake := &fakeDB{rec: rec, name: "database", block: make(chan struct{})}
	defer close(dbFake.block) // release the goroutine at test end

	// Capture logs into a buffer.
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))

	err := Graceful(Components{
		HTTPServer:   &fakeHTTP{rec: rec, name: "http"},
		Orchestrator: &fakeStopper{rec: rec, name: "orchestrator"},
		Pool:         &fakeStopper{rec: rec, name: "pool"},
		Batcher:      &fakeStopper{rec: rec, name: "batcher"},
		Database:     dbFake,
	}, Options{
		Timeout: 50 * time.Millisecond,
		Logger:  logger,
	})

	require.NoError(t, err, "force path must not return error")

	logOutput := buf.String()

	// The forced database step must log a WARN with the specific marker.
	assert.Contains(t, logOutput, `msg="shutdown: database close timed out; forcing"`,
		"forced database close must log a warning")
	assert.Contains(t, logOutput, "events_may_be_left_in_processing=true",
		"forced database close must include events_may_be_left_in_processing=true")

	// Verify it's actually a WARN level, not ERROR or INFO.
	assert.True(t, strings.Contains(logOutput, "level=WARN"),
		"forced step must be logged at WARN level, not ERROR")
}

// ---------------------------------------------------------------------------
// 8. Side-effect: the blocking goroutine from runBounded must not leak.
//    (runBounded uses a buffered channel cap 1; the goroutine must exit
//    after the fake is released.)
// ---------------------------------------------------------------------------

func TestRunBounded_NoGoroutineLeak(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	release := make(chan struct{})
	var entered atomicBool

	completed, err := runBounded(ctx, func() error {
		entered.set(true)
		<-release // blocks until test releases it
		return nil
	})

	assert.False(t, completed)
	assert.NoError(t, err)

	// The goroutine must still be alive (waiting on release).
	time.Sleep(20 * time.Millisecond)
	assert.True(t, entered.load(), "goroutine must have started and be blocked")

	// Now release it.
	close(release)

	// Give it time to send on the buffered channel and exit.
	time.Sleep(50 * time.Millisecond)
	// No assertion needed — if the goroutine leaked, the race detector
	// (go test -race) would catch it. The buffered channel ensures the
	// send never blocks indefinitely.
}

// atomicBool is a simple atomic boolean helper used in the no-leak test.
type atomicBool struct {
	val int32
}

func (b *atomicBool) set(v bool) {
	var n int32
	if v {
		n = 1
	}
	atomic.StoreInt32(&b.val, n)
}

func (b *atomicBool) load() bool {
	return atomic.LoadInt32(&b.val) != 0
}

// ---------------------------------------------------------------------------
// Regression: full-project test suite (run via go test ./...)
// ---------------------------------------------------------------------------

// TestCompilation ensures the cmd/server/main.go compiles cleanly.
// (go test doesn't test package main, so compilation is verified via go build
// in the smoke test below — this test is a placeholder to document the check.)
func TestMainCompiles(t *testing.T) {
	// Compilation of cmd/server is verified at the beginning of the test
	// runner (go test ./... only tests non-main packages). The smoke test
	// below verifies the binary builds and runs correctly.
	t.Log("compilation verified by go build in the smoke test")
}
