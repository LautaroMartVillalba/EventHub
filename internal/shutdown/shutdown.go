// Package shutdown implements the graceful shutdown sequence of the EventHub
// server (T18): stop the HTTP server, stop the poll orchestrator and the
// worker pool, drain the ingest batcher and close the database — in that
// order — bounded by a single global timeout budget.
//
// The sequence is exposed as shutdown.Graceful, which talks to every
// lifecycle owner through the minimal seams declared in this file (same
// pattern as the workers package: EventFetcher, EventStatusUpdater,
// FanOutRepository, or the Batcher's persistFunc). The seams let tests inject
// fakes — a fake that blocks forever exercises the force-continue path, an
// error-returning fake exercises the error-logging path, and a recording fake
// can assert the mandatory shutdown order (orchestrator before pool, the T10
// rule).
package shutdown

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// HTTPServer is the seam for the net/http server; *http.Server satisfies it.
// Shutdown must stop accepting new connections and wait for in-flight
// requests, or return when ctx expires.
type HTTPServer interface {
	Shutdown(ctx context.Context) error
}

// OrchestratorStopper is the seam for the poll orchestrator
// (*workers.Orchestrator). Shutdown cancels the poll loop and waits for the
// poll goroutine to exit; it never closes ReadyEvents.
type OrchestratorStopper interface {
	Shutdown()
}

// PoolStopper is the seam for the level-2 worker pool (*workers.Pool).
// Shutdown closes the ReadyEvents channel and waits for the workers to drain
// the remaining events and exit.
type PoolStopper interface {
	Shutdown()
}

// Batcher is the seam for the ingest batcher (*ratelimit.Batcher). Shutdown
// stops accepting events and persists everything still queued; the counter
// getters are logged around the drain.
type Batcher interface {
	Shutdown()
	Pending() int64
	Processed() int64
	Failed() int64
}

// Database is the seam for the SQLite handle (*sql.DB). Close must release
// the file handle; *sql.DB.Close is idempotent, so the shutdown sequence can
// close it explicitly even though main also keeps a defer as a panic safety
// net.
//
// Force-path caveat: when the Database step is forced past its deadline the
// process exits with the pool's workers still draining in the background, so
// events they were mid-processing can be left stranded in status
// "processing". Those events require manual recovery (or the future
// RecoverStaleProcessing feature, outside T18 scope) — the forced close logs
// events_may_be_left_in_processing=true to surface the condition.
type Database interface {
	Close() error
}

// Components bundles every lifecycle owner the server drains during graceful
// shutdown, in shutdown order. A nil field is skipped, which keeps the
// sequence usable in tests that only exercise a subset of the steps.
type Components struct {
	// HTTPServer is stopped first: it stops accepting new connections, so no
	// new events are submitted while the rest of the pipeline drains.
	HTTPServer HTTPServer

	// Orchestrator must be stopped BEFORE Pool — the T10 critical rule:
	// Pool.Shutdown closes ReadyEvents and a poll-loop send on the closed
	// channel would panic. Stopping the orchestrator first guarantees the
	// poll loop has exited (or, on the force path, has at least been
	// cancelled) before the channel is closed.
	//
	// Force-path caveat (accepted trade-off): when the global timeout expires
	// mid-orchestrator the sequence forces past it without waiting for the
	// poll goroutine. Orchestrator.Shutdown invokes its cancel() synchronously
	// before Pool.Shutdown is even launched, so the poll loop gets a
	// scheduling head start to observe the cancellation before the channel is
	// closed. The residual send-on-closed-channel risk is therefore
	// theoretical — a poll send already picked before the cancel lands — and
	// bounded to this degraded mode, where the process exits anyway via
	// os.Exit(0) right after the sequence.
	Orchestrator OrchestratorStopper
	Pool         PoolStopper

	// Batcher must drain after HTTPServer (the handlers submit to it) and
	// before Database closes (persistence must still be available).
	Batcher  Batcher
	Database Database
}

// Options configures the graceful shutdown sequence.
type Options struct {
	// Timeout is the global budget for the whole sequence. Every blocking
	// step waits on the same deadline; when it expires the step is logged as
	// forced (Warning) and the sequence continues with the next one — a slow
	// step never aborts the remaining steps. A value <= 0 means no deadline.
	Timeout time.Duration

	// Logger receives the per-step progress logs. If nil, slog.Default() is
	// used.
	Logger *slog.Logger
}

// Graceful runs the shutdown sequence over the given components, bounded by
// options.Timeout.
//
// Every step is logged at Info level on start and on completion, at Warning
// level when the global budget expires mid-step (the step is then forced:
// its goroutine is left running, because the process is exiting anyway), and
// at Error level when the step reports a failure. Steps are never skipped
// because an earlier one failed or timed out.
//
// The returned error aggregates one wrapped error per failed step
// (errors.Join); it is nil when every step completed cleanly. The caller is
// expected to log the returned error and exit.
func Graceful(components Components, options Options) error {
	logger := options.Logger
	if logger == nil {
		// Deliberately self-contained: the package decouples from
		// eventhub/internal/logging so it can be exercised in isolation by
		// tests with zero package dependencies. Production always injects
		// the JSON logger from main via Options.Logger; this fallback only
		// fires when a test (or a misconfigured caller) passes none.
		logger = slog.Default()
	}

	shutdownCtx, cancel := newShutdownContext(options.Timeout)
	defer cancel()

	var stepErrors []error

	if components.HTTPServer != nil {
		logger.Info("shutdown: stopping HTTP server")
		completed, err := runBounded(shutdownCtx, func() error {
			return components.HTTPServer.Shutdown(shutdownCtx)
		})
		switch {
		case err != nil:
			logger.Error("shutdown: HTTP server failed to stop", "error", err)
			stepErrors = append(stepErrors, fmt.Errorf("shutdown: HTTP server: %w", err))
		case !completed:
			logger.Warn("shutdown: HTTP server shutdown timed out; forcing")
		default:
			logger.Info("shutdown: HTTP server stopped")
		}
	}

	if components.Orchestrator != nil {
		logger.Info("shutdown: stopping orchestrator")
		completed, err := runBounded(shutdownCtx, func() error {
			components.Orchestrator.Shutdown()
			return nil
		})
		switch {
		case err != nil:
			logger.Error("shutdown: orchestrator failed to stop", "error", err)
			stepErrors = append(stepErrors, fmt.Errorf("shutdown: orchestrator: %w", err))
		case !completed:
			logger.Warn("shutdown: orchestrator shutdown timed out; forcing")
		default:
			logger.Info("shutdown: orchestrator stopped")
		}
	}

	if components.Pool != nil {
		logger.Info("shutdown: stopping worker pool")
		completed, err := runBounded(shutdownCtx, func() error {
			components.Pool.Shutdown()
			return nil
		})
		switch {
		case err != nil:
			logger.Error("shutdown: worker pool failed to stop", "error", err)
			stepErrors = append(stepErrors, fmt.Errorf("shutdown: worker pool: %w", err))
		case !completed:
			logger.Warn("shutdown: worker pool shutdown timed out; forcing")
		default:
			logger.Info("shutdown: worker pool stopped")
		}
	}

	if components.Batcher != nil {
		logger.Info("shutdown: draining ingest queue",
			"pending_events", components.Batcher.Pending(),
		)
		completed, err := runBounded(shutdownCtx, func() error {
			components.Batcher.Shutdown()
			return nil
		})
		switch {
		case err != nil:
			logger.Error("shutdown: ingest queue drain failed", "error", err)
			stepErrors = append(stepErrors, fmt.Errorf("shutdown: ingest queue: %w", err))
		case !completed:
			logger.Warn("shutdown: ingest queue drain timed out; forcing")
		default:
			logger.Info("shutdown: ingest queue drained",
				"processed_events", components.Batcher.Processed(),
				"failed_events", components.Batcher.Failed(),
			)
		}
	}

	if components.Database != nil {
		logger.Info("shutdown: closing database")
		completed, err := runBounded(shutdownCtx, func() error {
			return components.Database.Close()
		})
		switch {
		case err != nil:
			logger.Error("shutdown: failed to close database", "error", err)
			stepErrors = append(stepErrors, fmt.Errorf("shutdown: database: %w", err))
		case !completed:
			// Forced close: the pool's workers may still be draining in the
			// background, so events they were mid-processing can be stranded
			// in status "processing" (manual recovery, or the future
			// RecoverStaleProcessing feature, applies to them). Surface the
			// condition explicitly in the log.
			logger.Warn("shutdown: database close timed out; forcing",
				"events_may_be_left_in_processing", true,
			)
		default:
			logger.Info("shutdown: database closed")
		}
	}

	return errors.Join(stepErrors...)
}

// newShutdownContext returns the context that bounds the whole sequence: a
// timeout context when options.Timeout is positive, a plain cancellable
// context otherwise (no deadline, the force path never triggers).
func newShutdownContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(context.Background(), timeout)
	}
	return context.WithCancel(context.Background())
}

// runBounded waits for fn to return, bounded by ctx. It reports whether fn
// completed before ctx expired and, if so, the error fn returned.
//
// On timeout the caller is expected to force the next step: fn keeps running
// in the background — the done channel is buffered with capacity 1, so the
// goroutine never leaks — because the process is exiting anyway and the
// remaining steps must still run.
func runBounded(ctx context.Context, fn func() error) (completed bool, err error) {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return true, err
	case <-ctx.Done():
		return false, nil
	}
}
