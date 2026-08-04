// Command server starts the EventHub HTTP API server.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"eventhub/internal/config"
	"eventhub/internal/dispatch"
	"eventhub/internal/httpapi"
	"eventhub/internal/logging"
	"eventhub/internal/ratelimit"
	"eventhub/internal/retry"
	"eventhub/internal/shutdown"
	"eventhub/internal/storage"
	"eventhub/internal/workers"
)

func main() {
	appConfig, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	logger := logging.New(logging.ParseLevel(appConfig.LogLevel))
	logger.Info("EventHub server starting",
		"http_port", appConfig.HTTPPort,
		"db_path", appConfig.DBPath,
	)

	db, err := storage.NewDB(appConfig.DBPath)
	if err != nil {
		logger.Error("failed to open database", "error", err)
		os.Exit(1)
	}
	// Safety-net defer for panic paths only: os.Exit below never runs
	// defers, so the canonical close of the database is the explicit
	// Database step of shutdown.Graceful. If a panic escapes before that
	// step (e.g. during component wiring), this defer still releases the
	// file handle. *sql.DB.Close is idempotent, so the double path is safe.
	defer db.Close()

	repository := storage.NewRepository(db)

	// The ingest batcher decouples POST /events from the database: the
	// handler blocks when the queue is full (natural backpressure, never a
	// 429) and the consumer goroutine persists events asynchronously.
	//
	// The batcher lifecycle context is deliberately independent from the
	// signal context: it stays alive through graceful shutdown so the queue
	// drains cleanly, and is cancelled via defer when main returns. The
	// consumer derives its write context with context.WithoutCancel, so
	// cancelling batcherCtx never aborts in-flight database writes.
	batcherCtx, cancelBatcher := context.WithCancel(context.Background())
	defer cancelBatcher()
	batcher := ratelimit.New(appConfig.RateBatchSize, repository, logger)
	batcher.Start(batcherCtx)

	// Dispatch registry (T12/T17): the level-3 fan-out resolves every event
	// type to its registered processes through this registry. T17 populates
	// the registry with the four real event types; until then it stays empty
	// and FanOut logs a warning for every event type it receives.
	registry := dispatch.NewRegistry()

	// Retry policy for the fan-out (T13): retry.NewCalculator parses the
	// backoff schedule as a CSV string, while the configuration exposes it
	// as a []time.Duration — join the String() forms so the schedule round
	// trips exactly (e.g. "2s,5s,15s,30s,60s").
	scheduleParts := make([]string, len(appConfig.BackoffSchedule))
	for scheduleIndex, duration := range appConfig.BackoffSchedule {
		scheduleParts[scheduleIndex] = duration.String()
	}
	calculator, err := retry.NewCalculator(strings.Join(scheduleParts, ","), appConfig.MaxAttempts)
	if err != nil {
		logger.Error("failed to build retry calculator", "error", err)
		os.Exit(1)
	}

	fanOut := workers.NewFanOut(registry, repository, calculator)

	// Level-2 pipeline (T09/T10/T11): the orchestrator polls the repository
	// for ready events and forwards them on ReadyEvents; the worker pool
	// drains that channel through the fan-out processor. The channel
	// capacity is double the worker count (the Orchestrator.New contract).
	orchestrator := workers.New(
		appConfig.OrchestratorPoll,
		appConfig.WorkersLevel2Count,
		appConfig.WorkersLevel2Count*2,
		repository,
	)
	pool := workers.NewPool(appConfig.WorkersLevel2Count, orchestrator.ReadyEvents, fanOut, repository)

	server := httpapi.NewServer(repository, logger, batcher)

	httpServer := &http.Server{
		Addr:    ":" + appConfig.HTTPPort,
		Handler: server.Handler(),
	}

	// Graceful shutdown on SIGINT/SIGTERM. NotifyContext arranges for
	// shutdownContext to be cancelled when the signal lands, and stop()
	// restores the default handler when main returns.
	//
	// Smoke-test caveat (T18): the shutdown smoke test must run the COMPILED
	// binary — `go build -o bin/server . && ./bin/server &` — never
	// `go run . &`. With `go run`, $! is the PID of the go-run wrapper, not
	// of the server binary: SIGTERM sent to that PID is delivered to the
	// wrapper, which does not reliably forward it to its child, so the real
	// server keeps running, holds the port, and `wait` hangs. The compiled
	// binary receives the signal directly and this graceful path runs.
	shutdownContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("HTTP server listening", "addr", httpServer.Addr)
		serveErr <- httpServer.ListenAndServe()
	}()

	// baseCtx carries the logger through the pipeline: the orchestrator, the
	// pool and the fan-out resolve it from the context via
	// logging.FromContext and never receive it as an argument.
	baseCtx := logging.WithContext(context.Background(), logger)

	orchestrator.Start(baseCtx)
	pool.Start(baseCtx)

	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	case <-shutdownContext.Done():
		logger.Info("shutdown signal received")
	}

	// Drain every lifecycle owner in the mandatory T18 order: HTTP server,
	// orchestrator (BEFORE the pool — Pool.Shutdown closes ReadyEvents and a
	// late poll-loop send on the closed channel would panic), worker pool,
	// ingest batcher, database. The sequence runs explicitly instead of via
	// defer: os.Exit below never executes defers, and the per-step timeout
	// forcing lives in shutdown.Graceful.
	err = shutdown.Graceful(shutdown.Components{
		HTTPServer:   httpServer,
		Orchestrator: orchestrator,
		Pool:         pool,
		Batcher:      batcher,
		Database:     db,
	}, shutdown.Options{
		Timeout: appConfig.ShutdownTimeout,
		Logger:  logger,
	})
	if err != nil {
		logger.Error("graceful shutdown completed with errors", "error", err)
	}

	logger.Info("shutdown complete")
	os.Exit(0)
}
