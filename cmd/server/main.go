// Command server starts the EventHub HTTP API server.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"eventhub/internal/config"
	"eventhub/internal/httpapi"
	"eventhub/internal/logging"
	"eventhub/internal/ratelimit"
	"eventhub/internal/storage"
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

	server := httpapi.NewServer(repository, logger, batcher)

	httpServer := &http.Server{
		Addr:    ":" + appConfig.HTTPPort,
		Handler: server.Handler(),
	}

	// Graceful shutdown on SIGINT/SIGTERM.
	shutdownContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("HTTP server listening", "addr", httpServer.Addr)
		serveErr <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	case <-shutdownContext.Done():
		logger.Info("shutdown signal received")
	}

	shutdownTimeout := appConfig.ShutdownTimeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
	logger.Info("HTTP server stopped; draining ingest queue",
		"pending_events", batcher.Pending(),
	)

	// Drain the batcher AFTER the HTTP server stopped accepting requests:
	// every event that was accepted (201 already returned, or still being
	// submitted by in-flight handlers) must be persisted before exiting.
	batcher.Shutdown()
	logger.Info("ingest queue drained",
		"processed_events", batcher.Processed(),
		"failed_events", batcher.Failed(),
	)

	logger.Info("server stopped gracefully")
}
