package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"eventhub/internal/idempotency"
	"eventhub/internal/ratelimit"
	"eventhub/internal/storage"
)

// Server wires the HTTP router, middlewares and handlers for the EventHub API.
type Server struct {
	repository *storage.Repository
	batcher    *ratelimit.Batcher
	logger     *slog.Logger
	handler    http.Handler
}

// NewServer builds the HTTP API server backed by the given repository, ingest
// batcher and logger. The configured router is exposed through Handler so the
// caller can serve it with any net/http server.
//
// POST /events enqueues events through the batcher instead of writing to the
// repository directly: the batcher applies natural backpressure (the handler
// blocks when the queue is full) and persists events asynchronously.
//
// Middleware order (outermost first): recovery, requestID, withLogger,
// slogAccessLog. The idempotency middleware is applied only to POST /events.
func NewServer(repository *storage.Repository, logger *slog.Logger, batcher *ratelimit.Batcher) *Server {
	server := &Server{
		repository: repository,
		batcher:    batcher,
		logger:     logger,
	}

	router := chi.NewRouter()
	router.Use(
		server.recovery,
		server.requestID,
		server.withLogger,
		server.slogAccessLog,
	)

	router.With(idempotency.Middleware).Post("/events", server.createEvent)
	router.Get("/events", server.listEvents)
	router.Get("/events/{eventID}", server.getEvent)
	router.Post("/admin/events/{eventID}/requeue", server.requeueEvent)

	server.handler = router
	return server
}

// Handler returns the configured http.Handler for serving the API.
func (server *Server) Handler() http.Handler {
	return server.handler
}
