package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"

	"eventhub/internal/logging"
)

// requestIDContextKey is a private, unexported context key used to store the
// per-request ID. Using a custom type prevents collisions with context values
// defined in other packages.
type requestIDContextKey struct{}

// withRequestID returns a new context carrying the given request ID.
func withRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDContextKey{}, requestID)
}

// requestIDFromContext returns the request ID stored in the context, or an
// empty string when none is present.
func requestIDFromContext(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDContextKey{}).(string)
	return requestID
}

// requestID assigns a unique UUID to every request, stores it in the request
// context and reflects it back in the X-Request-ID response header. An
// incoming X-Request-ID header is honoured so callers can correlate requests
// across services.
func (server *Server) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}
		w.Header().Set("X-Request-ID", requestID)
		next.ServeHTTP(w, r.WithContext(withRequestID(r.Context(), requestID)))
	})
}

// recovery converts panics into a logged 500 JSON response instead of letting
// the panic propagate and kill the HTTP connection.
func (server *Server) recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				// With the middleware ordering used by NewServer, withLogger
				// runs after recovery, so the request context captured here has
				// no logger yet; log with the server logger explicitly.
				server.logger.Error("panic recovered",
					"request_id", w.Header().Get("X-Request-ID"),
					"method", r.Method,
					"path", r.URL.Path,
					"panic", fmt.Sprintf("%v", recovered),
					"stack", string(debug.Stack()),
				)
				writeError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// withLogger injects the server logger into the request context so handlers
// and downstream middlewares can retrieve it with logging.FromContext.
func (server *Server) withLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(logging.WithContext(r.Context(), server.logger)))
	})
}

// slogAccessLog logs one structured line per request with method, path,
// status code, duration and request ID. chi/middleware.WrapResponseWriter is
// used only to capture the response status code.
func (server *Server) slogAccessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		wrappedWriter := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(wrappedWriter, r)

		status := wrappedWriter.Status()
		if status == 0 {
			status = http.StatusOK
		}

		logging.FromContext(r.Context()).Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", status,
			"duration_ms", float64(time.Since(startedAt).Microseconds())/1000.0,
			"request_id", requestIDFromContext(r.Context()),
		)
	})
}
