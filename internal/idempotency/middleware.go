package idempotency

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
)

// maxBodySize is the maximum number of bytes the middleware will read from a
// request body when scanning for idempotency key fields. Bodies larger than
// this trigger an HTTP 413 response.
const maxBodySize int64 = 1 << 20 // 1 MiB

// bodyFields is the subset of a request body that matters for idempotency
// key resolution.
type bodyFields struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key"`
}

// Middleware returns an HTTP handler that attempts to determine an idempotency
// key for every request using the following priority:
//
//  1. The Idempotency-Key HTTP header.
//  2. The "idempotency_key" JSON field in the request body.
//  3. A deterministic key generated via GenerateKey(type, payload) from the
//     JSON body fields "type" and "payload".
//
// The key is injected into the request context via WithContext so downstream
// handlers can retrieve it with FromContext. The request body is buffered and
// restored so it remains readable by subsequent handlers.
//
// If no key can be determined (e.g. invalid JSON and no header), the request
// still proceeds without a key; it is up to the handler to decide whether to
// reject the request.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var key string
		var determined bool

		// Priority 1: HTTP header (fast path — avoids reading the body).
		if h := r.Header.Get("Idempotency-Key"); h != "" {
			key = h
			determined = true
		}

		// Priority 2 & 3: JSON body fields (only reached when header was empty).
		if !determined {
			// Read at most maxBodySize+1 bytes so we can detect truncation
			// without misinterpreting an exact-sized body as oversized.
			limitedBody := io.LimitReader(r.Body, maxBodySize+1).(*io.LimitedReader)
			bodyBytes, err := io.ReadAll(limitedBody)
			_ = r.Body.Close() // Safe to ignore — we already consumed all bytes from the body.

			if int64(len(bodyBytes)) > maxBodySize {
				slog.Warn("idempotency: request body too large",
					"limit", maxBodySize,
					"method", r.Method,
					"path", r.URL.Path,
				)
				http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
				return
			}

			if err == nil && len(bodyBytes) > 0 {
				var body bodyFields
				if jsonErr := json.Unmarshal(bodyBytes, &body); jsonErr == nil {
					// Priority 2: explicit idempotency_key in body.
					if body.IdempotencyKey != "" {
						key = body.IdempotencyKey
						determined = true
					}

					// Priority 3: deterministic generation.
					if !determined && body.Type != "" {
						key = GenerateKey(body.Type, string(body.Payload))
						determined = true
					}
				} else {
					slog.Debug("idempotency: failed to parse request body as JSON",
						"error", jsonErr,
						"method", r.Method,
						"path", r.URL.Path,
					)
				}
			}

			// Restore body so downstream handlers can read it.
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		}

		if determined {
			r = r.WithContext(WithContext(r.Context(), key))
		}

		next.ServeHTTP(w, r)
	})
}
