// Package ratelimit_test contains the throughput and end-to-end tests for the
// ingest batcher. It lives in an external test package so it can import
// eventhub/internal/httpapi (which itself imports ratelimit) without creating
// an import cycle.
package ratelimit_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"eventhub/internal/httpapi"
	"eventhub/internal/ratelimit"
	"eventhub/internal/storage"
)

// TestThroughput_3000ConcurrentPosts pushes 3000 events through the full HTTP
// stack (router + middlewares + batcher + SQLite) and asserts:
//
//   - every request returns 201 (no 4xx/5xx, no losses),
//   - the database ends up with exactly 3000 events,
//   - the batcher counters agree.
//
// It also reports the measured throughput (events/second) and total time via
// t.Logf. Event ordering is NOT asserted: 3000 concurrent handlers race to
// Submit, so the persistence order is not deterministic. The only guarantees
// that are deterministic — and therefore verified — are that every event is
// persisted and none is lost. FIFO ordering per sender is guaranteed by the
// Go channel semantics of the Batcher.
func TestThroughput_3000ConcurrentPosts(t *testing.T) {
	const (
		totalRequests = 3000
		batchSize     = 300
		workerCount   = 100
	)

	// Real database, real repository, real batcher.
	dbPath := filepath.Join(t.TempDir(), "throughput.db")
	db, err := storage.NewDB(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	repo := storage.NewRepository(db)
	logger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	batcher := ratelimit.New(batchSize, repo, logger)
	batcher.Start(context.Background())
	t.Cleanup(batcher.Shutdown)

	// Real HTTP server over the full router (middlewares included).
	apiServer := httpapi.NewServer(repo, logger, batcher)
	httpTestServer := httptest.NewServer(apiServer.Handler())
	t.Cleanup(httpTestServer.Close)

	// A client pool sized for high concurrency: the default transport would
	// reuse at most 2 idle connections per host and throttle the test.
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        workerCount,
			MaxIdleConnsPerHost: workerCount,
			MaxConnsPerHost:     workerCount,
		},
		Timeout: 60 * time.Second,
	}

	// Deterministic, unique payload per request → unique idempotency keys →
	// no 409s.
	payloads := make([]string, totalRequests)
	for index := 0; index < totalRequests; index++ {
		payloads[index] = fmt.Sprintf(
			`{"type":"purchase_completed","payload":{"order_id":"order-%d"}}`, index)
	}

	statuses := make([]int, totalRequests)
	var statusMutex sync.Mutex
	var clientErrors atomic.Int32

	startedAt := time.Now()

	// Worker pool of workerCount goroutines, each draining the job channel.
	jobChannel := make(chan int)
	var workers sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobChannel {
				response, postErr := client.Post(
					httpTestServer.URL+"/events",
					"application/json",
					bytes.NewReader([]byte(payloads[index])),
				)
				if postErr != nil {
					clientErrors.Add(1)
					continue
				}
				status := response.StatusCode
				response.Body.Close()
				statusMutex.Lock()
				statuses[index] = status
				statusMutex.Unlock()
			}
		}()
	}
	for index := 0; index < totalRequests; index++ {
		jobChannel <- index
	}
	close(jobChannel)
	workers.Wait()

	elapsed := time.Since(startedAt)

	// ---- Assertions: zero losses, exact counts ----
	require.Zero(t, clientErrors.Load(), "client-side POST failures")

	nonCreated := 0
	for index := 0; index < totalRequests; index++ {
		if statuses[index] != http.StatusCreated {
			nonCreated++
		}
	}
	require.Zero(t, nonCreated, "requests that did not return 201")

	persistedEvents, err := repo.FetchAll(context.Background())
	require.NoError(t, err)
	require.Len(t, persistedEvents, totalRequests, "database must contain every submitted event")

	require.Equal(t, int64(totalRequests), batcher.Submitted())
	require.Equal(t, int64(totalRequests), batcher.Processed())
	require.Zero(t, batcher.Failed())

	// ---- Report ----
	throughput := float64(totalRequests) / elapsed.Seconds()
	t.Logf("stress: processed=%d total_time=%s throughput=%.0f events/s (batch_size=%d workers=%d)",
		totalRequests, elapsed.Round(time.Millisecond), throughput, batchSize, workerCount)
}
