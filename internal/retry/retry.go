// Package retry provides the retry policy used by EventHub's workers: a
// backoff schedule and a maximum attempt count bundled into a single
// Calculator. The package is self-contained (stdlib only) and deliberately
// does not import config or workers, so the policy can be constructed,
// validated and unit-tested in isolation while the workers consume an
// already-parsed instance.
package retry

import (
	"fmt"
	"strings"
	"time"
)

// fallbackBackoff is the retry delay used when the Calculator has no
// schedule (an empty or whitespace-only backoff schedule string): a
// misconfigured fan-out still retries instead of hammering the repository.
// It preserves the behaviour of the fallback the FanOut previously applied
// for empty schedules.
const fallbackBackoff = 1 * time.Second

// Calculator computes the retry policy for failed work items: how long to
// wait before the next retry, and when the item must be dead-lettered.
//
// Attempts are 1-based: the attempt that just failed. Attempt 1 maps to
// schedule[0], attempt 2 to schedule[1], and so on; attempts beyond the
// schedule's length are clamped to its last entry. An item is dead once its
// attempt count reaches maxAttempts. NextRetry anchors the delay to
// time.Now().UTC() so retry timestamps are always expressed in UTC, while
// ScheduleFor exposes the raw indexed delay for callers that only need the
// duration.
type Calculator struct {
	schedule    []time.Duration
	maxAttempts int
}

// NewCalculator builds a Calculator from a comma-separated backoff schedule
// and the maximum number of attempts.
//
// backoffSchedule is parsed like the configuration env var BACKOFF_SCHEDULE
// (default "2s,5s,15s,30s,60s"): each comma-separated part is trimmed and
// parsed with time.ParseDuration. Empty parts are skipped, so an empty or
// whitespace-only schedule yields a valid Calculator whose ScheduleFor and
// NextRetry fall back to a 1-second delay. If any non-empty part is not a
// valid duration, or if maxAttempts < 1, NewCalculator returns an error
// instead of a Calculator.
func NewCalculator(backoffSchedule string, maxAttempts int) (*Calculator, error) {
	if maxAttempts < 1 {
		return nil, fmt.Errorf("retry: maxAttempts must be at least 1, got %d", maxAttempts)
	}
	parts := strings.Split(backoffSchedule, ",")
	schedule := make([]time.Duration, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		duration, err := time.ParseDuration(part)
		if err != nil {
			return nil, fmt.Errorf("retry: invalid backoff duration %q: %w", part, err)
		}
		schedule = append(schedule, duration)
	}
	return &Calculator{schedule: schedule, maxAttempts: maxAttempts}, nil
}

// NextRetry returns the UTC time at which the work item may run again after
// the given failed attempt, plus whether the item is dead (no more retries
// left).
//
// attempts is the 1-based number of the attempt that just failed. If
// attempts >= maxAttempts the item is dead: isDead is true and the returned
// time is the zero time. Otherwise nextRetryAt is time.Now().UTC() plus the
// schedule entry indexed by the attempt (clamped to the last entry), so the
// next retry is always in the future and expressed in UTC regardless of the
// host clock's location.
func (calculator *Calculator) NextRetry(attempts int) (nextRetryAt time.Time, isDead bool) {
	if attempts >= calculator.maxAttempts {
		return time.Time{}, true
	}
	return time.Now().UTC().Add(calculator.ScheduleFor(attempts)), false
}

// ShouldRetry reports whether a failed attempt may still be retried, i.e.
// whether attempts is below maxAttempts. It is the inverse of NextRetry's
// isDead result and the single check workers use to decide between
// scheduling a retry and dead-lettering the item.
func (calculator *Calculator) ShouldRetry(attempts int) bool {
	return attempts < calculator.maxAttempts
}

// ScheduleFor returns the delay before the next retry after the given failed
// attempt, without anchoring it to the current time. Indexing matches
// NextRetry: attempt 1 uses schedule[0], attempt 2 uses schedule[1], and
// attempts beyond the schedule's length use its last entry. An empty
// schedule falls back to fallbackBackoff.
//
// attempts must be at least 1 (workers always pass newAttempts = previous
// attempts + 1). The dead boundary (attempts >= maxAttempts) is deliberately
// ignored: ScheduleFor returns the indexed delay for any attempt so callers
// can read it even while NextRetry would report the item as dead.
func (calculator *Calculator) ScheduleFor(attempts int) time.Duration {
	if len(calculator.schedule) == 0 {
		return fallbackBackoff
	}
	index := attempts - 1
	if index >= len(calculator.schedule) {
		index = len(calculator.schedule) - 1
	}
	return calculator.schedule[index]
}
