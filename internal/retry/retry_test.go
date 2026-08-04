package retry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertNextRetryAtApprox verifies that nextRetryAt is approximately
// beforeCall + expectedBackoff, within the window between beforeCall and the
// assertion moment (same formula as the production code:
// time.Now().UTC().Add(backoff), same pattern as the fanout test helper).
func assertNextRetryAtApprox(t *testing.T, expectedBackoff time.Duration, actual time.Time, beforeCall time.Time) {
	t.Helper()
	lowerBound := beforeCall.Add(expectedBackoff)
	upperBound := time.Now().UTC().Add(expectedBackoff)
	assert.False(t, actual.Before(lowerBound),
		"nextRetryAt %v should be >= lower bound %v (beforeCall + backoff)", actual, lowerBound)
	assert.False(t, actual.After(upperBound),
		"nextRetryAt %v should be <= upper bound %v (now + backoff)", actual, upperBound)
}

// ---------------------------------------------------------------------------
// NewCalculator
// ---------------------------------------------------------------------------

func TestNewCalculator_ScheduleParsing(t *testing.T) {
	calculator, err := NewCalculator("2s,5s,15s,30s,60s", 5)
	require.NoError(t, err)
	require.NotNil(t, calculator)

	// Each position maps to the parsed duration.
	assert.Equal(t, 2*time.Second, calculator.ScheduleFor(1))
	assert.Equal(t, 5*time.Second, calculator.ScheduleFor(2))
	assert.Equal(t, 15*time.Second, calculator.ScheduleFor(3))
	assert.Equal(t, 30*time.Second, calculator.ScheduleFor(4))
	assert.Equal(t, 60*time.Second, calculator.ScheduleFor(5))
}

func TestNewCalculator_ScheduleParsing_TrimsSpaces(t *testing.T) {
	calculator, err := NewCalculator(" 2s , 5s , 15s ", 5)
	require.NoError(t, err)
	require.NotNil(t, calculator)

	assert.Equal(t, 2*time.Second, calculator.ScheduleFor(1))
	assert.Equal(t, 5*time.Second, calculator.ScheduleFor(2))
	assert.Equal(t, 15*time.Second, calculator.ScheduleFor(3))
}

func TestNewCalculator_InvalidSchedule(t *testing.T) {
	testCases := []struct {
		name     string
		schedule string
	}{
		{name: "invalid unit mid-schedule", schedule: "2s,foo"},
		{name: "not a duration", schedule: "abc"},
		{name: "bare numbers", schedule: "2,5"},
		{name: "missing unit on last part", schedule: "2s,5"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			calculator, err := NewCalculator(testCase.schedule, 5)
			assert.Error(t, err, "schedule %q must be rejected", testCase.schedule)
			assert.Nil(t, calculator)
		})
	}
}

func TestNewCalculator_EmptySchedule(t *testing.T) {
	for _, schedule := range []string{"", "   ", " , "} {
		calculator, err := NewCalculator(schedule, 5)
		require.NoError(t, err, "schedule %q must be valid", schedule)
		require.NotNil(t, calculator)
		assert.Equal(t, 1*time.Second, calculator.ScheduleFor(1), "fallback for schedule %q", schedule)
		assert.Equal(t, 1*time.Second, calculator.ScheduleFor(10), "fallback for schedule %q", schedule)
	}
}

func TestNewCalculator_MaxAttemptsInvalid(t *testing.T) {
	for _, maxAttempts := range []int{0, -1, -100} {
		calculator, err := NewCalculator("2s,5s", maxAttempts)
		assert.Error(t, err, "maxAttempts %d must be rejected", maxAttempts)
		assert.Nil(t, calculator)
	}
}

// ---------------------------------------------------------------------------
// ScheduleFor
// ---------------------------------------------------------------------------

func TestScheduleFor_Positions(t *testing.T) {
	calculator, err := NewCalculator("2s,5s,15s,30s,60s", 5)
	require.NoError(t, err)

	// Attempt 1 → schedule[0], attempt 2 → schedule[1], ..., attempt n → last.
	assert.Equal(t, 2*time.Second, calculator.ScheduleFor(1))
	assert.Equal(t, 5*time.Second, calculator.ScheduleFor(2))
	assert.Equal(t, 15*time.Second, calculator.ScheduleFor(3))
	assert.Equal(t, 30*time.Second, calculator.ScheduleFor(4))
	assert.Equal(t, 60*time.Second, calculator.ScheduleFor(5))

	// Clamping: attempts beyond the schedule's length use the last entry.
	assert.Equal(t, 60*time.Second, calculator.ScheduleFor(6))
	assert.Equal(t, 60*time.Second, calculator.ScheduleFor(100))
}

// ---------------------------------------------------------------------------
// NextRetry
// ---------------------------------------------------------------------------

func TestNextRetry_SchedulePositions(t *testing.T) {
	calculator, err := NewCalculator("2s,5s,15s", 10)
	require.NoError(t, err)

	// Attempt 1 → schedule[0] = 2s.
	beforeCall := time.Now().UTC()
	nextRetryAt, isDead := calculator.NextRetry(1)
	assert.False(t, isDead)
	assertNextRetryAtApprox(t, 2*time.Second, nextRetryAt, beforeCall)

	// Attempt 2 → schedule[1] = 5s.
	beforeCall = time.Now().UTC()
	nextRetryAt, isDead = calculator.NextRetry(2)
	assert.False(t, isDead)
	assertNextRetryAtApprox(t, 5*time.Second, nextRetryAt, beforeCall)

	// Attempt 3 → schedule[2] = 15s (last entry).
	beforeCall = time.Now().UTC()
	nextRetryAt, isDead = calculator.NextRetry(3)
	assert.False(t, isDead)
	assertNextRetryAtApprox(t, 15*time.Second, nextRetryAt, beforeCall)

	// Clamping: attempt 4 > len(schedule) → last entry.
	beforeCall = time.Now().UTC()
	nextRetryAt, isDead = calculator.NextRetry(4)
	assert.False(t, isDead)
	assertNextRetryAtApprox(t, 15*time.Second, nextRetryAt, beforeCall)
}

func TestNextRetry_DeadBoundary(t *testing.T) {
	calculator, err := NewCalculator("2s,5s,15s,30s,60s", 5)
	require.NoError(t, err)

	// attempts == maxAttempts → dead: isDead true, zero nextRetryAt.
	nextRetryAt, isDead := calculator.NextRetry(5)
	assert.True(t, isDead, "attempts == maxAttempts must be dead")
	assert.True(t, nextRetryAt.IsZero(), "dead attempts must return the zero time")

	// attempts == maxAttempts - 1 → still retryable, nextRetryAt > zero and
	// ≈ now + schedule[3] = 30s.
	beforeCall := time.Now().UTC()
	nextRetryAt, isDead = calculator.NextRetry(4)
	assert.False(t, isDead, "attempts == maxAttempts-1 must still be retryable")
	assert.False(t, nextRetryAt.IsZero(), "nextRetryAt must be set for retryable attempts")
	assertNextRetryAtApprox(t, 30*time.Second, nextRetryAt, beforeCall)

	// Attempts beyond maxAttempts are dead too.
	nextRetryAt, isDead = calculator.NextRetry(6)
	assert.True(t, isDead)
	assert.True(t, nextRetryAt.IsZero())
}

func TestNextRetry_UsesUTC(t *testing.T) {
	calculator, err := NewCalculator("2s,5s", 5)
	require.NoError(t, err)

	nextRetryAt, isDead := calculator.NextRetry(1)
	assert.False(t, isDead)
	assert.Equal(t, time.UTC, nextRetryAt.Location(), "nextRetryAt must be expressed in UTC")
}

// ---------------------------------------------------------------------------
// ShouldRetry
// ---------------------------------------------------------------------------

func TestShouldRetry_Boundary(t *testing.T) {
	calculator, err := NewCalculator("2s,5s", 3)
	require.NoError(t, err)

	assert.True(t, calculator.ShouldRetry(1))
	assert.True(t, calculator.ShouldRetry(2))
	assert.False(t, calculator.ShouldRetry(3), "attempts == maxAttempts must not be retried")
	assert.False(t, calculator.ShouldRetry(4))
	assert.False(t, calculator.ShouldRetry(100))
}
