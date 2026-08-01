package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadDefaults(t *testing.T) {
	// Clear env vars to test defaults
	envVars := []string{
		"HTTP_PORT", "DB_PATH", "WORKERS_LEVEL2_COUNT", "MAX_ATTEMPTS",
		"RATE_BATCH_SIZE", "ORCHESTRATOR_POLL", "SHUTDOWN_TIMEOUT",
		"BACKOFF_SCHEDULE", "LOG_LEVEL",
	}
	for _, key := range envVars {
		os.Unsetenv(key)
	}

	config, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "8080", config.HTTPPort)
	assert.Equal(t, "eventhub.db", config.DBPath)
	assert.Equal(t, 5, config.WorkersLevel2Count)
	assert.Equal(t, 5, config.MaxAttempts)
	assert.Equal(t, 300, config.RateBatchSize)
	assert.Equal(t, 5*time.Second, config.OrchestratorPoll)
	assert.Equal(t, 30*time.Second, config.ShutdownTimeout)
	assert.Equal(t, "info", config.LogLevel)
	assert.Equal(t, []time.Duration{
		2 * time.Second,
		5 * time.Second,
		15 * time.Second,
		30 * time.Second,
		60 * time.Second,
	}, config.BackoffSchedule)
}

func TestLoadFromEnv(t *testing.T) {
	os.Setenv("HTTP_PORT", "9090")
	os.Setenv("DB_PATH", "/tmp/test.db")
	os.Setenv("WORKERS_LEVEL2_COUNT", "10")
	os.Setenv("MAX_ATTEMPTS", "3")
	os.Setenv("RATE_BATCH_SIZE", "500")
	os.Setenv("ORCHESTRATOR_POLL", "10s")
	os.Setenv("SHUTDOWN_TIMEOUT", "60s")
	os.Setenv("BACKOFF_SCHEDULE", "1s,3s,10s")
	os.Setenv("LOG_LEVEL", "debug")
	defer func() {
		for _, key := range []string{
			"HTTP_PORT", "DB_PATH", "WORKERS_LEVEL2_COUNT", "MAX_ATTEMPTS",
			"RATE_BATCH_SIZE", "ORCHESTRATOR_POLL", "SHUTDOWN_TIMEOUT",
			"BACKOFF_SCHEDULE", "LOG_LEVEL",
		} {
			os.Unsetenv(key)
		}
	}()

	config, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "9090", config.HTTPPort)
	assert.Equal(t, "/tmp/test.db", config.DBPath)
	assert.Equal(t, 10, config.WorkersLevel2Count)
	assert.Equal(t, 3, config.MaxAttempts)
	assert.Equal(t, 500, config.RateBatchSize)
	assert.Equal(t, 10*time.Second, config.OrchestratorPoll)
	assert.Equal(t, 60*time.Second, config.ShutdownTimeout)
	assert.Equal(t, "debug", config.LogLevel)
	assert.Equal(t, []time.Duration{1 * time.Second, 3 * time.Second, 10 * time.Second}, config.BackoffSchedule)
}

func TestLoadInvalidDuration(t *testing.T) {
	os.Setenv("ORCHESTRATOR_POLL", "not-a-duration")
	defer os.Unsetenv("ORCHESTRATOR_POLL")

	_, err := Load()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ORCHESTRATOR_POLL")
}
