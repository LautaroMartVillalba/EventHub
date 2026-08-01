// Package config provides application configuration loaded from environment
// variables with sensible defaults for development.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all application configuration values.
type Config struct {
	// HTTPPort is the port the HTTP server listens on.
	HTTPPort string
	// DBPath is the file path for the SQLite database.
	DBPath string
	// WorkersLevel2Count is the number of level-2 worker goroutines.
	WorkersLevel2Count int
	// MaxAttempts is the maximum number of retry attempts before dead-lettering.
	MaxAttempts int
	// RateBatchSize is the batch size for the rate limiter / ingest batcher.
	RateBatchSize int
	// OrchestratorPoll is the interval between orchestrator poll ticks.
	OrchestratorPoll time.Duration
	// ShutdownTimeout is the maximum time to wait for graceful shutdown.
	ShutdownTimeout time.Duration
	// BackoffSchedule is the list of backoff durations for retries.
	BackoffSchedule []time.Duration
	// LogLevel is the minimum log level (debug, info, warn, error).
	LogLevel string
}

// Load reads configuration from environment variables, falling back to defaults.
func Load() (*Config, error) {
	config := &Config{
		HTTPPort:           envOrDefault("HTTP_PORT", "8080"),
		DBPath:             envOrDefault("DB_PATH", "eventhub.db"),
		WorkersLevel2Count: envOrDefaultInt("WORKERS_LEVEL2_COUNT", 5),
		MaxAttempts:        envOrDefaultInt("MAX_ATTEMPTS", 5),
		RateBatchSize:      envOrDefaultInt("RATE_BATCH_SIZE", 300),
		LogLevel:           envOrDefault("LOG_LEVEL", "info"),
	}

	// Duration fields
	var err error

	config.OrchestratorPoll, err = parseDurationEnv("ORCHESTRATOR_POLL", 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("ORCHESTRATOR_POLL: %w", err)
	}

	config.ShutdownTimeout, err = parseDurationEnv("SHUTDOWN_TIMEOUT", 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("SHUTDOWN_TIMEOUT: %w", err)
	}

	config.BackoffSchedule, err = parseBackoffEnv("BACKOFF_SCHEDULE", []time.Duration{
		2 * time.Second,
		5 * time.Second,
		15 * time.Second,
		30 * time.Second,
		60 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("BACKOFF_SCHEDULE: %w", err)
	}

	return config, nil
}

// envOrDefault reads an environment variable or returns the default value.
func envOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// envOrDefaultInt reads an environment variable as int or returns the default.
func envOrDefaultInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parsedInt, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return parsedInt
}

// parseDurationEnv parses a duration environment variable or returns the default.
func parseDurationEnv(key string, defaultValue time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue, nil
	}
	return time.ParseDuration(value)
}

// parseBackoffEnv parses a comma-separated list of durations from an env var.
func parseBackoffEnv(key string, defaultValue []time.Duration) ([]time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue, nil
	}
	parts := strings.Split(value, ",")
	durations := make([]time.Duration, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		duration, err := time.ParseDuration(part)
		if err != nil {
			return nil, fmt.Errorf("invalid duration %q: %w", part, err)
		}
		durations = append(durations, duration)
	}
	if len(durations) == 0 {
		return defaultValue, nil
	}
	return durations, nil
}
