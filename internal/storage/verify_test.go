package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaApplies(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"

	db, err := NewDB(tmpFile)
	require.NoError(t, err, "NewDB should succeed")
	defer db.Close()

	// Verify tables exist
	tables := []string{"events", "event_processes", "dead_letter"}
	for _, tbl := range tables {
		var name string
		err := db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?", tbl).Scan(&name)
		assert.NoError(t, err, "table %q should exist", tbl)
		assert.Equal(t, tbl, name)
	}

	// Verify indexes exist
	indexes := []string{
		"idx_events_status",
		"idx_events_next_retry_at",
		"idx_event_processes_status",
		"idx_event_processes_next_retry_at",
		"idx_event_processes_event_id",
	}
	for _, idx := range indexes {
		var name string
		err := db.QueryRow("SELECT name FROM sqlite_master WHERE type='index' AND name=?", idx).Scan(&name)
		assert.NoError(t, err, "index %q should exist", idx)
		assert.Equal(t, idx, name)
	}

	// Verify CHECK constraint on events.status
	_, err = db.Exec("INSERT INTO events (id, type, payload, idempotency_key, status) VALUES ('a','t','{}','k1','INVALID')")
	assert.Error(t, err, "CHECK constraint on events.status should reject invalid value")

	// Insert valid event
	_, err = db.Exec("INSERT INTO events (id, type, payload, idempotency_key, status) VALUES ('e1','t','{}','k2','pending')")
	require.NoError(t, err, "should insert valid event")

	// Verify CHECK constraint on event_processes.status
	_, err = db.Exec("INSERT INTO event_processes (id, event_id, process_name, status) VALUES ('p1','e1','proc','INVALID')")
	assert.Error(t, err, "CHECK constraint on event_processes.status should reject invalid value")

	// Verify idempotency_key UNIQUE constraint
	_, err = db.Exec("INSERT INTO events (id, type, payload, idempotency_key, status) VALUES ('e2','t','{}','k2','pending')")
	assert.Error(t, err, "UNIQUE constraint on idempotency_key should reject duplicate")

	// Verify FK constraint (need to enable FK enforcement in SQLite)
	_, _ = db.Exec("PRAGMA foreign_keys = ON")
	_, err = db.Exec("INSERT INTO event_processes (id, event_id, process_name, status) VALUES ('p2','nonexistent','proc','pending')")
	assert.Error(t, err, "FK constraint on event_processes.event_id should reject nonexistent event")

	// Cleanup
	os.Remove(tmpFile)
}
