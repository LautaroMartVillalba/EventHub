-- EventHub SQLite schema
-- Tables: events, event_processes, dead_letter

CREATE TABLE IF NOT EXISTS events (
    id               TEXT PRIMARY KEY,                          -- UUID v4
    type             TEXT NOT NULL,                             -- event type (e.g. user_created)
    payload          TEXT NOT NULL DEFAULT '{}',                -- JSON payload
    idempotency_key  TEXT NOT NULL UNIQUE,                      -- client-provided or derived hash
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'processing', 'completed', 'partial_failed', 'dead')),
    attempts         INTEGER NOT NULL DEFAULT 0,                -- legacy; source of truth is event_processes
    next_retry_at    TIMESTAMP NULL,
    created_at       TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at       TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS event_processes (
    id             TEXT PRIMARY KEY,                            -- UUID v4
    event_id       TEXT NOT NULL REFERENCES events(id),         -- FK to events
    process_name   TEXT NOT NULL,                               -- name of the process (e.g. send_welcome_email)
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'dead')),
    attempts       INTEGER NOT NULL DEFAULT 0,
    next_retry_at  TIMESTAMP NULL,
    error_msg      TEXT NULL,
    created_at     TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at     TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS dead_letter (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id       TEXT NOT NULL,                               -- reference to the dead event
    reason         TEXT NOT NULL,                               -- why it was dead-lettered
    snapshot_event TEXT NOT NULL,                               -- JSON snapshot of the event at time of death
    moved_at       TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- Indexes for query performance

CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);
CREATE INDEX IF NOT EXISTS idx_events_next_retry_at ON events(next_retry_at);

CREATE INDEX IF NOT EXISTS idx_event_processes_status ON event_processes(status);
CREATE INDEX IF NOT EXISTS idx_event_processes_next_retry_at ON event_processes(next_retry_at);
CREATE INDEX IF NOT EXISTS idx_event_processes_event_id ON event_processes(event_id);
