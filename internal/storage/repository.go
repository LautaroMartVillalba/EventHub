package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"eventhub/internal/domain"
)

// Repository implements CRUD and workflow operations for EventHub events and
// processes using an SQLite backend. All write operations are executed within
// database transactions.
type Repository struct {
	db *sql.DB
}

// NewRepository returns a Repository backed by the given *sql.DB.
func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

// ---------------------------------------------------------------------------
// InsertEvent
// ---------------------------------------------------------------------------

// InsertEvent inserts an event together with its processes inside a single
// transaction. If the idempotency key already exists ErrConflict is returned.
func (repository *Repository) InsertEvent(ctx context.Context, event domain.Event) error {
	return repository.withTx(ctx, func(tx *sql.Tx) error {
		// Insert the event row with atomic idempotency check.
		// ON CONFLICT uses the UNIQUE constraint on idempotency_key.
		result, err := tx.ExecContext(ctx,
			`INSERT INTO events (id, type, payload, idempotency_key, status, attempts, next_retry_at, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(idempotency_key) DO NOTHING`,
			event.ID, event.Type, event.Payload, event.IdempotencyKey,
			event.Status, event.Attempts, nullTime(event.NextRetryAt),
			event.CreatedAt.UTC(), event.UpdatedAt.UTC(),
		)
		if err != nil {
			return fmt.Errorf("insert event: %w", err)
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return ErrConflict
		}

		// Insert associated processes.
		for _, process := range event.Processes {
			_, err = tx.ExecContext(ctx,
				`INSERT INTO event_processes (id, event_id, process_name, status, attempts, next_retry_at, error_msg, created_at, updated_at)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				process.ID, process.EventID, process.ProcessName, process.Status, process.Attempts,
				nullTime(process.NextRetryAt), nullString(process.ErrorMsg),
				process.CreatedAt.UTC(), process.UpdatedAt.UTC(),
			)
			if err != nil {
				return fmt.Errorf("insert process %q: %w", process.ID, err)
			}
		}

		return nil
	})
}

// ---------------------------------------------------------------------------
// FetchReadyEvents
// ---------------------------------------------------------------------------

// FetchReadyEvents returns up to limit events whose status is pending or
// partial_failed and that have at least one non-completed process ready for
// retry (next_retry_at is NULL or in the past). Processes are populated for
// every returned event.
func (repository *Repository) FetchReadyEvents(ctx context.Context, limit int) ([]domain.Event, error) {
	rows, err := repository.db.QueryContext(ctx, `
		SELECT DISTINCT e.id, e.type, e.payload, e.idempotency_key,
		                e.status, e.attempts, e.next_retry_at,
		                e.created_at, e.updated_at
		FROM events e
		WHERE e.status IN ('pending', 'partial_failed')
		  AND EXISTS (
		      SELECT 1 FROM event_processes ep
		      WHERE ep.event_id = e.id
		        AND ep.status NOT IN ('completed', 'dead')
		        AND (ep.next_retry_at IS NULL OR ep.next_retry_at <= datetime('now'))
		  )
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch ready events: %w", err)
	}
	defer rows.Close()

	var events []domain.Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan ready event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	// Load processes for each event.
	for i := range events {
		processes, err := repository.loadProcesses(ctx, events[i].ID)
		if err != nil {
			return nil, fmt.Errorf("load processes for event %q: %w", events[i].ID, err)
		}
		events[i].Processes = processes
	}

	if events == nil {
		return []domain.Event{}, nil
	}
	return events, nil
}

// ---------------------------------------------------------------------------
// FetchByStatus
// ---------------------------------------------------------------------------

// FetchByStatus returns all events with the given status, with processes
// populated. Returns an empty slice when there are no matches.
func (repository *Repository) FetchByStatus(ctx context.Context, status domain.EventStatus) ([]domain.Event, error) {
	rows, err := repository.db.QueryContext(ctx, `
		SELECT id, type, payload, idempotency_key,
		       status, attempts, next_retry_at,
		       created_at, updated_at
		FROM events
		WHERE status = ?
	`, status)
	if err != nil {
		return nil, fmt.Errorf("fetch by status %q: %w", status, err)
	}
	defer rows.Close()

	var events []domain.Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	for i := range events {
		processes, err := repository.loadProcesses(ctx, events[i].ID)
		if err != nil {
			return nil, fmt.Errorf("load processes for event %q: %w", events[i].ID, err)
		}
		events[i].Processes = processes
	}

	if events == nil {
		return []domain.Event{}, nil
	}
	return events, nil
}

// ---------------------------------------------------------------------------
// FetchAll
// ---------------------------------------------------------------------------

// FetchAll returns all events regardless of status, with processes populated.
// Returns an empty slice when there are no events.
func (repository *Repository) FetchAll(ctx context.Context) ([]domain.Event, error) {
	rows, err := repository.db.QueryContext(ctx, `
		SELECT id, type, payload, idempotency_key,
		       status, attempts, next_retry_at,
		       created_at, updated_at
		FROM events
	`)
	if err != nil {
		return nil, fmt.Errorf("fetch all events: %w", err)
	}
	defer rows.Close()

	var events []domain.Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	for i := range events {
		processes, err := repository.loadProcesses(ctx, events[i].ID)
		if err != nil {
			return nil, fmt.Errorf("load processes for event %q: %w", events[i].ID, err)
		}
		events[i].Processes = processes
	}

	if events == nil {
		return []domain.Event{}, nil
	}
	return events, nil
}

// ---------------------------------------------------------------------------
// FetchByID
// ---------------------------------------------------------------------------

// FetchByID returns a single event by its ID together with its processes.
// Returns ErrNotFound if the event does not exist.
func (repository *Repository) FetchByID(ctx context.Context, id string) (domain.Event, error) {
	row := repository.db.QueryRowContext(ctx, `
		SELECT id, type, payload, idempotency_key,
		       status, attempts, next_retry_at,
		       created_at, updated_at
		FROM events
		WHERE id = ?
	`, id)

	event, err := scanEvent(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Event{}, fmt.Errorf("event %q: %w", id, ErrNotFound)
		}
		return domain.Event{}, fmt.Errorf("fetch event %q: %w", id, err)
	}

	processes, err := repository.loadProcesses(ctx, event.ID)
	if err != nil {
		return domain.Event{}, fmt.Errorf("load processes for event %q: %w", event.ID, err)
	}
	event.Processes = processes

	return event, nil
}

// ---------------------------------------------------------------------------
// InsertProcesses
// ---------------------------------------------------------------------------

// InsertProcesses inserts one or more processes for the given event inside a
// single transaction.
func (repository *Repository) InsertProcesses(ctx context.Context, eventID string, processes []domain.Process) error {
	return repository.withTx(ctx, func(tx *sql.Tx) error {
		for _, process := range processes {
			_, err := tx.ExecContext(ctx,
				`INSERT INTO event_processes (id, event_id, process_name, status, attempts, next_retry_at, error_msg, created_at, updated_at)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				process.ID, process.EventID, process.ProcessName, process.Status, process.Attempts,
				nullTime(process.NextRetryAt), nullString(process.ErrorMsg),
				process.CreatedAt.UTC(), process.UpdatedAt.UTC(),
			)
			if err != nil {
				return fmt.Errorf("insert process %q: %w", process.ID, err)
			}
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// FetchProcessesForRetry
// ---------------------------------------------------------------------------

// FetchProcessesForRetry returns all non-completed processes for the given
// event, ordered by creation time ascending.
func (repository *Repository) FetchProcessesForRetry(ctx context.Context, eventID string) ([]domain.Process, error) {
	rows, err := repository.db.QueryContext(ctx, `
		SELECT id, event_id, process_name, status, attempts,
		       next_retry_at, error_msg, created_at, updated_at
		FROM event_processes
		WHERE event_id = ? AND status NOT IN ('completed', 'dead')
		ORDER BY created_at ASC
	`, eventID)
	if err != nil {
		return nil, fmt.Errorf("fetch processes for retry: %w", err)
	}
	defer rows.Close()

	var processes []domain.Process
	for rows.Next() {
		process, err := scanProcess(rows)
		if err != nil {
			return nil, fmt.Errorf("scan process: %w", err)
		}
		processes = append(processes, process)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	if processes == nil {
		return []domain.Process{}, nil
	}
	return processes, nil
}

// ---------------------------------------------------------------------------
// UpdateEventStatus
// ---------------------------------------------------------------------------

// UpdateEventStatus updates the status and updated_at timestamp of an event.
func (repository *Repository) UpdateEventStatus(ctx context.Context, eventID string, status domain.EventStatus) error {
	return repository.withTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx,
			`UPDATE events SET status = ?, updated_at = datetime('now') WHERE id = ?`,
			status, eventID,
		)
		if err != nil {
			return fmt.Errorf("update event %q status: %w", eventID, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("update status: %w", err)
		}
		if affected == 0 {
			return fmt.Errorf("event %q: %w", eventID, ErrNotFound)
		}

		return nil
	})
}

// ---------------------------------------------------------------------------
// UpdateProcessStatus
// ---------------------------------------------------------------------------

// UpdateProcessStatus updates the status, attempt count, retry schedule and
// error message of a single process. Pass nil for nextRetryAt to set it NULL.
func (repository *Repository) UpdateProcessStatus(
	ctx context.Context,
	processID string,
	status domain.ProcessStatus,
	attempts int,
	nextRetryAt *time.Time,
	errorMsg string,
) error {
	return repository.withTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx,
			`UPDATE event_processes
			 SET status = ?, attempts = ?, next_retry_at = ?,
			     error_msg = ?, updated_at = datetime('now')
			 WHERE id = ?`,
			status, attempts, nullTime(nextRetryAt),
			nullStringPtr(&errorMsg), processID,
		)
		if err != nil {
			return fmt.Errorf("update process %q status: %w", processID, err)
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return fmt.Errorf("process %q: %w", processID, ErrNotFound)
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// MoveEventToDeadLetter
// ---------------------------------------------------------------------------

// MoveEventToDeadLetter takes a snapshot of the event, stores it in the
// dead_letter table and marks the event as dead. Returns ErrNotFound if the
// event does not exist.
func (repository *Repository) MoveEventToDeadLetter(ctx context.Context, eventID, reason string) error {
	return repository.withTx(ctx, func(tx *sql.Tx) error {
		// Fetch the current event.
		row := tx.QueryRowContext(ctx, `
			SELECT id, type, payload, idempotency_key,
			       status, attempts, next_retry_at,
			       created_at, updated_at
			FROM events
			WHERE id = ?
		`, eventID)

		event, err := scanEvent(row)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("event %q: %w", eventID, ErrNotFound)
			}
			return fmt.Errorf("fetch event %q: %w", eventID, err)
		}

		// Load processes (scanEvent above does NOT join them) so the
		// JSON snapshot includes every process of the event.
		processes, err := repository.loadProcesses(ctx, eventID)
		if err != nil {
			return fmt.Errorf("load processes for snapshot: %w", err)
		}
		event.Processes = processes

		// Build JSON snapshot.
		snapshot, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal event snapshot: %w", err)
		}

		// Insert into dead_letter.
		_, err = tx.ExecContext(ctx,
			`INSERT INTO dead_letter (event_id, reason, snapshot_event) VALUES (?, ?, ?)`,
			eventID, reason, string(snapshot),
		)
		if err != nil {
			return fmt.Errorf("insert dead_letter: %w", err)
		}

		// Mark event as dead.
		_, err = tx.ExecContext(ctx,
			`UPDATE events SET status = 'dead', updated_at = datetime('now') WHERE id = ?`,
			eventID,
		)
		if err != nil {
			return fmt.Errorf("mark event %q as dead: %w", eventID, err)
		}

		return nil
	})
}

// ---------------------------------------------------------------------------
// RequeueEvent
// ---------------------------------------------------------------------------

// RequeueEvent resets an event and all its non-completed processes back to
// pending with zero attempts and no retry schedule. Completed processes are
// left untouched.
func (repository *Repository) RequeueEvent(ctx context.Context, eventID string) error {
	return repository.withTx(ctx, func(tx *sql.Tx) error {
		// Reset non-completed processes.
		_, err := tx.ExecContext(ctx,
			`UPDATE event_processes
			 SET status = 'pending', attempts = 0,
			     next_retry_at = NULL, error_msg = '',
			     updated_at = datetime('now')
			 WHERE event_id = ? AND status NOT IN ('completed', 'dead')`,
			eventID,
		)
		if err != nil {
			return fmt.Errorf("reset processes for event %q: %w", eventID, err)
		}

		// Reset event.
		result, err := tx.ExecContext(ctx,
			`UPDATE events
			 SET status = 'pending', attempts = 0,
			     next_retry_at = NULL,
			     updated_at = datetime('now')
			 WHERE id = ?`,
			eventID,
		)
		if err != nil {
			return fmt.Errorf("requeue event %q: %w", eventID, err)
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return fmt.Errorf("event %q: %w", eventID, ErrNotFound)
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// helpers — transactions
// ---------------------------------------------------------------------------

// withTx runs callback inside a database transaction. If callback returns an error the
// transaction is rolled back; otherwise it is committed.
func (repository *Repository) withTx(ctx context.Context, callback func(*sql.Tx) error) error {
	tx, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	defer func() {
		// Rollback on panic so it doesn't leak the connection.
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	if err := callback(tx); err != nil {
		// Rollback on error; ignore rollback error itself.
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

// ---------------------------------------------------------------------------
// helpers — scan
// ---------------------------------------------------------------------------

// scanner is implemented by *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...interface{}) error
}

// scanEvent scans a row into a domain.Event.
func scanEvent(row scanner) (domain.Event, error) {
	var event domain.Event
	var nextRetryAt sql.NullTime

	err := row.Scan(
		&event.ID, &event.Type, &event.Payload, &event.IdempotencyKey,
		&event.Status, &event.Attempts, &nextRetryAt,
		&event.CreatedAt, &event.UpdatedAt,
	)
	if nextRetryAt.Valid {
		event.NextRetryAt = &nextRetryAt.Time
	}
	return event, err
}

// scanProcess scans a row into a domain.Process.
func scanProcess(row scanner) (domain.Process, error) {
	var process domain.Process
	var nextRetryAt sql.NullTime
	var errorMsg sql.NullString

	err := row.Scan(
		&process.ID, &process.EventID, &process.ProcessName, &process.Status, &process.Attempts,
		&nextRetryAt, &errorMsg,
		&process.CreatedAt, &process.UpdatedAt,
	)
	if nextRetryAt.Valid {
		process.NextRetryAt = &nextRetryAt.Time
	}
	if errorMsg.Valid {
		process.ErrorMsg = errorMsg.String
	}
	return process, err
}

// loadProcesses fetches all processes for the given event ID ordered by
// creation time ascending.
func (repository *Repository) loadProcesses(ctx context.Context, eventID string) ([]domain.Process, error) {
	rows, err := repository.db.QueryContext(ctx, `
		SELECT id, event_id, process_name, status, attempts,
		       next_retry_at, error_msg, created_at, updated_at
		FROM event_processes
		WHERE event_id = ?
		ORDER BY created_at ASC
	`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var processes []domain.Process
	for rows.Next() {
		process, err := scanProcess(rows)
		if err != nil {
			return nil, err
		}
		processes = append(processes, process)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if processes == nil {
		return []domain.Process{}, nil
	}
	return processes, nil
}

// ---------------------------------------------------------------------------
// helpers — SQL null converters
// ---------------------------------------------------------------------------

// nullTime converts a *time.Time to sql.NullTime.
func nullTime(timePtr *time.Time) sql.NullTime {
	if timePtr == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: *timePtr, Valid: true}
}

// nullString converts a string to sql.NullString. An empty string is treated
// as NULL.
func nullString(str string) sql.NullString {
	if str == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: str, Valid: true}
}

// nullStringPtr converts a *string to sql.NullString. A nil pointer or empty
// string is treated as NULL.
func nullStringPtr(strPtr *string) sql.NullString {
	if strPtr == nil || *strPtr == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: *strPtr, Valid: true}
}
