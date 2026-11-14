package store

import (
	"context"
	"database/sql"
	"time"
)

// outboxSchema holds the alerts that could not be delivered when they were
// raised — most often because the outage that raised them also took the
// internet connection with it — so that they go out once the channel works
// again rather than never. One row per alert per destination ("email", or
// "channel:<id>" for a Settings › Alerts channel). Rows are deleted when they
// are delivered or given up on (after a day), so the table holds only what is
// waiting. check_id is not a foreign key: an alert about a remote collector
// has no check row (its id is the agent's, negated); the engine drops a row
// whose check has gone instead. Applied by migrate() after the base schema.
const outboxSchema = `
CREATE TABLE IF NOT EXISTS alert_outbox (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  check_id INTEGER NOT NULL,
  target TEXT NOT NULL,
  kind TEXT NOT NULL,
  created_at TEXT NOT NULL,
  next_at TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT NOT NULL DEFAULT '',
  payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alert_outbox_target ON alert_outbox(target, id);
`

// QueuedAlert is one row of the alert outbox. Payload is the alert itself as
// the engine wrote it; the store does not look inside.
type QueuedAlert struct {
	ID        int64
	CheckID   int64
	Target    string
	Kind      string
	CreatedAt time.Time
	NextAt    time.Time
	Attempts  int
	LastError string
	Payload   string
}

// QueueAlert adds an alert to the outbox and returns its id.
func (s *Store) QueueAlert(ctx context.Context, q QueuedAlert) (int64, error) {
	return s.insertID(ctx, `INSERT INTO alert_outbox(check_id, target, kind, created_at, next_at, attempts, last_error, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		q.CheckID, q.Target, q.Kind, fmtTime(q.CreatedAt), fmtTime(q.NextAt), q.Attempts, q.LastError, q.Payload)
}

// QueuedAlerts returns everything in the outbox, oldest first. The outbox
// only ever holds what failed to send, so it is small.
func (s *Store) QueuedAlerts(ctx context.Context) ([]QueuedAlert, error) {
	rows, err := s.query(ctx, `SELECT id, check_id, target, kind, created_at, next_at, attempts, last_error, payload FROM alert_outbox ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QueuedAlert
	for rows.Next() {
		var q QueuedAlert
		var created, next sql.NullString
		if err := rows.Scan(&q.ID, &q.CheckID, &q.Target, &q.Kind, &created, &next, &q.Attempts, &q.LastError, &q.Payload); err != nil {
			return nil, err
		}
		if t := parseTime(created); t != nil {
			q.CreatedAt = *t
		}
		if t := parseTime(next); t != nil {
			q.NextAt = *t
		}
		out = append(out, q)
	}
	return out, rows.Err()
}

// RescheduleQueuedAlert records a failed attempt to send a queued alert.
func (s *Store) RescheduleQueuedAlert(ctx context.Context, id int64, attempts int, next time.Time, lastError string) error {
	_, err := s.exec(ctx, `UPDATE alert_outbox SET attempts = ?, next_at = ?, last_error = ? WHERE id = ?`, attempts, fmtTime(next), lastError, id)
	return err
}

// DeleteQueuedAlert removes a queued alert once it is sent or given up on.
func (s *Store) DeleteQueuedAlert(ctx context.Context, id int64) error {
	_, err := s.exec(ctx, `DELETE FROM alert_outbox WHERE id = ?`, id)
	return err
}
