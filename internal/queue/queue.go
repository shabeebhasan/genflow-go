// Package queue is a durable job queue on Postgres.
//
// Workers claim jobs with FOR UPDATE SKIP LOCKED and hold a time-limited lease that they renew
// while working. If a worker dies, its lease runs out and another worker picks the job up again,
// starting from the last saved checkpoint. Every write checks the lease owner, so a worker that
// lost its lease (it froze, the network split) cannot overwrite the new owner's progress.
package queue

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var Schema string

var (
	ErrBackpressure = errors.New("queue is full, try again later")
	ErrLostLease    = errors.New("lease lost: another worker owns this job now, or it was cancelled")
	ErrNotFound     = errors.New("job not found")
)

type Job struct {
	ID          string          `json:"id"`
	Kind        string          `json:"kind"`
	Input       json.RawMessage `json:"input"`
	State       string          `json:"state"`
	Step        string          `json:"step"`
	Checkpoint  json.RawMessage `json:"checkpoint"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	LastError   string          `json:"last_error,omitempty"`
	OutputKey   string          `json:"output_key,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

type Store struct{ Pool *pgxpool.Pool }

func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	_, err = conn.Conn().PgConn().Exec(ctx, Schema).ReadAll() // several statements: simple protocol
	return err
}

const cols = `id::text, kind, input, state, step, checkpoint, attempts, max_attempts, last_error, output_key, created_at, updated_at`

func scan(row pgx.Row) (*Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.Kind, &j.Input, &j.State, &j.Step, &j.Checkpoint, &j.Attempts, &j.MaxAttempts,
		&j.LastError, &j.OutputKey, &j.CreatedAt, &j.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &j, err
}

// Enqueue adds a job unless queued + running jobs already reach maxDepth (backpressure).
func (s *Store) Enqueue(ctx context.Context, kind string, input any, maxAttempts, maxDepth int) (string, error) {
	in, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	// one admission check at a time, so two requests cannot both squeeze past the limit
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(4242)`); err != nil {
		return "", err
	}
	var depth int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE state IN ('queued','running')`).Scan(&depth); err != nil {
		return "", err
	}
	if maxDepth > 0 && depth >= maxDepth {
		return "", ErrBackpressure
	}
	var id string
	err = tx.QueryRow(ctx, `INSERT INTO jobs (kind, input, max_attempts) VALUES ($1, $2, $3) RETURNING id::text`,
		kind, in, maxAttempts).Scan(&id)
	if err != nil {
		return "", err
	}
	return id, tx.Commit(ctx)
}

// Claim takes the oldest runnable job: queued and due, or running with an expired lease.
func (s *Store) Claim(ctx context.Context, owner string, lease time.Duration) (*Job, error) {
	j, err := scan(s.Pool.QueryRow(ctx, `
		UPDATE jobs SET state = 'running', lease_owner = $1, lease_until = now() + $2::interval,
		       attempts = attempts + 1, updated_at = now()
		WHERE id = (
			SELECT id FROM jobs
			WHERE (state = 'queued' AND run_after <= now()) OR (state = 'running' AND lease_until < now())
			ORDER BY run_after, created_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED)
		RETURNING `+cols, owner, lease.String()))
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	return j, err
}

func (s *Store) ownedUpdate(ctx context.Context, sql string, args ...any) error {
	tag, err := s.Pool.Exec(ctx, sql, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLostLease
	}
	return nil
}

// Heartbeat extends the lease. ErrLostLease means stop working on this job now.
func (s *Store) Heartbeat(ctx context.Context, id, owner string, lease time.Duration) error {
	return s.ownedUpdate(ctx, `UPDATE jobs SET lease_until = now() + $3::interval, updated_at = now()
		WHERE id = $1 AND lease_owner = $2 AND state = 'running'`, id, owner, lease.String())
}

// Checkpoint saves the step reached and its data, so a later attempt resumes from here.
func (s *Store) Checkpoint(ctx context.Context, id, owner, step string, data any) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return s.ownedUpdate(ctx, `UPDATE jobs SET step = $3, checkpoint = $4, updated_at = now()
		WHERE id = $1 AND lease_owner = $2 AND state = 'running'`, id, owner, step, b)
}

func (s *Store) Complete(ctx context.Context, id, owner, outputKey string) error {
	return s.ownedUpdate(ctx, `UPDATE jobs SET state = 'succeeded', step = 'done', output_key = $3, last_error = '',
		lease_owner = NULL, lease_until = NULL, updated_at = now()
		WHERE id = $1 AND lease_owner = $2 AND state = 'running'`, id, owner, outputKey)
}

// Retry puts the job back for a later attempt, or fails it when attempts are used up.
// It returns true if the job will run again.
func (s *Store) Retry(ctx context.Context, id, owner string, cause error, delay time.Duration) (bool, error) {
	var state string
	err := s.Pool.QueryRow(ctx, `UPDATE jobs SET
			state = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'queued' END,
			run_after = now() + $4::interval, last_error = $3, lease_owner = NULL, lease_until = NULL, updated_at = now()
		WHERE id = $1 AND lease_owner = $2 AND state = 'running' RETURNING state`,
		id, owner, cause.Error(), delay.String()).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrLostLease
	}
	return state == "queued", err
}

func (s *Store) Fail(ctx context.Context, id, owner string, cause error) error {
	return s.ownedUpdate(ctx, `UPDATE jobs SET state = 'failed', last_error = $3, lease_owner = NULL, lease_until = NULL,
		updated_at = now() WHERE id = $1 AND lease_owner = $2 AND state = 'running'`, id, owner, cause.Error())
}

// Cancel stops a queued or running job; a running worker notices at its next heartbeat.
func (s *Store) Cancel(ctx context.Context, id string) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE jobs SET state = 'cancelled', lease_owner = NULL, lease_until = NULL, updated_at = now()
		WHERE id = $1 AND state IN ('queued','running')`, id)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func (s *Store) Get(ctx context.Context, id string) (*Job, error) {
	return scan(s.Pool.QueryRow(ctx, `SELECT `+cols+` FROM jobs WHERE id = $1`, id))
}

// Stats counts jobs per state, for /metrics.
func (s *Store) Stats(ctx context.Context) (map[string]int, error) {
	rows, err := s.Pool.Query(ctx, `SELECT state, count(*) FROM jobs GROUP BY state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}
