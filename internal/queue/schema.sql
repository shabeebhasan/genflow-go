CREATE TABLE IF NOT EXISTS jobs (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    kind         text        NOT NULL,
    input        jsonb       NOT NULL,
    state        text        NOT NULL DEFAULT 'queued'
                 CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    step         text        NOT NULL DEFAULT '',
    checkpoint   jsonb       NOT NULL DEFAULT '{}',
    attempts     int         NOT NULL DEFAULT 0,
    max_attempts int         NOT NULL DEFAULT 5,
    run_after    timestamptz NOT NULL DEFAULT now(),
    lease_owner  text,
    lease_until  timestamptz,
    last_error   text        NOT NULL DEFAULT '',
    output_key   text        NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- claim order: runnable jobs first, oldest first
CREATE INDEX IF NOT EXISTS jobs_runnable ON jobs (run_after, created_at) WHERE state IN ('queued', 'running');
