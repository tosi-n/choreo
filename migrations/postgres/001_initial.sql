-- Choreo: Initial Schema for PostgreSQL
-- Supports: PostgreSQL 12+, Supabase, Neon, etc.

-- Events table: incoming events that trigger function runs
CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    data JSONB NOT NULL DEFAULT '{}',
    idempotency_key VARCHAR(255) UNIQUE,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    user_id VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_name ON events(name);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON events(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_idempotency ON events(idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Function runs table: execution instances of functions
CREATE TABLE IF NOT EXISTS function_runs (
    id UUID PRIMARY KEY,
    function_id VARCHAR(255) NOT NULL,
    event_id UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    status VARCHAR(50) NOT NULL DEFAULT 'queued',
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    input JSONB NOT NULL DEFAULT '{}',
    output JSONB,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    locked_until TIMESTAMPTZ,
    locked_by VARCHAR(255),
    concurrency_key VARCHAR(255),
    run_after TIMESTAMPTZ,
    CONSTRAINT valid_status CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled'))
);

CREATE INDEX IF NOT EXISTS idx_runs_status ON function_runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_function ON function_runs(function_id);
CREATE INDEX IF NOT EXISTS idx_runs_event ON function_runs(event_id);
CREATE INDEX IF NOT EXISTS idx_runs_created ON function_runs(created_at);
CREATE INDEX IF NOT EXISTS idx_runs_queue ON function_runs(created_at) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS idx_runs_stale ON function_runs(locked_until) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS idx_runs_concurrency ON function_runs(concurrency_key, status) WHERE concurrency_key IS NOT NULL;

-- Step runs table: durable execution checkpoints
CREATE TABLE IF NOT EXISTS step_runs (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES function_runs(id) ON DELETE CASCADE,
    step_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    input JSONB,
    output JSONB,
    error TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    CONSTRAINT unique_step_per_run UNIQUE (run_id, step_id),
    CONSTRAINT valid_step_status CHECK (status IN ('pending', 'running', 'completed', 'failed', 'skipped'))
);

CREATE INDEX IF NOT EXISTS idx_steps_run ON step_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_steps_status ON step_runs(run_id, status);

-- Distributed locks table
CREATE TABLE IF NOT EXISTS distributed_locks (
    key VARCHAR(255) PRIMARY KEY,
    holder VARCHAR(255) NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_locks_expires ON distributed_locks(expires_at);

-- Functions registry table: registered function definitions
CREATE TABLE IF NOT EXISTS functions (
    id VARCHAR(255) PRIMARY KEY,
    definition JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for finding functions by trigger event name
CREATE INDEX IF NOT EXISTS idx_functions_triggers ON functions USING GIN ((definition->'triggers'));
