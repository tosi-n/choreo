-- Choreo: Initial Schema for SQLite
-- Supports: SQLite 3.35+, Turso/libSQL

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    data TEXT NOT NULL DEFAULT '{}',
    idempotency_key TEXT UNIQUE,
    timestamp TEXT NOT NULL,
    user_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_events_name ON events(name);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON events(user_id);
CREATE INDEX IF NOT EXISTS idx_events_idempotency ON events(idempotency_key);

CREATE TABLE IF NOT EXISTS function_runs (
    id TEXT PRIMARY KEY,
    function_id TEXT NOT NULL,
    event_id TEXT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled')),
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    input TEXT NOT NULL DEFAULT '{}',
    output TEXT,
    error TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT,
    ended_at TEXT,
    locked_until TEXT,
    locked_by TEXT,
    concurrency_key TEXT,
    run_after TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_status ON function_runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_function ON function_runs(function_id);
CREATE INDEX IF NOT EXISTS idx_runs_event ON function_runs(event_id);
CREATE INDEX IF NOT EXISTS idx_runs_created ON function_runs(created_at);
CREATE INDEX IF NOT EXISTS idx_runs_queue ON function_runs(created_at) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS idx_runs_stale ON function_runs(locked_until) WHERE status = 'running';

CREATE TABLE IF NOT EXISTS step_runs (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES function_runs(id) ON DELETE CASCADE,
    step_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed', 'skipped')),
    input TEXT,
    output TEXT,
    error TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT,
    ended_at TEXT,
    UNIQUE (run_id, step_id)
);

CREATE INDEX IF NOT EXISTS idx_steps_run ON step_runs(run_id);

CREATE TABLE IF NOT EXISTS distributed_locks (
    key TEXT PRIMARY KEY,
    holder TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    acquired_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_locks_expires ON distributed_locks(expires_at);

-- Functions registry table
CREATE TABLE IF NOT EXISTS functions (
    id TEXT PRIMARY KEY,
    definition TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
