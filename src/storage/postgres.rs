//! PostgreSQL implementation of StateStore
//!
//! Uses PostgreSQL-specific features:
//! - `FOR UPDATE SKIP LOCKED` for efficient queue processing
//! - JSONB for flexible data storage
//! - Advisory locks for distributed coordination

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use uuid::Uuid;

use crate::error::{ChoreoError, Result};
use crate::models::{Event, FunctionRun, Lock, RunStatus, StepRun, StepStatus};
use crate::storage::StateStore;

/// PostgreSQL-backed state store
///
/// Suitable for production use with features like:
/// - Connection pooling
/// - FOR UPDATE SKIP LOCKED for queue operations
/// - JSONB for event/run data
///
/// # Example
///
/// ```ignore
/// let store = PostgresStore::connect("postgres://user:pass@localhost/choreo").await?;
/// // Or with Supabase
/// let store = PostgresStore::connect("postgres://user:pass@db.supabase.co:5432/postgres").await?;
/// ```
#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    /// Connect to PostgreSQL with default pool settings
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(url)
            .await?;

        Ok(Self { pool })
    }

    /// Connect with custom pool options
    pub async fn connect_with_options(url: &str, max_connections: u32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(url)
            .await?;

        Ok(Self { pool })
    }

    /// Create from existing pool
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Get reference to the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl StateStore for PostgresStore {
    // =========================================================================
    // Event Operations
    // =========================================================================

    async fn insert_event(&self, event: &Event) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO events (id, name, data, idempotency_key, timestamp, user_id)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(event.id)
        .bind(&event.name)
        .bind(&event.data)
        .bind(&event.idempotency_key)
        .bind(event.timestamp)
        .bind(&event.user_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_event(&self, id: Uuid) -> Result<Option<Event>> {
        let row = sqlx::query(
            r#"
            SELECT id, name, data, idempotency_key, timestamp, user_id
            FROM events WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| Event {
            id: r.get("id"),
            name: r.get("name"),
            data: r.get("data"),
            idempotency_key: r.get("idempotency_key"),
            timestamp: r.get("timestamp"),
            user_id: r.get("user_id"),
        }))
    }

    async fn get_events_by_name(&self, name: &str, limit: i64, offset: i64) -> Result<Vec<Event>> {
        let rows = sqlx::query(
            r#"
            SELECT id, name, data, idempotency_key, timestamp, user_id
            FROM events WHERE name = $1
            ORDER BY timestamp DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(name)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| Event {
                id: r.get("id"),
                name: r.get("name"),
                data: r.get("data"),
                idempotency_key: r.get("idempotency_key"),
                timestamp: r.get("timestamp"),
                user_id: r.get("user_id"),
            })
            .collect())
    }

    async fn list_events(&self, limit: i64, offset: i64) -> Result<Vec<Event>> {
        let rows = sqlx::query(
            r#"
            SELECT id, name, data, idempotency_key, timestamp, user_id
            FROM events
            ORDER BY timestamp DESC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| Event {
                id: r.get("id"),
                name: r.get("name"),
                data: r.get("data"),
                idempotency_key: r.get("idempotency_key"),
                timestamp: r.get("timestamp"),
                user_id: r.get("user_id"),
            })
            .collect())
    }

    async fn check_idempotency_key(&self, key: &str) -> Result<Option<Uuid>> {
        let row = sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM events WHERE idempotency_key = $1 LIMIT 1",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row)
    }

    // =========================================================================
    // Function Run Operations
    // =========================================================================

    async fn insert_run(&self, run: &FunctionRun) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO function_runs (
                id, function_id, event_id, status, attempt, max_attempts,
                input, output, error, created_at, started_at, ended_at,
                locked_until, locked_by, concurrency_key, run_after
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            "#,
        )
        .bind(run.id)
        .bind(&run.function_id)
        .bind(run.event_id)
        .bind(run.status.as_str())
        .bind(run.attempt)
        .bind(run.max_attempts)
        .bind(&run.input)
        .bind(&run.output)
        .bind(&run.error)
        .bind(run.created_at)
        .bind(run.started_at)
        .bind(run.ended_at)
        .bind(run.locked_until)
        .bind(&run.locked_by)
        .bind(&run.concurrency_key)
        .bind(run.run_after)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_run(&self, id: Uuid) -> Result<Option<FunctionRun>> {
        let row = sqlx::query(
            r#"
            SELECT id, function_id, event_id, status, attempt, max_attempts,
                   input, output, error, created_at, started_at, ended_at,
                   locked_until, locked_by, concurrency_key, run_after
            FROM function_runs WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| row_to_run(&r)))
    }

    async fn get_runs_by_event(&self, event_id: Uuid) -> Result<Vec<FunctionRun>> {
        let rows = sqlx::query(
            r#"
            SELECT id, function_id, event_id, status, attempt, max_attempts,
                   input, output, error, created_at, started_at, ended_at,
                   locked_until, locked_by, concurrency_key, run_after
            FROM function_runs WHERE event_id = $1
            ORDER BY created_at
            "#,
        )
        .bind(event_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(row_to_run).collect())
    }

    async fn get_runs_by_function(
        &self,
        function_id: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<FunctionRun>> {
        let rows = sqlx::query(
            r#"
            SELECT id, function_id, event_id, status, attempt, max_attempts,
                   input, output, error, created_at, started_at, ended_at,
                   locked_until, locked_by, concurrency_key, run_after
            FROM function_runs WHERE function_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(function_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(row_to_run).collect())
    }

    async fn list_runs(
        &self,
        limit: i64,
        offset: i64,
        status: Option<&str>,
        function_id: Option<&str>,
        event_id: Option<Uuid>,
    ) -> Result<Vec<FunctionRun>> {
        let rows = sqlx::query(
            r#"
            SELECT id, function_id, event_id, status, attempt, max_attempts,
                   input, output, error, created_at, started_at, ended_at,
                   locked_until, locked_by, concurrency_key, run_after
            FROM function_runs
            WHERE ($1::text IS NULL OR status = $1)
              AND ($2::text IS NULL OR function_id = $2)
              AND ($3::uuid IS NULL OR event_id = $3)
            ORDER BY created_at DESC
            LIMIT $4 OFFSET $5
            "#,
        )
        .bind(status)
        .bind(function_id)
        .bind(event_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(row_to_run).collect())
    }

    async fn lease_runs(
        &self,
        worker_id: &str,
        limit: i64,
        lock_duration_secs: i64,
    ) -> Result<Vec<FunctionRun>> {
        // Use FOR UPDATE SKIP LOCKED for efficient concurrent access
        let rows = sqlx::query(
            r#"
            UPDATE function_runs
            SET status = 'running',
                started_at = COALESCE(started_at, NOW()),
                locked_until = NOW() + $3 * INTERVAL '1 second',
                locked_by = $1,
                attempt = attempt + 1
            WHERE id IN (
                SELECT id FROM function_runs
                WHERE status = 'queued'
                  AND (run_after IS NULL OR run_after <= NOW())
                  AND (locked_until IS NULL OR locked_until < NOW())
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            )
            RETURNING id, function_id, event_id, status, attempt, max_attempts,
                      input, output, error, created_at, started_at, ended_at,
                      locked_until, locked_by, concurrency_key, run_after
            "#,
        )
        .bind(worker_id)
        .bind(limit)
        .bind(lock_duration_secs)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(row_to_run).collect())
    }

    async fn update_run_status(&self, id: Uuid, status: RunStatus) -> Result<()> {
        let ended_at = if status.is_terminal() {
            Some(Utc::now())
        } else {
            None
        };

        sqlx::query(
            r#"
            UPDATE function_runs
            SET status = $2, ended_at = COALESCE($3, ended_at)
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(status.as_str())
        .bind(ended_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn complete_run(&self, id: Uuid, output: serde_json::Value) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE function_runs
            SET status = 'completed', output = $2, ended_at = NOW(),
                locked_until = NULL, locked_by = NULL
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(output)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn fail_run(&self, id: Uuid, error: &str, can_retry: bool) -> Result<()> {
        let status = if can_retry { "queued" } else { "failed" };

        sqlx::query(
            r#"
            UPDATE function_runs
            SET status = $2, error = $3,
                ended_at = CASE WHEN $2 = 'failed' THEN NOW() ELSE ended_at END,
                locked_until = NULL, locked_by = NULL
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(status)
        .bind(error)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn cancel_run(&self, id: Uuid) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE function_runs
            SET status = 'cancelled', ended_at = NOW(),
                locked_until = NULL, locked_by = NULL
            WHERE id = $1 AND status NOT IN ('completed', 'failed', 'cancelled')
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn extend_run_lock(
        &self,
        id: Uuid,
        worker_id: &str,
        lock_duration_secs: i64,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE function_runs
            SET locked_until = NOW() + $3 * INTERVAL '1 second'
            WHERE id = $1 AND locked_by = $2 AND status = 'running'
            "#,
        )
        .bind(id)
        .bind(worker_id)
        .bind(lock_duration_secs)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn release_run_lock(&self, id: Uuid, worker_id: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE function_runs
            SET locked_until = NULL, locked_by = NULL
            WHERE id = $1 AND locked_by = $2
            "#,
        )
        .bind(id)
        .bind(worker_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_stale_runs(&self, limit: i64) -> Result<Vec<FunctionRun>> {
        let rows = sqlx::query(
            r#"
            SELECT id, function_id, event_id, status, attempt, max_attempts,
                   input, output, error, created_at, started_at, ended_at,
                   locked_until, locked_by, concurrency_key, run_after
            FROM function_runs
            WHERE status = 'running' AND locked_until < NOW()
            ORDER BY locked_until
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(row_to_run).collect())
    }

    async fn retry_run(&self, id: Uuid, run_after: Option<DateTime<Utc>>) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE function_runs
            SET status = 'queued', run_after = $2,
                locked_until = NULL, locked_by = NULL
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(run_after)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // =========================================================================
    // Step Operations
    // =========================================================================

    async fn get_step(&self, run_id: Uuid, step_id: &str) -> Result<Option<StepRun>> {
        let row = sqlx::query(
            r#"
            SELECT id, run_id, step_id, status, input, output, error,
                   attempt, created_at, started_at, ended_at
            FROM step_runs WHERE run_id = $1 AND step_id = $2
            "#,
        )
        .bind(run_id)
        .bind(step_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| row_to_step(&r)))
    }

    async fn get_steps_for_run(&self, run_id: Uuid) -> Result<Vec<StepRun>> {
        let rows = sqlx::query(
            r#"
            SELECT id, run_id, step_id, status, input, output, error,
                   attempt, created_at, started_at, ended_at
            FROM step_runs WHERE run_id = $1
            ORDER BY created_at
            "#,
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(row_to_step).collect())
    }

    async fn upsert_step(&self, step: &StepRun) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO step_runs (id, run_id, step_id, status, input, output, error,
                                   attempt, created_at, started_at, ended_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (run_id, step_id) DO UPDATE SET
                status = EXCLUDED.status,
                output = EXCLUDED.output,
                error = EXCLUDED.error,
                attempt = EXCLUDED.attempt,
                started_at = EXCLUDED.started_at,
                ended_at = EXCLUDED.ended_at
            "#,
        )
        .bind(step.id)
        .bind(step.run_id)
        .bind(&step.step_id)
        .bind(step.status.as_str())
        .bind(&step.input)
        .bind(&step.output)
        .bind(&step.error)
        .bind(step.attempt)
        .bind(step.created_at)
        .bind(step.started_at)
        .bind(step.ended_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn start_step(&self, run_id: Uuid, step_id: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE step_runs
            SET status = 'running', started_at = NOW(), attempt = attempt + 1
            WHERE run_id = $1 AND step_id = $2
            "#,
        )
        .bind(run_id)
        .bind(step_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn complete_step(
        &self,
        run_id: Uuid,
        step_id: &str,
        output: serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE step_runs
            SET status = 'completed', output = $3, ended_at = NOW()
            WHERE run_id = $1 AND step_id = $2
            "#,
        )
        .bind(run_id)
        .bind(step_id)
        .bind(output)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn fail_step(&self, run_id: Uuid, step_id: &str, error: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE step_runs
            SET status = 'failed', error = $3, ended_at = NOW()
            WHERE run_id = $1 AND step_id = $2
            "#,
        )
        .bind(run_id)
        .bind(step_id)
        .bind(error)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // =========================================================================
    // Distributed Locking
    // =========================================================================

    async fn try_acquire_lock(&self, key: &str, holder: &str, ttl_secs: i64) -> Result<bool> {
        let result = sqlx::query(
            r#"
            INSERT INTO distributed_locks (key, holder, expires_at, acquired_at)
            VALUES ($1, $2, NOW() + $3 * INTERVAL '1 second', NOW())
            ON CONFLICT (key) DO UPDATE SET
                holder = EXCLUDED.holder,
                expires_at = EXCLUDED.expires_at,
                acquired_at = EXCLUDED.acquired_at
            WHERE distributed_locks.expires_at < NOW()
               OR distributed_locks.holder = $2
            "#,
        )
        .bind(key)
        .bind(holder)
        .bind(ttl_secs)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn release_lock(&self, key: &str, holder: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM distributed_locks WHERE key = $1 AND holder = $2")
            .bind(key)
            .bind(holder)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn extend_lock(&self, key: &str, holder: &str, ttl_secs: i64) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE distributed_locks
            SET expires_at = NOW() + $3 * INTERVAL '1 second'
            WHERE key = $1 AND holder = $2
            "#,
        )
        .bind(key)
        .bind(holder)
        .bind(ttl_secs)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn get_lock(&self, key: &str) -> Result<Option<Lock>> {
        let row = sqlx::query(
            "SELECT key, holder, expires_at, acquired_at FROM distributed_locks WHERE key = $1",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| Lock {
            key: r.get("key"),
            holder: r.get("holder"),
            expires_at: r.get("expires_at"),
            acquired_at: r.get("acquired_at"),
        }))
    }

    // =========================================================================
    // Function Registry
    // =========================================================================

    async fn register_function(
        &self,
        function_id: &str,
        definition: &serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO functions (id, definition, created_at, updated_at)
            VALUES ($1, $2, NOW(), NOW())
            ON CONFLICT (id) DO UPDATE SET
                definition = EXCLUDED.definition,
                updated_at = NOW()
            "#,
        )
        .bind(function_id)
        .bind(definition)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn list_functions(&self) -> Result<Vec<serde_json::Value>> {
        let rows = sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT definition FROM functions ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    async fn get_function(&self, function_id: &str) -> Result<Option<serde_json::Value>> {
        let row = sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT definition FROM functions WHERE id = $1",
        )
        .bind(function_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row)
    }

    async fn get_functions_for_event(&self, event_name: &str) -> Result<Vec<serde_json::Value>> {
        // Query functions where triggers array contains an event trigger with matching name
        let rows = sqlx::query_scalar::<_, serde_json::Value>(
            r#"
            SELECT definition FROM functions
            WHERE definition->'triggers' @> $1::jsonb
            "#,
        )
        .bind(serde_json::json!([{"type": "event", "name": event_name}]))
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    // =========================================================================
    // Worker Management
    // =========================================================================

    async fn extend_run_leases(&self, worker_id: &str, run_ids: &[Uuid]) -> Result<()> {
        if run_ids.is_empty() {
            return Ok(());
        }

        sqlx::query(
            r#"
            UPDATE function_runs
            SET locked_until = NOW() + INTERVAL '5 minutes'
            WHERE id = ANY($1) AND locked_by = $2 AND status = 'running'
            "#,
        )
        .bind(run_ids)
        .bind(worker_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // =========================================================================
    // Maintenance
    // =========================================================================

    async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations/postgres")
            .run(&self.pool)
            .await
            .map_err(|e| {
                ChoreoError::Database(crate::error::DatabaseError::Migration(e.to_string()))
            })?;
        Ok(())
    }

    async fn cleanup_old_runs(&self, older_than: DateTime<Utc>) -> Result<u64> {
        // First delete steps
        sqlx::query(
            r#"
            DELETE FROM step_runs WHERE run_id IN (
                SELECT id FROM function_runs
                WHERE status IN ('completed', 'failed', 'cancelled')
                  AND ended_at < $1
            )
            "#,
        )
        .bind(older_than)
        .execute(&self.pool)
        .await?;

        // Then delete runs
        let result = sqlx::query(
            r#"
            DELETE FROM function_runs
            WHERE status IN ('completed', 'failed', 'cancelled')
              AND ended_at < $1
            "#,
        )
        .bind(older_than)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    async fn cleanup_expired_locks(&self) -> Result<u64> {
        let result = sqlx::query("DELETE FROM distributed_locks WHERE expires_at < NOW()")
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected())
    }
}

// Helper functions
fn row_to_run(row: &sqlx::postgres::PgRow) -> FunctionRun {
    let status_str: String = row.get("status");
    FunctionRun {
        id: row.get("id"),
        function_id: row.get("function_id"),
        event_id: row.get("event_id"),
        status: RunStatus::parse(&status_str).unwrap_or(RunStatus::Queued),
        attempt: row.get("attempt"),
        max_attempts: row.get("max_attempts"),
        input: row.get("input"),
        output: row.get("output"),
        error: row.get("error"),
        created_at: row.get("created_at"),
        started_at: row.get("started_at"),
        ended_at: row.get("ended_at"),
        locked_until: row.get("locked_until"),
        locked_by: row.get("locked_by"),
        concurrency_key: row.get("concurrency_key"),
        run_after: row.get("run_after"),
    }
}

fn row_to_step(row: &sqlx::postgres::PgRow) -> StepRun {
    let status_str: String = row.get("status");
    StepRun {
        id: row.get("id"),
        run_id: row.get("run_id"),
        step_id: row.get("step_id"),
        status: StepStatus::parse(&status_str).unwrap_or(StepStatus::Pending),
        input: row.get("input"),
        output: row.get("output"),
        error: row.get("error"),
        attempt: row.get("attempt"),
        created_at: row.get("created_at"),
        started_at: row.get("started_at"),
        ended_at: row.get("ended_at"),
    }
}
