//! SQLite implementation of StateStore
//!
//! Ideal for:
//! - Local development
//! - Embedded/edge deployments
//! - Single-node applications
//! - Testing

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use uuid::Uuid;

use crate::error::{ChoreoError, Result};
use crate::models::{Event, FunctionRun, Lock, RunStatus, StepRun, StepStatus};
use crate::storage::StateStore;

/// SQLite-backed state store
///
/// Uses SQLite-specific features:
/// - WAL mode for better concurrency
/// - JSON1 extension for data storage
/// - `RETURNING` clause (SQLite 3.35+)
///
/// # Example
///
/// ```ignore
/// // File-based for persistence
/// let store = SqliteStore::connect("sqlite://choreo.db").await?;
///
/// // In-memory for testing
/// let store = SqliteStore::connect("sqlite::memory:").await?;
/// ```
#[derive(Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    /// Connect to SQLite database
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1) // SQLite works best with single writer
            .connect(url)
            .await?;

        // Enable WAL mode for better concurrent reads
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&pool)
            .await?;

        // Enable foreign keys
        sqlx::query("PRAGMA foreign_keys=ON").execute(&pool).await?;

        Ok(Self { pool })
    }

    /// Create in-memory store (useful for testing)
    pub async fn in_memory() -> Result<Self> {
        Self::connect("sqlite::memory:").await
    }

    /// Create from existing pool
    pub fn from_pool(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Get reference to the connection pool
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

#[async_trait]
impl StateStore for SqliteStore {
    // =========================================================================
    // Event Operations
    // =========================================================================

    async fn insert_event(&self, event: &Event) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO events (id, name, data, idempotency_key, timestamp, user_id)
            VALUES (?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(event.id.to_string())
        .bind(&event.name)
        .bind(serde_json::to_string(&event.data).unwrap_or_default())
        .bind(&event.idempotency_key)
        .bind(event.timestamp.to_rfc3339())
        .bind(&event.user_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_event(&self, id: Uuid) -> Result<Option<Event>> {
        let row = sqlx::query(
            r#"
            SELECT id, name, data, idempotency_key, timestamp, user_id
            FROM events WHERE id = ?
            "#,
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| row_to_event(&r)))
    }

    async fn get_events_by_name(&self, name: &str, limit: i64, offset: i64) -> Result<Vec<Event>> {
        let rows = sqlx::query(
            r#"
            SELECT id, name, data, idempotency_key, timestamp, user_id
            FROM events WHERE name = ?
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
            "#,
        )
        .bind(name)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(row_to_event).collect())
    }

    async fn check_idempotency_key(&self, key: &str) -> Result<Option<Uuid>> {
        let row: Option<String> =
            sqlx::query_scalar("SELECT id FROM events WHERE idempotency_key = ? LIMIT 1")
                .bind(key)
                .fetch_optional(&self.pool)
                .await?;

        Ok(row.and_then(|s| Uuid::parse_str(&s).ok()))
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(run.id.to_string())
        .bind(&run.function_id)
        .bind(run.event_id.to_string())
        .bind(run.status.as_str())
        .bind(run.attempt)
        .bind(run.max_attempts)
        .bind(serde_json::to_string(&run.input).unwrap_or_default())
        .bind(
            run.output
                .as_ref()
                .map(|v| serde_json::to_string(v).unwrap_or_default()),
        )
        .bind(&run.error)
        .bind(run.created_at.to_rfc3339())
        .bind(run.started_at.map(|t| t.to_rfc3339()))
        .bind(run.ended_at.map(|t| t.to_rfc3339()))
        .bind(run.locked_until.map(|t| t.to_rfc3339()))
        .bind(&run.locked_by)
        .bind(&run.concurrency_key)
        .bind(run.run_after.map(|t| t.to_rfc3339()))
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
            FROM function_runs WHERE id = ?
            "#,
        )
        .bind(id.to_string())
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
            FROM function_runs WHERE event_id = ?
            ORDER BY created_at
            "#,
        )
        .bind(event_id.to_string())
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
            FROM function_runs WHERE function_id = ?
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            "#,
        )
        .bind(function_id)
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
        // SQLite doesn't have FOR UPDATE SKIP LOCKED, so we use a transaction
        let now = Utc::now();
        let lock_until = now + chrono::Duration::seconds(lock_duration_secs);

        // Get IDs of runs to lease
        let ids: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT id FROM function_runs
            WHERE status = 'queued'
              AND (run_after IS NULL OR run_after <= ?)
              AND (locked_until IS NULL OR locked_until < ?)
            ORDER BY created_at
            LIMIT ?
            "#,
        )
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        if ids.is_empty() {
            return Ok(vec![]);
        }

        // Update them atomically
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let query = format!(
            r#"
            UPDATE function_runs
            SET status = 'running',
                started_at = COALESCE(started_at, ?),
                locked_until = ?,
                locked_by = ?,
                attempt = attempt + 1
            WHERE id IN ({})
            "#,
            placeholders
        );

        let mut q = sqlx::query(&query)
            .bind(now.to_rfc3339())
            .bind(lock_until.to_rfc3339())
            .bind(worker_id);

        for id in &ids {
            q = q.bind(id);
        }

        q.execute(&self.pool).await?;

        // Fetch the updated runs
        let mut runs = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(run) = self.get_run(Uuid::parse_str(&id).unwrap()).await? {
                runs.push(run);
            }
        }

        Ok(runs)
    }

    async fn update_run_status(&self, id: Uuid, status: RunStatus) -> Result<()> {
        let ended_at = if status.is_terminal() {
            Some(Utc::now().to_rfc3339())
        } else {
            None
        };

        sqlx::query(
            r#"
            UPDATE function_runs
            SET status = ?, ended_at = COALESCE(?, ended_at)
            WHERE id = ?
            "#,
        )
        .bind(status.as_str())
        .bind(ended_at)
        .bind(id.to_string())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn complete_run(&self, id: Uuid, output: serde_json::Value) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE function_runs
            SET status = 'completed', output = ?, ended_at = ?,
                locked_until = NULL, locked_by = NULL
            WHERE id = ?
            "#,
        )
        .bind(serde_json::to_string(&output).unwrap_or_default())
        .bind(Utc::now().to_rfc3339())
        .bind(id.to_string())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn fail_run(&self, id: Uuid, error: &str, can_retry: bool) -> Result<()> {
        let status = if can_retry { "queued" } else { "failed" };
        let ended_at = if can_retry {
            None
        } else {
            Some(Utc::now().to_rfc3339())
        };

        sqlx::query(
            r#"
            UPDATE function_runs
            SET status = ?, error = ?, ended_at = COALESCE(?, ended_at),
                locked_until = NULL, locked_by = NULL
            WHERE id = ?
            "#,
        )
        .bind(status)
        .bind(error)
        .bind(ended_at)
        .bind(id.to_string())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn cancel_run(&self, id: Uuid) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE function_runs
            SET status = 'cancelled', ended_at = ?,
                locked_until = NULL, locked_by = NULL
            WHERE id = ? AND status NOT IN ('completed', 'failed', 'cancelled')
            "#,
        )
        .bind(Utc::now().to_rfc3339())
        .bind(id.to_string())
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
        let lock_until = Utc::now() + chrono::Duration::seconds(lock_duration_secs);

        let result = sqlx::query(
            r#"
            UPDATE function_runs
            SET locked_until = ?
            WHERE id = ? AND locked_by = ? AND status = 'running'
            "#,
        )
        .bind(lock_until.to_rfc3339())
        .bind(id.to_string())
        .bind(worker_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn release_run_lock(&self, id: Uuid, worker_id: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE function_runs
            SET locked_until = NULL, locked_by = NULL
            WHERE id = ? AND locked_by = ?
            "#,
        )
        .bind(id.to_string())
        .bind(worker_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_stale_runs(&self, limit: i64) -> Result<Vec<FunctionRun>> {
        let now = Utc::now().to_rfc3339();

        let rows = sqlx::query(
            r#"
            SELECT id, function_id, event_id, status, attempt, max_attempts,
                   input, output, error, created_at, started_at, ended_at,
                   locked_until, locked_by, concurrency_key, run_after
            FROM function_runs
            WHERE status = 'running' AND locked_until < ?
            ORDER BY locked_until
            LIMIT ?
            "#,
        )
        .bind(now)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(row_to_run).collect())
    }

    async fn retry_run(&self, id: Uuid, run_after: Option<DateTime<Utc>>) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE function_runs
            SET status = 'queued', run_after = ?,
                locked_until = NULL, locked_by = NULL
            WHERE id = ?
            "#,
        )
        .bind(run_after.map(|t| t.to_rfc3339()))
        .bind(id.to_string())
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
            FROM step_runs WHERE run_id = ? AND step_id = ?
            "#,
        )
        .bind(run_id.to_string())
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
            FROM step_runs WHERE run_id = ?
            ORDER BY created_at
            "#,
        )
        .bind(run_id.to_string())
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(row_to_step).collect())
    }

    async fn upsert_step(&self, step: &StepRun) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO step_runs (id, run_id, step_id, status, input, output, error,
                                   attempt, created_at, started_at, ended_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (run_id, step_id) DO UPDATE SET
                status = excluded.status,
                output = excluded.output,
                error = excluded.error,
                attempt = excluded.attempt,
                started_at = excluded.started_at,
                ended_at = excluded.ended_at
            "#,
        )
        .bind(step.id.to_string())
        .bind(step.run_id.to_string())
        .bind(&step.step_id)
        .bind(step.status.as_str())
        .bind(
            step.input
                .as_ref()
                .map(|v| serde_json::to_string(v).unwrap_or_default()),
        )
        .bind(
            step.output
                .as_ref()
                .map(|v| serde_json::to_string(v).unwrap_or_default()),
        )
        .bind(&step.error)
        .bind(step.attempt)
        .bind(step.created_at.to_rfc3339())
        .bind(step.started_at.map(|t| t.to_rfc3339()))
        .bind(step.ended_at.map(|t| t.to_rfc3339()))
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn start_step(&self, run_id: Uuid, step_id: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE step_runs
            SET status = 'running', started_at = ?, attempt = attempt + 1
            WHERE run_id = ? AND step_id = ?
            "#,
        )
        .bind(Utc::now().to_rfc3339())
        .bind(run_id.to_string())
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
            SET status = 'completed', output = ?, ended_at = ?
            WHERE run_id = ? AND step_id = ?
            "#,
        )
        .bind(serde_json::to_string(&output).unwrap_or_default())
        .bind(Utc::now().to_rfc3339())
        .bind(run_id.to_string())
        .bind(step_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn fail_step(&self, run_id: Uuid, step_id: &str, error: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE step_runs
            SET status = 'failed', error = ?, ended_at = ?
            WHERE run_id = ? AND step_id = ?
            "#,
        )
        .bind(error)
        .bind(Utc::now().to_rfc3339())
        .bind(run_id.to_string())
        .bind(step_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // =========================================================================
    // Distributed Locking
    // =========================================================================

    async fn try_acquire_lock(&self, key: &str, holder: &str, ttl_secs: i64) -> Result<bool> {
        let now = Utc::now();
        let expires_at = now + chrono::Duration::seconds(ttl_secs);

        // Try insert or update if expired/same holder
        let result = sqlx::query(
            r#"
            INSERT INTO distributed_locks (key, holder, expires_at, acquired_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (key) DO UPDATE SET
                holder = excluded.holder,
                expires_at = excluded.expires_at,
                acquired_at = excluded.acquired_at
            WHERE distributed_locks.expires_at < ? OR distributed_locks.holder = ?
            "#,
        )
        .bind(key)
        .bind(holder)
        .bind(expires_at.to_rfc3339())
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .bind(holder)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn release_lock(&self, key: &str, holder: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM distributed_locks WHERE key = ? AND holder = ?")
            .bind(key)
            .bind(holder)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn extend_lock(&self, key: &str, holder: &str, ttl_secs: i64) -> Result<bool> {
        let expires_at = Utc::now() + chrono::Duration::seconds(ttl_secs);

        let result = sqlx::query(
            r#"
            UPDATE distributed_locks
            SET expires_at = ?
            WHERE key = ? AND holder = ?
            "#,
        )
        .bind(expires_at.to_rfc3339())
        .bind(key)
        .bind(holder)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn get_lock(&self, key: &str) -> Result<Option<Lock>> {
        let row = sqlx::query(
            "SELECT key, holder, expires_at, acquired_at FROM distributed_locks WHERE key = ?",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| {
            let expires_at: String = r.get("expires_at");
            let acquired_at: String = r.get("acquired_at");
            Lock {
                key: r.get("key"),
                holder: r.get("holder"),
                expires_at: DateTime::parse_from_rfc3339(&expires_at)
                    .map(|d| d.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now()),
                acquired_at: DateTime::parse_from_rfc3339(&acquired_at)
                    .map(|d| d.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now()),
            }
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
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO functions (id, definition, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                definition = excluded.definition,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(function_id)
        .bind(serde_json::to_string(definition).unwrap_or_default())
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn list_functions(&self) -> Result<Vec<serde_json::Value>> {
        let rows: Vec<String> = sqlx::query_scalar("SELECT definition FROM functions ORDER BY id")
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .into_iter()
            .filter_map(|s| serde_json::from_str(&s).ok())
            .collect())
    }

    async fn get_function(&self, function_id: &str) -> Result<Option<serde_json::Value>> {
        let row: Option<String> =
            sqlx::query_scalar("SELECT definition FROM functions WHERE id = ?")
                .bind(function_id)
                .fetch_optional(&self.pool)
                .await?;

        Ok(row.and_then(|s| serde_json::from_str(&s).ok()))
    }

    async fn get_functions_for_event(&self, event_name: &str) -> Result<Vec<serde_json::Value>> {
        // SQLite JSON query for finding functions with matching event triggers
        let rows: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT definition FROM functions
            WHERE json_extract(definition, '$.triggers') LIKE ?
            "#,
        )
        .bind(format!("%\"name\":\"{}\",%", event_name))
        .fetch_all(&self.pool)
        .await?;

        // Filter more precisely in application code
        Ok(rows
            .into_iter()
            .filter_map(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
            .filter(|def| {
                if let Some(triggers) = def.get("triggers").and_then(|t| t.as_array()) {
                    triggers.iter().any(|t| {
                        t.get("type").and_then(|t| t.as_str()) == Some("event")
                            && t.get("name").and_then(|n| n.as_str()) == Some(event_name)
                    })
                } else {
                    false
                }
            })
            .collect())
    }

    // =========================================================================
    // Worker Management
    // =========================================================================

    async fn extend_run_leases(&self, worker_id: &str, run_ids: &[Uuid]) -> Result<()> {
        if run_ids.is_empty() {
            return Ok(());
        }

        let lock_until = (Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
        let placeholders = run_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let query = format!(
            r#"
            UPDATE function_runs
            SET locked_until = ?
            WHERE id IN ({}) AND locked_by = ? AND status = 'running'
            "#,
            placeholders
        );

        let mut q = sqlx::query(&query).bind(&lock_until);
        for id in run_ids {
            q = q.bind(id.to_string());
        }
        q = q.bind(worker_id);

        q.execute(&self.pool).await?;

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
        sqlx::migrate!("./migrations/sqlite")
            .run(&self.pool)
            .await
            .map_err(|e| {
                ChoreoError::Database(crate::error::DatabaseError::Migration(e.to_string()))
            })?;
        Ok(())
    }

    async fn cleanup_old_runs(&self, older_than: DateTime<Utc>) -> Result<u64> {
        let older_than_str = older_than.to_rfc3339();

        // First delete steps
        sqlx::query(
            r#"
            DELETE FROM step_runs WHERE run_id IN (
                SELECT id FROM function_runs
                WHERE status IN ('completed', 'failed', 'cancelled')
                  AND ended_at < ?
            )
            "#,
        )
        .bind(&older_than_str)
        .execute(&self.pool)
        .await?;

        // Then delete runs
        let result = sqlx::query(
            r#"
            DELETE FROM function_runs
            WHERE status IN ('completed', 'failed', 'cancelled')
              AND ended_at < ?
            "#,
        )
        .bind(&older_than_str)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    async fn cleanup_expired_locks(&self) -> Result<u64> {
        let now = Utc::now().to_rfc3339();

        let result = sqlx::query("DELETE FROM distributed_locks WHERE expires_at < ?")
            .bind(now)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected())
    }
}

// Helper functions
fn row_to_event(row: &sqlx::sqlite::SqliteRow) -> Event {
    let id: String = row.get("id");
    let data: String = row.get("data");
    let timestamp: String = row.get("timestamp");

    Event {
        id: Uuid::parse_str(&id).unwrap_or_else(|_| Uuid::nil()),
        name: row.get("name"),
        data: serde_json::from_str(&data).unwrap_or(serde_json::Value::Null),
        idempotency_key: row.get("idempotency_key"),
        timestamp: DateTime::parse_from_rfc3339(&timestamp)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        user_id: row.get("user_id"),
    }
}

fn row_to_run(row: &sqlx::sqlite::SqliteRow) -> FunctionRun {
    let id: String = row.get("id");
    let event_id: String = row.get("event_id");
    let status: String = row.get("status");
    let input: String = row.get("input");
    let output: Option<String> = row.get("output");
    let created_at: String = row.get("created_at");
    let started_at: Option<String> = row.get("started_at");
    let ended_at: Option<String> = row.get("ended_at");
    let locked_until: Option<String> = row.get("locked_until");
    let run_after: Option<String> = row.get("run_after");

    FunctionRun {
        id: Uuid::parse_str(&id).unwrap_or_else(|_| Uuid::nil()),
        function_id: row.get("function_id"),
        event_id: Uuid::parse_str(&event_id).unwrap_or_else(|_| Uuid::nil()),
        status: RunStatus::parse(&status).unwrap_or(RunStatus::Queued),
        attempt: row.get("attempt"),
        max_attempts: row.get("max_attempts"),
        input: serde_json::from_str(&input).unwrap_or(serde_json::Value::Null),
        output: output.and_then(|s| serde_json::from_str(&s).ok()),
        error: row.get("error"),
        created_at: parse_datetime(&created_at),
        started_at: started_at.as_ref().map(|s| parse_datetime(s)),
        ended_at: ended_at.as_ref().map(|s| parse_datetime(s)),
        locked_until: locked_until.as_ref().map(|s| parse_datetime(s)),
        locked_by: row.get("locked_by"),
        concurrency_key: row.get("concurrency_key"),
        run_after: run_after.as_ref().map(|s| parse_datetime(s)),
    }
}

fn row_to_step(row: &sqlx::sqlite::SqliteRow) -> StepRun {
    let id: String = row.get("id");
    let run_id: String = row.get("run_id");
    let status: String = row.get("status");
    let input: Option<String> = row.get("input");
    let output: Option<String> = row.get("output");
    let created_at: String = row.get("created_at");
    let started_at: Option<String> = row.get("started_at");
    let ended_at: Option<String> = row.get("ended_at");

    StepRun {
        id: Uuid::parse_str(&id).unwrap_or_else(|_| Uuid::nil()),
        run_id: Uuid::parse_str(&run_id).unwrap_or_else(|_| Uuid::nil()),
        step_id: row.get("step_id"),
        status: StepStatus::parse(&status).unwrap_or(StepStatus::Pending),
        input: input.and_then(|s| serde_json::from_str(&s).ok()),
        output: output.and_then(|s| serde_json::from_str(&s).ok()),
        error: row.get("error"),
        attempt: row.get("attempt"),
        created_at: parse_datetime(&created_at),
        started_at: started_at.as_ref().map(|s| parse_datetime(s)),
        ended_at: ended_at.as_ref().map(|s| parse_datetime(s)),
    }
}

fn parse_datetime(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}
