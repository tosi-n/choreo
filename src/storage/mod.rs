//! BYO Database abstraction layer
//!
//! Implement the `StateStore` trait for any database backend.
//! Choreo ships with PostgreSQL and SQLite implementations.

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "sqlite")]
pub mod sqlite;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::error::Result;
use crate::models::{Event, FunctionRun, Lock, RunStatus, StepRun, StepStatus};

/// Core storage trait - implement this for any database backend
///
/// This trait defines all database operations needed by Choreo.
/// Implementations should be thread-safe and support concurrent access.
///
/// # Example
///
/// ```ignore
/// use choreo::storage::StateStore;
///
/// // Use PostgreSQL
/// let store = PostgresStore::connect("postgres://localhost/choreo").await?;
///
/// // Or SQLite for development/embedded
/// let store = SqliteStore::connect("sqlite://choreo.db").await?;
///
/// // Or implement your own
/// struct MyCustomStore { /* ... */ }
/// impl StateStore for MyCustomStore { /* ... */ }
/// ```
#[async_trait]
pub trait StateStore: Send + Sync + 'static {
    // =========================================================================
    // Event Operations
    // =========================================================================

    /// Insert a new event
    async fn insert_event(&self, event: &Event) -> Result<()>;

    /// Get an event by ID
    async fn get_event(&self, id: Uuid) -> Result<Option<Event>>;

    /// Get events by name with pagination
    async fn get_events_by_name(&self, name: &str, limit: i64, offset: i64) -> Result<Vec<Event>>;

    /// Check idempotency key and return existing event ID if found
    async fn check_idempotency_key(&self, key: &str) -> Result<Option<Uuid>>;

    // =========================================================================
    // Function Run Operations
    // =========================================================================

    /// Insert a new function run
    async fn insert_run(&self, run: &FunctionRun) -> Result<()>;

    /// Get a function run by ID
    async fn get_run(&self, id: Uuid) -> Result<Option<FunctionRun>>;

    /// Get runs by event ID
    async fn get_runs_by_event(&self, event_id: Uuid) -> Result<Vec<FunctionRun>>;

    /// Get runs by function ID with pagination
    async fn get_runs_by_function(
        &self,
        function_id: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<FunctionRun>>;

    /// Lease pending runs for execution (atomic operation)
    ///
    /// This should atomically:
    /// 1. Find runs that are queued and ready to execute
    /// 2. Update their status to running and set lock
    /// 3. Return the leased runs
    ///
    /// Uses database-specific locking (e.g., FOR UPDATE SKIP LOCKED in PostgreSQL)
    async fn lease_runs(
        &self,
        worker_id: &str,
        limit: i64,
        lock_duration_secs: i64,
    ) -> Result<Vec<FunctionRun>>;

    /// Update run status
    async fn update_run_status(&self, id: Uuid, status: RunStatus) -> Result<()>;

    /// Complete a run successfully
    async fn complete_run(&self, id: Uuid, output: serde_json::Value) -> Result<()>;

    /// Fail a run
    async fn fail_run(&self, id: Uuid, error: &str, can_retry: bool) -> Result<()>;

    /// Cancel a run
    async fn cancel_run(&self, id: Uuid) -> Result<()>;

    /// Extend lock on a run
    async fn extend_run_lock(
        &self,
        id: Uuid,
        worker_id: &str,
        lock_duration_secs: i64,
    ) -> Result<bool>;

    /// Release lock on a run (on worker shutdown)
    async fn release_run_lock(&self, id: Uuid, worker_id: &str) -> Result<()>;

    /// Get runs with expired locks (for recovery)
    async fn get_stale_runs(&self, limit: i64) -> Result<Vec<FunctionRun>>;

    /// Increment attempt count and reset status to queued for retry
    async fn retry_run(&self, id: Uuid, run_after: Option<DateTime<Utc>>) -> Result<()>;

    // =========================================================================
    // Step Operations (Durable Execution)
    // =========================================================================

    /// Get a step by run ID and step ID
    async fn get_step(&self, run_id: Uuid, step_id: &str) -> Result<Option<StepRun>>;

    /// Get all steps for a run
    async fn get_steps_for_run(&self, run_id: Uuid) -> Result<Vec<StepRun>>;

    /// Insert or update a step
    async fn upsert_step(&self, step: &StepRun) -> Result<()>;

    /// Mark step as started
    async fn start_step(&self, run_id: Uuid, step_id: &str) -> Result<()>;

    /// Complete a step with output
    async fn complete_step(
        &self,
        run_id: Uuid,
        step_id: &str,
        output: serde_json::Value,
    ) -> Result<()>;

    /// Fail a step
    async fn fail_step(&self, run_id: Uuid, step_id: &str, error: &str) -> Result<()>;

    // =========================================================================
    // Distributed Locking
    // =========================================================================

    /// Try to acquire a distributed lock
    ///
    /// Returns true if lock was acquired, false if already held by another
    async fn try_acquire_lock(
        &self,
        key: &str,
        holder: &str,
        ttl_secs: i64,
    ) -> Result<bool>;

    /// Release a distributed lock
    ///
    /// Only releases if holder matches
    async fn release_lock(&self, key: &str, holder: &str) -> Result<bool>;

    /// Extend a lock's TTL
    ///
    /// Only extends if holder matches
    async fn extend_lock(&self, key: &str, holder: &str, ttl_secs: i64) -> Result<bool>;

    /// Get lock info
    async fn get_lock(&self, key: &str) -> Result<Option<Lock>>;

    // =========================================================================
    // Function Registry
    // =========================================================================

    /// Register a function definition
    async fn register_function(&self, function_id: &str, definition: &serde_json::Value) -> Result<()>;

    /// List all registered functions
    async fn list_functions(&self) -> Result<Vec<serde_json::Value>>;

    /// Get a function definition
    async fn get_function(&self, function_id: &str) -> Result<Option<serde_json::Value>>;

    /// Get functions triggered by an event name
    async fn get_functions_for_event(&self, event_name: &str) -> Result<Vec<serde_json::Value>>;

    // =========================================================================
    // Worker Management
    // =========================================================================

    /// Extend leases on multiple runs for a worker (heartbeat)
    async fn extend_run_leases(
        &self,
        worker_id: &str,
        run_ids: &[Uuid],
    ) -> Result<()>;

    // =========================================================================
    // Maintenance
    // =========================================================================

    /// Health check - verify database connectivity
    async fn ping(&self) -> Result<()>;

    /// Run database migrations
    async fn migrate(&self) -> Result<()>;

    /// Clean up old completed/failed runs
    async fn cleanup_old_runs(&self, older_than: DateTime<Utc>) -> Result<u64>;

    /// Clean up expired locks
    async fn cleanup_expired_locks(&self) -> Result<u64>;
}

/// Extension trait for StateStore with convenience methods
#[async_trait]
pub trait StateStoreExt: StateStore {
    /// Insert event and get runs it triggers
    async fn process_event(&self, event: Event, function_ids: &[String]) -> Result<Vec<Uuid>> {
        self.insert_event(&event).await?;

        let mut run_ids = Vec::with_capacity(function_ids.len());
        for function_id in function_ids {
            let run = FunctionRun::new(function_id.clone(), event.id, event.data.clone());
            self.insert_run(&run).await?;
            run_ids.push(run.id);
        }

        Ok(run_ids)
    }

    /// Get step output if completed, for durable execution replay
    async fn get_step_output(&self, run_id: Uuid, step_id: &str) -> Result<Option<serde_json::Value>> {
        if let Some(step) = self.get_step(run_id, step_id).await? {
            if step.status == StepStatus::Completed {
                return Ok(step.output);
            }
        }
        Ok(None)
    }
}

// Blanket implementation
impl<T: StateStore> StateStoreExt for T {}
