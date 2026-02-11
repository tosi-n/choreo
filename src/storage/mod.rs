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
use dashmap::DashMap;
use std::sync::Arc;
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
    async fn try_acquire_lock(&self, key: &str, holder: &str, ttl_secs: i64) -> Result<bool>;

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
    async fn register_function(
        &self,
        function_id: &str,
        definition: &serde_json::Value,
    ) -> Result<()>;

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
    async fn extend_run_leases(&self, worker_id: &str, run_ids: &[Uuid]) -> Result<()>;

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
    async fn get_step_output(
        &self,
        run_id: Uuid,
        step_id: &str,
    ) -> Result<Option<serde_json::Value>> {
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

/// In-memory storage implementation for testing
#[derive(Debug, Default, Clone)]
pub struct MemoryStore {
    events: Arc<DashMap<Uuid, Event>>,
    runs: Arc<DashMap<Uuid, FunctionRun>>,
    steps: Arc<DashMap<(Uuid, String), StepRun>>,
    locks: Arc<DashMap<String, Lock>>,
    functions: Arc<DashMap<String, serde_json::Value>>,
    #[doc(hidden)]
    pub idempotency_keys: Arc<DashMap<String, Uuid>>,
}

impl MemoryStore {
    /// Create a new in-memory store
    pub fn new() -> Self {
        Self {
            events: Arc::new(DashMap::new()),
            runs: Arc::new(DashMap::new()),
            steps: Arc::new(DashMap::new()),
            locks: Arc::new(DashMap::new()),
            functions: Arc::new(DashMap::new()),
            idempotency_keys: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait]
impl StateStore for MemoryStore {
    async fn insert_event(&self, event: &Event) -> Result<()> {
        self.events.insert(event.id, event.clone());
        Ok(())
    }

    async fn get_event(&self, id: Uuid) -> Result<Option<Event>> {
        Ok(self.events.get(&id).map(|e| e.clone()))
    }

    async fn get_events_by_name(&self, name: &str, limit: i64, offset: i64) -> Result<Vec<Event>> {
        let all: Vec<Event> = self
            .events
            .iter()
            .filter(|e| e.value().name == name)
            .map(|e| e.clone())
            .collect();
        let offset = offset as usize;
        let limit = limit as usize;
        Ok(all.into_iter().skip(offset).take(limit).collect())
    }

    async fn check_idempotency_key(&self, key: &str) -> Result<Option<Uuid>> {
        Ok(self.idempotency_keys.get(key).map(|e| *e.value()))
    }

    async fn insert_run(&self, run: &FunctionRun) -> Result<()> {
        self.runs.insert(run.id, run.clone());
        Ok(())
    }

    async fn get_run(&self, id: Uuid) -> Result<Option<FunctionRun>> {
        Ok(self.runs.get(&id).map(|r| r.clone()))
    }

    async fn get_runs_by_event(&self, event_id: Uuid) -> Result<Vec<FunctionRun>> {
        Ok(self
            .runs
            .iter()
            .filter(|r| r.value().event_id == event_id)
            .map(|r| r.clone())
            .collect())
    }

    async fn get_runs_by_function(
        &self,
        function_id: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<FunctionRun>> {
        let all: Vec<FunctionRun> = self
            .runs
            .iter()
            .filter(|r| r.value().function_id == function_id)
            .map(|r| r.clone())
            .collect();
        let offset = offset as usize;
        let limit = limit as usize;
        Ok(all.into_iter().skip(offset).take(limit).collect())
    }

    async fn lease_runs(
        &self,
        _worker_id: &str,
        _limit: i64,
        _lock_duration_secs: i64,
    ) -> Result<Vec<FunctionRun>> {
        Ok(vec![])
    }

    async fn update_run_status(&self, id: Uuid, status: RunStatus) -> Result<()> {
        if let Some(mut run) = self.runs.get_mut(&id) {
            run.status = status;
        }
        Ok(())
    }

    async fn complete_run(&self, id: Uuid, output: serde_json::Value) -> Result<()> {
        if let Some(mut run) = self.runs.get_mut(&id) {
            run.output = Some(output);
            run.status = RunStatus::Completed;
        }
        Ok(())
    }

    async fn fail_run(&self, id: Uuid, error: &str, _can_retry: bool) -> Result<()> {
        if let Some(mut run) = self.runs.get_mut(&id) {
            run.error = Some(error.to_string());
            run.status = RunStatus::Failed;
        }
        Ok(())
    }

    async fn cancel_run(&self, id: Uuid) -> Result<()> {
        if let Some(mut run) = self.runs.get_mut(&id) {
            run.status = RunStatus::Cancelled;
        }
        Ok(())
    }

    async fn extend_run_lock(
        &self,
        _id: Uuid,
        _worker_id: &str,
        _lock_duration_secs: i64,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn release_run_lock(&self, _id: Uuid, _worker_id: &str) -> Result<()> {
        Ok(())
    }

    async fn get_stale_runs(&self, _limit: i64) -> Result<Vec<FunctionRun>> {
        Ok(vec![])
    }

    async fn retry_run(&self, id: Uuid, run_after: Option<DateTime<Utc>>) -> Result<()> {
        if let Some(mut run) = self.runs.get_mut(&id) {
            run.status = RunStatus::Queued;
            run.attempt += 1;
            run.run_after = run_after;
        }
        Ok(())
    }

    async fn get_step(&self, run_id: Uuid, step_id: &str) -> Result<Option<StepRun>> {
        Ok(self
            .steps
            .get(&(run_id, step_id.to_string()))
            .map(|s| s.clone()))
    }

    async fn get_steps_for_run(&self, run_id: Uuid) -> Result<Vec<StepRun>> {
        Ok(self
            .steps
            .iter()
            .filter(|s| s.key().0 == run_id)
            .map(|s| s.clone())
            .collect())
    }

    async fn upsert_step(&self, step: &StepRun) -> Result<()> {
        self.steps
            .insert((step.run_id, step.step_id.clone()), step.clone());
        Ok(())
    }

    async fn start_step(&self, run_id: Uuid, step_id: &str) -> Result<()> {
        if let Some(mut step) = self.steps.get_mut(&(run_id, step_id.to_string())) {
            step.status = StepStatus::Running;
            step.started_at = Some(Utc::now());
        }
        Ok(())
    }

    async fn complete_step(
        &self,
        run_id: Uuid,
        step_id: &str,
        output: serde_json::Value,
    ) -> Result<()> {
        if let Some(mut step) = self.steps.get_mut(&(run_id, step_id.to_string())) {
            step.output = Some(output);
            step.status = StepStatus::Completed;
        }
        Ok(())
    }

    async fn fail_step(&self, run_id: Uuid, step_id: &str, error: &str) -> Result<()> {
        if let Some(mut step) = self.steps.get_mut(&(run_id, step_id.to_string())) {
            step.status = StepStatus::Failed;
            step.error = Some(error.to_string());
            step.ended_at = Some(Utc::now());
        }
        Ok(())
    }

    async fn try_acquire_lock(&self, key: &str, holder: &str, ttl_secs: i64) -> Result<bool> {
        if self.locks.contains_key(key) {
            return Ok(false);
        }
        let lock = Lock {
            key: key.to_string(),
            holder: holder.to_string(),
            expires_at: Utc::now() + chrono::Duration::seconds(ttl_secs),
            acquired_at: Utc::now(),
        };
        self.locks.insert(key.to_string(), lock);
        Ok(true)
    }

    async fn release_lock(&self, key: &str, holder: &str) -> Result<bool> {
        // Avoid holding a map guard while calling remove on the same key.
        // DashMap can deadlock when mutating a shard while a read guard is alive.
        let can_release = self
            .locks
            .get(key)
            .map(|lock| lock.holder == holder)
            .unwrap_or(false);
        if can_release {
            self.locks.remove(key);
            return Ok(true);
        }
        Ok(false)
    }

    async fn extend_lock(&self, _key: &str, _holder: &str, _ttl_secs: i64) -> Result<bool> {
        Ok(true)
    }

    async fn get_lock(&self, key: &str) -> Result<Option<Lock>> {
        Ok(self.locks.get(key).map(|l| l.clone()))
    }

    async fn register_function(
        &self,
        function_id: &str,
        definition: &serde_json::Value,
    ) -> Result<()> {
        self.functions
            .insert(function_id.to_string(), definition.clone());
        Ok(())
    }

    async fn list_functions(&self) -> Result<Vec<serde_json::Value>> {
        Ok(self.functions.iter().map(|f| f.value().clone()).collect())
    }

    async fn get_function(&self, function_id: &str) -> Result<Option<serde_json::Value>> {
        Ok(self.functions.get(function_id).map(|f| f.clone()))
    }

    async fn get_functions_for_event(&self, _event_name: &str) -> Result<Vec<serde_json::Value>> {
        Ok(vec![])
    }

    async fn extend_run_leases(&self, _worker_id: &str, _run_ids: &[Uuid]) -> Result<()> {
        Ok(())
    }

    async fn ping(&self) -> Result<()> {
        Ok(())
    }

    async fn migrate(&self) -> Result<()> {
        Ok(())
    }

    async fn cleanup_old_runs(&self, _older_than: DateTime<Utc>) -> Result<u64> {
        Ok(0)
    }

    async fn cleanup_expired_locks(&self) -> Result<u64> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_memory_store_new() {
        let store = MemoryStore::new();
        assert!(store.ping().await.is_ok());
    }

    #[tokio::test]
    async fn test_memory_store_insert_event() {
        let store = MemoryStore::new();
        let event = Event::new("test-event".to_string(), json!({"key": "value"}));
        store.insert_event(&event).await.unwrap();
        let retrieved = store.get_event(event.id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "test-event");
    }

    #[tokio::test]
    async fn test_memory_store_get_events_by_name() {
        let store = MemoryStore::new();
        store
            .insert_event(&Event::new("user.created".to_string(), json!({})))
            .await
            .unwrap();
        store
            .insert_event(&Event::new("user.created".to_string(), json!({})))
            .await
            .unwrap();
        store
            .insert_event(&Event::new("user.deleted".to_string(), json!({})))
            .await
            .unwrap();

        let events = store
            .get_events_by_name("user.created", 10, 0)
            .await
            .unwrap();
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn test_memory_store_insert_run() {
        let store = MemoryStore::new();
        let run = FunctionRun::new(
            "test-function".to_string(),
            Uuid::new_v4(),
            json!({"input": "data"}),
        );
        store.insert_run(&run).await.unwrap();
        let retrieved = store.get_run(run.id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().function_id, "test-function");
    }

    #[tokio::test]
    async fn test_memory_store_update_run_status() {
        let store = MemoryStore::new();
        let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
        store.insert_run(&run).await.unwrap();

        store
            .update_run_status(run.id, RunStatus::Running)
            .await
            .unwrap();
        let updated = store.get_run(run.id).await.unwrap().unwrap();
        assert_eq!(updated.status, RunStatus::Running);
    }

    #[tokio::test]
    async fn test_memory_store_list_runs() {
        let store = MemoryStore::new();
        let event_id = Uuid::new_v4();
        store
            .insert_run(&FunctionRun::new("func1".to_string(), event_id, json!({})))
            .await
            .unwrap();
        store
            .insert_run(&FunctionRun::new("func1".to_string(), event_id, json!({})))
            .await
            .unwrap();

        let runs = store.get_runs_by_function("func1", 10, 0).await.unwrap();
        assert_eq!(runs.len(), 2);
    }

    #[tokio::test]
    async fn test_memory_store_save_and_get_step() {
        let store = MemoryStore::new();
        let run_id = Uuid::new_v4();
        let step = StepRun::new(run_id, "test-step");
        store.upsert_step(&step).await.unwrap();

        let retrieved = store.get_step(run_id, "test-step").await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().step_id, "test-step");
    }

    #[tokio::test]
    async fn test_memory_store_upsert_step() {
        let store = MemoryStore::new();
        let run_id = Uuid::new_v4();
        let step = StepRun::new(run_id, "step-to-update");

        store.upsert_step(&step).await.unwrap();
        assert!(store
            .get_step(run_id, "step-to-update")
            .await
            .unwrap()
            .is_some());

        let mut updated_step = step.clone();
        updated_step.status = StepStatus::Running;
        store.upsert_step(&updated_step).await.unwrap();

        let retrieved = store
            .get_step(run_id, "step-to-update")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved.status, StepStatus::Running);
    }

    #[tokio::test]
    async fn test_memory_store_complete_step() {
        let store = MemoryStore::new();
        let run_id = Uuid::new_v4();
        let step = StepRun::new(run_id, "step-to-complete");
        store.upsert_step(&step).await.unwrap();

        store
            .complete_step(run_id, "step-to-complete", json!({"result": "success"}))
            .await
            .unwrap();

        let retrieved = store
            .get_step(run_id, "step-to-complete")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved.status, StepStatus::Completed);
        assert_eq!(retrieved.output, Some(json!({"result": "success"})));
    }

    #[tokio::test]
    async fn test_memory_store_fail_step() {
        let store = MemoryStore::new();
        let run_id = Uuid::new_v4();
        let step = StepRun::new(run_id, "failing-step");
        store.upsert_step(&step).await.unwrap();

        store
            .fail_step(run_id, "failing-step", "something went wrong")
            .await
            .unwrap();

        let retrieved = store
            .get_step(run_id, "failing-step")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved.status, StepStatus::Failed);
    }

    #[tokio::test]
    async fn test_memory_store_get_steps_for_run() {
        let store = MemoryStore::new();
        let run_id = Uuid::new_v4();
        store
            .upsert_step(&StepRun::new(run_id, "step1"))
            .await
            .unwrap();
        store
            .upsert_step(&StepRun::new(run_id, "step2"))
            .await
            .unwrap();
        store
            .upsert_step(&StepRun::new(Uuid::new_v4(), "other-step"))
            .await
            .unwrap();

        let steps = store.get_steps_for_run(run_id).await.unwrap();
        assert_eq!(steps.len(), 2);
    }

    #[tokio::test]
    async fn test_memory_store_acquire_lock() {
        let store = MemoryStore::new();
        let lock = store
            .try_acquire_lock("test-lock", "worker-1", 30)
            .await
            .unwrap();
        assert!(lock);
        assert!(store.get_lock("test-lock").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_memory_store_lock_contention() {
        let store = MemoryStore::new();
        let _lock1 = store
            .try_acquire_lock("contested-lock", "worker-1", 30)
            .await
            .unwrap();
        let lock2 = store
            .try_acquire_lock("contested-lock", "worker-2", 30)
            .await
            .unwrap();
        assert!(!lock2);
    }

    #[tokio::test]
    async fn test_memory_store_extend_lock() {
        let store = MemoryStore::new();
        store
            .try_acquire_lock("extendable-lock", "worker-1", 10)
            .await
            .unwrap();
        let extended = store
            .extend_lock("extendable-lock", "worker-1", 30)
            .await
            .unwrap();
        assert!(extended);
    }

    #[tokio::test]
    async fn test_memory_store_release_lock() {
        let store = MemoryStore::new();
        store
            .try_acquire_lock("release-me", "worker-1", 30)
            .await
            .unwrap();
        let released = store.release_lock("release-me", "worker-1").await.unwrap();
        assert!(released);
        assert!(store.get_lock("release-me").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_memory_store_release_lock_wrong_owner() {
        let store = MemoryStore::new();
        store
            .try_acquire_lock("locked", "worker-1", 30)
            .await
            .unwrap();
        let released = store.release_lock("locked", "worker-2").await.unwrap();
        assert!(!released);
    }

    #[tokio::test]
    async fn test_memory_store_register_function() {
        let store = MemoryStore::new();
        let definition = json!({"name": "test", "steps": []});
        store
            .register_function("test-function", &definition)
            .await
            .unwrap();
        let retrieved = store.get_function("test-function").await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap()["name"], "test");
    }

    #[tokio::test]
    async fn test_memory_store_list_functions() {
        let store = MemoryStore::new();
        store.register_function("func1", &json!({})).await.unwrap();
        store.register_function("func2", &json!({})).await.unwrap();

        let funcs = store.list_functions().await.unwrap();
        assert_eq!(funcs.len(), 2);
    }

    #[tokio::test]
    async fn test_memory_store_check_idempotency_key() {
        let store = MemoryStore::new();
        let run_id = Uuid::new_v4();
        store
            .idempotency_keys
            .insert("idemp-key".to_string(), run_id);

        let retrieved = store.check_idempotency_key("idemp-key").await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), run_id);
    }

    #[tokio::test]
    async fn test_memory_store_cleanup_old_runs() {
        let store = MemoryStore::new();
        let cleaned = store.cleanup_old_runs(chrono::Utc::now()).await.unwrap();
        assert_eq!(cleaned, 0);
    }

    #[tokio::test]
    async fn test_memory_store_cleanup_expired_locks() {
        let store = MemoryStore::new();
        let cleaned = store.cleanup_expired_locks().await.unwrap();
        assert_eq!(cleaned, 0);
    }

    #[tokio::test]
    async fn test_memory_store_clone() {
        let store = MemoryStore::new();
        let cloned = store.clone();
        assert!(cloned.ping().await.is_ok());
    }

    #[tokio::test]
    async fn test_state_store_ext_get_step_output() {
        let store = MemoryStore::new();
        let run_id = Uuid::new_v4();
        let mut step = StepRun::new(run_id, "output-step");
        step.status = StepStatus::Completed;
        step.output = Some(json!({"data": "value"}));
        store.upsert_step(&step).await.unwrap();

        let output = store.get_step_output(run_id, "output-step").await.unwrap();
        assert!(output.is_some());
        assert_eq!(output.unwrap(), json!({"data": "value"}));
    }

    #[tokio::test]
    async fn test_state_store_ext_get_step_output_not_completed() {
        let store = MemoryStore::new();
        let run_id = Uuid::new_v4();
        let step = StepRun::new(run_id, "pending-step");
        store.upsert_step(&step).await.unwrap();

        let output = store.get_step_output(run_id, "pending-step").await.unwrap();
        assert!(output.is_none());
    }

    #[tokio::test]
    async fn test_state_store_ext_process_event() {
        let store = MemoryStore::new();
        let event = Event::new("test.event".to_string(), json!({"key": "value"}));
        let run_ids = store
            .process_event(event, &["func1".to_string(), "func2".to_string()])
            .await
            .unwrap();
        assert_eq!(run_ids.len(), 2);
    }
}
