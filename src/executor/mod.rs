//! Executor - Core durable execution engine
//!
//! The executor is responsible for:
//! - Polling for pending runs
//! - Executing functions with durable step context
//! - Handling retries with exponential backoff
//! - Managing locks and heartbeats

mod step;

pub use step::{StepContext, StepError};

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Semaphore};
use tokio::time::interval;
use tracing::{debug, error, info, warn, Instrument};
use uuid::Uuid;

use crate::config::WorkerConfig;
use crate::error::Result;
use crate::models::FunctionRun;
use crate::registry::Registry;
use crate::storage::StateStore;

/// Executor configuration
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Unique worker ID
    pub worker_id: String,
    /// Poll interval for new work
    pub poll_interval: Duration,
    /// Number of runs to lease at once
    pub batch_size: i64,
    /// Lock duration in seconds
    pub lock_duration_secs: i64,
    /// Heartbeat interval (should be < lock_duration)
    pub heartbeat_interval: Duration,
    /// Maximum concurrent runs
    pub max_concurrent: usize,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            worker_id: format!("worker-{}", Uuid::now_v7()),
            poll_interval: Duration::from_secs(1),
            batch_size: 10,
            lock_duration_secs: 300,
            heartbeat_interval: Duration::from_secs(60),
            max_concurrent: 10,
        }
    }
}

impl From<WorkerConfig> for ExecutorConfig {
    fn from(c: WorkerConfig) -> Self {
        let poll_interval = c.poll_interval();
        let heartbeat_interval = Duration::from_secs(c.lock_duration_secs as u64 / 3);
        Self {
            worker_id: c.worker_id,
            poll_interval,
            batch_size: c.batch_size,
            lock_duration_secs: c.lock_duration_secs,
            heartbeat_interval,
            max_concurrent: c.max_concurrent,
        }
    }
}

/// The main executor that processes function runs
pub struct Executor<S: StateStore> {
    store: S,
    registry: Arc<Registry<S>>,
    config: ExecutorConfig,
    shutdown_tx: broadcast::Sender<()>,
    concurrency_semaphore: Arc<Semaphore>,
}

impl<S: StateStore + Clone + 'static> Executor<S> {
    /// Create a new executor
    pub fn new(store: S, registry: Arc<Registry<S>>, config: ExecutorConfig) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        let concurrency_semaphore = Arc::new(Semaphore::new(config.max_concurrent));

        Self {
            store,
            registry,
            config,
            shutdown_tx,
            concurrency_semaphore,
        }
    }

    /// Get a shutdown handle to stop the executor
    pub fn shutdown_handle(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
    }

    /// Run the executor loop
    pub async fn run(&self) -> Result<()> {
        info!(worker_id = %self.config.worker_id, "Starting executor");

        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let mut poll_interval = interval(self.config.poll_interval);

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Executor received shutdown signal");
                    break;
                }
                _ = poll_interval.tick() => {
                    if let Err(e) = self.poll_and_execute().await {
                        error!(error = %e, "Error in poll cycle");
                    }
                }
            }
        }

        info!("Executor stopped");
        Ok(())
    }

    /// Poll for pending runs and execute them
    async fn poll_and_execute(&self) -> Result<()> {
        // Check available permits
        let available = self.concurrency_semaphore.available_permits();
        if available == 0 {
            debug!("No available permits, skipping poll");
            return Ok(());
        }

        let batch_size = std::cmp::min(available as i64, self.config.batch_size);

        // Lease runs
        let runs = self
            .store
            .lease_runs(
                &self.config.worker_id,
                batch_size,
                self.config.lock_duration_secs,
            )
            .await?;

        if runs.is_empty() {
            return Ok(());
        }

        debug!(count = runs.len(), "Leased runs for execution");

        // Spawn execution tasks
        for run in runs {
            let permit = self
                .concurrency_semaphore
                .clone()
                .acquire_owned()
                .await
                .unwrap();
            let store = self.store.clone();
            let registry = self.registry.clone();
            let config = self.config.clone();

            tokio::spawn(async move {
                let _permit = permit; // Hold permit until task completes
                let run_id = run.id;
                let span = tracing::info_span!("execute_run", run_id = %run_id);

                if let Err(e) = execute_run(store, registry, config, run)
                    .instrument(span)
                    .await
                {
                    error!(run_id = %run_id, error = %e, "Run execution failed");
                }
            });
        }

        Ok(())
    }

    /// Process a single run (useful for testing or manual execution)
    pub async fn execute_single(&self, run_id: Uuid) -> Result<()> {
        let run = self
            .store
            .get_run(run_id)
            .await?
            .ok_or(crate::error::ChoreoError::RunNotFound { id: run_id })?;

        execute_run(
            self.store.clone(),
            self.registry.clone(),
            self.config.clone(),
            run,
        )
        .await
    }
}

/// Execute a single function run
async fn execute_run<S: StateStore + Clone>(
    store: S,
    registry: Arc<Registry<S>>,
    config: ExecutorConfig,
    run: FunctionRun,
) -> Result<()> {
    let run_id = run.id;
    info!(run_id = %run_id, function = %run.function_id, attempt = run.attempt, "Executing run");

    // Look up the function handler
    let handler = match registry.get_handler(&run.function_id) {
        Some(h) => h,
        None => {
            error!(run_id = %run_id, function = %run.function_id, "Function not registered");
            store
                .fail_run(run_id, "Function not registered", false)
                .await?;
            return Ok(());
        }
    };

    // Create step context for durable execution
    let step_ctx = StepContext::new(run_id, store.clone());

    // Spawn heartbeat task to keep lock alive
    let heartbeat_store = store.clone();
    let heartbeat_worker_id = config.worker_id.clone();
    let heartbeat_interval = config.heartbeat_interval;
    let lock_duration = config.lock_duration_secs;
    let (heartbeat_tx, mut heartbeat_rx) = tokio::sync::oneshot::channel::<()>();

    let heartbeat_handle = tokio::spawn(async move {
        let mut interval = interval(heartbeat_interval);
        loop {
            tokio::select! {
                _ = &mut heartbeat_rx => break,
                _ = interval.tick() => {
                    if let Err(e) = heartbeat_store
                        .extend_run_lock(run_id, &heartbeat_worker_id, lock_duration)
                        .await
                    {
                        warn!(run_id = %run_id, error = %e, "Failed to extend lock");
                        break;
                    }
                    debug!(run_id = %run_id, "Extended lock");
                }
            }
        }
    });

    // Execute the function
    let result = handler.call(run.clone(), step_ctx).await;

    // Stop heartbeat
    let _ = heartbeat_tx.send(());
    let _ = heartbeat_handle.await;

    // Handle result
    match result {
        Ok(output) => {
            info!(run_id = %run_id, "Run completed successfully");
            store.complete_run(run_id, output).await?;
        }
        Err(e) => {
            let can_retry = run.attempt < run.max_attempts;
            error!(
                run_id = %run_id,
                error = %e,
                attempt = run.attempt,
                max_attempts = run.max_attempts,
                can_retry = can_retry,
                "Run failed"
            );

            if can_retry {
                // Calculate backoff delay
                let delay = calculate_backoff(run.attempt, 1000, 60000, 2.0);
                let run_after = chrono::Utc::now() + chrono::Duration::milliseconds(delay as i64);
                store.fail_run(run_id, &e.to_string(), true).await?;
                store.retry_run(run_id, Some(run_after)).await?;
            } else {
                store.fail_run(run_id, &e.to_string(), false).await?;
            }
        }
    }

    Ok(())
}

/// Calculate exponential backoff with jitter
fn calculate_backoff(attempt: i32, initial_ms: u64, max_ms: u64, multiplier: f64) -> u64 {
    let base_delay = initial_ms as f64 * multiplier.powi(attempt - 1);
    let capped = base_delay.min(max_ms as f64);

    // Add jitter (±25%)
    let jitter = capped * 0.25 * (rand_simple() * 2.0 - 1.0);
    ((capped + jitter) as u64).max(initial_ms)
}

/// Simple pseudo-random for jitter (no external crate needed)
fn rand_simple() -> f64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    (nanos as f64) / (u32::MAX as f64)
}
