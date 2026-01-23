//! StepContext - Durable step execution with automatic replay
//!
//! The StepContext provides the core durable execution primitive.
//! Each step is:
//! 1. Checked for existing output (replay on restart)
//! 2. Executed if not cached
//! 3. Persisted before returning
//!
//! This ensures functions can resume from the last successful step
//! after crashes, restarts, or failures.

use serde::{de::DeserializeOwned, Serialize};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::models::{StepRun, StepStatus};
use crate::storage::StateStore;

/// Errors that can occur during step execution
#[derive(Error, Debug)]
pub enum StepError {
    #[error("Step execution failed: {0}")]
    ExecutionFailed(String),

    #[error("Step timed out after {0:?}")]
    Timeout(Duration),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Step was cancelled")]
    Cancelled,
}

impl From<crate::error::ChoreoError> for StepError {
    fn from(e: crate::error::ChoreoError) -> Self {
        StepError::Storage(e.to_string())
    }
}

/// Context for executing durable steps within a function run
pub struct StepContext<S: StateStore> {
    run_id: Uuid,
    store: S,
    /// Track executed steps in this run for ordering
    executed_steps: Arc<Mutex<Vec<String>>>,
}

impl<S: StateStore + Clone> StepContext<S> {
    /// Create a new step context for a run
    pub fn new(run_id: Uuid, store: S) -> Self {
        Self {
            run_id,
            store,
            executed_steps: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Execute a step with durable caching
    ///
    /// If the step was already completed in a previous attempt,
    /// returns the cached output. Otherwise executes the function
    /// and persists the result.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let user = step.run("fetch-user", || async {
    ///     fetch_user_from_db(user_id).await
    /// }).await?;
    /// ```
    pub async fn run<F, Fut, T>(&self, step_id: &str, func: F) -> Result<T, StepError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, StepError>>,
        T: Serialize + DeserializeOwned,
    {
        // Check if step already completed (replay)
        if let Some(step) = self.store.get_step(self.run_id, step_id).await? {
            if step.status == StepStatus::Completed {
                if let Some(output) = step.output {
                    debug!(run_id = %self.run_id, step_id = %step_id, "Replaying cached step");
                    let value: T = serde_json::from_value(output)?;
                    return Ok(value);
                }
            }
        }

        info!(run_id = %self.run_id, step_id = %step_id, "Executing step");

        // Create/update step as running
        let mut step = StepRun::new(self.run_id, step_id);
        step.status = StepStatus::Running;
        step.started_at = Some(chrono::Utc::now());
        self.store.upsert_step(&step).await?;

        // Execute the step
        match func().await {
            Ok(result) => {
                let output = serde_json::to_value(&result)?;
                self.store
                    .complete_step(self.run_id, step_id, output)
                    .await?;

                // Track execution order
                self.executed_steps.lock().await.push(step_id.to_string());

                info!(run_id = %self.run_id, step_id = %step_id, "Step completed");
                Ok(result)
            }
            Err(e) => {
                self.store
                    .fail_step(self.run_id, step_id, &e.to_string())
                    .await?;
                warn!(run_id = %self.run_id, step_id = %step_id, error = %e, "Step failed");
                Err(e)
            }
        }
    }

    /// Execute a step with timeout
    pub async fn run_with_timeout<F, Fut, T>(
        &self,
        step_id: &str,
        timeout: Duration,
        func: F,
    ) -> Result<T, StepError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, StepError>>,
        T: Serialize + DeserializeOwned,
    {
        // Check cache first
        if let Some(step) = self.store.get_step(self.run_id, step_id).await? {
            if step.status == StepStatus::Completed {
                if let Some(output) = step.output {
                    let value: T = serde_json::from_value(output)?;
                    return Ok(value);
                }
            }
        }

        // Execute with timeout
        match tokio::time::timeout(timeout, self.run(step_id, func)).await {
            Ok(result) => result,
            Err(_) => {
                self.store
                    .fail_step(
                        self.run_id,
                        step_id,
                        &format!("Timeout after {:?}", timeout),
                    )
                    .await?;
                Err(StepError::Timeout(timeout))
            }
        }
    }

    /// Sleep for a duration (durable - survives restarts)
    ///
    /// The sleep is persisted, so if the function restarts,
    /// it will skip already-elapsed sleep time.
    pub async fn sleep(&self, step_id: &str, duration: Duration) -> Result<(), StepError> {
        self.run(step_id, || async move {
            tokio::time::sleep(duration).await;
            Ok(())
        })
        .await
    }

    /// Sleep until a specific timestamp (durable)
    pub async fn sleep_until(
        &self,
        step_id: &str,
        until: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StepError> {
        self.run(step_id, || async move {
            let now = chrono::Utc::now();
            if until > now {
                let duration = (until - now).to_std().unwrap_or(Duration::ZERO);
                tokio::time::sleep(duration).await;
            }
            Ok(())
        })
        .await
    }

    /// Send an event from within a function (for fan-out patterns)
    pub async fn send_event(
        &self,
        step_id: &str,
        event_name: &str,
        data: serde_json::Value,
    ) -> Result<Uuid, StepError> {
        let event_name = event_name.to_string();
        self.run(step_id, || async move {
            let event = crate::models::Event::new(event_name, data);
            // Note: In a real implementation, this would insert via store
            // and trigger any matching functions
            Ok(event.id)
        })
        .await
    }

    /// Invoke another function and wait for result
    pub async fn invoke<T: DeserializeOwned + Serialize>(
        &self,
        step_id: &str,
        _function_id: &str,
        _input: serde_json::Value,
    ) -> Result<T, StepError> {
        // This is a placeholder - full implementation would:
        // 1. Create a new run for the target function
        // 2. Wait for it to complete
        // 3. Return the result
        self.run(step_id, || async move {
            Err(StepError::ExecutionFailed(
                "invoke() not yet implemented".to_string(),
            ))
        })
        .await
    }

    /// Get all completed steps for this run
    pub async fn get_completed_steps(&self) -> Result<Vec<StepRun>, StepError> {
        let steps = self.store.get_steps_for_run(self.run_id).await?;
        Ok(steps
            .into_iter()
            .filter(|s| s.status == StepStatus::Completed)
            .collect())
    }

    /// Get the run ID
    pub fn run_id(&self) -> Uuid {
        self.run_id
    }
}

impl<S: StateStore> Clone for StepContext<S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            run_id: self.run_id,
            store: self.store.clone(),
            executed_steps: self.executed_steps.clone(),
        }
    }
}
