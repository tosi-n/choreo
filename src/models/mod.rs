//! Domain models for Choreo workflow orchestration

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Status of a function run
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Waiting to be picked up by executor
    Queued,
    /// Currently executing
    Running,
    /// Successfully completed
    Completed,
    /// Failed after all retries exhausted
    Failed,
    /// Manually cancelled
    Cancelled,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "queued" => Some(Self::Queued),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Status of an individual step within a run
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    /// Step is pending execution
    Pending,
    /// Step is currently running
    Running,
    /// Step completed successfully
    Completed,
    /// Step failed (may be retried)
    Failed,
    /// Step was skipped (conditional logic)
    Skipped,
}

impl StepStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "skipped" => Some(Self::Skipped),
            _ => None,
        }
    }
}

/// An event that triggers function runs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Unique event ID (UUIDv7 for time-ordering)
    pub id: Uuid,
    /// Event name/type (e.g., "user.created", "order.placed")
    pub name: String,
    /// Event payload
    pub data: serde_json::Value,
    /// Optional idempotency key to prevent duplicate processing
    pub idempotency_key: Option<String>,
    /// Timestamp when event was received
    pub timestamp: DateTime<Utc>,
    /// Optional user/tenant context
    pub user_id: Option<String>,
}

impl Event {
    pub fn new(name: impl Into<String>, data: serde_json::Value) -> Self {
        Self {
            id: Uuid::now_v7(),
            name: name.into(),
            data,
            idempotency_key: None,
            timestamp: Utc::now(),
            user_id: None,
        }
    }

    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }
}

/// A function run represents a single execution of a workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionRun {
    /// Unique run ID
    pub id: Uuid,
    /// Function identifier (e.g., "process-order", "send-welcome-email")
    pub function_id: String,
    /// Event that triggered this run
    pub event_id: Uuid,
    /// Current status
    pub status: RunStatus,
    /// Current attempt number (1-indexed)
    pub attempt: i32,
    /// Maximum retry attempts
    pub max_attempts: i32,
    /// Input data for the function
    pub input: serde_json::Value,
    /// Output data (set on completion)
    pub output: Option<serde_json::Value>,
    /// Error message (set on failure)
    pub error: Option<String>,
    /// When the run was created
    pub created_at: DateTime<Utc>,
    /// When execution started
    pub started_at: Option<DateTime<Utc>>,
    /// When execution completed/failed
    pub ended_at: Option<DateTime<Utc>>,
    /// Lock expiration (for distributed execution)
    pub locked_until: Option<DateTime<Utc>>,
    /// Worker ID that holds the lock
    pub locked_by: Option<String>,
    /// Optional concurrency key for limiting parallel executions
    pub concurrency_key: Option<String>,
    /// Optional scheduling - run after this time
    pub run_after: Option<DateTime<Utc>>,
}

impl FunctionRun {
    pub fn new(function_id: impl Into<String>, event_id: Uuid, input: serde_json::Value) -> Self {
        Self {
            id: Uuid::now_v7(),
            function_id: function_id.into(),
            event_id,
            status: RunStatus::Queued,
            attempt: 0,
            max_attempts: 3,
            input,
            output: None,
            error: None,
            created_at: Utc::now(),
            started_at: None,
            ended_at: None,
            locked_until: None,
            locked_by: None,
            concurrency_key: None,
            run_after: None,
        }
    }

    pub fn with_max_attempts(mut self, max: i32) -> Self {
        self.max_attempts = max;
        self
    }

    pub fn with_concurrency_key(mut self, key: impl Into<String>) -> Self {
        self.concurrency_key = Some(key.into());
        self
    }

    pub fn with_delay(mut self, run_after: DateTime<Utc>) -> Self {
        self.run_after = Some(run_after);
        self
    }

    pub fn can_retry(&self) -> bool {
        self.attempt < self.max_attempts && !self.status.is_terminal()
    }
}

/// A step within a function run (durable execution checkpoint)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepRun {
    /// Unique step run ID
    pub id: Uuid,
    /// Parent function run ID
    pub run_id: Uuid,
    /// Step identifier (unique within the run, e.g., "fetch-user", "send-email")
    pub step_id: String,
    /// Current status
    pub status: StepStatus,
    /// Step input data
    pub input: Option<serde_json::Value>,
    /// Step output data (cached for replay)
    pub output: Option<serde_json::Value>,
    /// Error message if failed
    pub error: Option<String>,
    /// Current attempt for this step
    pub attempt: i32,
    /// When step was created
    pub created_at: DateTime<Utc>,
    /// When step started executing
    pub started_at: Option<DateTime<Utc>>,
    /// When step completed/failed
    pub ended_at: Option<DateTime<Utc>>,
}

impl StepRun {
    pub fn new(run_id: Uuid, step_id: impl Into<String>) -> Self {
        Self {
            id: Uuid::now_v7(),
            run_id,
            step_id: step_id.into(),
            status: StepStatus::Pending,
            input: None,
            output: None,
            error: None,
            attempt: 0,
            created_at: Utc::now(),
            started_at: None,
            ended_at: None,
        }
    }

    pub fn with_input(mut self, input: serde_json::Value) -> Self {
        self.input = Some(input);
        self
    }
}

/// Distributed lock for concurrency control
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lock {
    /// Lock key
    pub key: String,
    /// Lock holder identifier
    pub holder: String,
    /// When the lock expires
    pub expires_at: DateTime<Utc>,
    /// When the lock was acquired
    pub acquired_at: DateTime<Utc>,
}

/// Function registration (which events trigger which functions)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionConfig {
    /// Unique function identifier
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Events that trigger this function
    pub triggers: Vec<String>,
    /// Retry configuration
    pub retries: RetryConfig,
    /// Concurrency configuration
    pub concurrency: Option<ConcurrencyConfig>,
    /// Timeout in seconds
    pub timeout_secs: u64,
}

/// Retry configuration for a function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of attempts
    pub max_attempts: i32,
    /// Initial delay between retries (milliseconds)
    pub initial_delay_ms: u64,
    /// Maximum delay between retries (milliseconds)
    pub max_delay_ms: u64,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay_ms: 1000,
            max_delay_ms: 60000,
            backoff_multiplier: 2.0,
        }
    }
}

/// Concurrency configuration for a function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcurrencyConfig {
    /// Maximum concurrent executions
    pub limit: usize,
    /// Optional key expression for per-key limits (e.g., "event.data.user_id")
    pub key: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_status_roundtrip() {
        for status in [
            RunStatus::Queued,
            RunStatus::Running,
            RunStatus::Completed,
            RunStatus::Failed,
            RunStatus::Cancelled,
        ] {
            assert_eq!(RunStatus::from_str(status.as_str()), Some(status));
        }
    }

    #[test]
    fn test_event_creation() {
        let event = Event::new("user.created", serde_json::json!({"user_id": "123"}))
            .with_idempotency_key("user-123-created")
            .with_user_id("tenant-1");

        assert_eq!(event.name, "user.created");
        assert_eq!(event.idempotency_key, Some("user-123-created".to_string()));
        assert_eq!(event.user_id, Some("tenant-1".to_string()));
    }

    #[test]
    fn test_function_run_retry_logic() {
        let mut run =
            FunctionRun::new("test", Uuid::now_v7(), serde_json::json!({})).with_max_attempts(3);

        assert!(run.can_retry());

        run.attempt = 3;
        assert!(!run.can_retry());

        run.attempt = 1;
        run.status = RunStatus::Completed;
        assert!(!run.can_retry());
    }
}
