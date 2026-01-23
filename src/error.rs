//! Error types for Choreo

use thiserror::Error;

/// Core error type for Choreo operations
#[derive(Error, Debug)]
pub enum ChoreoError {
    /// Database operation failed
    #[error("Database error: {0}")]
    Database(#[from] DatabaseError),

    /// Lock acquisition failed
    #[error("Failed to acquire lock: {key}")]
    LockFailed { key: String },

    /// Lock not held by caller
    #[error("Lock not held: {key}")]
    LockNotHeld { key: String },

    /// Run not found
    #[error("Run not found: {id}")]
    RunNotFound { id: uuid::Uuid },

    /// Event not found
    #[error("Event not found: {id}")]
    EventNotFound { id: uuid::Uuid },

    /// Step not found
    #[error("Step not found: run={run_id}, step={step_id}")]
    StepNotFound { run_id: uuid::Uuid, step_id: String },

    /// Function not registered
    #[error("Function not registered: {function_id}")]
    FunctionNotFound { function_id: String },

    /// Duplicate idempotency key
    #[error("Duplicate idempotency key: {key}")]
    DuplicateIdempotencyKey { key: String },

    /// Invalid state transition
    #[error("Invalid state transition from {from} to {to}")]
    InvalidStateTransition { from: String, to: String },

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Timeout
    #[error("Operation timed out")]
    Timeout,

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Database-specific errors
#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Connection failed: {0}")]
    Connection(String),

    #[error("Query failed: {0}")]
    Query(String),

    #[error("Migration failed: {0}")]
    Migration(String),

    #[error("Transaction failed: {0}")]
    Transaction(String),

    #[error("Constraint violation: {0}")]
    Constraint(String),

    #[error("Not found")]
    NotFound,

    #[error("Conflict: {0}")]
    Conflict(String),
}

impl From<sqlx::Error> for DatabaseError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::RowNotFound => Self::NotFound,
            sqlx::Error::Database(db_err) => {
                let code = db_err.code().map(|c| c.to_string()).unwrap_or_default();
                // PostgreSQL constraint violation codes
                if code.starts_with("23") {
                    Self::Constraint(db_err.message().to_string())
                } else {
                    Self::Query(db_err.message().to_string())
                }
            }
            sqlx::Error::PoolTimedOut => Self::Connection("Pool timeout".to_string()),
            _ => Self::Query(err.to_string()),
        }
    }
}

impl From<sqlx::Error> for ChoreoError {
    fn from(err: sqlx::Error) -> Self {
        Self::Database(err.into())
    }
}

/// Result type alias for Choreo operations
pub type Result<T> = std::result::Result<T, ChoreoError>;
