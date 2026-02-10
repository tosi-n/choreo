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

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_choreo_error_lock_failed() {
        let err = ChoreoError::LockFailed {
            key: "test-lock".to_string(),
        };
        assert_eq!(err.to_string(), "Failed to acquire lock: test-lock");
    }

    #[test]
    fn test_choreo_error_lock_not_held() {
        let err = ChoreoError::LockNotHeld {
            key: "test-lock".to_string(),
        };
        assert_eq!(err.to_string(), "Lock not held: test-lock");
    }

    #[test]
    fn test_choreo_error_run_not_found() {
        let id = Uuid::new_v4();
        let err = ChoreoError::RunNotFound { id };
        assert!(err.to_string().contains("Run not found"));
    }

    #[test]
    fn test_choreo_error_event_not_found() {
        let id = Uuid::new_v4();
        let err = ChoreoError::EventNotFound { id };
        assert!(err.to_string().contains("Event not found"));
    }

    #[test]
    fn test_choreo_error_step_not_found() {
        let run_id = Uuid::new_v4();
        let err = ChoreoError::StepNotFound {
            run_id,
            step_id: "test-step".to_string(),
        };
        assert!(err.to_string().contains("Step not found"));
    }

    #[test]
    fn test_choreo_error_function_not_found() {
        let err = ChoreoError::FunctionNotFound {
            function_id: "test-function".to_string(),
        };
        assert!(err.to_string().contains("Function not registered"));
    }

    #[test]
    fn test_choreo_error_duplicate_idempotency_key() {
        let err = ChoreoError::DuplicateIdempotencyKey {
            key: "test-key".to_string(),
        };
        assert!(err.to_string().contains("Duplicate idempotency key"));
    }

    #[test]
    fn test_choreo_error_invalid_state_transition() {
        let err = ChoreoError::InvalidStateTransition {
            from: "pending".to_string(),
            to: "completed".to_string(),
        };
        assert!(err.to_string().contains("Invalid state transition"));
    }

    #[test]
    fn test_choreo_error_config() {
        let err = ChoreoError::Config("invalid config".to_string());
        assert_eq!(err.to_string(), "Configuration error: invalid config");
    }

    #[test]
    fn test_choreo_error_timeout() {
        let err = ChoreoError::Timeout;
        assert_eq!(err.to_string(), "Operation timed out");
    }

    #[test]
    fn test_choreo_error_internal() {
        let err = ChoreoError::Internal("internal error".to_string());
        assert_eq!(err.to_string(), "Internal error: internal error");
    }

    #[test]
    fn test_choreo_error_from_serialization() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
        let err: ChoreoError = json_err.into();
        assert!(err.to_string().contains("Serialization error"));
    }

    #[test]
    fn test_database_error_connection() {
        let err = DatabaseError::Connection("connection refused".to_string());
        assert_eq!(err.to_string(), "Connection failed: connection refused");
    }

    #[test]
    fn test_database_error_query() {
        let err = DatabaseError::Query("syntax error".to_string());
        assert_eq!(err.to_string(), "Query failed: syntax error");
    }

    #[test]
    fn test_database_error_migration() {
        let err = DatabaseError::Migration("migration failed".to_string());
        assert_eq!(err.to_string(), "Migration failed: migration failed");
    }

    #[test]
    fn test_database_error_transaction() {
        let err = DatabaseError::Transaction("rollback".to_string());
        assert_eq!(err.to_string(), "Transaction failed: rollback");
    }

    #[test]
    fn test_database_error_constraint() {
        let err = DatabaseError::Constraint("unique violation".to_string());
        assert_eq!(err.to_string(), "Constraint violation: unique violation");
    }

    #[test]
    fn test_database_error_not_found() {
        let err = DatabaseError::NotFound;
        assert_eq!(err.to_string(), "Not found");
    }

    #[test]
    fn test_database_error_conflict() {
        let err = DatabaseError::Conflict("version mismatch".to_string());
        assert_eq!(err.to_string(), "Conflict: version mismatch");
    }

    #[test]
    fn test_error_debug_format() {
        let err = ChoreoError::FunctionNotFound {
            function_id: "test".to_string(),
        };
        let debug_format = format!("{:?}", err);
        assert!(debug_format.contains("FunctionNotFound"));
    }

    #[test]
    fn test_error_partial_eq() {
        let err1 = ChoreoError::Config("error1".to_string());
        let err2 = ChoreoError::Config("error1".to_string());
        let err3 = ChoreoError::Config("error2".to_string());
        assert_eq!(err1.to_string(), err2.to_string());
        assert_ne!(err1.to_string(), err3.to_string());
    }
}
