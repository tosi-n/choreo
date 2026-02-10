use uuid::Uuid;

#[test]
fn test_choreo_error_database() {
    let err = choreo::error::ChoreoError::Database(choreo::error::DatabaseError::Connection(
        "test".to_string(),
    ));
    assert_eq!(err.to_string(), "Database error: Connection failed: test");
}

#[test]
fn test_choreo_error_lock_failed() {
    let err = choreo::error::ChoreoError::LockFailed {
        key: "test-lock".to_string(),
    };
    assert_eq!(err.to_string(), "Failed to acquire lock: test-lock");
}

#[test]
fn test_choreo_error_lock_not_held() {
    let err = choreo::error::ChoreoError::LockNotHeld {
        key: "test-lock".to_string(),
    };
    assert_eq!(err.to_string(), "Lock not held: test-lock");
}

#[test]
fn test_choreo_error_run_not_found() {
    let id = Uuid::new_v4();
    let err = choreo::error::ChoreoError::RunNotFound { id };
    assert!(err.to_string().contains("Run not found"));
}

#[test]
fn test_choreo_error_event_not_found() {
    let id = Uuid::new_v4();
    let err = choreo::error::ChoreoError::EventNotFound { id };
    assert!(err.to_string().contains("Event not found"));
}

#[test]
fn test_choreo_error_step_not_found() {
    let run_id = Uuid::new_v4();
    let err = choreo::error::ChoreoError::StepNotFound {
        run_id,
        step_id: "test-step".to_string(),
    };
    assert!(err.to_string().contains("Step not found"));
}

#[test]
fn test_choreo_error_function_not_found() {
    let err = choreo::error::ChoreoError::FunctionNotFound {
        function_id: "test-function".to_string(),
    };
    assert!(err.to_string().contains("Function not registered"));
}

#[test]
fn test_choreo_error_duplicate_idempotency_key() {
    let err = choreo::error::ChoreoError::DuplicateIdempotencyKey {
        key: "test-key".to_string(),
    };
    assert!(err.to_string().contains("Duplicate idempotency key"));
}

#[test]
fn test_choreo_error_invalid_state_transition() {
    let err = choreo::error::ChoreoError::InvalidStateTransition {
        from: "pending".to_string(),
        to: "completed".to_string(),
    };
    assert!(err.to_string().contains("Invalid state transition"));
}

#[test]
fn test_choreo_error_config() {
    let err = choreo::error::ChoreoError::Config("invalid config".to_string());
    assert_eq!(err.to_string(), "Configuration error: invalid config");
}

#[test]
fn test_choreo_error_timeout() {
    let err = choreo::error::ChoreoError::Timeout;
    assert_eq!(err.to_string(), "Operation timed out");
}

#[test]
fn test_choreo_error_internal() {
    let err = choreo::error::ChoreoError::Internal("internal error".to_string());
    assert_eq!(err.to_string(), "Internal error: internal error");
}

#[test]
fn test_choreo_error_from_serialization() {
    let json_err =
        serde_json::Error::io(std::io::Error::new(std::io::ErrorKind::Other, "test error"));
    let err: choreo::ChoreoError = json_err.into();
    assert!(err.to_string().contains("Serialization error"));
}

#[test]
fn test_database_error_connection() {
    let err = choreo::error::DatabaseError::Connection("connection refused".to_string());
    assert_eq!(err.to_string(), "Connection failed: connection refused");
}

#[test]
fn test_database_error_query() {
    let err = choreo::error::DatabaseError::Query("syntax error".to_string());
    assert_eq!(err.to_string(), "Query failed: syntax error");
}

#[test]
fn test_database_error_migration() {
    let err = choreo::error::DatabaseError::Migration("migration failed".to_string());
    assert_eq!(err.to_string(), "Migration failed: migration failed");
}

#[test]
fn test_database_error_transaction() {
    let err = choreo::error::DatabaseError::Transaction("rollback".to_string());
    assert_eq!(err.to_string(), "Transaction failed: rollback");
}

#[test]
fn test_database_error_constraint() {
    let err = choreo::error::DatabaseError::Constraint("unique violation".to_string());
    assert_eq!(err.to_string(), "Constraint violation: unique violation");
}

#[test]
fn test_database_error_not_found() {
    let err = choreo::error::DatabaseError::NotFound;
    assert_eq!(err.to_string(), "Not found");
}

#[test]
fn test_database_error_conflict() {
    let err = choreo::error::DatabaseError::Conflict("version mismatch".to_string());
    assert_eq!(err.to_string(), "Conflict: version mismatch");
}

#[test]
fn test_error_debug_format() {
    let err = choreo::error::ChoreoError::FunctionNotFound {
        function_id: "test".to_string(),
    };
    let debug_format = format!("{:?}", err);
    assert!(debug_format.contains("FunctionNotFound"));
}

#[test]
fn test_error_message_comparison() {
    let err1 = choreo::error::ChoreoError::Config("error1".to_string());
    let err2 = choreo::error::ChoreoError::Config("error1".to_string());
    let err3 = choreo::error::ChoreoError::Config("error2".to_string());

    // Errors with same message should have same string representation
    assert_eq!(err1.to_string(), err2.to_string());
    // Errors with different messages should have different string representation
    assert_ne!(err1.to_string(), err3.to_string());
}
