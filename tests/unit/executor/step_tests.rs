//! Step context tests - Unit tests

use std::time::Duration;
use uuid::Uuid;
use choreo::{StepContext, StepError};
use choreo::storage::{MemoryStore, StateStore};
use serde_json::json;

#[tokio::test]
async fn test_step_error_execution_failed() {
    let err = StepError::ExecutionFailed("test error".to_string());
    assert!(err.to_string().contains("Step execution failed"));
}

#[tokio::test]
async fn test_step_error_timeout() {
    let duration = Duration::from_secs(30);
    let err = StepError::Timeout(duration);
    assert!(err.to_string().contains("timed out"));
}

#[tokio::test]
async fn test_step_error_cancelled() {
    let err = StepError::Cancelled;
    assert_eq!(err.to_string(), "Step was cancelled");
}

#[tokio::test]
async fn test_step_error_serialization() {
    let err = StepError::from(
        serde_json::Error::io(std::io::Error::new(std::io::ErrorKind::Other, "test"))
    );
    assert!(err.to_string().contains("Serialization error"));
}

#[tokio::test]
async fn test_step_context_new() {
    let run_id = Uuid::new_v4();
    let store = MemoryStore::new();
    let ctx = StepContext::new(run_id, store);
    assert_eq!(ctx.run_id(), run_id);
}

#[tokio::test]
async fn test_step_run_replays_cached_output() {
    use choreo::models::{StepRun, StepStatus};

    let run_id = Uuid::new_v4();
    let store = MemoryStore::new();

    // Pre-populate with completed step
    let mut cached_step = StepRun::new(run_id, "cached-step");
    cached_step.status = StepStatus::Completed;
    cached_step.output = Some(json!({"cached": true}));
    store.upsert_step(&cached_step).await.unwrap();

    let ctx = StepContext::new(run_id, store);

    let result: Result<serde_json::Value, StepError> = ctx.run("cached-step", || async {
        panic!("This should not be called");
    }).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), json!({"cached": true}));
}

#[tokio::test]
async fn test_step_executes_when_no_cache() {
    let run_id = Uuid::new_v4();
    let store = MemoryStore::new();
    let ctx = StepContext::new(run_id, store);

    let result: Result<serde_json::Value, StepError> = ctx.run("new-step", || async {
        Ok(json!({"executed": true}))
    }).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), json!({"executed": true}));
}

#[tokio::test]
async fn test_step_error_propagation() {
    let run_id = Uuid::new_v4();
    let store = MemoryStore::new();
    let ctx = StepContext::new(run_id, store);

    let result: Result<serde_json::Value, StepError> = ctx.run("failing-step", || async {
        Err(StepError::ExecutionFailed("test failure".to_string()))
    }).await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("test failure"));
}

#[tokio::test]
async fn test_step_with_timeout_success() {
    let run_id = Uuid::new_v4();
    let store = MemoryStore::new();
    let ctx = StepContext::new(run_id, store);

    let result: Result<serde_json::Value, StepError> = ctx.run_with_timeout(
        "timeout-step",
        Duration::from_secs(5),
        || async {
            Ok(json!({"completed": true}))
        },
    ).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), json!({"completed": true}));
}

#[tokio::test]
async fn test_step_with_timeout_exceeds() {
    let run_id = Uuid::new_v4();
    let store = MemoryStore::new();
    let ctx = StepContext::new(run_id, store);

    let result: Result<serde_json::Value, StepError> = ctx.run_with_timeout(
        "slow-step",
        Duration::from_millis(50),
        || async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(json!({"slow": true}))
        },
    ).await;

    assert!(result.is_err());
    matches!(result.unwrap_err(), StepError::Timeout(_));
}

#[tokio::test]
async fn test_get_completed_steps() {
    let run_id = Uuid::new_v4();
    let store = MemoryStore::new();
    let ctx = StepContext::new(run_id, store.clone());

    ctx.run("step1", || async { Ok(json!({})) }).await.unwrap();
    ctx.run("step2", || async { Ok(json!({})) }).await.unwrap();

    let completed = ctx.get_completed_steps().await.unwrap();
    assert_eq!(completed.len(), 2);
}

#[tokio::test]
async fn test_step_sleep() {
    let run_id = Uuid::new_v4();
    let store = MemoryStore::new();
    let ctx = StepContext::new(run_id, store);

    let start = std::time::Instant::now();
    let result = ctx.sleep("sleep-step", Duration::from_millis(50)).await;
    let elapsed = start.elapsed();

    assert!(result.is_ok());
    assert!(elapsed >= Duration::from_millis(45));
}