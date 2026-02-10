//! Storage module tests

use choreo::storage::{MemoryStore, StateStore, StateStoreExt};
use choreo::{Event, FunctionRun, StepRun, RunStatus, StepStatus};
use serde_json::json;
use uuid::Uuid;

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
    store.insert_event(&Event::new("user.created".to_string(), json!({}))).await.unwrap();
    store.insert_event(&Event::new("user.created".to_string(), json!({}))).await.unwrap();
    store.insert_event(&Event::new("user.deleted".to_string(), json!({}))).await.unwrap();

    let events = store.get_events_by_name("user.created", 10, 0).await.unwrap();
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
async fn test_memory_store_get_run_by_event() {
    let store = MemoryStore::new();
    let event_id = Uuid::new_v4();
    store.insert_run(&FunctionRun::new("func1".to_string(), event_id, json!({}))).await.unwrap();
    store.insert_run(&FunctionRun::new("func2".to_string(), event_id, json!({}))).await.unwrap();

    let runs = store.get_runs_by_event(event_id).await.unwrap();
    assert_eq!(runs.len(), 2);
}

#[tokio::test]
async fn test_memory_store_get_run_by_function() {
    let store = MemoryStore::new();
    store.insert_run(&FunctionRun::new("func1".to_string(), Uuid::new_v4(), json!({}))).await.unwrap();
    store.insert_run(&FunctionRun::new("func1".to_string(), Uuid::new_v4(), json!({}))).await.unwrap();
    store.insert_run(&FunctionRun::new("func2".to_string(), Uuid::new_v4(), json!({}))).await.unwrap();

    let runs = store.get_runs_by_function("func1", 10, 0).await.unwrap();
    assert_eq!(runs.len(), 2);
}

#[tokio::test]
async fn test_memory_store_update_run_status() {
    let store = MemoryStore::new();
    let run = FunctionRun::new(
        "test-function".to_string(),
        Uuid::new_v4(),
        json!({}),
    );
    store.insert_run(&run).await.unwrap();

    store.update_run_status(run.id, RunStatus::Running).await.unwrap();
    let updated = store.get_run(run.id).await.unwrap().unwrap();
    assert_eq!(updated.status, RunStatus::Running);
}

#[tokio::test]
async fn test_memory_store_complete_run() {
    let store = MemoryStore::new();
    let run = FunctionRun::new(
        "test-function".to_string(),
        Uuid::new_v4(),
        json!({}),
    );
    store.insert_run(&run).await.unwrap();

    store.complete_run(run.id, json!({"result": "success"})).await.unwrap();
    let completed = store.get_run(run.id).await.unwrap().unwrap();
    assert_eq!(completed.status, RunStatus::Completed);
    assert_eq!(completed.output, Some(json!({"result": "success"})));
}

#[tokio::test]
async fn test_memory_store_fail_run() {
    let store = MemoryStore::new();
    let run = FunctionRun::new(
        "test-function".to_string(),
        Uuid::new_v4(),
        json!({}),
    );
    store.insert_run(&run).await.unwrap();

    store.fail_run(run.id, "something went wrong", false).await.unwrap();
    let failed = store.get_run(run.id).await.unwrap().unwrap();
    assert_eq!(failed.status, RunStatus::Failed);
    assert_eq!(failed.error, Some("something went wrong".to_string()));
}

#[tokio::test]
async fn test_memory_store_cancel_run() {
    let store = MemoryStore::new();
    let run = FunctionRun::new(
        "test-function".to_string(),
        Uuid::new_v4(),
        json!({}),
    );
    store.insert_run(&run).await.unwrap();

    store.cancel_run(run.id).await.unwrap();
    let cancelled = store.get_run(run.id).await.unwrap().unwrap();
    assert_eq!(cancelled.status, RunStatus::Cancelled);
}

#[tokio::test]
async fn test_memory_store_retry_run() {
    let store = MemoryStore::new();
    let run = FunctionRun::new(
        "test-function".to_string(),
        Uuid::new_v4(),
        json!({}),
    );
    store.insert_run(&run).await.unwrap();
    store.update_run_status(run.id, RunStatus::Failed).await.unwrap();

    store.retry_run(run.id, None).await.unwrap();
    let retried = store.get_run(run.id).await.unwrap().unwrap();
    assert_eq!(retried.status, RunStatus::Queued);
}

#[tokio::test]
async fn test_memory_store_lease_runs() {
    let store = MemoryStore::new();
    let runs = store.lease_runs("worker-1", 10, 300).await.unwrap();
    assert!(runs.is_empty());
}

#[tokio::test]
async fn test_memory_store_get_stale_runs() {
    let store = MemoryStore::new();
    let runs = store.get_stale_runs(10).await.unwrap();
    assert!(runs.is_empty());
}

#[tokio::test]
async fn test_memory_store_upsert_step() {
    let store = MemoryStore::new();
    let run_id = Uuid::new_v4();
    let step = StepRun::new(run_id, "step-to-update");

    store.upsert_step(&step).await.unwrap();
    assert!(store.get_step(run_id, "step-to-update").await.unwrap().is_some());

    let mut updated_step = step.clone();
    updated_step.status = StepStatus::Running;
    store.upsert_step(&updated_step).await.unwrap();

    let retrieved = store.get_step(run_id, "step-to-update").await.unwrap().unwrap();
    assert_eq!(retrieved.status, StepStatus::Running);
}

#[tokio::test]
async fn test_memory_store_start_step() {
    let store = MemoryStore::new();
    let run_id = Uuid::new_v4();
    let step = StepRun::new(run_id, "step-to-start");
    store.upsert_step(&step).await.unwrap();

    store.start_step(run_id, "step-to-start").await.unwrap();
    let started = store.get_step(run_id, "step-to-start").await.unwrap().unwrap();
    assert_eq!(started.status, StepStatus::Running);
    assert!(started.started_at.is_some());
}

#[tokio::test]
async fn test_memory_store_complete_step() {
    let store = MemoryStore::new();
    let run_id = Uuid::new_v4();
    let step = StepRun::new(run_id, "step-to-complete");
    store.upsert_step(&step).await.unwrap();

    store.complete_step(run_id, "step-to-complete", json!({"result": "success"}))
        .await
        .unwrap();

    let retrieved = store.get_step(run_id, "step-to-complete").await.unwrap().unwrap();
    assert_eq!(retrieved.status, StepStatus::Completed);
    assert_eq!(retrieved.output, Some(json!({"result": "success"})));
}

#[tokio::test]
async fn test_memory_store_fail_step() {
    let store = MemoryStore::new();
    let run_id = Uuid::new_v4();
    let step = StepRun::new(run_id, "failing-step");
    store.upsert_step(&step).await.unwrap();

    store.fail_step(run_id, "failing-step", "something went wrong")
        .await
        .unwrap();

    let retrieved = store.get_step(run_id, "failing-step").await.unwrap().unwrap();
    assert_eq!(retrieved.status, StepStatus::Failed);
}

#[tokio::test]
async fn test_memory_store_get_steps_for_run() {
    let store = MemoryStore::new();
    let run_id = Uuid::new_v4();
    store.upsert_step(&StepRun::new(run_id, "step1")).await.unwrap();
    store.upsert_step(&StepRun::new(run_id, "step2")).await.unwrap();
    store.upsert_step(&StepRun::new(Uuid::new_v4(), "other-step")).await.unwrap();

    let steps = store.get_steps_for_run(run_id).await.unwrap();
    assert_eq!(steps.len(), 2);
}

#[tokio::test]
async fn test_memory_store_try_acquire_lock() {
    let store = MemoryStore::new();
    let lock = store.try_acquire_lock("test-lock", "worker-1", 30).await.unwrap();
    assert!(lock);
    assert!(store.get_lock("test-lock").await.unwrap().is_some());
}

#[tokio::test]
async fn test_memory_store_lock_contention() {
    let store = MemoryStore::new();
    let lock1 = store.try_acquire_lock("contested-lock", "worker-1", 30).await.unwrap();
    assert!(lock1);
    let lock2 = store.try_acquire_lock("contested-lock", "worker-2", 30).await.unwrap();
    assert!(!lock2);
}

#[tokio::test]
async fn test_memory_store_extend_lock() {
    let store = MemoryStore::new();
    store.try_acquire_lock("extendable-lock", "worker-1", 10).await.unwrap();
    let extended = store.extend_lock("extendable-lock", "worker-1", 30).await.unwrap();
    assert!(extended);
}

#[tokio::test]
#[ignore]
async fn test_memory_store_release_lock() {
    let store = MemoryStore::new();
    store.try_acquire_lock("release-me", "worker-1", 30).await.unwrap();
    let released = store.release_lock("release-me", "worker-1").await.unwrap();
    assert!(released);
    assert!(store.get_lock("release-me").await.unwrap().is_none());
}

#[tokio::test]
async fn test_memory_store_release_lock_wrong_owner() {
    let store = MemoryStore::new();
    store.try_acquire_lock("locked", "worker-1", 30).await.unwrap();
    let released = store.release_lock("locked", "worker-2").await.unwrap();
    assert!(!released);
}

#[tokio::test]
async fn test_memory_store_register_function() {
    let store = MemoryStore::new();
    let definition = json!({"name": "test", "steps": []});
    store.register_function("test-function", &definition).await.unwrap();
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
    store.idempotency_keys.insert("idemp-key".to_string(), run_id);

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
async fn test_memory_store_extend_run_lock() {
    let store = MemoryStore::new();
    let result = store.extend_run_lock(Uuid::new_v4(), "worker-1", 300).await.unwrap();
    assert!(result);
}

#[tokio::test]
async fn test_memory_store_release_run_lock() {
    let store = MemoryStore::new();
    store.release_run_lock(Uuid::new_v4(), "worker-1").await.unwrap();
}

#[tokio::test]
async fn test_memory_store_extend_run_leases() {
    let store = MemoryStore::new();
    store.extend_run_leases("worker-1", &[]).await.unwrap();
}

#[tokio::test]
async fn test_memory_store_get_functions_for_event() {
    let store = MemoryStore::new();
    let funcs = store.get_functions_for_event("test.event").await.unwrap();
    assert!(funcs.is_empty());
}

#[tokio::test]
async fn test_memory_store_migrate() {
    let store = MemoryStore::new();
    store.migrate().await.unwrap();
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
    let run_ids = store.process_event(event, &["func1".to_string(), "func2".to_string()]).await.unwrap();
    assert_eq!(run_ids.len(), 2);
}

#[tokio::test]
async fn test_memory_store_get_nonexistent_event() {
    let store = MemoryStore::new();
    let event = store.get_event(Uuid::new_v4()).await.unwrap();
    assert!(event.is_none());
}

#[tokio::test]
async fn test_memory_store_get_nonexistent_run() {
    let store = MemoryStore::new();
    let run = store.get_run(Uuid::new_v4()).await.unwrap();
    assert!(run.is_none());
}

#[tokio::test]
async fn test_memory_store_get_nonexistent_step() {
    let store = MemoryStore::new();
    let step = store.get_step(Uuid::new_v4(), "nonexistent").await.unwrap();
    assert!(step.is_none());
}

#[tokio::test]
async fn test_memory_store_get_nonexistent_lock() {
    let store = MemoryStore::new();
    let lock = store.get_lock("nonexistent-lock").await.unwrap();
    assert!(lock.is_none());
}

#[tokio::test]
async fn test_memory_store_get_nonexistent_function() {
    let store = MemoryStore::new();
    let func = store.get_function("nonexistent-function").await.unwrap();
    assert!(func.is_none());
}

#[tokio::test]
async fn test_memory_store_get_events_with_pagination() {
    let store = MemoryStore::new();
    for i in 0..10 {
        store.insert_event(&Event::new("paginated.event".to_string(), json!({"index": i}))).await.unwrap();
    }

    let first_batch = store.get_events_by_name("paginated.event", 5, 0).await.unwrap();
    assert_eq!(first_batch.len(), 5);

    let second_batch = store.get_events_by_name("paginated.event", 5, 5).await.unwrap();
    assert_eq!(second_batch.len(), 5);
}

#[tokio::test]
async fn test_memory_store_get_runs_by_function_pagination() {
    let store = MemoryStore::new();
    for i in 0..10 {
        store.insert_run(&FunctionRun::new("paginated-func".to_string(), Uuid::new_v4(), json!({"index": i}))).await.unwrap();
    }

    let first_batch = store.get_runs_by_function("paginated-func", 5, 0).await.unwrap();
    assert_eq!(first_batch.len(), 5);

    let second_batch = store.get_runs_by_function("paginated-func", 5, 5).await.unwrap();
    assert_eq!(second_batch.len(), 5);
}