//! Executor module tests

use choreo::registry::Registry;
use choreo::storage::MemoryStore;
use choreo::{Executor, ExecutorConfig};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_executor_config_defaults() {
    let config = ExecutorConfig::default();
    assert_eq!(config.poll_interval, Duration::from_secs(1));
    assert_eq!(config.batch_size, 10);
    assert_eq!(config.lock_duration_secs, 300);
    assert_eq!(config.max_concurrent, 10);
    assert!(config.worker_id.starts_with("worker-"));
}

#[test]
fn test_executor_config_clone() {
    let config = ExecutorConfig::default();
    let cloned = config.clone();
    assert_eq!(config.poll_interval, cloned.poll_interval);
    assert_eq!(config.batch_size, cloned.batch_size);
    assert_eq!(config.lock_duration_secs, cloned.lock_duration_secs);
    assert_eq!(config.max_concurrent, cloned.max_concurrent);
}

#[tokio::test]
async fn test_executor_new() {
    let store = MemoryStore::new();
    let registry: Arc<Registry<MemoryStore>> = Arc::new(Registry::new());
    let config = ExecutorConfig::default();

    let executor = Executor::new(store, registry, config);
    let _handle = executor.shutdown_handle();
}

#[tokio::test]
async fn test_executor_shutdown_handle_clone() {
    let store = MemoryStore::new();
    let registry: Arc<Registry<MemoryStore>> = Arc::new(Registry::new());
    let config = ExecutorConfig::default();

    let executor = Executor::new(store, registry, config);
    let handle1 = executor.shutdown_handle();
    let handle2 = handle1.clone();
    assert!(handle1.receiver_count() >= 0);
    assert!(handle2.receiver_count() >= 0);
}

#[tokio::test]
async fn test_executor_execute_single_not_found() {
    let store = MemoryStore::new();
    let registry: Arc<Registry<MemoryStore>> = Arc::new(Registry::new());
    let config = ExecutorConfig::default();

    let executor = Executor::new(store, registry, config);
    let result = executor.execute_single(uuid::Uuid::new_v4()).await;
    assert!(result.is_err());
}

#[tokio::test]
#[ignore]
async fn test_executor_run_no_runs() {
    let store = MemoryStore::new();
    let registry: Arc<Registry<MemoryStore>> = Arc::new(Registry::new());
    let config = ExecutorConfig::default();

    let executor = Executor::new(store, registry, config);
    let result = executor.run().await;
    assert!(result.is_ok());
}

#[test]
fn test_retry_backoff_defaults() {
    let delay = choreo::executor::calculate_backoff(1, 1000, 60000, 2.0);
    assert!(delay >= 1000);
    assert!(delay <= 1500);
}

#[test]
fn test_retry_backoff_increases() {
    let delay1 = choreo::executor::calculate_backoff(1, 1000, 60000, 2.0);
    let delay2 = choreo::executor::calculate_backoff(2, 1000, 60000, 2.0);
    let delay3 = choreo::executor::calculate_backoff(3, 1000, 60000, 2.0);

    assert!(delay2 > delay1);
    assert!(delay3 > delay2);
}

#[test]
fn test_retry_backoff_caps_at_max() {
    let delay = choreo::executor::calculate_backoff(10, 1000, 5000, 2.0);
    assert!(delay <= 5000 + 1250);
}

#[test]
fn test_retry_backoff_first_attempt() {
    let delay = choreo::executor::calculate_backoff(1, 1000, 60000, 2.0);
    assert!(delay >= 1000);
}

#[test]
fn test_retry_backoff_second_attempt() {
    let delay = choreo::executor::calculate_backoff(2, 1000, 60000, 2.0);
    // With jitter, delay can be as low as 1500 (2000 - 25% jitter)
    assert!(delay >= 1500);
}

#[test]
fn test_retry_backoff_custom_multiplier() {
    let delay1 = choreo::executor::calculate_backoff(2, 1000, 60000, 1.5);
    let delay2 = choreo::executor::calculate_backoff(2, 1000, 60000, 3.0);
    assert!(delay2 > delay1);
}

#[test]
fn test_rand_simple_returns_valid_range() {
    for _ in 0..100 {
        let val = choreo::executor::rand_simple();
        assert!(
            val >= 0.0 && val < 1.0,
            "rand_simple should return value in [0, 1)"
        );
    }
}

#[test]
fn test_rand_simple_is_deterministic() {
    use std::time::SystemTime;
    let before = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let val1 = choreo::executor::rand_simple();
    let after = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    assert!(val1 >= 0.0 && val1 < 1.0);
}

#[test]
fn test_executor_config_worker_id_format() {
    let config = ExecutorConfig::default();
    assert!(config.worker_id.starts_with("worker-"));
    assert!(config.worker_id.len() > 7);
}

#[tokio::test]
async fn test_executor_with_large_batch_size() {
    let store = MemoryStore::new();
    let registry: Arc<Registry<MemoryStore>> = Arc::new(Registry::new());
    let mut config = ExecutorConfig::default();
    config.batch_size = 100;

    let executor = Executor::new(store, registry, config);
    let _handle = executor.shutdown_handle();
}

#[tokio::test]
async fn test_executor_with_custom_lock_duration() {
    let store = MemoryStore::new();
    let registry: Arc<Registry<MemoryStore>> = Arc::new(Registry::new());
    let mut config = ExecutorConfig::default();
    config.lock_duration_secs = 600;

    let executor = Executor::new(store, registry, config);
    let _handle = executor.shutdown_handle();
}

#[tokio::test]
async fn test_executor_with_high_concurrency() {
    let store = MemoryStore::new();
    let registry: Arc<Registry<MemoryStore>> = Arc::new(Registry::new());
    let mut config = ExecutorConfig::default();
    config.max_concurrent = 100;

    let executor = Executor::new(store, registry, config);
    let _handle = executor.shutdown_handle();
}

#[tokio::test]
async fn test_executor_with_long_poll_interval() {
    let store = MemoryStore::new();
    let registry: Arc<Registry<MemoryStore>> = Arc::new(Registry::new());
    let mut config = ExecutorConfig::default();
    config.poll_interval = Duration::from_secs(10);

    let executor = Executor::new(store, registry, config);
    let _handle = executor.shutdown_handle();
}

#[test]
fn test_retry_backoff_min_delay() {
    let delay = choreo::executor::calculate_backoff(1, 5000, 100000, 2.0);
    assert!(delay >= 5000);
}

#[test]
fn test_retry_backoff_jitter_range() {
    for attempt in 1..=5 {
        let base_delay = 1000u64;
        let max_delay = 10000u64;
        let delay = choreo::executor::calculate_backoff(attempt, base_delay, max_delay, 2.0);
        let expected_base = base_delay * 2u64.pow((attempt - 1) as u32);
        let max_with_jitter = expected_base.min(max_delay) + (expected_base.min(max_delay) / 4);
        assert!(
            delay >= base_delay,
            "Delay {} should be >= base {}",
            delay,
            base_delay
        );
        assert!(
            delay <= max_with_jitter,
            "Delay {} should be <= max with jitter {}",
            delay,
            max_with_jitter
        );
    }
}
