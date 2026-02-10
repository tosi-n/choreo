//! Concurrency module tests

use choreo::concurrency::{
    ConcurrencyConfig, ConcurrencyError, ConcurrencyManager, ConcurrencyMetrics,
};
use std::sync::atomic::Ordering;
use std::time::Duration;

#[tokio::test]
async fn test_global_concurrency() {
    let manager = ConcurrencyManager::new(ConcurrencyConfig {
        global_limit: 2,
        ..Default::default()
    });

    let p1 = manager.try_acquire(None).unwrap();
    let _p2 = manager.try_acquire(None).unwrap();

    assert!(manager.try_acquire(None).is_err());

    drop(p1);

    let _p3 = manager.try_acquire(None).unwrap();
}

#[tokio::test]
async fn test_key_concurrency() {
    let manager = ConcurrencyManager::new(ConcurrencyConfig {
        global_limit: 10,
        default_key_limit: 1,
        ..Default::default()
    });

    let _p1 = manager.try_acquire(Some("user-123")).unwrap();

    assert!(manager.try_acquire(Some("user-123")).is_err());

    let _p2 = manager.try_acquire(Some("user-456")).unwrap();
}

#[tokio::test]
async fn test_concurrency_config_defaults() {
    let config = ConcurrencyConfig::default();
    assert_eq!(config.global_limit, 100);
    assert_eq!(config.default_key_limit, 1);
    assert_eq!(config.max_queue_size, 10000);
    assert_eq!(config.queue_timeout.as_secs(), 300);
}

#[tokio::test]
async fn test_concurrency_metrics_snapshot() {
    let metrics = ConcurrencyMetrics::default();
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.acquisitions, 0);
    assert_eq!(snapshot.releases, 0);
    assert_eq!(snapshot.rejections, 0);
    assert_eq!(snapshot.timeouts, 0);
}

#[tokio::test]
async fn test_available_permits() {
    let manager = ConcurrencyManager::new(ConcurrencyConfig {
        global_limit: 5,
        ..Default::default()
    });

    assert_eq!(manager.available_permits(), 5);

    let _permit = manager.try_acquire(None).unwrap();
    assert_eq!(manager.available_permits(), 4);
}

#[tokio::test]
async fn test_key_active_count() {
    let manager = ConcurrencyManager::new(ConcurrencyConfig {
        global_limit: 10,
        default_key_limit: 2,
        ..Default::default()
    });

    assert_eq!(manager.key_active_count("user-123"), 0);

    let _p1 = manager.try_acquire(Some("user-123")).unwrap();
    assert_eq!(manager.key_active_count("user-123"), 1);

    let _p2 = manager.try_acquire(Some("user-123")).unwrap();
    assert_eq!(manager.key_active_count("user-123"), 2);
}

#[tokio::test]
async fn test_concurrency_timeout() {
    let manager = ConcurrencyManager::new(ConcurrencyConfig {
        global_limit: 1,
        queue_timeout: Duration::from_millis(100),
        ..Default::default()
    });

    let _p1 = manager.try_acquire(None).unwrap();

    let result = manager
        .acquire_with_timeout(None, Duration::from_millis(50))
        .await;
    assert!(matches!(result, Err(ConcurrencyError::Timeout(_))));
}

#[tokio::test]
async fn test_acquire_with_permit_drop() {
    let manager = ConcurrencyManager::new(ConcurrencyConfig {
        global_limit: 1,
        ..Default::default()
    });

    let permit = manager.try_acquire(None).unwrap();
    assert_eq!(manager.available_permits(), 0);

    drop(permit);
    assert_eq!(manager.available_permits(), 1);
}

#[tokio::test]
async fn test_global_and_key_limits_combined() {
    let manager = ConcurrencyManager::new(ConcurrencyConfig {
        global_limit: 2,
        default_key_limit: 2,
        ..Default::default()
    });

    let _p1 = manager.try_acquire(Some("user-1")).unwrap();
    let _p2 = manager.try_acquire(Some("user-2")).unwrap();

    assert!(manager.try_acquire(None).is_err());
    assert!(manager.try_acquire(Some("user-1")).is_err());
    assert!(manager.try_acquire(Some("user-2")).is_err());
}

#[tokio::test]
async fn test_acquire_with_timeout_success() {
    let manager = ConcurrencyManager::new(ConcurrencyConfig {
        global_limit: 1,
        queue_timeout: Duration::from_secs(5),
        ..Default::default()
    });

    let _p1 = manager.try_acquire(None).unwrap();

    let result = manager
        .acquire_with_timeout(None, Duration::from_secs(1))
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_key_concurrency_different_keys() {
    let manager = ConcurrencyManager::new(ConcurrencyConfig {
        global_limit: 5,
        default_key_limit: 1,
        ..Default::default()
    });

    for i in 0..5 {
        let _permit = manager.try_acquire(Some(&format!("user-{}", i))).unwrap();
        assert_eq!(manager.key_active_count(&format!("user-{}", i)), 1);
    }

    assert!(manager.try_acquire(None).is_err());
}

#[tokio::test]
async fn test_concurrency_metrics_track_acquisitions() {
    let manager = ConcurrencyManager::new(ConcurrencyConfig {
        global_limit: 10,
        ..Default::default()
    });

    let _p1 = manager.try_acquire(None).unwrap();
    let _p2 = manager.try_acquire(None).unwrap();

    let snapshot = manager.metrics();
    assert_eq!(snapshot.acquisitions, 2);
}

#[tokio::test]
async fn test_concurrency_metrics_track_rejections() {
    let manager = ConcurrencyManager::new(ConcurrencyConfig {
        global_limit: 1,
        ..Default::default()
    });

    let _p1 = manager.try_acquire(None).unwrap();
    let _result = manager.try_acquire(None);

    let snapshot = manager.metrics();
    assert!(snapshot.rejections >= 1);
}

#[tokio::test]
async fn test_concurrency_cleanup_idle_keys() {
    let manager = ConcurrencyManager::new(ConcurrencyConfig {
        global_limit: 10,
        default_key_limit: 1,
        ..Default::default()
    });

    let _p1 = manager.try_acquire(Some("idle-key")).unwrap();
    drop(_p1);

    manager.cleanup_idle_keys();
}

#[tokio::test]
async fn test_acquire_no_key() {
    let manager = ConcurrencyManager::new(ConcurrencyConfig {
        global_limit: 5,
        default_key_limit: 1,
        ..Default::default()
    });

    let permit = manager.try_acquire(None).unwrap();
    assert_eq!(manager.available_permits(), 4);
}

#[tokio::test]
async fn test_concurrency_error_queue_full() {
    let manager = ConcurrencyManager::new(ConcurrencyConfig {
        global_limit: 1,
        ..Default::default()
    });

    let _p1 = manager.try_acquire(None).unwrap();
    let result = manager.try_acquire(None);

    assert!(matches!(result, Err(ConcurrencyError::QueueFull(_))));
}

#[tokio::test]
async fn test_concurrency_error_key_limit_reached() {
    let manager = ConcurrencyManager::new(ConcurrencyConfig {
        global_limit: 10,
        default_key_limit: 1,
        ..Default::default()
    });

    let _p1 = manager.try_acquire(Some("same-key")).unwrap();
    let result = manager.try_acquire(Some("same-key"));

    assert!(matches!(result, Err(ConcurrencyError::KeyLimitReached(_))));
}

#[tokio::test]
async fn test_permit_lifecycle() {
    let manager = ConcurrencyManager::new(ConcurrencyConfig {
        global_limit: 2,
        default_key_limit: 2,
        ..Default::default()
    });

    let metrics_before = manager.metrics();
    assert_eq!(metrics_before.active, 0);

    let p1 = manager.try_acquire(None).unwrap();
    let p2 = manager.try_acquire(Some("key")).unwrap();

    let metrics_during = manager.metrics();
    assert_eq!(metrics_during.active, 2);

    drop(p1);
    drop(p2);

    let metrics_after = manager.metrics();
    assert!(metrics_after.active <= 1);
}

#[tokio::test]
async fn test_concurrency_peak_tracking() {
    let manager = ConcurrencyManager::new(ConcurrencyConfig {
        global_limit: 5,
        default_key_limit: 1,
        ..Default::default()
    });

    let _p1 = manager.try_acquire(None).unwrap();
    let snapshot1 = manager.metrics();
    assert_eq!(snapshot1.peak, 1);

    let _p2 = manager.try_acquire(None).unwrap();
    let snapshot2 = manager.metrics();
    assert_eq!(snapshot2.peak, 2);

    drop(_p2);
    let snapshot3 = manager.metrics();
    assert_eq!(snapshot3.peak, 2);
}

#[tokio::test]
async fn test_concurrency_config_clone() {
    let config = ConcurrencyConfig::default();
    let cloned = config.clone();
    assert_eq!(config.global_limit, cloned.global_limit);
    assert_eq!(config.default_key_limit, cloned.default_key_limit);
    assert_eq!(config.max_queue_size, cloned.max_queue_size);
    assert_eq!(config.queue_timeout, cloned.queue_timeout);
}
