//! Concurrency Manager - Global and per-key concurrency control
//!
//! Provides:
//! - Global concurrency limits (max N functions running)
//! - Per-key limits (max 1 per user_id, etc.)
//! - Queue overflow handling
//! - Fair scheduling

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::debug;

/// Concurrency manager configuration
#[derive(Debug, Clone)]
pub struct ConcurrencyConfig {
    /// Global maximum concurrent executions
    pub global_limit: usize,
    /// Default per-key limit
    pub default_key_limit: usize,
    /// Maximum queue size before rejecting
    pub max_queue_size: usize,
    /// Queue timeout (how long to wait for a slot)
    pub queue_timeout: Duration,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            global_limit: 100,
            default_key_limit: 1,
            max_queue_size: 10000,
            queue_timeout: Duration::from_secs(300),
        }
    }
}

/// Manages concurrency limits for function execution
pub struct ConcurrencyManager {
    config: ConcurrencyConfig,
    /// Global semaphore for overall concurrency
    global_semaphore: Arc<Semaphore>,
    /// Per-key semaphores (using Arc<Semaphore> for owned permits)
    key_semaphores: Arc<DashMap<String, Arc<Semaphore>>>,
    /// Per-key active counts
    key_active: Arc<DashMap<String, AtomicUsize>>,
    /// Metrics
    metrics: Arc<ConcurrencyMetrics>,
}

/// Concurrency metrics
#[derive(Default)]
pub struct ConcurrencyMetrics {
    pub acquisitions: AtomicU64,
    pub releases: AtomicU64,
    pub rejections: AtomicU64,
    pub timeouts: AtomicU64,
    pub active: AtomicUsize,
    pub peak: AtomicUsize,
}

impl ConcurrencyMetrics {
    pub fn snapshot(&self) -> ConcurrencySnapshot {
        ConcurrencySnapshot {
            acquisitions: self.acquisitions.load(Ordering::Relaxed),
            releases: self.releases.load(Ordering::Relaxed),
            rejections: self.rejections.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            active: self.active.load(Ordering::Relaxed),
            peak: self.peak.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of concurrency metrics
#[derive(Debug, Clone)]
pub struct ConcurrencySnapshot {
    pub acquisitions: u64,
    pub releases: u64,
    pub rejections: u64,
    pub timeouts: u64,
    pub active: usize,
    pub peak: usize,
}

/// A permit that must be held while executing
pub struct ConcurrencyPermit {
    _global_permit: OwnedSemaphorePermit,
    _key_permit: Option<OwnedSemaphorePermit>,
    key: Option<String>,
    key_active: Arc<DashMap<String, AtomicUsize>>,
    metrics: Arc<ConcurrencyMetrics>,
}

impl Drop for ConcurrencyPermit {
    fn drop(&mut self) {
        // Decrement key active count
        if let Some(key) = &self.key {
            if let Some(counter) = self.key_active.get(key) {
                let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                    count.checked_sub(1)
                });
            }
        }
        decrement_active(&self.metrics.active);
        self.metrics.releases.fetch_add(1, Ordering::Relaxed);
        debug!(key = ?self.key, "Released concurrency permit");
    }
}

/// Error when acquiring concurrency permit
#[derive(Debug, thiserror::Error)]
pub enum ConcurrencyError {
    #[error("Queue is full ({0} items)")]
    QueueFull(usize),

    #[error("Timeout waiting for permit after {0:?}")]
    Timeout(Duration),

    #[error("Concurrency limit reached for key: {0}")]
    KeyLimitReached(String),
}

impl ConcurrencyManager {
    /// Create a new concurrency manager
    pub fn new(config: ConcurrencyConfig) -> Self {
        Self {
            global_semaphore: Arc::new(Semaphore::new(config.global_limit)),
            key_semaphores: Arc::new(DashMap::new()),
            key_active: Arc::new(DashMap::new()),
            metrics: Arc::new(ConcurrencyMetrics::default()),
            config,
        }
    }

    /// Acquire a permit for execution
    pub async fn acquire(&self, key: Option<&str>) -> Result<ConcurrencyPermit, ConcurrencyError> {
        self.acquire_with_timeout(key, self.config.queue_timeout)
            .await
    }

    /// Acquire a permit with custom timeout
    pub async fn acquire_with_timeout(
        &self,
        key: Option<&str>,
        timeout: Duration,
    ) -> Result<ConcurrencyPermit, ConcurrencyError> {
        // Try to acquire global permit
        let global_permit = match tokio::time::timeout(
            timeout,
            self.global_semaphore.clone().acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                self.record_rejection();
                return Err(ConcurrencyError::QueueFull(
                    self.config.global_limit - self.global_semaphore.available_permits(),
                ));
            }
            Err(_) => {
                self.record_timeout();
                return Err(ConcurrencyError::Timeout(timeout));
            }
        };

        // Try to acquire key permit if key is provided
        let (key_string, key_permit) = if let Some(k) = key {
            let sem = self
                .key_semaphores
                .entry(k.to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(self.config.default_key_limit)))
                .clone();

            match tokio::time::timeout(timeout, sem.acquire_owned()).await {
                Ok(Ok(permit)) => {
                    // Track active count
                    self.key_active
                        .entry(k.to_string())
                        .or_insert_with(|| AtomicUsize::new(0))
                        .fetch_add(1, Ordering::Relaxed);
                    (Some(k.to_string()), Some(permit))
                }
                Ok(Err(_)) => {
                    self.record_rejection();
                    return Err(ConcurrencyError::KeyLimitReached(k.to_string()));
                }
                Err(_) => {
                    self.record_timeout();
                    return Err(ConcurrencyError::Timeout(timeout));
                }
            }
        } else {
            (None, None)
        };

        // Update metrics
        self.record_acquisition();

        Ok(ConcurrencyPermit {
            _global_permit: global_permit,
            _key_permit: key_permit,
            key: key_string,
            key_active: self.key_active.clone(),
            metrics: self.metrics.clone(),
        })
    }

    /// Try to acquire a permit without waiting
    pub fn try_acquire(&self, key: Option<&str>) -> Result<ConcurrencyPermit, ConcurrencyError> {
        // Try global
        let global_permit = self
            .global_semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                self.record_rejection();
                ConcurrencyError::QueueFull(
                    self.config.global_limit - self.global_semaphore.available_permits(),
                )
            })?;

        // Try key
        let (key_string, key_permit) = if let Some(k) = key {
            let sem = self
                .key_semaphores
                .entry(k.to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(self.config.default_key_limit)))
                .clone();

            match sem.try_acquire_owned() {
                Ok(permit) => {
                    self.key_active
                        .entry(k.to_string())
                        .or_insert_with(|| AtomicUsize::new(0))
                        .fetch_add(1, Ordering::Relaxed);
                    (Some(k.to_string()), Some(permit))
                }
                Err(_) => {
                    self.record_rejection();
                    return Err(ConcurrencyError::KeyLimitReached(k.to_string()));
                }
            }
        } else {
            (None, None)
        };

        // Update metrics
        self.record_acquisition();

        Ok(ConcurrencyPermit {
            _global_permit: global_permit,
            _key_permit: key_permit,
            key: key_string,
            key_active: self.key_active.clone(),
            metrics: self.metrics.clone(),
        })
    }

    /// Get current metrics
    pub fn metrics(&self) -> ConcurrencySnapshot {
        self.metrics.snapshot()
    }

    /// Get available global permits
    pub fn available_permits(&self) -> usize {
        self.global_semaphore.available_permits()
    }

    /// Get active count for a specific key
    pub fn key_active_count(&self, key: &str) -> usize {
        self.key_active
            .get(key)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Clean up unused key semaphores
    pub fn cleanup_idle_keys(&self) {
        self.key_semaphores.retain(|k, _| {
            self.key_active
                .get(k)
                .map(|c| c.load(Ordering::Relaxed) > 0)
                .unwrap_or(false)
        });
    }

    fn record_acquisition(&self) {
        self.metrics.acquisitions.fetch_add(1, Ordering::Relaxed);
        let active = self.metrics.active.fetch_add(1, Ordering::Relaxed) + 1;
        update_peak(&self.metrics.peak, active);
    }

    fn record_rejection(&self) {
        self.metrics.rejections.fetch_add(1, Ordering::Relaxed);
    }

    fn record_timeout(&self) {
        self.metrics.timeouts.fetch_add(1, Ordering::Relaxed);
    }
}

fn update_peak(peak: &AtomicUsize, active: usize) {
    let mut current_peak = peak.load(Ordering::Relaxed);
    while active > current_peak {
        match peak.compare_exchange_weak(
            current_peak,
            active,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(next_peak) => current_peak = next_peak,
        }
    }
}

fn decrement_active(active: &AtomicUsize) {
    let mut current = active.load(Ordering::Relaxed);
    while current > 0 {
        match active.compare_exchange_weak(
            current,
            current - 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_global_concurrency() {
        let manager = ConcurrencyManager::new(ConcurrencyConfig {
            global_limit: 2,
            ..Default::default()
        });

        let p1 = manager.try_acquire(None).unwrap();
        let _p2 = manager.try_acquire(None).unwrap();

        // Third should fail
        assert!(manager.try_acquire(None).is_err());

        drop(p1);

        // Now should succeed
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

        // Same key should fail
        assert!(manager.try_acquire(Some("user-123")).is_err());

        // Different key should succeed
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

        // This should timeout
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
}
