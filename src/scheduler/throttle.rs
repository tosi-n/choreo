//! Throttler - Rate limiting over time windows

use dashmap::DashMap;
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Throttle configuration
#[derive(Debug, Clone)]
pub struct ThrottleConfig {
    /// Maximum executions per period
    pub limit: usize,
    /// Time period
    pub period: Duration,
    /// Whether to use per-key throttling
    pub per_key: bool,
}

impl Default for ThrottleConfig {
    fn default() -> Self {
        Self {
            limit: 10,
            period: Duration::from_secs(60),
            per_key: false,
        }
    }
}

/// Sliding window rate limiter
pub struct Throttler {
    config: ThrottleConfig,
    /// Global window (if not per-key)
    global_window: Mutex<VecDeque<Instant>>,
    /// Per-key windows
    key_windows: DashMap<String, Mutex<VecDeque<Instant>>>,
}

impl Throttler {
    /// Create a new throttler
    pub fn new(config: ThrottleConfig) -> Self {
        Self {
            config,
            global_window: Mutex::new(VecDeque::new()),
            key_windows: DashMap::new(),
        }
    }

    /// Check if execution is allowed
    pub async fn check(&self, key: Option<&str>) -> bool {
        if self.config.per_key {
            if let Some(k) = key {
                self.check_key(k).await
            } else {
                true // No key provided, allow
            }
        } else {
            self.check_global().await
        }
    }

    /// Record an execution
    pub async fn record(&self, key: Option<&str>) {
        let now = Instant::now();

        if self.config.per_key {
            if let Some(k) = key {
                self.record_key(k, now).await;
            }
        } else {
            self.record_global(now).await;
        }
    }

    /// Check and record atomically, returning true if allowed
    pub async fn acquire(&self, key: Option<&str>) -> bool {
        if self.check(key).await {
            self.record(key).await;
            true
        } else {
            false
        }
    }

    async fn check_global(&self) -> bool {
        let mut window = self.global_window.lock().await;
        self.cleanup_window(&mut window);
        window.len() < self.config.limit
    }

    async fn record_global(&self, now: Instant) {
        let mut window = self.global_window.lock().await;
        self.cleanup_window(&mut window);
        window.push_back(now);
    }

    async fn check_key(&self, key: &str) -> bool {
        let window_mutex = self
            .key_windows
            .entry(key.to_string())
            .or_insert_with(|| Mutex::new(VecDeque::new()));

        let mut window = window_mutex.lock().await;
        self.cleanup_window(&mut window);
        window.len() < self.config.limit
    }

    async fn record_key(&self, key: &str, now: Instant) {
        let window_mutex = self
            .key_windows
            .entry(key.to_string())
            .or_insert_with(|| Mutex::new(VecDeque::new()));

        let mut window = window_mutex.lock().await;
        self.cleanup_window(&mut window);
        window.push_back(now);
    }

    fn cleanup_window(&self, window: &mut VecDeque<Instant>) {
        let cutoff = Instant::now() - self.config.period;
        while let Some(front) = window.front() {
            if *front < cutoff {
                window.pop_front();
            } else {
                break;
            }
        }
    }

    /// Get current count for global window
    pub async fn global_count(&self) -> usize {
        let mut window = self.global_window.lock().await;
        self.cleanup_window(&mut window);
        window.len()
    }

    /// Get current count for a key
    pub async fn key_count(&self, key: &str) -> usize {
        if let Some(window_mutex) = self.key_windows.get(key) {
            let mut window = window_mutex.lock().await;
            self.cleanup_window(&mut window);
            window.len()
        } else {
            0
        }
    }

    /// Get remaining quota
    pub async fn remaining(&self, key: Option<&str>) -> usize {
        let count = if self.config.per_key {
            if let Some(k) = key {
                self.key_count(k).await
            } else {
                0
            }
        } else {
            self.global_count().await
        };

        self.config.limit.saturating_sub(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_throttle_global() {
        let throttler = Throttler::new(ThrottleConfig {
            limit: 3,
            period: Duration::from_secs(1),
            per_key: false,
        });

        assert!(throttler.acquire(None).await);
        assert!(throttler.acquire(None).await);
        assert!(throttler.acquire(None).await);
        assert!(!throttler.acquire(None).await); // Should be throttled
    }

    #[tokio::test]
    async fn test_throttle_per_key() {
        let throttler = Throttler::new(ThrottleConfig {
            limit: 2,
            period: Duration::from_secs(1),
            per_key: true,
        });

        assert!(throttler.acquire(Some("key1")).await);
        assert!(throttler.acquire(Some("key1")).await);
        assert!(!throttler.acquire(Some("key1")).await); // key1 throttled

        assert!(throttler.acquire(Some("key2")).await); // key2 still has quota
    }
}
