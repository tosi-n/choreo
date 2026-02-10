//! Scheduler - Cron triggers, throttling, debouncing, and priority queues
//!
//! Handles:
//! - Cron-based scheduled function execution
//! - Throttling (rate limiting over time)
//! - Debouncing (coalescing rapid events)
//! - Priority queue management

mod cron;
mod debounce;
mod priority;
mod throttle;

pub use cron::CronScheduler;
pub use debounce::{DebounceConfig, Debouncer};
pub use priority::{PriorityItem, PriorityQueue};
pub use throttle::{ThrottleConfig, Throttler};

use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::info;

use crate::registry::Registry;
use crate::storage::StateStore;

/// Combined scheduler that manages all scheduling concerns
pub struct Scheduler<S: StateStore> {
    cron: CronScheduler<S>,
    throttlers: dashmap::DashMap<String, Throttler>,
    debouncers: dashmap::DashMap<String, Debouncer>,
    shutdown_tx: broadcast::Sender<()>,
}

impl<S: StateStore + Clone + 'static> Scheduler<S> {
    /// Create a new scheduler
    pub fn new(store: S, registry: Arc<Registry<S>>) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);

        Self {
            cron: CronScheduler::new(store, registry),
            throttlers: dashmap::DashMap::new(),
            debouncers: dashmap::DashMap::new(),
            shutdown_tx,
        }
    }

    /// Get shutdown handle
    pub fn shutdown_handle(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
    }

    /// Start all schedulers
    pub async fn run(&self) -> crate::error::Result<()> {
        info!("Starting scheduler");

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Scheduler received shutdown signal");
            }
            result = self.cron.run() => {
                if let Err(e) = result {
                    tracing::error!(error = %e, "Cron scheduler error");
                }
            }
        }

        info!("Scheduler stopped");
        Ok(())
    }

    /// Register a throttler for a function
    pub fn register_throttle(&self, function_id: &str, config: ThrottleConfig) {
        self.throttlers
            .insert(function_id.to_string(), Throttler::new(config));
    }

    /// Register a debouncer for a function
    pub fn register_debounce(&self, function_id: &str, config: DebounceConfig) {
        self.debouncers
            .insert(function_id.to_string(), Debouncer::new(config));
    }

    /// Check if execution is allowed by throttle
    pub async fn check_throttle(&self, function_id: &str, key: Option<&str>) -> bool {
        if let Some(throttler) = self.throttlers.get(function_id) {
            throttler.check(key).await
        } else {
            true // No throttle configured
        }
    }

    /// Record execution for throttle tracking
    pub async fn record_throttle(&self, function_id: &str, key: Option<&str>) {
        if let Some(throttler) = self.throttlers.get(function_id) {
            throttler.record(key).await;
        }
    }

    /// Check debounce and return whether to execute
    pub async fn check_debounce(&self, function_id: &str, key: Option<&str>) -> bool {
        if let Some(debouncer) = self.debouncers.get(function_id) {
            debouncer.should_execute(key).await
        } else {
            true // No debounce configured
        }
    }

    /// Get cron scheduler reference
    pub fn cron(&self) -> &CronScheduler<S> {
        &self.cron
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::Registry;
    use crate::storage::MemoryStore;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn test_scheduler_new() {
        let store = MemoryStore::new();
        let registry: Arc<Registry<MemoryStore>> = Arc::new(Registry::new());
        let scheduler = Scheduler::new(store, registry);
        let _shutdown = scheduler.shutdown_handle();
    }

    #[test]
    fn test_scheduler_shutdown_handle_clone() {
        let store = MemoryStore::new();
        let registry: Arc<Registry<MemoryStore>> = Arc::new(Registry::new());
        let scheduler = Scheduler::new(store, registry);
        let handle1 = scheduler.shutdown_handle();
        let _handle2 = handle1.clone();
    }

    #[tokio::test]
    async fn test_scheduler_register_throttle() {
        let store = MemoryStore::new();
        let registry: Arc<Registry<MemoryStore>> = Arc::new(Registry::new());
        let scheduler = Scheduler::new(store, registry);

        scheduler.register_throttle(
            "test-function",
            ThrottleConfig {
                limit: 10,
                period: Duration::from_secs(60),
                per_key: false,
            },
        );

        assert!(scheduler.throttlers.contains_key("test-function"));
    }

    #[tokio::test]
    async fn test_scheduler_register_debounce() {
        let store = MemoryStore::new();
        let registry: Arc<Registry<MemoryStore>> = Arc::new(Registry::new());
        let scheduler = Scheduler::new(store, registry);

        scheduler.register_debounce(
            "test-function",
            DebounceConfig {
                period: Duration::from_secs(10),
                per_key: true,
                max_wait: None,
            },
        );

        assert!(scheduler.debouncers.contains_key("test-function"));
    }

    #[tokio::test]
    async fn test_scheduler_check_throttle_no_config() {
        let store = MemoryStore::new();
        let registry: Arc<Registry<MemoryStore>> = Arc::new(Registry::new());
        let scheduler = Scheduler::new(store, registry);

        assert!(scheduler.check_throttle("unknown-function", None).await);
    }

    #[tokio::test]
    async fn test_scheduler_check_debounce_no_config() {
        let store = MemoryStore::new();
        let registry: Arc<Registry<MemoryStore>> = Arc::new(Registry::new());
        let scheduler = Scheduler::new(store, registry);

        assert!(scheduler.check_debounce("unknown-function", None).await);
    }

    #[tokio::test]
    async fn test_scheduler_record_throttle_no_config() {
        let store = MemoryStore::new();
        let registry: Arc<Registry<MemoryStore>> = Arc::new(Registry::new());
        let scheduler = Scheduler::new(store, registry);

        scheduler.record_throttle("unknown-function", None).await;
    }
}
