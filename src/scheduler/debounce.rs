//! Debouncer - Coalesce rapid events into single executions

use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify};
use tokio::time::sleep;

/// Debounce configuration
#[derive(Debug, Clone)]
pub struct DebounceConfig {
    /// Debounce period - wait this long after last event before executing
    pub period: Duration,
    /// Whether to use per-key debouncing
    pub per_key: bool,
    /// Maximum wait time (execute even if events keep coming)
    pub max_wait: Option<Duration>,
}

impl Default for DebounceConfig {
    fn default() -> Self {
        Self {
            period: Duration::from_secs(1),
            per_key: false,
            max_wait: Some(Duration::from_secs(30)),
        }
    }
}

/// Debouncer state for a single key
struct DebounceState {
    /// Last event timestamp
    last_event: Instant,
    /// First event timestamp (for max_wait)
    first_event: Instant,
    /// Whether execution is pending
    pending: bool,
    /// Notify when debounce period elapses
    _notify: Arc<Notify>,
}

/// Debouncer - delays execution until events stop arriving
pub struct Debouncer {
    config: DebounceConfig,
    /// Global state (if not per-key)
    global_state: Mutex<Option<DebounceState>>,
    /// Per-key states
    key_states: DashMap<String, Arc<Mutex<DebounceState>>>,
}

impl Debouncer {
    /// Create a new debouncer
    pub fn new(config: DebounceConfig) -> Self {
        Self {
            config,
            global_state: Mutex::new(None),
            key_states: DashMap::new(),
        }
    }

    /// Record an event and check if we should execute
    ///
    /// Returns true if this event should trigger execution
    /// (i.e., the debounce period has elapsed)
    pub async fn should_execute(&self, key: Option<&str>) -> bool {
        if self.config.per_key {
            if let Some(k) = key {
                self.should_execute_key(k).await
            } else {
                true
            }
        } else {
            self.should_execute_global().await
        }
    }

    /// Record an event without checking execution
    pub async fn record_event(&self, key: Option<&str>) {
        let now = Instant::now();

        if self.config.per_key {
            if let Some(k) = key {
                self.record_key(k, now).await;
            }
        } else {
            self.record_global(now).await;
        }
    }

    /// Wait for debounce period to elapse
    ///
    /// Call this after record_event to wait for the right time to execute
    pub async fn wait(&self, key: Option<&str>) {
        if self.config.per_key {
            if let Some(k) = key {
                self.wait_key(k).await;
            }
        } else {
            self.wait_global().await;
        }
    }

    async fn should_execute_global(&self) -> bool {
        let mut state = self.global_state.lock().await;
        let now = Instant::now();

        match &mut *state {
            None => {
                // First event - start debounce timer
                *state = Some(DebounceState {
                    last_event: now,
                    first_event: now,
                    pending: true,
                    _notify: Arc::new(Notify::new()),
                });
                false // Don't execute yet, wait for debounce
            }
            Some(s) => {
                let elapsed_since_last = now.duration_since(s.last_event);
                let elapsed_since_first = now.duration_since(s.first_event);

                // Check max_wait
                if let Some(max_wait) = self.config.max_wait {
                    if elapsed_since_first >= max_wait {
                        // Max wait exceeded, execute now
                        *state = None;
                        return true;
                    }
                }

                // Check if debounce period elapsed
                if elapsed_since_last >= self.config.period {
                    *state = None;
                    true
                } else {
                    // Update last event time
                    s.last_event = now;
                    false
                }
            }
        }
    }

    async fn record_global(&self, now: Instant) {
        let mut state = self.global_state.lock().await;

        match &mut *state {
            None => {
                *state = Some(DebounceState {
                    last_event: now,
                    first_event: now,
                    pending: true,
                    _notify: Arc::new(Notify::new()),
                });
            }
            Some(s) => {
                s.last_event = now;
            }
        }
    }

    async fn wait_global(&self) {
        loop {
            let wait_until = {
                let state = self.global_state.lock().await;
                match &*state {
                    None => return,
                    Some(s) => s.last_event + self.config.period,
                }
            };

            let now = Instant::now();
            if now >= wait_until {
                return;
            }

            sleep(wait_until - now).await;

            // Check if new events arrived
            let state = self.global_state.lock().await;
            if let Some(s) = &*state {
                if s.last_event + self.config.period <= Instant::now() {
                    return;
                }
                // New event arrived, loop again
            } else {
                return;
            }
        }
    }

    async fn should_execute_key(&self, key: &str) -> bool {
        let state_mutex = self
            .key_states
            .entry(key.to_string())
            .or_insert_with(|| {
                Arc::new(Mutex::new(DebounceState {
                    last_event: Instant::now(),
                    first_event: Instant::now(),
                    pending: false,
                    _notify: Arc::new(Notify::new()),
                }))
            })
            .clone();

        let mut state = state_mutex.lock().await;
        let now = Instant::now();

        if !state.pending {
            // First event for this debounce cycle
            state.first_event = now;
            state.last_event = now;
            state.pending = true;
            return false;
        }

        let elapsed_since_last = now.duration_since(state.last_event);
        let elapsed_since_first = now.duration_since(state.first_event);

        // Check max_wait
        if let Some(max_wait) = self.config.max_wait {
            if elapsed_since_first >= max_wait {
                state.pending = false;
                return true;
            }
        }

        // Check if debounce period elapsed
        if elapsed_since_last >= self.config.period {
            state.pending = false;
            true
        } else {
            state.last_event = now;
            false
        }
    }

    async fn record_key(&self, key: &str, now: Instant) {
        let state_mutex = self
            .key_states
            .entry(key.to_string())
            .or_insert_with(|| {
                Arc::new(Mutex::new(DebounceState {
                    last_event: now,
                    first_event: now,
                    pending: true,
                    _notify: Arc::new(Notify::new()),
                }))
            })
            .clone();

        let mut state = state_mutex.lock().await;
        if !state.pending {
            state.first_event = now;
            state.pending = true;
        }
        state.last_event = now;
    }

    async fn wait_key(&self, key: &str) {
        let state_mutex = match self.key_states.get(key) {
            Some(s) => s.clone(),
            None => return,
        };

        loop {
            let wait_until = {
                let state = state_mutex.lock().await;
                state.last_event + self.config.period
            };

            let now = Instant::now();
            if now >= wait_until {
                return;
            }

            sleep(wait_until - now).await;

            let state = state_mutex.lock().await;
            if state.last_event + self.config.period <= Instant::now() {
                return;
            }
        }
    }

    /// Clean up old key states
    pub fn cleanup(&self) {
        self.key_states.retain(|_, v| {
            if let Ok(state) = v.try_lock() {
                state.pending
            } else {
                true // Keep if locked (in use)
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_debounce_coalesces_rapid_events() {
        let debouncer = Debouncer::new(DebounceConfig {
            period: Duration::from_millis(100),
            per_key: false,
            max_wait: None,
        });

        // Rapid events should not trigger execution
        assert!(!debouncer.should_execute(None).await);
        assert!(!debouncer.should_execute(None).await);
        assert!(!debouncer.should_execute(None).await);

        // Wait for debounce period
        sleep(Duration::from_millis(150)).await;

        // Now should execute
        assert!(debouncer.should_execute(None).await);
    }

    #[tokio::test]
    async fn test_debounce_config_defaults() {
        let config = DebounceConfig::default();
        assert_eq!(config.period, Duration::from_secs(1));
        assert!(!config.per_key);
        assert_eq!(config.max_wait, Some(Duration::from_secs(30)));
    }

    #[tokio::test]
    async fn test_debounce_per_key() {
        let debouncer = Debouncer::new(DebounceConfig {
            period: Duration::from_millis(100),
            per_key: true,
            max_wait: None,
        });

        // First event for key1 - should not execute
        assert!(!debouncer.should_execute(Some("key1")).await);

        // First event for key2 - should not execute
        assert!(!debouncer.should_execute(Some("key2")).await);

        sleep(Duration::from_millis(150)).await;

        // Both should now execute
        assert!(debouncer.should_execute(Some("key1")).await);
        assert!(debouncer.should_execute(Some("key2")).await);
    }

    #[tokio::test]
    async fn test_debounce_max_wait() {
        let debouncer = Debouncer::new(DebounceConfig {
            period: Duration::from_secs(10),
            per_key: false,
            max_wait: Some(Duration::from_millis(150)),
        });

        // First event
        assert!(!debouncer.should_execute(None).await);

        // Rapid events keeping it from executing
        for _ in 0..5 {
            sleep(Duration::from_millis(50)).await;
            debouncer.record_event(None).await;
        }

        // After max_wait, should execute even though period hasn't elapsed
        assert!(debouncer.should_execute(None).await);
    }

    #[tokio::test]
    async fn test_debounce_record_event() {
        let debouncer = Debouncer::new(DebounceConfig {
            period: Duration::from_millis(100),
            per_key: false,
            max_wait: None,
        });

        debouncer.record_event(None).await;
        debouncer.record_event(None).await;
        debouncer.record_event(None).await;

        sleep(Duration::from_millis(150)).await;

        // Should now execute
        assert!(debouncer.should_execute(None).await);
    }

    #[tokio::test]
    async fn test_debounce_no_key_per_key_mode() {
        let debouncer = Debouncer::new(DebounceConfig {
            period: Duration::from_millis(100),
            per_key: true,
            max_wait: None,
        });

        // No key should execute immediately
        assert!(debouncer.should_execute(None).await);
    }

    #[tokio::test]
    async fn test_debounce_wait() {
        let debouncer = Debouncer::new(DebounceConfig {
            period: Duration::from_millis(50),
            per_key: false,
            max_wait: None,
        });

        debouncer.record_event(None).await;

        // Should return quickly after period
        let start = Instant::now();
        debouncer.wait(None).await;
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(50));
    }
}
