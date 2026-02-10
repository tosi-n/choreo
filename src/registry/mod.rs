//! Function Registry - Maps events to function handlers
//!
//! The registry maintains:
//! - Function definitions with their triggers
//! - Event-to-function routing
//! - Handler execution dispatch

use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tracing::info;

use crate::executor::{StepContext, StepError};
use crate::models::{ConcurrencyConfig, FunctionRun, RetryConfig};
use crate::storage::StateStore;

/// Function handler trait - implement this to define function behavior
#[async_trait]
pub trait FunctionHandler<S: StateStore>: Send + Sync {
    /// Execute the function with the given run and step context
    async fn call(
        &self,
        run: FunctionRun,
        step: StepContext<S>,
    ) -> Result<serde_json::Value, StepError>;
}

/// Wrapper for async function closures
pub struct AsyncFnHandler<S, F>
where
    S: StateStore,
    F: Fn(
            FunctionRun,
            StepContext<S>,
        ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, StepError>> + Send>>
        + Send
        + Sync,
{
    func: F,
    _marker: std::marker::PhantomData<S>,
}

impl<S, F> AsyncFnHandler<S, F>
where
    S: StateStore,
    F: Fn(
            FunctionRun,
            StepContext<S>,
        ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, StepError>> + Send>>
        + Send
        + Sync,
{
    pub fn new(func: F) -> Self {
        Self {
            func,
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<S, F> FunctionHandler<S> for AsyncFnHandler<S, F>
where
    S: StateStore + 'static,
    F: Fn(
            FunctionRun,
            StepContext<S>,
        ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, StepError>> + Send>>
        + Send
        + Sync,
{
    async fn call(
        &self,
        run: FunctionRun,
        step: StepContext<S>,
    ) -> Result<serde_json::Value, StepError> {
        (self.func)(run, step).await
    }
}

/// Function definition with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionDef {
    /// Unique function identifier
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Events that trigger this function
    pub triggers: Vec<TriggerDef>,
    /// Retry configuration
    pub retries: RetryConfig,
    /// Concurrency configuration
    pub concurrency: Option<ConcurrencyConfig>,
    /// Timeout in seconds
    pub timeout_secs: u64,
    /// Priority (higher = processed first)
    pub priority: i32,
    /// Whether batching is enabled
    pub batch: Option<BatchConfig>,
    /// Throttle configuration
    pub throttle: Option<ThrottleConfig>,
    /// Debounce configuration
    pub debounce: Option<DebounceConfig>,
}

impl FunctionDef {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: String::new(),
            triggers: vec![],
            retries: RetryConfig::default(),
            concurrency: None,
            timeout_secs: 300,
            priority: 0,
            batch: None,
            throttle: None,
            debounce: None,
        }
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn trigger_event(mut self, event: impl Into<String>) -> Self {
        self.triggers.push(TriggerDef::Event {
            name: event.into(),
            filter: None,
        });
        self
    }

    pub fn trigger_cron(mut self, cron: impl Into<String>) -> Self {
        self.triggers.push(TriggerDef::Cron {
            schedule: cron.into(),
        });
        self
    }

    pub fn retries(mut self, max_attempts: i32) -> Self {
        self.retries.max_attempts = max_attempts;
        self
    }

    pub fn concurrency(mut self, limit: usize, key: Option<String>) -> Self {
        self.concurrency = Some(ConcurrencyConfig { limit, key });
        self
    }

    pub fn timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub fn batch(mut self, max_size: usize, timeout_secs: u64) -> Self {
        self.batch = Some(BatchConfig {
            max_size,
            timeout_secs,
        });
        self
    }

    pub fn throttle(mut self, limit: usize, period_secs: u64, key: Option<String>) -> Self {
        self.throttle = Some(ThrottleConfig {
            limit,
            period_secs,
            key,
        });
        self
    }

    pub fn debounce(mut self, period_secs: u64, key: Option<String>) -> Self {
        self.debounce = Some(DebounceConfig { period_secs, key });
        self
    }
}

/// Trigger definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TriggerDef {
    /// Event-based trigger
    Event {
        /// Event name pattern (supports wildcards)
        name: String,
        /// Optional CEL expression to filter events
        filter: Option<String>,
    },
    /// Cron-based trigger
    Cron {
        /// Cron expression (e.g., "0 9 * * *")
        schedule: String,
    },
}

/// Batch configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Maximum number of events to batch
    pub max_size: usize,
    /// Maximum time to wait for batch to fill (seconds)
    pub timeout_secs: u64,
}

/// Throttle configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThrottleConfig {
    /// Maximum executions per period
    pub limit: usize,
    /// Period in seconds
    pub period_secs: u64,
    /// Optional key for per-key throttling
    pub key: Option<String>,
}

/// Debounce configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebounceConfig {
    /// Debounce period in seconds
    pub period_secs: u64,
    /// Optional key for per-key debouncing
    pub key: Option<String>,
}

/// Function registry - stores function definitions and handlers
pub struct Registry<S: StateStore> {
    /// Function definitions
    definitions: DashMap<String, FunctionDef>,
    /// Function handlers
    handlers: DashMap<String, Arc<dyn FunctionHandler<S>>>,
    /// Event to function mapping (event_name -> [function_ids])
    event_map: DashMap<String, Vec<String>>,
    /// Cron triggers (schedule -> function_id)
    cron_map: DashMap<String, String>,
}

impl<S: StateStore + 'static> Registry<S> {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            definitions: DashMap::new(),
            handlers: DashMap::new(),
            event_map: DashMap::new(),
            cron_map: DashMap::new(),
        }
    }

    /// Register a function with its handler
    pub fn register<H: FunctionHandler<S> + 'static>(&self, def: FunctionDef, handler: H) -> &Self {
        let function_id = def.id.clone();

        // Store definition
        self.definitions.insert(function_id.clone(), def.clone());

        // Store handler
        self.handlers.insert(function_id.clone(), Arc::new(handler));

        // Build trigger mappings
        for trigger in &def.triggers {
            match trigger {
                TriggerDef::Event { name, .. } => {
                    self.event_map
                        .entry(name.clone())
                        .or_default()
                        .push(function_id.clone());
                }
                TriggerDef::Cron { schedule } => {
                    self.cron_map.insert(schedule.clone(), function_id.clone());
                }
            }
        }

        info!(function_id = %function_id, "Registered function");
        self
    }

    /// Register a function using a closure
    pub fn register_fn<F>(&self, def: FunctionDef, func: F) -> &Self
    where
        F: Fn(
                FunctionRun,
                StepContext<S>,
            )
                -> Pin<Box<dyn Future<Output = Result<serde_json::Value, StepError>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        self.register(def, AsyncFnHandler::new(func))
    }

    /// Get handler for a function
    pub fn get_handler(&self, function_id: &str) -> Option<Arc<dyn FunctionHandler<S>>> {
        self.handlers.get(function_id).map(|h| h.clone())
    }

    /// Get function definition
    pub fn get_definition(&self, function_id: &str) -> Option<FunctionDef> {
        self.definitions.get(function_id).map(|d| d.clone())
    }

    /// Get function IDs triggered by an event
    pub fn get_functions_for_event(&self, event_name: &str) -> Vec<String> {
        // Exact match
        if let Some(functions) = self.event_map.get(event_name) {
            return functions.clone();
        }

        // Wildcard matching (e.g., "user.*" matches "user.created")
        let mut matches = Vec::new();
        for entry in self.event_map.iter() {
            let pattern = entry.key();
            if matches_event_pattern(pattern, event_name) {
                matches.extend(entry.value().clone());
            }
        }

        matches
    }

    /// Get all cron schedules and their function IDs
    pub fn get_cron_triggers(&self) -> Vec<(String, String)> {
        self.cron_map
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    /// Get all registered function IDs
    pub fn get_function_ids(&self) -> Vec<String> {
        self.definitions.iter().map(|e| e.key().clone()).collect()
    }

    /// Get all function definitions
    pub fn get_all_definitions(&self) -> Vec<FunctionDef> {
        self.definitions.iter().map(|e| e.value().clone()).collect()
    }
}

impl<S: StateStore> Default for Registry<S> {
    fn default() -> Self {
        Self::new()
    }
}

/// Match event name against pattern (supports wildcards)
fn matches_event_pattern(pattern: &str, event_name: &str) -> bool {
    if pattern == event_name {
        return true;
    }

    // Handle wildcard patterns like "user.*" or "*.created"
    if pattern.contains('*') {
        let regex_pattern = pattern.replace('.', "\\.").replace('*', ".*");

        if let Ok(re) = regex_lite::Regex::new(&format!("^{}$", regex_pattern)) {
            return re.is_match(event_name);
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStore;

    #[tokio::test]
    async fn test_registry_creation() {
        let registry = Registry::<MemoryStore>::new();
        assert_eq!(registry.get_function_ids().len(), 0);
    }

    #[tokio::test]
    async fn test_function_definition_builder() {
        let def = FunctionDef::new("test-function")
            .name("Test Function")
            .trigger_event("test.event")
            .trigger_cron("0 9 * * *")
            .retries(5)
            .concurrency(10, Some("event.data.user_id".to_string()))
            .timeout(600)
            .priority(5);

        assert_eq!(def.id, "test-function");
        assert_eq!(def.name, "Test Function");
        assert_eq!(def.triggers.len(), 2);
        assert_eq!(def.retries.max_attempts, 5);
        assert_eq!(def.timeout_secs, 600);
        assert_eq!(def.priority, 5);
    }

    #[tokio::test]
    async fn test_wildcard_event_matching() {
        assert!(matches_event_pattern("user.*", "user.created"));
        assert!(matches_event_pattern("*.created", "user.created"));
        assert!(!matches_event_pattern("user.*", "order.created"));
    }

    #[tokio::test]
    async fn test_trigger_def_event() {
        let trigger = TriggerDef::Event {
            name: "user.created".to_string(),
            filter: None,
        };
        match trigger {
            TriggerDef::Event { name, filter } => {
                assert_eq!(name, "user.created");
                assert!(filter.is_none());
            }
            _ => panic!("Expected Event trigger"),
        }
    }

    #[tokio::test]
    async fn test_trigger_def_cron() {
        let trigger = TriggerDef::Cron {
            schedule: "0 9 * * *".to_string(),
        };
        match trigger {
            TriggerDef::Cron { schedule } => {
                assert_eq!(schedule, "0 9 * * *");
            }
            _ => panic!("Expected Cron trigger"),
        }
    }
}
