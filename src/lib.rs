//! # Choreo
//!
//! Durable workflow orchestration with BYO database.
//!
//! Choreo is an open-source task orchestrator that provides:
//! - **Durable Execution**: Steps survive crashes with automatic replay
//! - **Event-Driven**: Functions triggered by events with fan-out
//! - **BYO Database**: PostgreSQL, SQLite, or bring your own
//! - **Exactly-Once**: Idempotency keys prevent duplicate processing
//! - **Concurrency Control**: Global and per-key limits
//! - **Scheduling**: Cron triggers, throttling, debouncing
//! - **Priority Queues**: Process important tasks first
//! - **Middleware**: Hooks for logging, metrics, custom behavior
//!
//! ## Quick Start
//!
//! ```ignore
//! use choreo::{Choreo, Config, Registry, Executor};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Connect to database
//!     let store = choreo::SqliteStore::connect("sqlite::memory:").await?;
//!     store.migrate().await?;
//!
//!     // Create registry and register functions
//!     let registry = Arc::new(Registry::new());
//!     registry.register(
//!         FunctionDef::new("process-order")
//!             .trigger_event("order.created")
//!             .retries(3),
//!         |run, step| Box::pin(async move {
//!             let user = step.run("fetch-user", || async {
//!                 Ok(serde_json::json!({"id": "123"}))
//!             }).await?;
//!             Ok(serde_json::json!({"processed": true}))
//!         }),
//!     );
//!
//!     // Create and run executor
//!     let executor = Executor::new(store, registry, Default::default());
//!     executor.run().await?;
//!
//!     Ok(())
//! }
//! ```

pub mod api;
pub mod concurrency;
pub mod config;
pub mod error;
pub mod executor;
pub mod metrics;
pub mod middleware;
pub mod models;
pub mod registry;
pub mod scheduler;
pub mod storage;

// Re-exports for convenience
pub use concurrency::{ConcurrencyConfig, ConcurrencyManager, ConcurrencyPermit};
pub use config::{Config, DatabaseConfig, WorkerConfig};
pub use error::{ChoreoError, Result};
pub use executor::{Executor, ExecutorConfig, StepContext, StepError};
pub use metrics::{ChoreoMetrics, MetricsRegistry};
pub use middleware::{Middleware, MiddlewareChain, MiddlewareContext, MiddlewareResult};
pub use models::{Event, FunctionRun, RetryConfig, RunStatus, StepRun, StepStatus};
pub use registry::{FunctionDef, FunctionHandler, Registry, TriggerDef};
pub use scheduler::{
    CronScheduler, DebounceConfig, Debouncer, PriorityQueue, Scheduler, ThrottleConfig, Throttler,
};
pub use storage::StateStore;

#[cfg(feature = "postgres")]
pub use storage::postgres::PostgresStore;

#[cfg(feature = "sqlite")]
pub use storage::sqlite::SqliteStore;

/// Prelude for common imports
pub mod prelude {
    pub use crate::{
        ChoreoError, ConcurrencyManager, Config, Event, Executor, ExecutorConfig, FunctionDef,
        FunctionRun, Registry, Result, RunStatus, Scheduler, StateStore, StepContext, StepError,
        StepRun, StepStatus, TriggerDef,
    };

    #[cfg(feature = "postgres")]
    pub use crate::PostgresStore;

    #[cfg(feature = "sqlite")]
    pub use crate::SqliteStore;
}
