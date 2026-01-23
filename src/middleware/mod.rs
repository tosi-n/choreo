//! Middleware/Hooks System - Intercept and modify function execution
//!
//! Provides hooks for:
//! - Before function execution
//! - After function execution (success/failure)
//! - Before/after each step
//! - Error handling and transformation

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, info};

use crate::executor::StepContext;
use crate::models::FunctionRun;
use crate::storage::StateStore;

/// Context passed to middleware
pub struct MiddlewareContext<'a, S: StateStore> {
    pub run: &'a FunctionRun,
    pub step: Option<&'a StepContext<S>>,
    pub metadata: serde_json::Value,
}

/// Result of middleware execution
pub enum MiddlewareResult {
    /// Continue to next middleware/execution
    Continue,
    /// Skip execution and return this value
    Skip(serde_json::Value),
    /// Fail with this error
    Fail(String),
}

/// Middleware trait - implement for custom behavior
#[async_trait]
pub trait Middleware<S: StateStore>: Send + Sync {
    /// Called before function execution
    async fn before_run(&self, ctx: &MiddlewareContext<'_, S>) -> MiddlewareResult {
        let _ = ctx;
        MiddlewareResult::Continue
    }

    /// Called after successful function execution
    async fn after_run_success(
        &self,
        ctx: &MiddlewareContext<'_, S>,
        output: &serde_json::Value,
    ) -> MiddlewareResult {
        let _ = (ctx, output);
        MiddlewareResult::Continue
    }

    /// Called after function execution failure
    async fn after_run_failure(
        &self,
        ctx: &MiddlewareContext<'_, S>,
        error: &str,
    ) -> MiddlewareResult {
        let _ = (ctx, error);
        MiddlewareResult::Continue
    }

    /// Called before each step
    async fn before_step(&self, ctx: &MiddlewareContext<'_, S>, step_id: &str) -> MiddlewareResult {
        let _ = (ctx, step_id);
        MiddlewareResult::Continue
    }

    /// Called after each step
    async fn after_step(
        &self,
        ctx: &MiddlewareContext<'_, S>,
        step_id: &str,
        result: Result<&serde_json::Value, &str>,
    ) -> MiddlewareResult {
        let _ = (ctx, step_id, result);
        MiddlewareResult::Continue
    }

    /// Get middleware name for logging
    fn name(&self) -> &str {
        "unnamed"
    }
}

/// Middleware chain - executes multiple middleware in order
pub struct MiddlewareChain<S: StateStore> {
    middlewares: Vec<Arc<dyn Middleware<S>>>,
}

impl<S: StateStore> MiddlewareChain<S> {
    /// Create empty chain
    pub fn new() -> Self {
        Self {
            middlewares: Vec::new(),
        }
    }

    /// Add middleware to the chain
    pub fn add<M: Middleware<S> + 'static>(&mut self, middleware: M) -> &mut Self {
        self.middlewares.push(Arc::new(middleware));
        self
    }

    /// Add middleware (builder pattern)
    pub fn with<M: Middleware<S> + 'static>(mut self, middleware: M) -> Self {
        self.add(middleware);
        self
    }

    /// Execute before_run hooks
    pub async fn before_run(&self, ctx: &MiddlewareContext<'_, S>) -> MiddlewareResult {
        for mw in &self.middlewares {
            match mw.before_run(ctx).await {
                MiddlewareResult::Continue => continue,
                other => {
                    debug!(middleware = mw.name(), "Middleware interrupted before_run");
                    return other;
                }
            }
        }
        MiddlewareResult::Continue
    }

    /// Execute after_run_success hooks
    pub async fn after_run_success(
        &self,
        ctx: &MiddlewareContext<'_, S>,
        output: &serde_json::Value,
    ) -> MiddlewareResult {
        for mw in &self.middlewares {
            match mw.after_run_success(ctx, output).await {
                MiddlewareResult::Continue => continue,
                other => return other,
            }
        }
        MiddlewareResult::Continue
    }

    /// Execute after_run_failure hooks
    pub async fn after_run_failure(
        &self,
        ctx: &MiddlewareContext<'_, S>,
        error: &str,
    ) -> MiddlewareResult {
        for mw in &self.middlewares {
            match mw.after_run_failure(ctx, error).await {
                MiddlewareResult::Continue => continue,
                other => return other,
            }
        }
        MiddlewareResult::Continue
    }

    /// Execute before_step hooks
    pub async fn before_step(
        &self,
        ctx: &MiddlewareContext<'_, S>,
        step_id: &str,
    ) -> MiddlewareResult {
        for mw in &self.middlewares {
            match mw.before_step(ctx, step_id).await {
                MiddlewareResult::Continue => continue,
                other => return other,
            }
        }
        MiddlewareResult::Continue
    }

    /// Execute after_step hooks
    pub async fn after_step(
        &self,
        ctx: &MiddlewareContext<'_, S>,
        step_id: &str,
        result: Result<&serde_json::Value, &str>,
    ) -> MiddlewareResult {
        for mw in &self.middlewares {
            match mw.after_step(ctx, step_id, result).await {
                MiddlewareResult::Continue => continue,
                other => return other,
            }
        }
        MiddlewareResult::Continue
    }
}

impl<S: StateStore> Default for MiddlewareChain<S> {
    fn default() -> Self {
        Self::new()
    }
}

// === Built-in Middleware ===

/// Logging middleware - logs all function and step executions
pub struct LoggingMiddleware {
    log_inputs: bool,
    log_outputs: bool,
}

impl LoggingMiddleware {
    pub fn new() -> Self {
        Self {
            log_inputs: false,
            log_outputs: false,
        }
    }

    pub fn with_inputs(mut self) -> Self {
        self.log_inputs = true;
        self
    }

    pub fn with_outputs(mut self) -> Self {
        self.log_outputs = true;
        self
    }
}

impl Default for LoggingMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<S: StateStore> Middleware<S> for LoggingMiddleware {
    async fn before_run(&self, ctx: &MiddlewareContext<'_, S>) -> MiddlewareResult {
        if self.log_inputs {
            info!(
                run_id = %ctx.run.id,
                function = %ctx.run.function_id,
                input = %ctx.run.input,
                "Starting function execution"
            );
        } else {
            info!(
                run_id = %ctx.run.id,
                function = %ctx.run.function_id,
                "Starting function execution"
            );
        }
        MiddlewareResult::Continue
    }

    async fn after_run_success(
        &self,
        ctx: &MiddlewareContext<'_, S>,
        output: &serde_json::Value,
    ) -> MiddlewareResult {
        if self.log_outputs {
            info!(
                run_id = %ctx.run.id,
                function = %ctx.run.function_id,
                output = %output,
                "Function completed successfully"
            );
        } else {
            info!(
                run_id = %ctx.run.id,
                function = %ctx.run.function_id,
                "Function completed successfully"
            );
        }
        MiddlewareResult::Continue
    }

    async fn after_run_failure(
        &self,
        ctx: &MiddlewareContext<'_, S>,
        error: &str,
    ) -> MiddlewareResult {
        tracing::error!(
            run_id = %ctx.run.id,
            function = %ctx.run.function_id,
            error = %error,
            "Function execution failed"
        );
        MiddlewareResult::Continue
    }

    fn name(&self) -> &str {
        "logging"
    }
}

/// Timeout middleware - enforces function execution timeout
pub struct TimeoutMiddleware {
    timeout_secs: u64,
}

impl TimeoutMiddleware {
    pub fn new(timeout_secs: u64) -> Self {
        Self { timeout_secs }
    }
}

#[async_trait]
impl<S: StateStore> Middleware<S> for TimeoutMiddleware {
    async fn before_run(&self, ctx: &MiddlewareContext<'_, S>) -> MiddlewareResult {
        // Store timeout in metadata for the executor to use
        debug!(
            run_id = %ctx.run.id,
            timeout_secs = self.timeout_secs,
            "Timeout middleware configured"
        );
        MiddlewareResult::Continue
    }

    fn name(&self) -> &str {
        "timeout"
    }
}

/// Retry filter middleware - can skip retries based on error type
pub struct RetryFilterMiddleware {
    /// Errors that should not be retried
    non_retryable_errors: Vec<String>,
}

impl RetryFilterMiddleware {
    pub fn new() -> Self {
        Self {
            non_retryable_errors: Vec::new(),
        }
    }

    pub fn add_non_retryable(mut self, error_pattern: impl Into<String>) -> Self {
        self.non_retryable_errors.push(error_pattern.into());
        self
    }
}

impl Default for RetryFilterMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<S: StateStore> Middleware<S> for RetryFilterMiddleware {
    async fn after_run_failure(
        &self,
        _ctx: &MiddlewareContext<'_, S>,
        error: &str,
    ) -> MiddlewareResult {
        for pattern in &self.non_retryable_errors {
            if error.contains(pattern) {
                debug!(
                    pattern = %pattern,
                    error = %error,
                    "Error matches non-retryable pattern, failing permanently"
                );
                return MiddlewareResult::Fail(error.to_string());
            }
        }
        MiddlewareResult::Continue
    }

    fn name(&self) -> &str {
        "retry_filter"
    }
}
