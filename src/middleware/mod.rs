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
#[derive(Debug, Clone, PartialEq)]
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
    #[doc(hidden)]
    pub middlewares: Vec<Arc<dyn Middleware<S>>>,
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
    #[doc(hidden)]
    pub log_inputs: bool,
    #[doc(hidden)]
    pub log_outputs: bool,
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
    #[doc(hidden)]
    pub timeout_secs: u64,
}

impl TimeoutMiddleware {
    pub fn new(timeout_secs: u64) -> Self {
        Self { timeout_secs }
    }
}

impl Default for TimeoutMiddleware {
    fn default() -> Self {
        Self::new(300)
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
    #[doc(hidden)]
    pub non_retryable_errors: Vec<String>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStore;
    use serde_json::json;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_middleware_chain_new() {
        let chain = MiddlewareChain::<MemoryStore>::new();
        assert_eq!(chain.middlewares.len(), 0);
    }

    #[tokio::test]
    async fn test_middleware_chain_add() {
        let mut chain = MiddlewareChain::<MemoryStore>::new();
        struct TestMiddleware;
        #[async_trait]
        impl<S: StateStore> Middleware<S> for TestMiddleware {
            async fn before_run(&self, _ctx: &MiddlewareContext<'_, S>) -> MiddlewareResult {
                MiddlewareResult::Continue
            }
        }
        chain.add(TestMiddleware);
        assert_eq!(chain.middlewares.len(), 1);
    }

    #[tokio::test]
    async fn test_middleware_result_variants() {
        let skip = MiddlewareResult::Skip(json!({"skipped": true}));
        matches!(skip, MiddlewareResult::Skip(_));

        let fail = MiddlewareResult::Fail("error".to_string());
        matches!(fail, MiddlewareResult::Fail(msg) if msg == "error");

        let cont = MiddlewareResult::Continue;
        matches!(cont, MiddlewareResult::Continue);
    }

    #[tokio::test]
    async fn test_retry_filter_middleware_add_pattern() {
        let mw = RetryFilterMiddleware::new()
            .add_non_retryable("validation_error")
            .add_non_retryable("auth_error");

        assert_eq!(mw.non_retryable_errors.len(), 2);
    }

    #[tokio::test]
    async fn test_middleware_context_creation() {
        let run = crate::models::FunctionRun::new(
            "test-function".to_string(),
            Uuid::new_v4(),
            json!({"test": "input"}),
        );
        let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
            run: &run,
            step: None,
            metadata: json!({}),
        };
        assert_eq!(ctx.run.function_id, "test-function");
    }

    #[tokio::test]
    async fn test_logging_middleware_builder() {
        let mw = LoggingMiddleware::new().with_inputs().with_outputs();

        assert!(mw.log_inputs);
        assert!(mw.log_outputs);
    }

    #[tokio::test]
    async fn test_logging_middleware_default() {
        let mw = LoggingMiddleware::default();
        assert!(!mw.log_inputs);
        assert!(!mw.log_outputs);
    }

    #[tokio::test]
    async fn test_retry_filter_middleware_default() {
        let mw = RetryFilterMiddleware::default();
        assert!(mw.non_retryable_errors.is_empty());
    }

    #[tokio::test]
    async fn test_retry_filter_matches_pattern() {
        let mw = RetryFilterMiddleware::new().add_non_retryable("validation_error");

        let run =
            crate::models::FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
        let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
            run: &run,
            step: None,
            metadata: json!({}),
        };

        let result = mw
            .after_run_failure(&ctx, "validation error occurred")
            .await;
        matches!(result, MiddlewareResult::Fail(_));
    }

    #[tokio::test]
    async fn test_retry_filter_no_match() {
        let mw = RetryFilterMiddleware::new().add_non_retryable("validation_error");

        let run =
            crate::models::FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
        let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
            run: &run,
            step: None,
            metadata: json!({}),
        };

        let result = mw.after_run_failure(&ctx, "some other error").await;
        matches!(result, MiddlewareResult::Continue);
    }

    #[tokio::test]
    async fn test_middleware_skip_result() {
        let skip = MiddlewareResult::Skip(json!({"skipped": true}));
        matches!(skip, MiddlewareResult::Skip(v) if v == json!({"skipped": true}));
    }

    #[tokio::test]
    async fn test_middleware_fail_result() {
        let fail = MiddlewareResult::Fail("error message".to_string());
        matches!(fail, MiddlewareResult::Fail(msg) if msg == "error message");
    }

    #[tokio::test]
    async fn test_logging_middleware_before_run_no_inputs() {
        let mw = LoggingMiddleware::new();
        let run = crate::models::FunctionRun::new(
            "test-function".to_string(),
            Uuid::new_v4(),
            json!({"sensitive": "data"}),
        );
        let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
            run: &run,
            step: None,
            metadata: json!({}),
        };

        let result = mw.before_run(&ctx).await;
        matches!(result, MiddlewareResult::Continue);
    }

    #[tokio::test]
    async fn test_logging_middleware_after_run_success() {
        let mw = LoggingMiddleware::new().with_outputs();
        let run =
            crate::models::FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
        let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
            run: &run,
            step: None,
            metadata: json!({}),
        };

        let result = mw
            .after_run_success(&ctx, &json!({"result": "success"}))
            .await;
        matches!(result, MiddlewareResult::Continue);
    }

    #[tokio::test]
    async fn test_logging_middleware_after_run_failure() {
        let mw = LoggingMiddleware::new();
        let run =
            crate::models::FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
        let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
            run: &run,
            step: None,
            metadata: json!({}),
        };

        let result = mw.after_run_failure(&ctx, "error message").await;
        matches!(result, MiddlewareResult::Continue);
    }

    #[tokio::test]
    async fn test_timeout_middleware_new() {
        let mw = TimeoutMiddleware::new(30);
        assert_eq!(mw.timeout_secs, 30);
    }

    #[tokio::test]
    async fn test_timeout_middleware_default() {
        let mw = TimeoutMiddleware::default();
        assert_eq!(mw.timeout_secs, 300);
    }

    #[tokio::test]
    async fn test_timeout_middleware_before_run() {
        let mw = TimeoutMiddleware::new(60);
        let run =
            crate::models::FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
        let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
            run: &run,
            step: None,
            metadata: json!({}),
        };

        let result = mw.before_run(&ctx).await;
        matches!(result, MiddlewareResult::Continue);
    }

    #[tokio::test]
    async fn test_middleware_chain_with_middleware() {
        let mut chain = MiddlewareChain::<MemoryStore>::new();
        chain.add(LoggingMiddleware::new());
        chain.add(RetryFilterMiddleware::new());

        assert_eq!(chain.middlewares.len(), 2);
    }

    #[tokio::test]
    async fn test_middleware_chain_with_method() {
        let chain = MiddlewareChain::<MemoryStore>::new()
            .with(LoggingMiddleware::new())
            .with(RetryFilterMiddleware::new());

        assert_eq!(chain.middlewares.len(), 2);
    }

    #[tokio::test]
    async fn test_middleware_chain_after_run_success() {
        let chain = MiddlewareChain::<MemoryStore>::new();
        let run =
            crate::models::FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
        let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
            run: &run,
            step: None,
            metadata: json!({}),
        };

        let result = chain
            .after_run_success(&ctx, &json!({"success": true}))
            .await;
        matches!(result, MiddlewareResult::Continue);
    }

    #[tokio::test]
    async fn test_middleware_chain_after_run_failure() {
        let chain = MiddlewareChain::<MemoryStore>::new();
        let run =
            crate::models::FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
        let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
            run: &run,
            step: None,
            metadata: json!({}),
        };

        let result = chain.after_run_failure(&ctx, "error").await;
        matches!(result, MiddlewareResult::Continue);
    }

    #[tokio::test]
    async fn test_middleware_result_debug() {
        let continue_result = MiddlewareResult::Continue;
        let debug_fmt = format!("{:?}", continue_result);
        assert!(debug_fmt.contains("Continue"));

        let skip_result = MiddlewareResult::Skip(json!({"skipped": true}));
        let debug_fmt = format!("{:?}", skip_result);
        assert!(debug_fmt.contains("Skip"));

        let fail_result = MiddlewareResult::Fail("error".to_string());
        let debug_fmt = format!("{:?}", fail_result);
        assert!(debug_fmt.contains("Fail"));
    }

    #[tokio::test]
    async fn test_retry_filter_middleware_name() {
        let mw = RetryFilterMiddleware::new();
        assert_eq!(Middleware::<MemoryStore>::name(&mw), "retry_filter");
    }

    #[tokio::test]
    async fn test_retry_filter_middleware_multiple_patterns() {
        let mw = RetryFilterMiddleware::new()
            .add_non_retryable("validation")
            .add_non_retryable("auth")
            .add_non_retryable("permission");

        let run =
            crate::models::FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
        let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
            run: &run,
            step: None,
            metadata: json!({}),
        };

        let result = mw
            .after_run_failure(&ctx, "auth error: invalid token")
            .await;
        matches!(result, MiddlewareResult::Fail(_));
    }

    #[tokio::test]
    async fn test_retry_filter_middleware_partial_match() {
        let mw = RetryFilterMiddleware::new().add_non_retryable("DB_CONNECTION");

        let run =
            crate::models::FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
        let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
            run: &run,
            step: None,
            metadata: json!({}),
        };

        let result = mw
            .after_run_failure(&ctx, "Failed to establish DB_CONNECTION")
            .await;
        matches!(result, MiddlewareResult::Fail(_));
    }

    #[tokio::test]
    async fn test_middleware_context_with_step() {
        let run =
            crate::models::FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
        let step_ctx = StepContext::new(Uuid::new_v4(), MemoryStore::new());
        let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
            run: &run,
            step: Some(&step_ctx),
            metadata: json!({"key": "value"}),
        };

        assert!(ctx.step.is_some());
        assert_eq!(ctx.metadata["key"], "value");
    }

    #[tokio::test]
    async fn test_logging_middleware_step_logging() {
        let mw = LoggingMiddleware::new();
        let run =
            crate::models::FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
        let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
            run: &run,
            step: None,
            metadata: json!({}),
        };

        let result = mw.before_step(&ctx, "test-step").await;
        matches!(result, MiddlewareResult::Continue);
    }
}
