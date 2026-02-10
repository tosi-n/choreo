//! Middleware module tests

use choreo::middleware::{
    LoggingMiddleware, Middleware, MiddlewareChain, MiddlewareContext, MiddlewareResult,
    RetryFilterMiddleware, TimeoutMiddleware,
};
use choreo::storage::MemoryStore;
use choreo::FunctionRun;
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
    #[async_trait::async_trait]
    impl<S: choreo::storage::StateStore> Middleware<S> for TestMiddleware {
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
    let run = FunctionRun::new(
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

    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
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

    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
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
    let run = FunctionRun::new(
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
    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
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
    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
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
    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
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
    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
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
    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
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

    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
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

    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
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
    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
    let step_ctx = choreo::StepContext::new(Uuid::new_v4(), MemoryStore::new());
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
    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
    let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
        run: &run,
        step: None,
        metadata: json!({}),
    };

    let result = mw.before_step(&ctx, "test-step").await;
    matches!(result, MiddlewareResult::Continue);
}

#[tokio::test]
async fn test_middleware_chain_before_run_empty() {
    let chain = MiddlewareChain::<MemoryStore>::new();
    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
    let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
        run: &run,
        step: None,
        metadata: json!({}),
    };

    let result = chain.before_run(&ctx).await;
    matches!(result, MiddlewareResult::Continue);
}

#[tokio::test]
async fn test_middleware_chain_before_step() {
    let chain = MiddlewareChain::<MemoryStore>::new();
    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
    let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
        run: &run,
        step: None,
        metadata: json!({}),
    };

    let result = chain.before_step(&ctx, "test-step").await;
    matches!(result, MiddlewareResult::Continue);
}

#[tokio::test]
async fn test_middleware_chain_after_step() {
    let chain = MiddlewareChain::<MemoryStore>::new();
    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
    let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
        run: &run,
        step: None,
        metadata: json!({}),
    };

    let result = chain
        .after_step(&ctx, "test-step", Ok(&json!({"result": "ok"})))
        .await;
    matches!(result, MiddlewareResult::Continue);
}

#[tokio::test]
async fn test_middleware_chain_after_step_error() {
    let chain = MiddlewareChain::<MemoryStore>::new();
    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
    let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
        run: &run,
        step: None,
        metadata: json!({}),
    };

    let result = chain
        .after_step(&ctx, "test-step", Err("step failed"))
        .await;
    matches!(result, MiddlewareResult::Continue);
}

#[tokio::test]
async fn test_middleware_skip_short_circuits_chain() {
    struct SkipMiddleware;
    #[async_trait::async_trait]
    impl<S: choreo::storage::StateStore> Middleware<S> for SkipMiddleware {
        async fn before_run(&self, _ctx: &MiddlewareContext<'_, S>) -> MiddlewareResult {
            MiddlewareResult::Skip(json!({"skipped": true}))
        }
    }

    let mut chain = MiddlewareChain::<MemoryStore>::new();
    chain.add(SkipMiddleware);
    chain.add(LoggingMiddleware::new());

    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
    let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
        run: &run,
        step: None,
        metadata: json!({}),
    };

    let result = chain.before_run(&ctx).await;
    matches!(result, MiddlewareResult::Skip(v) if v == json!({"skipped": true}));
}

#[tokio::test]
async fn test_middleware_fail_short_circuits_chain() {
    struct FailMiddleware;
    #[async_trait::async_trait]
    impl<S: choreo::storage::StateStore> Middleware<S> for FailMiddleware {
        async fn before_run(&self, _ctx: &MiddlewareContext<'_, S>) -> MiddlewareResult {
            MiddlewareResult::Fail("intentional failure".to_string())
        }
    }

    let mut chain = MiddlewareChain::<MemoryStore>::new();
    chain.add(FailMiddleware);
    chain.add(LoggingMiddleware::new());

    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
    let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
        run: &run,
        step: None,
        metadata: json!({}),
    };

    let result = chain.before_run(&ctx).await;
    matches!(result, MiddlewareResult::Fail(msg) if msg == "intentional failure");
}

#[tokio::test]
async fn test_logging_middleware_name() {
    let mw = LoggingMiddleware::new();
    assert_eq!(Middleware::<MemoryStore>::name(&mw), "logging");
}

#[tokio::test]
async fn test_timeout_middleware_name() {
    let mw = TimeoutMiddleware::new(30);
    assert_eq!(Middleware::<MemoryStore>::name(&mw), "timeout");
}

#[tokio::test]
async fn test_retry_filter_middleware_case_sensitive() {
    let mw = RetryFilterMiddleware::new().add_non_retryable("ERROR");

    let run = FunctionRun::new("test-function".to_string(), Uuid::new_v4(), json!({}));
    let ctx: MiddlewareContext<'_, MemoryStore> = MiddlewareContext {
        run: &run,
        step: None,
        metadata: json!({}),
    };

    let result = mw.after_run_failure(&ctx, "error").await;
    matches!(result, MiddlewareResult::Continue);
}
