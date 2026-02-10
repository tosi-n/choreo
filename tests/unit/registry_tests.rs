//! Registry module tests - Unit tests

use choreo::registry::{AsyncFnHandler, FunctionDef, Registry, TriggerDef};
use choreo::storage::MemoryStore;
use serde_json::json;
use std::sync::Arc;

#[test]
fn test_registry_creation() {
    let registry = Registry::<MemoryStore>::new();
    assert_eq!(registry.get_function_ids().len(), 0);
}

#[test]
fn test_function_definition_builder() {
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

#[test]
fn test_function_definition_with_batch() {
    let def = FunctionDef::new("batch-function").batch(100, 30);

    assert!(def.batch.is_some());
    let batch = def.batch.unwrap();
    assert_eq!(batch.max_size, 100);
    assert_eq!(batch.timeout_secs, 30);
}

#[test]
fn test_function_definition_with_throttle() {
    let def = FunctionDef::new("throttle-function").throttle(50, 60, None);

    assert!(def.throttle.is_some());
    let throttle = def.throttle.unwrap();
    assert_eq!(throttle.limit, 50);
    assert_eq!(throttle.period_secs, 60);
}

#[test]
fn test_function_definition_with_debounce() {
    let def = FunctionDef::new("debounce-function")
        .debounce(5000, Some("event.data.user_id".to_string()));

    assert!(def.debounce.is_some());
    let debounce = def.debounce.unwrap();
    assert_eq!(debounce.period_secs, 5000);
}

#[test]
fn test_trigger_def_event() {
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

#[test]
fn test_trigger_def_cron() {
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

#[test]
fn test_registry_register_function() {
    let registry = Registry::<MemoryStore>::new();

    let def = FunctionDef::new("test-function").name("Test Function");

    let handler = AsyncFnHandler::new(|_run, _step| Box::pin(async { Ok(json!({})) }) as _);
    registry.register(def, handler);

    assert_eq!(registry.get_function_ids().len(), 1);
}

#[test]
fn test_registry_get_function() {
    let registry = Registry::<MemoryStore>::new();

    let def = FunctionDef::new("get-test-function").name("Get Test");

    let handler = AsyncFnHandler::new(|_run, _step| Box::pin(async { Ok(json!({})) }) as _);
    registry.register(def, handler);

    let func = registry.get_definition("get-test-function");
    assert!(func.is_some());
    assert_eq!(func.unwrap().id, "get-test-function");
}

#[test]
fn test_registry_list_functions() {
    let registry = Registry::<MemoryStore>::new();

    let handler1 = AsyncFnHandler::new(|_run, _step| Box::pin(async { Ok(json!({})) }) as _);
    registry.register(FunctionDef::new("func-1"), handler1);

    let handler2 = AsyncFnHandler::new(|_run, _step| Box::pin(async { Ok(json!({})) }) as _);
    registry.register(FunctionDef::new("func-2"), handler2);

    let funcs = registry.get_all_definitions();
    assert_eq!(funcs.len(), 2);
}

#[test]
fn test_registry_get_function_not_found() {
    let registry = Registry::<MemoryStore>::new();

    let func = registry.get_definition("non-existent");
    assert!(func.is_none());
}

#[test]
fn test_registry_empty_list() {
    let registry = Registry::<MemoryStore>::new();

    let funcs = registry.get_all_definitions();
    assert!(funcs.is_empty());

    let ids = registry.get_function_ids();
    assert!(ids.is_empty());
}

#[test]
fn test_function_def_clone() {
    let def1 = FunctionDef::new("test").name("Test");

    let def2 = def1.clone();

    assert_eq!(def1.id, def2.id);
    assert_eq!(def1.name, def2.name);
}

#[test]
fn test_registry_get_functions_for_event() {
    let registry = Registry::<MemoryStore>::new();

    let def = FunctionDef::new("test-function").trigger_event("user.created");

    let handler = AsyncFnHandler::new(|_run, _step| Box::pin(async { Ok(json!({})) }) as _);
    registry.register(def, handler);

    let funcs = registry.get_functions_for_event("user.created");
    assert_eq!(funcs.len(), 1);
    assert_eq!(funcs[0], "test-function");
}

#[test]
fn test_registry_get_functions_for_event_not_found() {
    let registry = Registry::<MemoryStore>::new();

    let funcs = registry.get_functions_for_event("unknown.event");
    assert!(funcs.is_empty());
}

#[test]
fn test_registry_get_cron_triggers() {
    let registry = Registry::<MemoryStore>::new();

    let def = FunctionDef::new("scheduled-function").trigger_cron("0 9 * * *");

    let handler = AsyncFnHandler::new(|_run, _step| Box::pin(async { Ok(json!({})) }) as _);
    registry.register(def, handler);

    let triggers = registry.get_cron_triggers();
    assert_eq!(triggers.len(), 1);
}

#[test]
fn test_registry_get_handler() {
    let registry = Registry::<MemoryStore>::new();

    let def = FunctionDef::new("handler-test");

    let handler = AsyncFnHandler::new(|_run, _step| Box::pin(async { Ok(json!({})) }) as _);
    registry.register(def, handler);

    let handler = registry.get_handler("handler-test");
    assert!(handler.is_some());
}

#[test]
fn test_registry_get_handler_not_found() {
    let registry = Registry::<MemoryStore>::new();

    let handler = registry.get_handler("unknown");
    assert!(handler.is_none());
}
