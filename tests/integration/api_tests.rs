//! API module tests - Integration tests
//!
//! These tests verify the API router configuration and basic HTTP handling.

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use choreo::api::AppState;
use choreo::storage::{MemoryStore, StateStore};
use choreo::SqliteStore;
use serde_json::json;
use std::sync::Arc;
use tower::ServiceExt;

fn create_test_router() -> Router {
    let store = MemoryStore::new();
    let state = Arc::new(AppState { store });
    choreo::api::router(state)
}

async fn create_sqlite_test_router() -> Router {
    let store = SqliteStore::in_memory().await.unwrap();
    store.migrate().await.unwrap();
    let state = Arc::new(AppState { store });
    choreo::api::router(state)
}

#[tokio::test]
async fn test_health_endpoint() {
    let router = create_test_router();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_list_functions_empty() {
    let router = create_test_router();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/functions")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_get_nonexistent_event() {
    let router = create_test_router();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/events/00000000-0000-0000-0000-000000000000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_get_nonexistent_run() {
    let router = create_test_router();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/runs/00000000-0000-0000-0000-000000000000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_cancel_nonexistent_run() {
    let router = create_test_router();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/runs/00000000-0000-0000-0000-000000000000/cancel")
                .method("POST")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_complete_nonexistent_run() {
    let router = create_test_router();

    let request_body = json!({
        "output": {"result": "success"}
    });

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/runs/00000000-0000-0000-0000-000000000000/complete")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(request_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_fail_nonexistent_run() {
    let router = create_test_router();

    let request_body = json!({
        "error": "something went wrong",
        "should_retry": false
    });

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/runs/00000000-0000-0000-0000-000000000000/fail")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(request_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_get_run_steps_nonexistent() {
    let router = create_test_router();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/runs/00000000-0000-0000-0000-000000000000/steps")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_save_step_nonexistent_run() {
    let router = create_test_router();

    let request_body = json!({
        "output": {"result": "step complete"}
    });

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/runs/00000000-0000-0000-0000-000000000000/steps/test-step")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(request_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_lease_runs() {
    let router = create_test_router();

    let request_body = json!({
        "worker_id": "test-worker",
        "limit": 10,
        "lease_duration_secs": 300
    });

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/worker/lease-runs")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(request_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_worker_heartbeat() {
    let router = create_test_router();

    let request_body = json!({
        "worker_id": "test-worker",
        "run_ids": []
    });

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/worker/heartbeat")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(request_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_api_routes_exist() {
    let router = create_test_router();

    let routes = [
        "/events",
        "/health",
        "/functions",
        "/worker/lease-runs",
        "/worker/heartbeat",
    ];

    for route in routes {
        let response = router
            .clone()
            .oneshot(Request::builder().uri(route).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_ne!(
            response.status(),
            StatusCode::NOT_FOUND,
            "Route {} should exist",
            route
        );
    }
}

#[tokio::test]
async fn test_api_error_handling() {
    let router = create_test_router();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/nonexistent-route")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_register_functions() {
    let router = create_test_router();

    let request_body = json!({
        "functions": [
            {
                "id": "test-function",
                "name": "Test Function",
                "triggers": [
                    {"type": "event", "name": "test.event"}
                ],
                "retries": {"max_attempts": 3},
                "timeout_secs": 300,
                "concurrency": None::<serde_json::Value>,
                "throttle": None::<serde_json::Value>,
                "debounce": None::<serde_json::Value>,
                "priority": 0
            }
        ]
    });

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/functions")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(request_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_app_state_creation() {
    let store = MemoryStore::new();
    let _state = Arc::new(AppState { store });
}

#[tokio::test]
async fn test_memory_store_in_api_context() {
    let store = MemoryStore::new();
    store
        .insert_event(&choreo::models::Event::new(
            "test.event".to_string(),
            json!({"key": "value"}),
        ))
        .await
        .unwrap();

    let events = store.get_events_by_name("test.event", 10, 0).await.unwrap();
    assert_eq!(events.len(), 1);
}

#[tokio::test]
async fn test_send_event_with_idempotency_key() {
    let router = create_test_router();

    let request_body = json!({
        "name": "test.event",
        "data": {"key": "value"},
        "idempotency_key": "unique-key-123",
        "user_id": None::<String>
    });

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/events")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(request_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_send_event_uses_function_retry_max_attempts() {
    let router = create_sqlite_test_router().await;

    let register_body = json!({
        "functions": [
            {
                "id": "retry-function",
                "name": "Retry Function",
                "triggers": [
                    {"type": "event", "name": "retry.event"}
                ],
                "retries": {"max_attempts": 7},
                "timeout_secs": 300,
                "concurrency": null,
                "throttle": null,
                "debounce": null,
                "priority": 0
            }
        ]
    });

    let register_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/functions")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(register_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(register_response.status(), StatusCode::OK);

    let send_event_body = json!({
        "name": "retry.event",
        "data": {"order_id": "ord_123"}
    });

    let send_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/events")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(send_event_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(send_response.status(), StatusCode::OK);
    let send_body = to_bytes(send_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let send_json: serde_json::Value = serde_json::from_slice(&send_body).unwrap();

    let run_ids = send_json["run_ids"].as_array().unwrap();
    assert_eq!(run_ids.len(), 1);
    let run_id = run_ids[0].as_str().unwrap();

    let run_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/runs/{}", run_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(run_response.status(), StatusCode::OK);
    let run_body = to_bytes(run_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let run_json: serde_json::Value = serde_json::from_slice(&run_body).unwrap();
    assert_eq!(run_json["max_attempts"], 7);
}

#[tokio::test]
async fn test_list_events_and_runs_with_filters() {
    let router = create_sqlite_test_router().await;

    let register_body = json!({
        "functions": [
            {
                "id": "order-processor",
                "name": "Order Processor",
                "triggers": [
                    {"type": "event", "name": "order.created"}
                ],
                "retries": {"max_attempts": 3},
                "timeout_secs": 300,
                "concurrency": null,
                "throttle": null,
                "debounce": null,
                "priority": 0
            }
        ]
    });

    let register_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/functions")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(register_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(register_response.status(), StatusCode::OK);

    let send_event_body = json!({
        "name": "order.created",
        "data": {"order_id": "ord_987"}
    });

    let send_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/events")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(send_event_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(send_response.status(), StatusCode::OK);
    let send_body = to_bytes(send_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let send_json: serde_json::Value = serde_json::from_slice(&send_body).unwrap();
    let event_id = send_json["event_id"].as_str().unwrap().to_string();
    let run_id = send_json["run_ids"][0].as_str().unwrap().to_string();

    let events_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/events?limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(events_response.status(), StatusCode::OK);
    let events_body = to_bytes(events_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let events_json: serde_json::Value = serde_json::from_slice(&events_body).unwrap();
    assert!(events_json
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["id"] == event_id));

    let filtered_events_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/events?name=order.created&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(filtered_events_response.status(), StatusCode::OK);
    let filtered_events_body = to_bytes(filtered_events_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let filtered_events_json: serde_json::Value =
        serde_json::from_slice(&filtered_events_body).unwrap();
    assert_eq!(filtered_events_json.as_array().unwrap().len(), 1);

    let runs_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/runs?limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(runs_response.status(), StatusCode::OK);
    let runs_body = to_bytes(runs_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let runs_json: serde_json::Value = serde_json::from_slice(&runs_body).unwrap();
    assert!(runs_json
        .as_array()
        .unwrap()
        .iter()
        .any(|run| run["id"] == run_id));

    let filtered_runs_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/runs?status=queued&function_id=order-processor&event_id={}&limit=10",
                    event_id
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(filtered_runs_response.status(), StatusCode::OK);
    let filtered_runs_body = to_bytes(filtered_runs_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let filtered_runs_json: serde_json::Value =
        serde_json::from_slice(&filtered_runs_body).unwrap();
    assert_eq!(filtered_runs_json.as_array().unwrap().len(), 1);
    assert_eq!(filtered_runs_json[0]["id"], run_id);
}
