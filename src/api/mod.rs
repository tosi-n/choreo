//! REST API for Choreo

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::error::ChoreoError;
use crate::models::{Event, FunctionRun, StepRun};
use crate::storage::StateStore;

/// Application state shared across handlers
pub struct AppState<S: StateStore> {
    pub store: S,
}

/// Create the API router
pub fn router<S: StateStore + Clone>(state: Arc<AppState<S>>) -> Router {
    Router::new()
        // Events
        .route("/events", post(send_event::<S>))
        .route("/events/:id", get(get_event::<S>))
        // Runs
        .route("/runs/:id", get(get_run::<S>))
        .route("/runs/:id/cancel", post(cancel_run::<S>))
        .route("/runs/:id/complete", post(complete_run::<S>))
        .route("/runs/:id/fail", post(fail_run::<S>))
        .route("/runs/:id/steps", get(get_run_steps::<S>))
        .route("/runs/:id/steps/:step_id", post(save_step::<S>))
        // Worker
        .route("/worker/lease-runs", post(lease_runs::<S>))
        .route("/worker/heartbeat", post(worker_heartbeat::<S>))
        .route("/functions", post(register_functions::<S>))
        .route("/functions", get(list_functions::<S>))
        // Health
        .route("/health", get(health_check::<S>))
        .with_state(state)
}

// === Request/Response types ===

#[derive(Debug, Deserialize)]
pub struct SendEventRequest {
    pub name: String,
    pub data: serde_json::Value,
    pub idempotency_key: Option<String>,
    pub user_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SendEventResponse {
    pub event_id: Uuid,
    pub run_ids: Vec<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct PaginationQuery {
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 {
    50
}

#[derive(Debug, Deserialize)]
pub struct LeaseRunsRequest {
    pub worker_id: String,
    #[serde(default = "default_lease_limit")]
    pub limit: i64,
    #[serde(default = "default_lease_duration")]
    pub lease_duration_secs: i64,
}

fn default_lease_limit() -> i64 {
    10
}
fn default_lease_duration() -> i64 {
    300
}

#[derive(Debug, Serialize)]
pub struct LeaseRunsResponse {
    pub runs: Vec<LeasedRunResponse>,
}

#[derive(Debug, Serialize)]
pub struct LeasedRunResponse {
    pub id: Uuid,
    pub function_id: String,
    pub event_id: Uuid,
    pub attempt: i32,
    pub max_attempts: i32,
    pub input: serde_json::Value,
    pub event: EventResponse,
    pub cached_steps: Vec<StepResponse>,
}

#[derive(Debug, Serialize)]
pub struct EventResponse {
    pub id: Uuid,
    pub name: String,
    pub data: serde_json::Value,
    pub timestamp: String,
    pub idempotency_key: Option<String>,
    pub user_id: Option<String>,
}

impl From<Event> for EventResponse {
    fn from(e: Event) -> Self {
        Self {
            id: e.id,
            name: e.name,
            data: e.data,
            timestamp: e.timestamp.to_rfc3339(),
            idempotency_key: e.idempotency_key,
            user_id: e.user_id,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CompleteRunRequest {
    pub output: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct FailRunRequest {
    pub error: String,
    #[serde(default)]
    pub should_retry: bool,
}

#[derive(Debug, Deserialize)]
pub struct SaveStepRequest {
    pub output: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct WorkerHeartbeatRequest {
    pub worker_id: String,
    pub run_ids: Vec<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct RegisterFunctionsRequest {
    pub functions: Vec<FunctionDefRequest>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FunctionDefRequest {
    pub id: String,
    pub name: String,
    pub triggers: Vec<TriggerDefRequest>,
    pub retries: Option<RetryConfig>,
    pub timeout_secs: Option<i32>,
    pub concurrency: Option<ConcurrencyConfig>,
    pub throttle: Option<ThrottleConfig>,
    pub debounce: Option<DebounceConfig>,
    pub priority: Option<i32>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TriggerDefRequest {
    #[serde(rename = "type")]
    pub trigger_type: String,
    pub name: Option<String>,     // for event triggers
    pub schedule: Option<String>, // for cron triggers
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RetryConfig {
    pub max_attempts: i32,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ConcurrencyConfig {
    pub limit: i32,
    pub key: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ThrottleConfig {
    pub limit: i32,
    pub period_secs: i32,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DebounceConfig {
    pub period_secs: i32,
}

#[derive(Debug, Serialize)]
pub struct RunResponse {
    pub id: Uuid,
    pub function_id: String,
    pub event_id: Uuid,
    pub status: String,
    pub attempt: i32,
    pub max_attempts: i32,
    pub input: serde_json::Value,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
}

impl From<FunctionRun> for RunResponse {
    fn from(r: FunctionRun) -> Self {
        Self {
            id: r.id,
            function_id: r.function_id,
            event_id: r.event_id,
            status: r.status.as_str().to_string(),
            attempt: r.attempt,
            max_attempts: r.max_attempts,
            input: r.input,
            output: r.output,
            error: r.error,
            created_at: r.created_at.to_rfc3339(),
            started_at: r.started_at.map(|t| t.to_rfc3339()),
            ended_at: r.ended_at.map(|t| t.to_rfc3339()),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct StepResponse {
    pub id: Uuid,
    pub step_id: String,
    pub status: String,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    pub attempt: i32,
    pub created_at: String,
    pub ended_at: Option<String>,
}

impl From<StepRun> for StepResponse {
    fn from(s: StepRun) -> Self {
        Self {
            id: s.id,
            step_id: s.step_id,
            status: s.status.as_str().to_string(),
            output: s.output,
            error: s.error,
            attempt: s.attempt,
            created_at: s.created_at.to_rfc3339(),
            ended_at: s.ended_at.map(|t| t.to_rfc3339()),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub database: String,
}

// === Handlers ===

async fn send_event<S: StateStore>(
    State(state): State<Arc<AppState<S>>>,
    Json(req): Json<SendEventRequest>,
) -> Result<Json<SendEventResponse>, AppError> {
    // Check idempotency
    if let Some(ref key) = req.idempotency_key {
        if let Some(existing_id) = state.store.check_idempotency_key(key).await? {
            let runs = state.store.get_runs_by_event(existing_id).await?;
            return Ok(Json(SendEventResponse {
                event_id: existing_id,
                run_ids: runs.into_iter().map(|r| r.id).collect(),
            }));
        }
    }

    // Create event
    let mut event = Event::new(req.name, req.data);
    if let Some(key) = req.idempotency_key {
        event = event.with_idempotency_key(key);
    }
    if let Some(user_id) = req.user_id {
        event = event.with_user_id(user_id);
    }

    state.store.insert_event(&event).await?;

    // Match event to registered functions and create runs
    let matching_functions = state.store.get_functions_for_event(&event.name).await?;
    let mut run_ids = Vec::new();

    for func_json in &matching_functions {
        if let Some(function_id) = func_json.get("id").and_then(|v| v.as_str()) {
            let mut run = FunctionRun::new(function_id.to_string(), event.id, event.data.clone());
            if let Some(max_attempts) = function_retry_max_attempts(func_json) {
                run = run.with_max_attempts(max_attempts);
            }
            state.store.insert_run(&run).await?;
            tracing::info!(
                event_name = %event.name,
                function_id = function_id,
                run_id = %run.id,
                "Created run from event"
            );
            run_ids.push(run.id);
        }
    }

    Ok(Json(SendEventResponse {
        event_id: event.id,
        run_ids,
    }))
}

fn function_retry_max_attempts(function_def: &serde_json::Value) -> Option<i32> {
    function_def
        .get("retries")
        .and_then(|v| v.get("max_attempts"))
        .and_then(|v| v.as_i64())
        .and_then(|v| i32::try_from(v).ok())
        .filter(|v| *v > 0)
}

async fn get_event<S: StateStore>(
    State(state): State<Arc<AppState<S>>>,
    Path(id): Path<Uuid>,
) -> Result<Json<Event>, AppError> {
    let event = state
        .store
        .get_event(id)
        .await?
        .ok_or(ChoreoError::EventNotFound { id })?;
    Ok(Json(event))
}

async fn get_run<S: StateStore>(
    State(state): State<Arc<AppState<S>>>,
    Path(id): Path<Uuid>,
) -> Result<Json<RunResponse>, AppError> {
    let run = state
        .store
        .get_run(id)
        .await?
        .ok_or(ChoreoError::RunNotFound { id })?;
    Ok(Json(run.into()))
}

async fn cancel_run<S: StateStore>(
    State(state): State<Arc<AppState<S>>>,
    Path(id): Path<Uuid>,
) -> Result<Json<RunResponse>, AppError> {
    state.store.cancel_run(id).await?;
    let run = state
        .store
        .get_run(id)
        .await?
        .ok_or(ChoreoError::RunNotFound { id })?;
    Ok(Json(run.into()))
}

async fn get_run_steps<S: StateStore>(
    State(state): State<Arc<AppState<S>>>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<StepResponse>>, AppError> {
    state
        .store
        .get_run(id)
        .await?
        .ok_or(ChoreoError::RunNotFound { id })?;
    let steps = state.store.get_steps_for_run(id).await?;
    Ok(Json(steps.into_iter().map(Into::into).collect()))
}

async fn health_check<S: StateStore>(
    State(state): State<Arc<AppState<S>>>,
) -> Result<Json<HealthResponse>, AppError> {
    let db_status = match state.store.ping().await {
        Ok(_) => "healthy",
        Err(_) => "unhealthy",
    };

    Ok(Json(HealthResponse {
        status: "ok".to_string(),
        database: db_status.to_string(),
    }))
}

async fn lease_runs<S: StateStore>(
    State(state): State<Arc<AppState<S>>>,
    Json(req): Json<LeaseRunsRequest>,
) -> Result<Json<LeaseRunsResponse>, AppError> {
    let runs = state
        .store
        .lease_runs(&req.worker_id, req.limit, req.lease_duration_secs)
        .await?;

    let mut leased_runs = Vec::with_capacity(runs.len());
    for run in runs {
        // Get the event for this run
        let event = state
            .store
            .get_event(run.event_id)
            .await?
            .ok_or(ChoreoError::EventNotFound { id: run.event_id })?;

        // Get cached steps for replay
        let steps = state.store.get_steps_for_run(run.id).await?;

        leased_runs.push(LeasedRunResponse {
            id: run.id,
            function_id: run.function_id.clone(),
            event_id: run.event_id,
            attempt: run.attempt,
            max_attempts: run.max_attempts,
            input: run.input.clone(),
            event: event.into(),
            cached_steps: steps.into_iter().map(Into::into).collect(),
        });
    }

    Ok(Json(LeaseRunsResponse { runs: leased_runs }))
}

async fn complete_run<S: StateStore>(
    State(state): State<Arc<AppState<S>>>,
    Path(id): Path<Uuid>,
    Json(req): Json<CompleteRunRequest>,
) -> Result<Json<RunResponse>, AppError> {
    state.store.complete_run(id, req.output).await?;
    let run = state
        .store
        .get_run(id)
        .await?
        .ok_or(ChoreoError::RunNotFound { id })?;
    Ok(Json(run.into()))
}

async fn fail_run<S: StateStore>(
    State(state): State<Arc<AppState<S>>>,
    Path(id): Path<Uuid>,
    Json(req): Json<FailRunRequest>,
) -> Result<Json<RunResponse>, AppError> {
    state
        .store
        .fail_run(id, &req.error, req.should_retry)
        .await?;
    let run = state
        .store
        .get_run(id)
        .await?
        .ok_or(ChoreoError::RunNotFound { id })?;
    Ok(Json(run.into()))
}

async fn save_step<S: StateStore>(
    State(state): State<Arc<AppState<S>>>,
    Path((run_id, step_id)): Path<(Uuid, String)>,
    Json(req): Json<SaveStepRequest>,
) -> Result<Json<StepResponse>, AppError> {
    state
        .store
        .get_run(run_id)
        .await?
        .ok_or(ChoreoError::RunNotFound { id: run_id })?;

    state
        .store
        .complete_step(run_id, &step_id, req.output)
        .await?;

    // Get the updated step
    let step = state
        .store
        .get_step(run_id, &step_id)
        .await?
        .ok_or(ChoreoError::StepNotFound { run_id, step_id })?;

    Ok(Json(step.into()))
}

async fn worker_heartbeat<S: StateStore>(
    State(state): State<Arc<AppState<S>>>,
    Json(req): Json<WorkerHeartbeatRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    state
        .store
        .extend_run_leases(&req.worker_id, &req.run_ids)
        .await?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}

async fn register_functions<S: StateStore>(
    State(state): State<Arc<AppState<S>>>,
    Json(req): Json<RegisterFunctionsRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let count = req.functions.len();
    for func in req.functions {
        state
            .store
            .register_function(&func.id, &serde_json::to_value(&func)?)
            .await?;
    }
    Ok(Json(serde_json::json!({ "registered": count })))
}

async fn list_functions<S: StateStore>(
    State(state): State<Arc<AppState<S>>>,
) -> Result<Json<Vec<serde_json::Value>>, AppError> {
    let functions = state.store.list_functions().await?;
    Ok(Json(functions))
}

// === Error handling ===

pub struct AppError(ChoreoError);

impl From<ChoreoError> for AppError {
    fn from(e: ChoreoError) -> Self {
        Self(e)
    }
}

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        Self(ChoreoError::Serialization(e))
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match &self.0 {
            ChoreoError::RunNotFound { .. }
            | ChoreoError::EventNotFound { .. }
            | ChoreoError::StepNotFound { .. } => (StatusCode::NOT_FOUND, self.0.to_string()),
            ChoreoError::DuplicateIdempotencyKey { .. } => {
                (StatusCode::CONFLICT, self.0.to_string())
            }
            ChoreoError::InvalidStateTransition { .. } => {
                (StatusCode::BAD_REQUEST, self.0.to_string())
            }
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_string(),
            ),
        };

        let body = serde_json::json!({ "error": message });
        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStore;
    use serde_json::json;

    #[test]
    fn test_app_state_new() {
        let store = MemoryStore::new();
        let _state = AppState { store };
    }

    #[test]
    fn test_event_response_from_event() {
        let event = Event::new("test.event".to_string(), json!({"key": "value"}));
        let response = EventResponse::from(event);
        assert_eq!(response.name, "test.event");
        assert_eq!(response.data, json!({"key": "value"}));
        assert!(response.timestamp.len() > 0);
    }

    #[test]
    fn test_send_event_request() {
        let req = SendEventRequest {
            name: "test.event".to_string(),
            data: json!({"key": "value"}),
            idempotency_key: Some("test-key".to_string()),
            user_id: Some("user-123".to_string()),
        };
        assert_eq!(req.name, "test.event");
        assert_eq!(req.idempotency_key, Some("test-key".to_string()));
    }

    #[test]
    fn test_complete_run_request() {
        let req = CompleteRunRequest {
            output: json!({"result": "success"}),
        };
        assert_eq!(req.output, json!({"result": "success"}));
    }

    #[test]
    fn test_fail_run_request() {
        let req = FailRunRequest {
            error: "test error".to_string(),
            should_retry: true,
        };
        assert_eq!(req.error, "test error");
        assert!(req.should_retry);
    }

    #[test]
    fn test_save_step_request() {
        let req = SaveStepRequest {
            output: json!({"step": "result"}),
        };
        assert_eq!(req.output, json!({"step": "result"}));
    }

    #[test]
    fn test_worker_heartbeat_request() {
        let req = WorkerHeartbeatRequest {
            worker_id: "worker-1".to_string(),
            run_ids: vec![Uuid::new_v4()],
        };
        assert_eq!(req.worker_id, "worker-1");
        assert_eq!(req.run_ids.len(), 1);
    }

    #[test]
    fn test_register_functions_request() {
        let req = RegisterFunctionsRequest {
            functions: vec![FunctionDefRequest {
                id: "test-func".to_string(),
                name: "Test Function".to_string(),
                triggers: vec![],
                retries: None,
                timeout_secs: None,
                concurrency: None,
                throttle: None,
                debounce: None,
                priority: None,
            }],
        };
        assert_eq!(req.functions.len(), 1);
        assert_eq!(req.functions[0].id, "test-func");
    }

    #[test]
    fn test_function_def_request() {
        let def = FunctionDefRequest {
            id: "my-function".to_string(),
            name: "My Function".to_string(),
            triggers: vec![TriggerDefRequest {
                trigger_type: "event".to_string(),
                name: Some("my.event".to_string()),
                schedule: None,
            }],
            retries: Some(RetryConfig { max_attempts: 3 }),
            timeout_secs: Some(300),
            concurrency: Some(ConcurrencyConfig {
                limit: 10,
                key: None,
            }),
            throttle: None,
            debounce: None,
            priority: Some(1),
        };
        assert_eq!(def.id, "my-function");
        assert_eq!(def.triggers.len(), 1);
        assert_eq!(def.retries.unwrap().max_attempts, 3);
    }

    #[test]
    fn test_trigger_def_request() {
        let event_trigger = TriggerDefRequest {
            trigger_type: "event".to_string(),
            name: Some("user.created".to_string()),
            schedule: None,
        };
        assert_eq!(event_trigger.trigger_type, "event");
        assert_eq!(event_trigger.name, Some("user.created".to_string()));

        let cron_trigger = TriggerDefRequest {
            trigger_type: "cron".to_string(),
            name: None,
            schedule: Some("0 9 * * *".to_string()),
        };
        assert_eq!(cron_trigger.trigger_type, "cron");
        assert_eq!(cron_trigger.schedule, Some("0 9 * * *".to_string()));
    }

    #[test]
    fn test_retry_config() {
        let config = RetryConfig { max_attempts: 5 };
        assert_eq!(config.max_attempts, 5);
    }

    #[test]
    fn test_concurrency_config() {
        let config = ConcurrencyConfig {
            limit: 10,
            key: Some("event.data.user_id".to_string()),
        };
        assert_eq!(config.limit, 10);
        assert_eq!(config.key, Some("event.data.user_id".to_string()));
    }

    #[test]
    fn test_throttle_config() {
        let config = ThrottleConfig {
            limit: 100,
            period_secs: 60,
        };
        assert_eq!(config.limit, 100);
        assert_eq!(config.period_secs, 60);
    }

    #[test]
    fn test_debounce_config() {
        let config = DebounceConfig { period_secs: 5000 };
        assert_eq!(config.period_secs, 5000);
    }

    #[test]
    fn test_run_response() {
        let response = RunResponse {
            id: Uuid::new_v4(),
            function_id: "test-function".to_string(),
            event_id: Uuid::new_v4(),
            status: "running".to_string(),
            attempt: 1,
            max_attempts: 3,
            input: json!({"input": "data"}),
            output: None,
            error: None,
            created_at: "2024-01-01T00:00:00Z".to_string(),
            started_at: None,
            ended_at: None,
        };
        assert_eq!(response.function_id, "test-function");
        assert_eq!(response.status, "running");
    }

    #[test]
    fn test_step_response() {
        let response = StepResponse {
            id: Uuid::new_v4(),
            step_id: "test-step".to_string(),
            status: "completed".to_string(),
            output: Some(json!({"result": "success"})),
            error: None,
            attempt: 1,
            created_at: "2024-01-01T00:00:00Z".to_string(),
            ended_at: None,
        };
        assert_eq!(response.step_id, "test-step");
        assert_eq!(response.status, "completed");
    }
}
