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

    // TODO: Match event to registered functions and create runs
    // For now, return empty run_ids - the function registry will handle this
    Ok(Json(SendEventResponse {
        event_id: event.id,
        run_ids: vec![],
    }))
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
        .complete_step(run_id, &step_id, req.output)
        .await?;

    // Get the updated step
    let steps = state.store.get_steps_for_run(run_id).await?;
    let step = steps
        .into_iter()
        .find(|s| s.step_id == step_id)
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
            ChoreoError::RunNotFound { .. } | ChoreoError::EventNotFound { .. } => {
                (StatusCode::NOT_FOUND, self.0.to_string())
            }
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
