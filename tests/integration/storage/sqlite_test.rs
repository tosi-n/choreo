use choreo::models::{Event, FunctionRun, StepRun};
use chrono::{Duration, Utc};
use serde_json::json;
use sqlx::{sqlite::SqlitePoolOptions, Pool, Sqlite};
use uuid::Uuid;

pub async fn create_sqlite_pool() -> Pool<Sqlite> {
    SqlitePoolOptions::new()
        .connect(":memory:")
        .await
        .expect("Failed to connect to SQLite database")
}

#[cfg(test)]
mod sqlite_tests {
    use super::*;

    #[tokio::test]
    async fn test_event_crud() {
        let pool = create_sqlite_pool().await;
        sqlx::query("CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, name TEXT, data TEXT, timestamp TEXT)").execute(&pool).await.unwrap();

        let event = Event::new("test.event", json!({"key": "value"}));
        sqlx::query("INSERT INTO events (id, name, data, timestamp) VALUES (?, ?, ?, ?)")
            .bind(event.id.to_string())
            .bind(&event.name)
            .bind(json!(event.data).to_string())
            .bind(event.timestamp.to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

        let retrieved: (String, String) =
            sqlx::query_as("SELECT name, data FROM events WHERE id = ?")
                .bind(event.id.to_string())
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(retrieved.0, "test.event");
    }

    #[tokio::test]
    async fn test_run_crud() {
        let pool = create_sqlite_pool().await;
        sqlx::query("CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, function_id TEXT, event_id TEXT, status TEXT, attempt INTEGER, max_attempts INTEGER, input TEXT)").execute(&pool).await.unwrap();

        let event_id = Uuid::new_v4();
        let run = FunctionRun::new("test-function", event_id, json!({"test": "data"}));

        sqlx::query("INSERT INTO runs (id, function_id, event_id, status, attempt, max_attempts, input) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(run.id.to_string())
            .bind(&run.function_id)
            .bind(run.event_id.to_string())
            .bind(run.status.as_str())
            .bind(run.attempt)
            .bind(run.max_attempts)
            .bind(json!(run.input).to_string())
            .execute(&pool)
            .await
            .unwrap();

        let retrieved: (String, i32) =
            sqlx::query_as("SELECT function_id, attempt FROM runs WHERE id = ?")
                .bind(run.id.to_string())
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(retrieved.0, "test-function");
        assert_eq!(retrieved.1, 0);
    }

    #[tokio::test]
    async fn test_run_status_transition() {
        let pool = create_sqlite_pool().await;
        sqlx::query("CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, function_id TEXT, event_id TEXT, status TEXT, attempt INTEGER, max_attempts INTEGER, input TEXT)").execute(&pool).await.unwrap();

        let run_id = Uuid::new_v4();

        sqlx::query("INSERT INTO runs (id, function_id, event_id, status, attempt, max_attempts, input) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(run_id.to_string())
            .bind("test")
            .bind(Uuid::new_v4().to_string())
            .bind("queued")
            .bind(0)
            .bind(3)
            .bind("{}")
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query("UPDATE runs SET status = ? WHERE id = ?")
            .bind("running")
            .bind(run_id.to_string())
            .execute(&pool)
            .await
            .unwrap();

        let status: String = sqlx::query_scalar("SELECT status FROM runs WHERE id = ?")
            .bind(run_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(status, "running");
    }

    #[tokio::test]
    async fn test_step_operations() {
        let pool = create_sqlite_pool().await;
        sqlx::query("CREATE TABLE IF NOT EXISTS steps (id TEXT PRIMARY KEY, run_id TEXT, step_id TEXT, status TEXT, input TEXT, output TEXT)").execute(&pool).await.unwrap();

        let run_id = Uuid::new_v4();
        let step = StepRun::new(run_id, "test-step").with_input(json!({"input": "value"}));

        sqlx::query(
            "INSERT INTO steps (id, run_id, step_id, status, input) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(step.id.to_string())
        .bind(run_id.to_string())
        .bind(&step.step_id)
        .bind(step.status.as_str())
        .bind(json!(step.input).to_string())
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query("UPDATE steps SET status = ?, output = ? WHERE id = ?")
            .bind("completed")
            .bind(json!({"result": "done"}).to_string())
            .bind(step.id.to_string())
            .execute(&pool)
            .await
            .unwrap();

        let output: String = sqlx::query_scalar("SELECT output FROM steps WHERE id = ?")
            .bind(step.id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();

        assert!(output.contains("result"));
    }

    #[tokio::test]
    async fn test_lock_operations() {
        let pool = create_sqlite_pool().await;
        sqlx::query("CREATE TABLE IF NOT EXISTS locks (key TEXT PRIMARY KEY, holder TEXT, expires_at TEXT, acquired_at TEXT)").execute(&pool).await.unwrap();

        let result = sqlx::query(
            "INSERT INTO locks (key, holder, expires_at, acquired_at) VALUES (?, ?, ?, ?) 
             ON CONFLICT(key) DO NOTHING",
        )
        .bind("test-lock")
        .bind("holder-1")
        .bind((Utc::now() + Duration::minutes(5)).to_rfc3339())
        .bind(Utc::now().to_rfc3339())
        .execute(&pool)
        .await
        .unwrap();

        assert!(result.rows_affected() > 0);
    }

    #[tokio::test]
    async fn test_get_run() {
        let pool = create_sqlite_pool().await;
        sqlx::query("CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, function_id TEXT, event_id TEXT, status TEXT, attempt INTEGER, max_attempts INTEGER, input TEXT)").execute(&pool).await.unwrap();

        let run_id = Uuid::new_v4();
        sqlx::query("INSERT INTO runs (id, function_id, event_id, status, attempt, max_attempts, input) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(run_id.to_string())
            .bind("test-func")
            .bind(Uuid::new_v4().to_string())
            .bind("queued")
            .bind(0)
            .bind(3)
            .bind("{}")
            .execute(&pool)
            .await
            .unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE id = ?")
            .bind(run_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_get_events_by_name() {
        let pool = create_sqlite_pool().await;
        sqlx::query("CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, name TEXT, data TEXT, timestamp TEXT)").execute(&pool).await.unwrap();

        for i in 0..5 {
            let event = Event::new(format!("test.event.{}", i), json!({"index": i}));
            sqlx::query("INSERT INTO events (id, name, data, timestamp) VALUES (?, ?, ?, ?)")
                .bind(event.id.to_string())
                .bind(&event.name)
                .bind(json!(event.data).to_string())
                .bind(event.timestamp.to_rfc3339())
                .execute(&pool)
                .await
                .unwrap();
        }

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE name LIKE 'test.event.%'")
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(count, 5);
    }
}
