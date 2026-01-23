//! Choreo Server
//!
//! Run with: `cargo run` or `choreo`
//!
//! Configuration via environment variables:
//! - CHOREO_DATABASE__TYPE: postgres or sqlite
//! - CHOREO_DATABASE__URL: connection string (postgres)
//! - CHOREO_DATABASE__PATH: file path (sqlite)
//! - CHOREO_SERVER__PORT: port to listen on (default: 8080)

use std::sync::Arc;

use choreo::{
    api::{self, AppState},
    config::{Config, DatabaseConfig},
    StateStore,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "choreo=info,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Starting Choreo server...");

    // Load config from environment or use defaults
    let config = Config::from_env().unwrap_or_else(|_| {
        tracing::warn!("No config found, using SQLite in-memory database");
        Config {
            server: Default::default(),
            database: DatabaseConfig::in_memory(),
            worker: Default::default(),
        }
    });

    // Connect to database based on config
    match &config.database {
        #[cfg(feature = "postgres")]
        DatabaseConfig::Postgres {
            url,
            max_connections,
        } => {
            use choreo::PostgresStore;

            tracing::info!("Connecting to PostgreSQL...");
            let store = PostgresStore::connect_with_options(url, *max_connections).await?;
            store.migrate().await?;
            tracing::info!("Database migrated");

            run_server(config, store).await
        }

        #[cfg(feature = "sqlite")]
        DatabaseConfig::Sqlite { path } => {
            use choreo::SqliteStore;

            tracing::info!("Connecting to SQLite at {}...", path);
            let url = if path == ":memory:" {
                "sqlite::memory:".to_string()
            } else {
                format!("sqlite://{}?mode=rwc", path)
            };
            let store = SqliteStore::connect(&url).await?;
            store.migrate().await?;
            tracing::info!("Database migrated");

            run_server(config, store).await
        }

        #[allow(unreachable_patterns)]
        _ => {
            anyhow::bail!("No database backend enabled. Compile with --features postgres or --features sqlite");
        }
    }
}

async fn run_server<S: choreo::StateStore + Clone>(config: Config, store: S) -> anyhow::Result<()> {
    let state = Arc::new(AppState { store });
    let app = api::router(state);

    let addr = config.server.address();
    tracing::info!("Listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
