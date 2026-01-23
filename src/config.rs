//! Configuration for Choreo

use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    #[serde(default)]
    pub worker: WorkerConfig,
}

impl Config {
    pub fn from_env() -> Result<Self, config::ConfigError> {
        config::Config::builder()
            .add_source(config::Environment::with_prefix("CHOREO").separator("__"))
            .build()?
            .try_deserialize()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { host: default_host(), port: default_port() }
    }
}

impl ServerConfig {
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

fn default_host() -> String { "0.0.0.0".to_string() }
fn default_port() -> u16 { 8080 }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DatabaseConfig {
    Postgres { url: String, #[serde(default = "default_max_conn")] max_connections: u32 },
    Sqlite { path: String },
}

impl DatabaseConfig {
    pub fn postgres(url: impl Into<String>) -> Self {
        Self::Postgres { url: url.into(), max_connections: default_max_conn() }
    }
    pub fn sqlite(path: impl Into<String>) -> Self {
        Self::Sqlite { path: path.into() }
    }
    pub fn in_memory() -> Self {
        Self::Sqlite { path: ":memory:".to_string() }
    }
}

fn default_max_conn() -> u32 { 10 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    #[serde(default = "default_worker_id")]
    pub worker_id: String,
    #[serde(default = "default_poll_interval")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: i64,
    #[serde(default = "default_lock_duration")]
    pub lock_duration_secs: i64,
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: default_worker_id(),
            poll_interval_ms: default_poll_interval(),
            batch_size: default_batch_size(),
            lock_duration_secs: default_lock_duration(),
            max_concurrent: default_max_concurrent(),
        }
    }
}

impl WorkerConfig {
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }
}

fn default_worker_id() -> String { format!("worker-{}", uuid::Uuid::now_v7()) }
fn default_poll_interval() -> u64 { 1000 }
fn default_batch_size() -> i64 { 10 }
fn default_lock_duration() -> i64 { 300 }
fn default_max_concurrent() -> usize { 10 }
