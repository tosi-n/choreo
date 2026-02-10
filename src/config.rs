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
        Self {
            host: default_host(),
            port: default_port(),
        }
    }
}

impl ServerConfig {
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    8080
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DatabaseConfig {
    Postgres {
        url: String,
        #[serde(default = "default_max_conn")]
        max_connections: u32,
    },
    Sqlite {
        path: String,
    },
}

impl DatabaseConfig {
    pub fn postgres(url: impl Into<String>) -> Self {
        Self::Postgres {
            url: url.into(),
            max_connections: default_max_conn(),
        }
    }
    pub fn sqlite(path: impl Into<String>) -> Self {
        Self::Sqlite { path: path.into() }
    }
    pub fn in_memory() -> Self {
        Self::Sqlite {
            path: ":memory:".to_string(),
        }
    }
}

fn default_max_conn() -> u32 {
    10
}

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

fn default_worker_id() -> String {
    format!("worker-{}", uuid::Uuid::now_v7())
}
fn default_poll_interval() -> u64 {
    1000
}
fn default_batch_size() -> i64 {
    10
}
fn default_lock_duration() -> i64 {
    300
}
fn default_max_concurrent() -> usize {
    10
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_config_sqlite() {
        let config = DatabaseConfig::sqlite("test.db");
        match config {
            DatabaseConfig::Sqlite { path } => {
                assert_eq!(path, "test.db");
            }
            _ => panic!("Expected Sqlite variant"),
        }
    }

    #[test]
    fn test_database_config_postgres() {
        let config = DatabaseConfig::postgres("postgres://localhost/choreo");
        match config {
            DatabaseConfig::Postgres {
                url,
                max_connections,
            } => {
                assert_eq!(url, "postgres://localhost/choreo");
                assert_eq!(max_connections, 10);
            }
            _ => panic!("Expected Postgres variant"),
        }
    }

    #[test]
    fn test_database_config_in_memory() {
        let config = DatabaseConfig::in_memory();
        match config {
            DatabaseConfig::Sqlite { path } => {
                assert_eq!(path, ":memory:");
            }
            _ => panic!("Expected Sqlite variant"),
        }
    }

    #[test]
    fn test_database_config_postgres_with_custom_pool() {
        let config = DatabaseConfig::Postgres {
            url: "postgres://localhost/choreo".to_string(),
            max_connections: 20,
        };
        match config {
            DatabaseConfig::Postgres {
                url,
                max_connections,
            } => {
                assert_eq!(url, "postgres://localhost/choreo");
                assert_eq!(max_connections, 20);
            }
            _ => panic!("Expected Postgres variant"),
        }
    }

    #[test]
    fn test_worker_config_new() {
        let config = WorkerConfig {
            worker_id: "test-worker".to_string(),
            poll_interval_ms: 2000,
            batch_size: 20,
            lock_duration_secs: 600,
            max_concurrent: 20,
        };
        assert_eq!(config.worker_id, "test-worker");
        assert_eq!(config.poll_interval(), Duration::from_secs(2));
        assert_eq!(config.batch_size, 20);
        assert_eq!(config.lock_duration_secs, 600);
        assert_eq!(config.max_concurrent, 20);
    }

    #[test]
    fn test_worker_config_default() {
        let config = WorkerConfig::default();
        assert!(config.worker_id.starts_with("worker-"));
        assert_eq!(config.poll_interval_ms, 1000);
        assert_eq!(config.batch_size, 10);
        assert_eq!(config.lock_duration_secs, 300);
        assert_eq!(config.max_concurrent, 10);
    }

    #[test]
    fn test_worker_config_poll_interval() {
        let config = WorkerConfig {
            poll_interval_ms: 500,
            ..Default::default()
        };
        assert_eq!(config.poll_interval(), Duration::from_millis(500));
    }

    #[test]
    fn test_config_with_sqlite() {
        let config = Config {
            server: ServerConfig::default(),
            database: DatabaseConfig::sqlite("choreo.db"),
            worker: WorkerConfig::default(),
        };
        match &config.database {
            DatabaseConfig::Sqlite { path } => {
                assert_eq!(path, "choreo.db");
            }
            _ => panic!("Expected Sqlite"),
        }
    }

    #[test]
    fn test_config_with_postgres() {
        let config = Config {
            server: ServerConfig::default(),
            database: DatabaseConfig::postgres("postgres://localhost/choreo"),
            worker: WorkerConfig::default(),
        };
        match &config.database {
            DatabaseConfig::Postgres { url, .. } => {
                assert!(url.contains("postgres"));
            }
            _ => panic!("Expected Postgres"),
        }
    }

    #[test]
    fn test_server_config_address() {
        let server = ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 3000,
        };
        assert_eq!(server.address(), "127.0.0.1:3000");
    }

    #[test]
    fn test_server_config_default() {
        let server = ServerConfig::default();
        assert_eq!(server.host, "0.0.0.0");
        assert_eq!(server.port, 8080);
        assert_eq!(server.address(), "0.0.0.0:8080");
    }

    #[test]
    fn test_worker_config_batch_size() {
        let config = WorkerConfig {
            batch_size: 100,
            ..Default::default()
        };
        assert_eq!(config.batch_size, 100);
    }

    #[test]
    fn test_worker_config_lock_duration() {
        let config = WorkerConfig {
            lock_duration_secs: 1200,
            ..Default::default()
        };
        assert_eq!(config.lock_duration_secs, 1200);
    }

    #[test]
    fn test_worker_config_max_concurrent() {
        let config = WorkerConfig {
            max_concurrent: 100,
            ..Default::default()
        };
        assert_eq!(config.max_concurrent, 100);
    }
}
