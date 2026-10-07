//! Process configuration loaded from a TOML file or environment variables.
//!
//! AWS credentials are intentionally not part of this config. The storage
//! client uses the AWS SDK default credential chain.

use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    Read(#[from] std::io::Error),

    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),

    #[error("invalid value for {0}")]
    InvalidValue(String),

    #[error("invalid configuration: {0}")]
    Validation(String),
}

/// Top-level runtime configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,

    #[serde(default)]
    pub storage: StorageConfig,

    #[serde(default)]
    pub retry: RetryConfig,

    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_grpc_addr")]
    pub grpc_addr: String,

    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: String,

    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_secs: u64,

    #[serde(default = "default_heartbeat_timeout")]
    pub heartbeat_timeout_secs: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// Custom S3 endpoint for MinIO or LocalStack.
    /// `None` uses the AWS SDK regional endpoint.
    #[serde(default)]
    pub endpoint: Option<String>,

    #[serde(default = "default_bucket")]
    pub bucket: String,

    #[serde(default = "default_region")]
    pub region: String,

    /// Path-style addressing. Needed by MinIO and LocalStack.
    /// Ignored when `endpoint` is unset.
    #[serde(default = "default_true")]
    pub force_path_style: bool,

    #[serde(default = "default_max_upload_size")]
    pub max_upload_size_bytes: u64,

    #[serde(default)]
    pub path_prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,

    #[serde(default = "default_initial_delay_ms")]
    pub initial_delay_ms: u64,

    #[serde(default = "default_max_delay_ms")]
    pub max_delay_ms: u64,

    #[serde(default = "default_multiplier")]
    pub multiplier: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,

    #[serde(default = "default_log_format")]
    pub format: String,
}

fn default_grpc_addr() -> String {
    "0.0.0.0:50051".to_string()
}
fn default_metrics_addr() -> String {
    "0.0.0.0:9090".to_string()
}
fn default_heartbeat_interval() -> u64 {
    30
}
fn default_heartbeat_timeout() -> u64 {
    90
}
fn default_bucket() -> String {
    "checkpoints".to_string()
}
fn default_region() -> String {
    "us-east-1".to_string()
}
fn default_true() -> bool {
    true
}
fn default_max_upload_size() -> u64 {
    5 * 1024 * 1024 * 1024
}
fn default_max_attempts() -> u32 {
    3
}
fn default_initial_delay_ms() -> u64 {
    100
}
fn default_max_delay_ms() -> u64 {
    5000
}
fn default_multiplier() -> f64 {
    2.0
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_log_format() -> String {
    "json".to_string()
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            grpc_addr: default_grpc_addr(),
            metrics_addr: default_metrics_addr(),
            heartbeat_interval_secs: default_heartbeat_interval(),
            heartbeat_timeout_secs: default_heartbeat_timeout(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            endpoint: None,
            bucket: default_bucket(),
            region: default_region(),
            force_path_style: true,
            max_upload_size_bytes: default_max_upload_size(),
            path_prefix: None,
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            initial_delay_ms: default_initial_delay_ms(),
            max_delay_ms: default_max_delay_ms(),
            multiplier: default_multiplier(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

impl AppConfig {
    /// Load configuration from a TOML file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&contents)?;
        config.validated()
    }

    /// Load configuration from process environment variables.
    ///
    /// Credentials are not read here. Set `AWS_ACCESS_KEY_ID` and
    /// `AWS_SECRET_ACCESS_KEY` (or a profile / role) only when the AWS SDK
    /// should see them through its default chain.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_vars(|key| std::env::var(key).ok())
    }

    /// Load configuration from an explicit lookup, used by tests and `from_env`.
    pub fn from_vars(mut var: impl FnMut(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let mut config = Self::default();

        if let Some(value) = var("PULSE_GRPC_ADDR") {
            config.server.grpc_addr = value;
        }
        if let Some(value) = var("PULSE_METRICS_ADDR") {
            config.server.metrics_addr = value;
        }
        if let Some(value) = var("PULSE_HEARTBEAT_INTERVAL_SECS") {
            config.server.heartbeat_interval_secs =
                parse_u64("PULSE_HEARTBEAT_INTERVAL_SECS", &value)?;
        }
        if let Some(value) = var("PULSE_HEARTBEAT_TIMEOUT_SECS") {
            config.server.heartbeat_timeout_secs =
                parse_u64("PULSE_HEARTBEAT_TIMEOUT_SECS", &value)?;
        }

        if let Some(value) = var("PULSE_S3_ENDPOINT") {
            config.storage.endpoint = if value.is_empty() { None } else { Some(value) };
        }
        if let Some(value) = var("PULSE_S3_BUCKET") {
            config.storage.bucket = value;
        }
        if let Some(value) = var("PULSE_S3_REGION") {
            config.storage.region = value;
        }
        if let Some(value) = var("PULSE_S3_FORCE_PATH_STYLE") {
            config.storage.force_path_style = parse_bool("PULSE_S3_FORCE_PATH_STYLE", &value)?;
        }
        if let Some(value) = var("PULSE_S3_PATH_PREFIX") {
            config.storage.path_prefix = if value.is_empty() { None } else { Some(value) };
        }

        if let Some(value) = var("PULSE_MAX_RETRIES") {
            config.retry.max_attempts = parse_u32("PULSE_MAX_RETRIES", &value)?;
        }
        if let Some(value) = var("PULSE_RETRY_DELAY_MS") {
            config.retry.initial_delay_ms = parse_u64("PULSE_RETRY_DELAY_MS", &value)?;
        }
        if let Some(value) = var("PULSE_LOG_LEVEL") {
            config.logging.level = value;
        }

        config.validated()
    }

    fn validated(mut self) -> Result<Self, ConfigError> {
        if let Some(endpoint) = &self.storage.endpoint {
            if endpoint.is_empty() {
                self.storage.endpoint = None;
            }
        }
        if self.storage.bucket.is_empty() {
            return Err(ConfigError::Validation(
                "S3 bucket name cannot be empty".into(),
            ));
        }
        if self.storage.region.is_empty() {
            return Err(ConfigError::Validation("S3 region cannot be empty".into()));
        }
        if self.retry.max_attempts == 0 {
            return Err(ConfigError::Validation(
                "max retry attempts must be at least 1".into(),
            ));
        }
        if self.server.heartbeat_timeout_secs == 0 {
            return Err(ConfigError::Validation(
                "heartbeat timeout must be at least 1 second".into(),
            ));
        }
        if self
            .server
            .grpc_addr
            .parse::<std::net::SocketAddr>()
            .is_err()
        {
            return Err(ConfigError::Validation(format!(
                "invalid grpc address {}",
                self.server.grpc_addr
            )));
        }
        if self
            .server
            .metrics_addr
            .parse::<std::net::SocketAddr>()
            .is_err()
        {
            return Err(ConfigError::Validation(format!(
                "invalid metrics address {}",
                self.server.metrics_addr
            )));
        }
        Ok(self)
    }
}

fn parse_u64(key: &str, raw: &str) -> Result<u64, ConfigError> {
    raw.parse()
        .map_err(|_| ConfigError::InvalidValue(format!("{key}={raw}")))
}

fn parse_u32(key: &str, raw: &str) -> Result<u32, ConfigError> {
    raw.parse()
        .map_err(|_| ConfigError::InvalidValue(format!("{key}={raw}")))
}

fn parse_bool(key: &str, raw: &str) -> Result<bool, ConfigError> {
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(ConfigError::InvalidValue(format!("{key}={raw}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_target_aws_endpoint_resolution() {
        let config = AppConfig::default();
        assert!(config.storage.endpoint.is_none());
        assert_eq!(config.storage.bucket, "checkpoints");
        assert_eq!(config.server.grpc_addr, "0.0.0.0:50051");
        assert!(config.storage.force_path_style);
    }

    #[test]
    fn rejects_empty_bucket() {
        let mut config = AppConfig::default();
        config.storage.bucket.clear();
        assert!(config.validated().is_err());
    }

    #[test]
    fn from_file_reads_custom_endpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
            [storage]
            endpoint = "http://127.0.0.1:9000"
            bucket = "lab"
            region = "us-east-1"
            force_path_style = true
            "#,
        )
        .unwrap();

        let config = AppConfig::from_file(&path).unwrap();
        assert_eq!(
            config.storage.endpoint.as_deref(),
            Some("http://127.0.0.1:9000")
        );
        assert_eq!(config.storage.bucket, "lab");
    }

    #[test]
    fn rejects_credential_fields_in_config_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
            [storage]
            bucket = "lab"
            access_key_id = "not-a-real-key"
            "#,
        )
        .unwrap();
        assert!(AppConfig::from_file(&path).is_err());
    }

    #[test]
    fn from_vars_sets_endpoint_and_retries() {
        let config = AppConfig::from_vars(|key| match key {
            "PULSE_S3_ENDPOINT" => Some("http://minio:9000".into()),
            "PULSE_S3_BUCKET" => Some("ckpt".into()),
            "PULSE_S3_FORCE_PATH_STYLE" => Some("true".into()),
            "PULSE_MAX_RETRIES" => Some("5".into()),
            "PULSE_GRPC_ADDR" => Some("127.0.0.1:50051".into()),
            _ => None,
        })
        .unwrap();

        assert_eq!(
            config.storage.endpoint.as_deref(),
            Some("http://minio:9000")
        );
        assert_eq!(config.storage.bucket, "ckpt");
        assert_eq!(config.retry.max_attempts, 5);
        assert_eq!(config.server.grpc_addr, "127.0.0.1:50051");
    }

    #[test]
    fn empty_endpoint_var_means_aws_default() {
        let config = AppConfig::from_vars(|key| match key {
            "PULSE_S3_ENDPOINT" => Some(String::new()),
            _ => None,
        })
        .unwrap();
        assert!(config.storage.endpoint.is_none());
    }

    #[test]
    fn rejects_bad_numbers() {
        let err = AppConfig::from_vars(|key| match key {
            "PULSE_MAX_RETRIES" => Some("zero".into()),
            _ => None,
        });
        assert!(err.is_err());
    }
}
