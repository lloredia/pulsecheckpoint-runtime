//! Configuration management for PulseCheckpoint Runtime

use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    ReadError(#[from] std::io::Error),
    
    #[error("Failed to parse config: {0}")]
    ParseError(#[from] toml::de::Error),
    
    #[error("Missing required environment variable: {0}")]
    MissingEnvVar(String),
    
    #[error("Invalid configuration: {0}")]
    ValidationError(String),
}

/// Main application configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageConfig {
    #[serde(default = "default_s3_endpoint")]
    pub endpoint: String,
    
    #[serde(default = "default_bucket")]
    pub bucket: String,
    
    #[serde(default = "default_region")]
    pub region: String,
    
    #[serde(default)]
    pub access_key_id: Option<String>,
    
    #[serde(default)]
    pub secret_access_key: Option<String>,
    
    #[serde(default = "default_max_upload_size")]
    pub max_upload_size_bytes: u64,
    
    #[serde(default)]
    pub path_prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    
    #[serde(default = "default_log_format")]
    pub format: String,
}

// Default value functions
fn default_grpc_addr() -> String { "0.0.0.0:50051".to_string() }
fn default_metrics_addr() -> String { "0.0.0.0:9090".to_string() }
fn default_heartbeat_interval() -> u64 { 30 }
fn default_heartbeat_timeout() -> u64 { 90 }
fn default_s3_endpoint() -> String { "http://localhost:9000".to_string() }
fn default_bucket() -> String { "checkpoints".to_string() }
fn default_region() -> String { "us-east-1".to_string() }
fn default_max_upload_size() -> u64 { 5 * 1024 * 1024 * 1024 } // 5GB
fn default_max_attempts() -> u32 { 3 }
fn default_initial_delay_ms() -> u64 { 100 }
fn default_max_delay_ms() -> u64 { 5000 }
fn default_multiplier() -> f64 { 2.0 }
fn default_log_level() -> String { "info".to_string() }
fn default_log_format() -> String { "json".to_string() }

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
            endpoint: default_s3_endpoint(),
            bucket: default_bucket(),
            region: default_region(),
            access_key_id: None,
            secret_access_key: None,
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

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            retry: RetryConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

impl AppConfig {
    /// Load configuration from a TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path)?;
        let config: AppConfig = toml::from_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self, ConfigError> {
        let mut config = AppConfig::default();

        // Server config
        if let Ok(val) = std::env::var("PULSE_GRPC_ADDR") {
            config.server.grpc_addr = val;
        }
        if let Ok(val) = std::env::var("PULSE_METRICS_ADDR") {
            config.server.metrics_addr = val;
        }
        if let Ok(val) = std::env::var("PULSE_HEARTBEAT_INTERVAL_SECS") {
            config.server.heartbeat_interval_secs = val.parse().unwrap_or(default_heartbeat_interval());
        }

        // Storage config
        if let Ok(val) = std::env::var("PULSE_S3_ENDPOINT") {
            config.storage.endpoint = val;
        }
        if let Ok(val) = std::env::var("PULSE_S3_BUCKET") {
            config.storage.bucket = val;
        }
        if let Ok(val) = std::env::var("PULSE_S3_REGION") {
            config.storage.region = val;
        }
        if let Ok(val) = std::env::var("AWS_ACCESS_KEY_ID") {
            config.storage.access_key_id = Some(val);
        }
        if let Ok(val) = std::env::var("AWS_SECRET_ACCESS_KEY") {
            config.storage.secret_access_key = Some(val);
        }
        if let Ok(val) = std::env::var("PULSE_S3_PATH_PREFIX") {
            config.storage.path_prefix = Some(val);
        }

        // Retry config
        if let Ok(val) = std::env::var("PULSE_MAX_RETRIES") {
            config.retry.max_attempts = val.parse().unwrap_or(default_max_attempts());
        }
        if let Ok(val) = std::env::var("PULSE_RETRY_DELAY_MS") {
            config.retry.initial_delay_ms = val.parse().unwrap_or(default_initial_delay_ms());
        }

        // Logging config
        if let Ok(val) = std::env::var("PULSE_LOG_LEVEL") {
            config.logging.level = val;
        }

        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration
    fn validate(&self) -> Result<(), ConfigError> {
        if self.storage.bucket.is_empty() {
            return Err(ConfigError::ValidationError("S3 bucket name cannot be empty".into()));
        }
        if self.retry.max_attempts == 0 {
            return Err(ConfigError::ValidationError("Max retry attempts must be at least 1".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AppConfig::default();
        assert_eq!(config.server.grpc_addr, "0.0.0.0:50051");
        assert_eq!(config.storage.bucket, "checkpoints");
    }

    #[test]
    fn test_config_validation() {
        let mut config = AppConfig::default();
        config.storage.bucket = "".to_string();
        assert!(config.validate().is_err());
    }
}
