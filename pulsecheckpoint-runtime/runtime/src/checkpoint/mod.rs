//! Checkpoint management with retry logic and idempotent writes

use backoff::ExponentialBackoff;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::config::RetryConfig;
use crate::metrics;
use crate::pulse::v1::{CheckpointInfo, CheckpointStatus, Metadata};
use crate::storage::{Storage, StorageError};

#[derive(Error, Debug)]
pub enum CheckpointError {
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),

    #[error("Checkpoint not found: {0}")]
    NotFound(String),

    #[error("Invalid checkpoint data: {0}")]
    InvalidData(String),

    #[error("Worker not registered: {0}")]
    WorkerNotRegistered(String),

    #[error("Checkpoint already exists with idempotency key: {0}")]
    IdempotentDuplicate(String),

    #[error("Upload failed after retries: {0}")]
    UploadFailed(String),
}

/// Checkpoint data stored in memory
#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub id: String,
    pub worker_id: String,
    pub storage_path: String,
    pub size_bytes: u64,
    pub checksum: String,
    pub metadata: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub status: CheckpointStatus,
}

impl Checkpoint {
    /// Convert to protobuf CheckpointInfo
    pub fn to_proto(&self) -> CheckpointInfo {
        CheckpointInfo {
            checkpoint_id: self.id.clone(),
            worker_id: self.worker_id.clone(),
            storage_path: self.storage_path.clone(),
            size_bytes: self.size_bytes as i64,
            checksum: self.checksum.clone(),
            metadata: Some(Metadata {
                labels: self.metadata.clone(),
            }),
            created_at: Some(crate::pulse::v1::Timestamp {
                seconds: self.created_at.timestamp(),
                nanos: self.created_at.timestamp_subsec_nanos() as i32,
            }),
            status: self.status.into(),
        }
    }
}

/// Checkpoint manager handling persistence and retrieval
pub struct CheckpointManager {
    storage: Arc<dyn Storage>,
    checkpoints: DashMap<String, Checkpoint>,
    idempotency_keys: DashMap<String, String>,
    retry_config: RetryConfig,
}

impl CheckpointManager {
    /// Create a new checkpoint manager
    pub fn new(storage: Arc<dyn Storage>, retry_config: RetryConfig) -> Self {
        Self {
            storage,
            checkpoints: DashMap::new(),
            idempotency_keys: DashMap::new(),
            retry_config,
        }
    }

    /// Save a checkpoint with retry logic
    #[instrument(skip(self, data), fields(worker_id = %worker_id, size = data.len()))]
    pub async fn save(
        &self,
        worker_id: &str,
        data: Bytes,
        metadata: HashMap<String, String>,
        idempotency_key: Option<String>,
    ) -> Result<Checkpoint, CheckpointError> {
        let timer = metrics::CHECKPOINT_DURATION.start_timer();

        // Check idempotency key
        if let Some(ref key) = idempotency_key {
            if let Some(existing_id) = self.idempotency_keys.get(key) {
                info!(
                    idempotency_key = %key,
                    checkpoint_id = %existing_id.value(),
                    "Returning existing checkpoint for idempotency key"
                );

                if let Some(checkpoint) = self.checkpoints.get(existing_id.value()) {
                    return Ok(checkpoint.clone());
                }
            }
        }

        // Generate checkpoint ID and calculate checksum
        let checkpoint_id = format!("chk_{}", Uuid::new_v4().to_string().replace("-", "")[..12].to_string());
        let checksum = compute_sha256(&data);
        let size = data.len() as u64;

        // Create storage path
        let storage_key = format!(
            "checkpoints/{}/{}/{}.bin",
            worker_id,
            Utc::now().format("%Y/%m/%d"),
            checkpoint_id
        );

        info!(
            checkpoint_id = %checkpoint_id,
            worker_id = %worker_id,
            size = size,
            storage_key = %storage_key,
            "Saving checkpoint"
        );

        // Create pending checkpoint
        let mut checkpoint = Checkpoint {
            id: checkpoint_id.clone(),
            worker_id: worker_id.to_string(),
            storage_path: String::new(),
            size_bytes: size,
            checksum: checksum.clone(),
            metadata,
            created_at: Utc::now(),
            status: CheckpointStatus::Uploading,
        };

        self.checkpoints.insert(checkpoint_id.clone(), checkpoint.clone());

        // Upload with retry
        let storage_path = self.upload_with_retry(&storage_key, data).await?;

        // Update checkpoint status
        checkpoint.storage_path = storage_path;
        checkpoint.status = CheckpointStatus::Completed;
        self.checkpoints.insert(checkpoint_id.clone(), checkpoint.clone());

        // Store idempotency key mapping
        if let Some(key) = idempotency_key {
            self.idempotency_keys.insert(key, checkpoint_id.clone());
        }

        // Update metrics
        metrics::CHECKPOINTS_TOTAL.inc();
        let duration_ms = timer.stop_and_record() * 1000.0;

        info!(
            checkpoint_id = %checkpoint_id,
            worker_id = %worker_id,
            storage_path = %checkpoint.storage_path,
            duration_ms = duration_ms,
            "Checkpoint saved successfully"
        );

        Ok(checkpoint)
    }

    /// Upload data to storage with exponential backoff retry
    async fn upload_with_retry(&self, key: &str, data: Bytes) -> Result<String, CheckpointError> {
        let backoff = ExponentialBackoff {
            initial_interval: Duration::from_millis(self.retry_config.initial_delay_ms),
            max_interval: Duration::from_millis(self.retry_config.max_delay_ms),
            multiplier: self.retry_config.multiplier,
            max_elapsed_time: Some(Duration::from_secs(60)),
            ..Default::default()
        };

        let mut attempt = 0;
        let max_attempts = self.retry_config.max_attempts;

        let result = backoff::future::retry(backoff, || {
            let storage = self.storage.clone();
            let key = key.to_string();
            let data = data.clone();
            attempt += 1;

            async move {
                debug!(attempt = attempt, max_attempts = max_attempts, key = %key, "Upload attempt");

                match storage.upload(&key, data).await {
                    Ok(path) => Ok(path),
                    Err(e) => {
                        warn!(
                            attempt = attempt,
                            error = %e,
                            key = %key,
                            "Upload attempt failed"
                        );

                        if attempt >= max_attempts {
                            Err(backoff::Error::permanent(e))
                        } else {
                            Err(backoff::Error::transient(e))
                        }
                    }
                }
            }
        })
        .await;

        result.map_err(|e| CheckpointError::UploadFailed(format!("{}", e)))
    }

    /// Get a checkpoint by ID
    pub fn get(&self, checkpoint_id: &str) -> Option<Checkpoint> {
        self.checkpoints.get(checkpoint_id).map(|c| c.clone())
    }

    /// Get checkpoint data from storage
    #[instrument(skip(self))]
    pub async fn get_data(&self, checkpoint_id: &str) -> Result<Bytes, CheckpointError> {
        let checkpoint = self.checkpoints
            .get(checkpoint_id)
            .ok_or_else(|| CheckpointError::NotFound(checkpoint_id.to_string()))?;

        // Extract the storage key from the path
        let storage_key = checkpoint.storage_path
            .strip_prefix("s3://")
            .and_then(|s| s.split_once('/'))
            .map(|(_, key)| key)
            .ok_or_else(|| CheckpointError::InvalidData("Invalid storage path".into()))?;

        let data = self.storage.download(storage_key).await?;

        // Verify checksum
        let actual_checksum = compute_sha256(&data);
        if actual_checksum != checkpoint.checksum {
            error!(
                checkpoint_id = %checkpoint_id,
                expected = %checkpoint.checksum,
                actual = %actual_checksum,
                "Checksum mismatch"
            );
            return Err(CheckpointError::InvalidData("Checksum mismatch".into()));
        }

        Ok(data)
    }

    /// List checkpoints, optionally filtered by worker
    pub fn list(
        &self,
        worker_id: Option<&str>,
        status_filter: Option<CheckpointStatus>,
    ) -> Vec<Checkpoint> {
        self.checkpoints
            .iter()
            .filter(|entry| {
                let checkpoint = entry.value();
                let worker_match = worker_id.map_or(true, |w| checkpoint.worker_id == w);
                let status_match = status_filter.map_or(true, |s| checkpoint.status == s);
                worker_match && status_match
            })
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Delete a checkpoint
    #[instrument(skip(self))]
    pub async fn delete(&self, checkpoint_id: &str) -> Result<(), CheckpointError> {
        let checkpoint = self.checkpoints
            .remove(checkpoint_id)
            .ok_or_else(|| CheckpointError::NotFound(checkpoint_id.to_string()))?
            .1;

        // Extract storage key and delete from storage
        if let Some(storage_key) = checkpoint.storage_path
            .strip_prefix("s3://")
            .and_then(|s| s.split_once('/'))
            .map(|(_, key)| key)
        {
            self.storage.delete(storage_key).await?;
        }

        info!(checkpoint_id = %checkpoint_id, "Checkpoint deleted");
        Ok(())
    }

    /// Get total checkpoint count
    pub fn count(&self) -> usize {
        self.checkpoints.len()
    }
}

/// Compute SHA-256 checksum of data
fn compute_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_sha256() {
        let data = b"hello world";
        let checksum = compute_sha256(data);
        assert_eq!(
            checksum,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_checkpoint_to_proto() {
        let checkpoint = Checkpoint {
            id: "chk_123".to_string(),
            worker_id: "worker-1".to_string(),
            storage_path: "s3://bucket/key".to_string(),
            size_bytes: 1024,
            checksum: "abc123".to_string(),
            metadata: HashMap::new(),
            created_at: Utc::now(),
            status: CheckpointStatus::Completed,
        };

        let proto = checkpoint.to_proto();
        assert_eq!(proto.checkpoint_id, "chk_123");
        assert_eq!(proto.worker_id, "worker-1");
        assert_eq!(proto.size_bytes, 1024);
    }
}