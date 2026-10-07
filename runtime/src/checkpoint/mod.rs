//! Checkpoint index plus retrying writes to a [`Storage`] backend.
//!
//! The index lives in memory. The bytes live in the storage backend.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::RetryConfig;
use crate::metrics;
use crate::pulse::v1::{CheckpointInfo, CheckpointStatus, Metadata};
use crate::storage::{Storage, StorageError};

#[derive(Debug, Error)]
pub enum CheckpointError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("checkpoint not found: {0}")]
    NotFound(String),

    #[error("invalid checkpoint data: {0}")]
    InvalidData(String),

    #[error("upload failed after retries: {0}")]
    UploadFailed(String),
}

#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub id: String,
    pub worker_id: String,
    pub storage_key: String,
    pub storage_path: String,
    pub size_bytes: u64,
    pub checksum: String,
    pub metadata: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub status: CheckpointStatus,
}

impl Checkpoint {
    pub fn to_proto(&self) -> CheckpointInfo {
        CheckpointInfo {
            checkpoint_id: self.id.clone(),
            worker_id: self.worker_id.clone(),
            storage_path: self.storage_path.clone(),
            size_bytes: i64::try_from(self.size_bytes).unwrap_or(i64::MAX),
            checksum: self.checksum.clone(),
            metadata: Some(Metadata {
                labels: self.metadata.clone(),
            }),
            created_at: Some(crate::pulse::v1::Timestamp {
                seconds: self.created_at.timestamp(),
                nanos: i32::try_from(self.created_at.timestamp_subsec_nanos()).unwrap_or(0),
            }),
            status: self.status.into(),
        }
    }
}

pub struct CheckpointManager {
    storage: Arc<dyn Storage>,
    checkpoints: DashMap<String, Checkpoint>,
    idempotency_keys: DashMap<String, String>,
    retry_config: RetryConfig,
}

impl CheckpointManager {
    pub fn new(storage: Arc<dyn Storage>, retry_config: RetryConfig) -> Self {
        Self {
            storage,
            checkpoints: DashMap::new(),
            idempotency_keys: DashMap::new(),
            retry_config,
        }
    }

    pub async fn save(
        &self,
        worker_id: &str,
        data: Bytes,
        metadata: HashMap<String, String>,
        idempotency_key: Option<String>,
    ) -> Result<Checkpoint, CheckpointError> {
        if let Some(existing) = self.existing_idempotent(idempotency_key.as_deref()) {
            return Ok(existing);
        }

        let timer = metrics::CHECKPOINT_DURATION.start_timer();
        let checkpoint_id = new_checkpoint_id();
        let checksum = sha256(&data);
        let size = u64::try_from(data.len()).unwrap_or(u64::MAX);
        let day = Utc::now().format("%Y/%m/%d");
        let storage_key = format!("checkpoints/{worker_id}/{day}/{checkpoint_id}.bin");

        let checkpoint = Checkpoint {
            id: checkpoint_id.clone(),
            worker_id: worker_id.to_string(),
            storage_key: storage_key.clone(),
            storage_path: String::new(),
            size_bytes: size,
            checksum: checksum.clone(),
            metadata,
            created_at: Utc::now(),
            status: CheckpointStatus::Uploading,
        };
        self.checkpoints
            .insert(checkpoint_id.clone(), checkpoint.clone());

        let storage_path = match self.upload_with_retry(&storage_key, data).await {
            Ok(path) => path,
            Err(err) => {
                if let Some(mut existing) = self.checkpoints.get_mut(&checkpoint_id) {
                    existing.status = CheckpointStatus::Failed;
                }
                timer.stop_and_discard();
                return Err(err);
            }
        };

        let finished = Checkpoint {
            storage_path: storage_path.clone(),
            status: CheckpointStatus::Completed,
            ..checkpoint
        };
        self.checkpoints
            .insert(checkpoint_id.clone(), finished.clone());
        if let Some(key) = idempotency_key {
            self.idempotency_keys.insert(key, checkpoint_id.clone());
        }
        metrics::CHECKPOINTS_TOTAL.inc();
        metrics::CHECKPOINT_BYTES_TOTAL.inc_by(size);
        timer.observe_duration();
        info!(checkpoint_id = %checkpoint_id, storage_path = %storage_path, "checkpoint saved");
        Ok(finished)
    }

    fn existing_idempotent(&self, key: Option<&str>) -> Option<Checkpoint> {
        let key = key?;
        let id = self.idempotency_keys.get(key)?.clone();
        let checkpoint = self.checkpoints.get(&id)?.clone();
        info!(idempotency_key = key, checkpoint_id = %checkpoint.id, "idempotent checkpoint hit");
        Some(checkpoint)
    }

    async fn upload_with_retry(&self, key: &str, data: Bytes) -> Result<String, CheckpointError> {
        let mut delay = self.retry_config.initial_delay_ms.max(1);
        let mut last_error = None;
        for attempt in 1..=self.retry_config.max_attempts {
            match self.storage.upload(key, data.clone()).await {
                Ok(path) => return Ok(path),
                Err(err) => {
                    warn!(attempt, error = %err, key, "checkpoint upload attempt failed");
                    last_error = Some(err);
                    if attempt == self.retry_config.max_attempts {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    let scaled = (delay as f64) * self.retry_config.multiplier;
                    delay = (scaled as u64).clamp(1, self.retry_config.max_delay_ms.max(1));
                }
            }
        }
        Err(CheckpointError::UploadFailed(
            last_error
                .map(|err| err.to_string())
                .unwrap_or_else(|| "upload failed".to_string()),
        ))
    }

    pub fn get(&self, checkpoint_id: &str) -> Option<Checkpoint> {
        self.checkpoints
            .get(checkpoint_id)
            .map(|checkpoint| checkpoint.clone())
    }

    pub async fn get_data(&self, checkpoint_id: &str) -> Result<Bytes, CheckpointError> {
        let checkpoint = self
            .checkpoints
            .get(checkpoint_id)
            .map(|checkpoint| checkpoint.clone())
            .ok_or_else(|| CheckpointError::NotFound(checkpoint_id.to_string()))?;
        if checkpoint.status != CheckpointStatus::Completed {
            return Err(CheckpointError::InvalidData(format!(
                "checkpoint {} is not complete",
                checkpoint.id
            )));
        }
        let data = self.storage.download(&checkpoint.storage_key).await?;
        let actual = sha256(&data);
        if actual != checkpoint.checksum {
            return Err(CheckpointError::InvalidData(format!(
                "checksum mismatch for {}",
                checkpoint.id
            )));
        }
        Ok(data)
    }

    pub fn list(
        &self,
        worker_id: Option<&str>,
        status_filter: Option<CheckpointStatus>,
    ) -> Vec<Checkpoint> {
        let mut checkpoints: Vec<_> = self
            .checkpoints
            .iter()
            .filter(|entry| {
                let checkpoint = entry.value();
                let worker_ok = worker_id.is_none_or(|id| checkpoint.worker_id == id);
                let status_ok = status_filter.is_none_or(|status| checkpoint.status == status);
                worker_ok && status_ok
            })
            .map(|entry| entry.value().clone())
            .collect();
        checkpoints.sort_by(|left, right| left.id.cmp(&right.id));
        checkpoints
    }

    pub async fn delete(&self, checkpoint_id: &str) -> Result<(), CheckpointError> {
        let (_, checkpoint) = self
            .checkpoints
            .remove(checkpoint_id)
            .ok_or_else(|| CheckpointError::NotFound(checkpoint_id.to_string()))?;
        self.idempotency_keys.retain(|_, id| id != checkpoint_id);
        if !checkpoint.storage_key.is_empty() {
            match self.storage.delete(&checkpoint.storage_key).await {
                Ok(()) => {}
                Err(StorageError::NotFound(_)) => {}
                Err(err) => return Err(err.into()),
            }
        }
        info!(checkpoint_id, "checkpoint deleted");
        Ok(())
    }

    pub async fn check_storage(&self) -> Result<(), CheckpointError> {
        self.storage
            .health()
            .await
            .map_err(CheckpointError::Storage)
    }

    pub fn count(&self) -> usize {
        self.checkpoints.len()
    }
}

fn new_checkpoint_id() -> String {
    let hex = Uuid::new_v4().simple().to_string();
    format!("chk_{}", &hex[..12])
}

pub fn sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStorage;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct FlakyStorage {
        inner: MemoryStorage,
        remaining_failures: AtomicU32,
    }

    #[async_trait::async_trait]
    impl Storage for FlakyStorage {
        async fn upload(&self, key: &str, data: Bytes) -> Result<String, StorageError> {
            let left = self.remaining_failures.load(Ordering::SeqCst);
            if left > 0 {
                self.remaining_failures.fetch_sub(1, Ordering::SeqCst);
                return Err(StorageError::Backend("temporary".into()));
            }
            self.inner.upload(key, data).await
        }
        async fn download(&self, key: &str) -> Result<Bytes, StorageError> {
            self.inner.download(key).await
        }
        async fn delete(&self, key: &str) -> Result<(), StorageError> {
            self.inner.delete(key).await
        }
        async fn exists(&self, key: &str) -> Result<bool, StorageError> {
            self.inner.exists(key).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
            self.inner.list(prefix).await
        }
        async fn head(&self, key: &str) -> Result<crate::storage::ObjectMetadata, StorageError> {
            self.inner.head(key).await
        }
        async fn health(&self) -> Result<(), StorageError> {
            self.inner.health().await
        }
    }

    fn fast_retry() -> RetryConfig {
        RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 1,
            max_delay_ms: 2,
            multiplier: 1.0,
        }
    }

    #[test]
    fn sha256_known_vector() {
        assert_eq!(
            sha256(b"hello world"),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[tokio::test]
    async fn save_get_delete_roundtrip() {
        let storage = Arc::new(MemoryStorage::new());
        let manager = CheckpointManager::new(storage.clone(), fast_retry());
        let saved = manager
            .save(
                "worker-1",
                Bytes::from_static(b"model"),
                HashMap::new(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(saved.status, CheckpointStatus::Completed);
        assert_eq!(
            manager.get_data(&saved.id).await.unwrap().as_ref(),
            b"model"
        );
        assert_eq!(saved.to_proto().worker_id, "worker-1");
        manager.delete(&saved.id).await.unwrap();
        assert!(manager.get(&saved.id).is_none());
        assert!(manager.delete(&saved.id).await.is_err());
    }

    #[tokio::test]
    async fn idempotency_key_returns_the_original_bytes() {
        let storage = Arc::new(MemoryStorage::new());
        let manager = CheckpointManager::new(storage.clone(), fast_retry());
        let first = manager
            .save(
                "worker-1",
                Bytes::from_static(b"one"),
                HashMap::new(),
                Some("same".into()),
            )
            .await
            .unwrap();
        let second = manager
            .save(
                "worker-1",
                Bytes::from_static(b"two"),
                HashMap::new(),
                Some("same".into()),
            )
            .await
            .unwrap();
        assert_eq!(first.id, second.id);
        assert_eq!(manager.get_data(&first.id).await.unwrap().as_ref(), b"one");
        assert_eq!(storage.list("checkpoints/").await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn checksum_mismatch_is_rejected() {
        let storage = Arc::new(MemoryStorage::new());
        let manager = CheckpointManager::new(storage.clone(), fast_retry());
        let saved = manager
            .save(
                "worker-1",
                Bytes::from_static(b"good"),
                HashMap::new(),
                None,
            )
            .await
            .unwrap();
        storage
            .upload(&saved.storage_key, Bytes::from_static(b"tampered"))
            .await
            .unwrap();
        assert!(matches!(
            manager.get_data(&saved.id).await,
            Err(CheckpointError::InvalidData(_))
        ));
    }

    #[tokio::test]
    async fn retries_then_succeeds() {
        let storage = Arc::new(FlakyStorage {
            inner: MemoryStorage::new(),
            remaining_failures: AtomicU32::new(2),
        });
        let manager = CheckpointManager::new(storage, fast_retry());
        let saved = manager
            .save("worker-1", Bytes::from_static(b"ok"), HashMap::new(), None)
            .await
            .unwrap();
        assert_eq!(saved.status, CheckpointStatus::Completed);
    }

    #[tokio::test]
    async fn retries_exhausted_mark_failed() {
        let storage = Arc::new(FlakyStorage {
            inner: MemoryStorage::new(),
            remaining_failures: AtomicU32::new(10),
        });
        let manager = CheckpointManager::new(storage, fast_retry());
        let err = manager
            .save("worker-1", Bytes::from_static(b"no"), HashMap::new(), None)
            .await
            .unwrap_err();
        assert!(matches!(err, CheckpointError::UploadFailed(_)));
        let listed = manager.list(Some("worker-1"), Some(CheckpointStatus::Failed));
        assert_eq!(listed.len(), 1);
    }
}
