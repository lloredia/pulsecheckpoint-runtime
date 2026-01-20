//! S3-compatible storage abstraction for checkpoint persistence

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use thiserror::Error;
use tracing::{debug, error, info, instrument, warn};

use crate::config::StorageConfig;
use crate::metrics;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("S3 client error: {0}")]
    S3Error(String),
    
    #[error("Object not found: {0}")]
    NotFound(String),
    
    #[error("Upload failed after retries: {0}")]
    UploadFailed(String),
    
    #[error("Download failed: {0}")]
    DownloadFailed(String),
    
    #[error("Delete failed: {0}")]
    DeleteFailed(String),
    
    #[error("Invalid configuration: {0}")]
    ConfigError(String),
}

/// Storage trait for checkpoint persistence
#[async_trait]
pub trait Storage: Send + Sync {
    /// Upload data to storage
    async fn upload(&self, key: &str, data: Bytes) -> Result<String, StorageError>;
    
    /// Download data from storage
    async fn download(&self, key: &str) -> Result<Bytes, StorageError>;
    
    /// Delete object from storage
    async fn delete(&self, key: &str) -> Result<(), StorageError>;
    
    /// Check if object exists
    async fn exists(&self, key: &str) -> Result<bool, StorageError>;
    
    /// List objects with prefix
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError>;
    
    /// Get object metadata
    async fn head(&self, key: &str) -> Result<ObjectMetadata, StorageError>;
}

#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: i64,
    pub etag: Option<String>,
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
}

/// S3-compatible storage implementation
pub struct S3Storage {
    client: S3Client,
    bucket: String,
    path_prefix: Option<String>,
}

impl S3Storage {
    /// Create a new S3 storage client
    pub async fn new(config: &StorageConfig) -> Result<Self, StorageError> {
        info!(
            endpoint = %config.endpoint,
            bucket = %config.bucket,
            region = %config.region,
            "Initializing S3 storage client"
        );

        // Build credentials if provided
        let credentials = match (&config.access_key_id, &config.secret_access_key) {
            (Some(access_key), Some(secret_key)) => {
                Some(Credentials::new(
                    access_key,
                    secret_key,
                    None,
                    None,
                    "pulse-runtime",
                ))
            }
            _ => None,
        };

        // Build S3 config
        let mut s3_config_builder = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .endpoint_url(&config.endpoint)
            .force_path_style(true); // Required for MinIO

        if let Some(creds) = credentials {
            s3_config_builder = s3_config_builder.credentials_provider(creds);
        }

        let s3_config = s3_config_builder.build();
        let client = S3Client::from_conf(s3_config);

        // Verify bucket exists or create it
        let storage = Self {
            client,
            bucket: config.bucket.clone(),
            path_prefix: config.path_prefix.clone(),
        };

        storage.ensure_bucket_exists().await?;

        info!(bucket = %config.bucket, "S3 storage client initialized");
        Ok(storage)
    }

    /// Ensure the bucket exists, creating it if necessary
    async fn ensure_bucket_exists(&self) -> Result<(), StorageError> {
        match self.client.head_bucket().bucket(&self.bucket).send().await {
            Ok(_) => {
                debug!(bucket = %self.bucket, "Bucket exists");
                Ok(())
            }
            Err(e) => {
                warn!(bucket = %self.bucket, error = %e, "Bucket not found, attempting to create");
                
                self.client
                    .create_bucket()
                    .bucket(&self.bucket)
                    .send()
                    .await
                    .map_err(|e| StorageError::S3Error(format!("Failed to create bucket: {}", e)))?;
                
                info!(bucket = %self.bucket, "Bucket created successfully");
                Ok(())
            }
        }
    }

    /// Get full key with optional prefix
    fn full_key(&self, key: &str) -> String {
        match &self.path_prefix {
            Some(prefix) => format!("{}/{}", prefix.trim_end_matches('/'), key),
            None => key.to_string(),
        }
    }
}

#[async_trait]
impl Storage for S3Storage {
    #[instrument(skip(self, data), fields(bucket = %self.bucket, size = data.len()))]
    async fn upload(&self, key: &str, data: Bytes) -> Result<String, StorageError> {
        let full_key = self.full_key(key);
        let size = data.len();
        let timer = metrics::S3_REQUEST_DURATION.with_label_values(&["upload"]).start_timer();
        
        debug!(key = %full_key, size = size, "Uploading object to S3");

        let result = self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .body(ByteStream::from(data))
            .send()
            .await;

        timer.observe_duration();

        match result {
            Ok(output) => {
                let etag = output.e_tag().unwrap_or("").to_string();
                metrics::S3_REQUESTS_TOTAL.with_label_values(&["upload", "success"]).inc();
                metrics::CHECKPOINT_BYTES_TOTAL.inc_by(size as u64);
                
                info!(
                    key = %full_key,
                    size = size,
                    etag = %etag,
                    "Object uploaded successfully"
                );
                
                Ok(format!("s3://{}/{}", self.bucket, full_key))
            }
            Err(e) => {
                metrics::S3_REQUESTS_TOTAL.with_label_values(&["upload", "error"]).inc();
                error!(key = %full_key, error = %e, "Failed to upload object");
                Err(StorageError::UploadFailed(e.to_string()))
            }
        }
    }

    #[instrument(skip(self), fields(bucket = %self.bucket))]
    async fn download(&self, key: &str) -> Result<Bytes, StorageError> {
        let full_key = self.full_key(key);
        let timer = metrics::S3_REQUEST_DURATION.with_label_values(&["download"]).start_timer();
        
        debug!(key = %full_key, "Downloading object from S3");

        let result = self.client
            .get_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await;

        timer.observe_duration();

        match result {
            Ok(output) => {
                let data = output.body
                    .collect()
                    .await
                    .map_err(|e| StorageError::DownloadFailed(e.to_string()))?
                    .into_bytes();
                
                metrics::S3_REQUESTS_TOTAL.with_label_values(&["download", "success"]).inc();
                
                info!(key = %full_key, size = data.len(), "Object downloaded successfully");
                Ok(data)
            }
            Err(e) => {
                metrics::S3_REQUESTS_TOTAL.with_label_values(&["download", "error"]).inc();
                
                if e.to_string().contains("NoSuchKey") || e.to_string().contains("404") {
                    Err(StorageError::NotFound(full_key))
                } else {
                    error!(key = %full_key, error = %e, "Failed to download object");
                    Err(StorageError::DownloadFailed(e.to_string()))
                }
            }
        }
    }

    #[instrument(skip(self), fields(bucket = %self.bucket))]
    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let full_key = self.full_key(key);
        let timer = metrics::S3_REQUEST_DURATION.with_label_values(&["delete"]).start_timer();
        
        debug!(key = %full_key, "Deleting object from S3");

        let result = self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await;

        timer.observe_duration();

        match result {
            Ok(_) => {
                metrics::S3_REQUESTS_TOTAL.with_label_values(&["delete", "success"]).inc();
                info!(key = %full_key, "Object deleted successfully");
                Ok(())
            }
            Err(e) => {
                metrics::S3_REQUESTS_TOTAL.with_label_values(&["delete", "error"]).inc();
                error!(key = %full_key, error = %e, "Failed to delete object");
                Err(StorageError::DeleteFailed(e.to_string()))
            }
        }
    }

    #[instrument(skip(self), fields(bucket = %self.bucket))]
    async fn exists(&self, key: &str) -> Result<bool, StorageError> {
        let full_key = self.full_key(key);
        
        match self.client
            .head_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.to_string().contains("404") || e.to_string().contains("NoSuchKey") {
                    Ok(false)
                } else {
                    Err(StorageError::S3Error(e.to_string()))
                }
            }
        }
    }

    #[instrument(skip(self), fields(bucket = %self.bucket))]
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let full_prefix = self.full_key(prefix);
        
        debug!(prefix = %full_prefix, "Listing objects with prefix");

        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self.client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full_prefix);

            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let response = request
                .send()
                .await
                .map_err(|e| StorageError::S3Error(e.to_string()))?;

            if let Some(contents) = response.contents {
                for obj in contents {
                    if let Some(key) = obj.key {
                        keys.push(key);
                    }
                }
            }

            if response.is_truncated.unwrap_or(false) {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }

        debug!(prefix = %full_prefix, count = keys.len(), "Listed objects");
        Ok(keys)
    }

    #[instrument(skip(self), fields(bucket = %self.bucket))]
    async fn head(&self, key: &str) -> Result<ObjectMetadata, StorageError> {
        let full_key = self.full_key(key);
        
        let response = self.client
            .head_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|e| {
                if e.to_string().contains("404") || e.to_string().contains("NoSuchKey") {
                    StorageError::NotFound(full_key.clone())
                } else {
                    StorageError::S3Error(e.to_string())
                }
            })?;

        Ok(ObjectMetadata {
            key: full_key,
            size: response.content_length.unwrap_or(0),
            etag: response.e_tag.map(|s| s.to_string()),
            last_modified: response.last_modified.map(|dt| {
                chrono::DateTime::from_timestamp(dt.secs(), dt.subsec_nanos())
                    .unwrap_or_default()
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_key_with_prefix() {
        let storage = S3Storage {
            client: todo!(), // Would need mock
            bucket: "test".to_string(),
            path_prefix: Some("prefix".to_string()),
        };
        assert_eq!(storage.full_key("key"), "prefix/key");
    }

    #[test]
    fn test_full_key_without_prefix() {
        let storage = S3Storage {
            client: todo!(), // Would need mock
            bucket: "test".to_string(),
            path_prefix: None,
        };
        assert_eq!(storage.full_key("key"), "key");
    }
}
