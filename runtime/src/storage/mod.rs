//! Checkpoint blob storage.
//!
//! [`MemoryStorage`] and [`FsStorage`] back unit tests. [`S3Storage`] talks to
//! AWS S3, MinIO, or LocalStack through the AWS SDK default credential chain.

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::create_bucket::CreateBucketError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_bucket::HeadBucketError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::error::Error as StdError;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::config::StorageConfig;
use crate::metrics;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("storage error: {0}")]
    Backend(String),

    #[error("object not found: {0}")]
    NotFound(String),

    #[error("invalid storage key: {0}")]
    InvalidKey(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: i64,
    pub etag: Option<String>,
    pub last_modified: Option<DateTime<Utc>>,
}

/// Persistence backend for checkpoint bytes.
#[async_trait]
pub trait Storage: Send + Sync {
    async fn upload(&self, key: &str, data: Bytes) -> Result<String, StorageError>;
    async fn download(&self, key: &str) -> Result<Bytes, StorageError>;
    async fn delete(&self, key: &str) -> Result<(), StorageError>;
    async fn exists(&self, key: &str) -> Result<bool, StorageError>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError>;
    async fn head(&self, key: &str) -> Result<ObjectMetadata, StorageError>;
    async fn health(&self) -> Result<(), StorageError>;
}

/// Join an optional key prefix with an object key.
pub fn prefixed_key(prefix: Option<&str>, key: &str) -> String {
    match prefix.map(str::trim).map(|value| value.trim_matches('/')) {
        Some(prefix) if !prefix.is_empty() => format!("{prefix}/{}", key.trim_start_matches('/')),
        _ => key.to_string(),
    }
}

fn validate_key(key: &str) -> Result<(), StorageError> {
    if key.is_empty() || key.contains('\0') {
        return Err(StorageError::InvalidKey(key.to_string()));
    }
    if key
        .split('/')
        .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(StorageError::InvalidKey(key.to_string()));
    }
    Ok(())
}

#[derive(Debug)]
struct MemoryObject {
    data: Bytes,
    modified: DateTime<Utc>,
}

/// In-memory backend. Objects disappear when the process exits.
#[derive(Debug, Default)]
pub struct MemoryStorage {
    objects: DashMap<String, MemoryObject>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn upload(&self, key: &str, data: Bytes) -> Result<String, StorageError> {
        validate_key(key)?;
        self.objects.insert(
            key.to_string(),
            MemoryObject {
                data,
                modified: Utc::now(),
            },
        );
        Ok(format!("memory://{key}"))
    }

    async fn download(&self, key: &str) -> Result<Bytes, StorageError> {
        validate_key(key)?;
        self.objects
            .get(key)
            .map(|obj| obj.data.clone())
            .ok_or_else(|| StorageError::NotFound(key.to_string()))
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        validate_key(key)?;
        if self.objects.remove(key).is_none() {
            return Err(StorageError::NotFound(key.to_string()));
        }
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool, StorageError> {
        validate_key(key)?;
        Ok(self.objects.contains_key(key))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let mut keys: Vec<_> = self
            .objects
            .iter()
            .filter(|entry| entry.key().starts_with(prefix))
            .map(|entry| entry.key().clone())
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata, StorageError> {
        validate_key(key)?;
        let obj = self
            .objects
            .get(key)
            .ok_or_else(|| StorageError::NotFound(key.to_string()))?;
        Ok(ObjectMetadata {
            key: key.to_string(),
            size: i64::try_from(obj.data.len()).unwrap_or(i64::MAX),
            etag: None,
            last_modified: Some(obj.modified),
        })
    }

    async fn health(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

/// Directory-backed backend used by unit tests.
#[derive(Debug, Clone)]
pub struct FsStorage {
    root: PathBuf,
}

impl FsStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path_for(&self, key: &str) -> Result<PathBuf, StorageError> {
        validate_key(key)?;
        let mut path = self.root.clone();
        for part in key.split('/') {
            path.push(part);
        }
        Ok(path)
    }
}

#[async_trait]
impl Storage for FsStorage {
    async fn upload(&self, key: &str, data: Bytes) -> Result<String, StorageError> {
        let path = self.path_for(key)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| StorageError::Backend(err.to_string()))?;
        }
        std::fs::write(&path, &data).map_err(|err| StorageError::Backend(err.to_string()))?;
        Ok(format!("file://{key}"))
    }

    async fn download(&self, key: &str) -> Result<Bytes, StorageError> {
        let path = self.path_for(key)?;
        match std::fs::read(&path) {
            Ok(bytes) => Ok(Bytes::from(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Err(StorageError::NotFound(key.to_string()))
            }
            Err(err) => Err(StorageError::Backend(err.to_string())),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let path = self.path_for(key)?;
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Err(StorageError::NotFound(key.to_string()))
            }
            Err(err) => Err(StorageError::Backend(err.to_string())),
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, StorageError> {
        Ok(self.path_for(key)?.is_file())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let mut keys = Vec::new();
        walk_files(&self.root, &self.root, prefix, &mut keys)?;
        keys.sort();
        Ok(keys)
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata, StorageError> {
        let path = self.path_for(key)?;
        let meta = match std::fs::metadata(&path) {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound(key.to_string()));
            }
            Err(err) => return Err(StorageError::Backend(err.to_string())),
        };
        let modified = meta.modified().ok().map(DateTime::<Utc>::from);
        Ok(ObjectMetadata {
            key: key.to_string(),
            size: i64::try_from(meta.len()).unwrap_or(i64::MAX),
            etag: None,
            last_modified: modified,
        })
    }

    async fn health(&self) -> Result<(), StorageError> {
        std::fs::create_dir_all(&self.root)
            .map_err(|err| StorageError::Backend(err.to_string()))?;
        Ok(())
    }
}

fn walk_files(
    dir: &Path,
    root: &Path,
    prefix: &str,
    out: &mut Vec<String>,
) -> Result<(), StorageError> {
    if !dir.exists() {
        return Ok(());
    }
    let entries = std::fs::read_dir(dir).map_err(|err| StorageError::Backend(err.to_string()))?;
    for entry in entries {
        let entry = entry.map_err(|err| StorageError::Backend(err.to_string()))?;
        let path = entry.path();
        if path.is_dir() {
            walk_files(&path, root, prefix, out)?;
            continue;
        }
        let rel = path
            .strip_prefix(root)
            .map_err(|err| StorageError::Backend(err.to_string()))?;
        let key = rel.to_string_lossy().replace('\\', "/");
        if key.starts_with(prefix) {
            out.push(key);
        }
    }
    Ok(())
}

/// S3-compatible client. Credentials come from the AWS SDK default chain.
pub struct S3Storage {
    client: S3Client,
    bucket: String,
    region: String,
    path_prefix: Option<String>,
}

impl S3Storage {
    pub async fn new(config: &StorageConfig) -> Result<Self, StorageError> {
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .load()
            .await;

        let mut builder = aws_sdk_s3::config::Builder::from(&shared);
        if let Some(endpoint) = config.endpoint.as_deref().filter(|value| !value.is_empty()) {
            info!(endpoint, bucket = %config.bucket, "using custom S3 endpoint");
            builder = builder
                .endpoint_url(endpoint)
                .force_path_style(config.force_path_style);
        } else {
            info!(bucket = %config.bucket, region = %config.region, "using AWS S3 endpoint");
        }

        let storage = Self {
            client: S3Client::from_conf(builder.build()),
            bucket: config.bucket.clone(),
            region: config.region.clone(),
            path_prefix: config.path_prefix.clone(),
        };
        storage.ensure_bucket().await?;
        Ok(storage)
    }

    fn full_key(&self, key: &str) -> Result<String, StorageError> {
        validate_key(key)?;
        Ok(prefixed_key(self.path_prefix.as_deref(), key))
    }

    async fn ensure_bucket(&self) -> Result<(), StorageError> {
        match self.client.head_bucket().bucket(&self.bucket).send().await {
            Ok(_) => Ok(()),
            Err(err) if bucket_is_missing(&err) => {
                warn!(bucket = %self.bucket, "bucket missing, creating it");
                self.create_bucket().await
            }
            Err(err) => Err(StorageError::Backend(format!(
                "head bucket {}: {}",
                self.bucket,
                describe_sdk_error(&err)
            ))),
        }
    }

    async fn create_bucket(&self) -> Result<(), StorageError> {
        let mut request = self.client.create_bucket().bucket(&self.bucket);
        if self.region != "us-east-1" {
            let constraint =
                aws_sdk_s3::types::BucketLocationConstraint::from(self.region.as_str());
            let configuration = aws_sdk_s3::types::CreateBucketConfiguration::builder()
                .location_constraint(constraint)
                .build();
            request = request.create_bucket_configuration(configuration);
        }

        match request.send().await {
            Ok(_) => {
                info!(bucket = %self.bucket, "created bucket");
                Ok(())
            }
            Err(err) if bucket_already_exists(&err) => Ok(()),
            Err(err) => Err(StorageError::Backend(format!(
                "create bucket {}: {}",
                self.bucket,
                describe_sdk_error(&err)
            ))),
        }
    }
}

fn describe_sdk_error<E>(err: &SdkError<E>) -> String
where
    E: StdError + ProvideErrorMetadata + 'static,
{
    // Walk the source chain only. SdkError's Debug form includes the signed
    // request, and these strings are returned on the gRPC status.
    let mut message = String::new();
    let mut current: Option<&dyn StdError> = Some(err);
    while let Some(inner) = current {
        if !message.is_empty() {
            message.push_str(": ");
        }
        message.push_str(&inner.to_string());
        current = inner.source();
    }
    if let Some(code) = err.code() {
        message.push_str(" (");
        message.push_str(code);
        message.push(')');
    }
    if let Some(status) = err
        .raw_response()
        .map(|response| response.status().as_u16())
    {
        message.push_str(" http=");
        message.push_str(&status.to_string());
    }
    message
}

fn status_is_not_found<E>(err: &SdkError<E>) -> bool {
    err.raw_response()
        .is_some_and(|response| response.status().as_u16() == 404)
}

fn is_missing_code(code: Option<&str>) -> bool {
    matches!(code, Some("NotFound" | "NoSuchBucket" | "NoSuchKey"))
}

/// MinIO answers HeadBucket for a missing bucket with `NoSuchBucket` (404).
/// AWS answers the same call with `NotFound`. `SdkError`'s Display is only
/// "service error", so the code and status have to be read from the SDK types.
fn bucket_is_missing(err: &SdkError<HeadBucketError>) -> bool {
    err.as_service_error()
        .is_some_and(HeadBucketError::is_not_found)
        || is_missing_code(err.code())
        || status_is_not_found(err)
}

fn object_missing_on_get(err: &SdkError<GetObjectError>) -> bool {
    err.as_service_error()
        .is_some_and(GetObjectError::is_no_such_key)
        || is_missing_code(err.code())
        || status_is_not_found(err)
}

fn object_missing_on_head(err: &SdkError<HeadObjectError>) -> bool {
    err.as_service_error()
        .is_some_and(HeadObjectError::is_not_found)
        || is_missing_code(err.code())
        || status_is_not_found(err)
}

fn bucket_already_exists(err: &SdkError<CreateBucketError>) -> bool {
    err.as_service_error().is_some_and(|service| {
        service.is_bucket_already_exists() || service.is_bucket_already_owned_by_you()
    }) || matches!(
        err.code(),
        Some("BucketAlreadyExists" | "BucketAlreadyOwnedByYou")
    )
}

#[async_trait]
impl Storage for S3Storage {
    async fn upload(&self, key: &str, data: Bytes) -> Result<String, StorageError> {
        let full_key = self.full_key(key)?;
        let timer = metrics::S3_REQUEST_DURATION
            .with_label_values(&["upload"])
            .start_timer();
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .body(ByteStream::from(data))
            .send()
            .await;
        timer.observe_duration();

        match result {
            Ok(_) => {
                metrics::S3_REQUESTS_TOTAL
                    .with_label_values(&["upload", "success"])
                    .inc();
                debug!(key = %full_key, "uploaded object");
                Ok(format!("s3://{}/{full_key}", self.bucket))
            }
            Err(err) => {
                metrics::S3_REQUESTS_TOTAL
                    .with_label_values(&["upload", "error"])
                    .inc();
                Err(StorageError::Backend(describe_sdk_error(&err)))
            }
        }
    }

    async fn download(&self, key: &str) -> Result<Bytes, StorageError> {
        let full_key = self.full_key(key)?;
        let timer = metrics::S3_REQUEST_DURATION
            .with_label_values(&["download"])
            .start_timer();
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await;
        timer.observe_duration();

        match result {
            Ok(output) => {
                let data = output
                    .body
                    .collect()
                    .await
                    .map_err(|err| StorageError::Backend(err.to_string()))?
                    .into_bytes();
                metrics::S3_REQUESTS_TOTAL
                    .with_label_values(&["download", "success"])
                    .inc();
                Ok(data)
            }
            Err(err) if object_missing_on_get(&err) => {
                metrics::S3_REQUESTS_TOTAL
                    .with_label_values(&["download", "error"])
                    .inc();
                Err(StorageError::NotFound(full_key))
            }
            Err(err) => {
                metrics::S3_REQUESTS_TOTAL
                    .with_label_values(&["download", "error"])
                    .inc();
                Err(StorageError::Backend(describe_sdk_error(&err)))
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let full_key = self.full_key(key)?;
        let timer = metrics::S3_REQUEST_DURATION
            .with_label_values(&["delete"])
            .start_timer();
        let result = self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await;
        timer.observe_duration();
        match result {
            Ok(_) => {
                metrics::S3_REQUESTS_TOTAL
                    .with_label_values(&["delete", "success"])
                    .inc();
                Ok(())
            }
            Err(err) => {
                metrics::S3_REQUESTS_TOTAL
                    .with_label_values(&["delete", "error"])
                    .inc();
                Err(StorageError::Backend(describe_sdk_error(&err)))
            }
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, StorageError> {
        match self.head(key).await {
            Ok(_) => Ok(true),
            Err(StorageError::NotFound(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let full_prefix = prefixed_key(self.path_prefix.as_deref(), prefix);
        let mut keys = Vec::new();
        let mut token = None;
        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full_prefix);
            if let Some(value) = &token {
                request = request.continuation_token(value);
            }
            let response = request
                .send()
                .await
                .map_err(|err| StorageError::Backend(describe_sdk_error(&err)))?;
            if let Some(contents) = response.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        keys.push(key);
                    }
                }
            }
            if response.is_truncated == Some(true) {
                token = response.next_continuation_token;
            } else {
                break;
            }
        }
        Ok(keys)
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata, StorageError> {
        let full_key = self.full_key(key)?;
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
        {
            Ok(response) => Ok(ObjectMetadata {
                key: full_key,
                size: response.content_length().unwrap_or(0),
                etag: response.e_tag().map(str::to_string),
                last_modified: response
                    .last_modified()
                    .and_then(|dt| DateTime::<Utc>::from_timestamp(dt.secs(), dt.subsec_nanos())),
            }),
            Err(err) if object_missing_on_head(&err) => Err(StorageError::NotFound(full_key)),
            Err(err) => Err(StorageError::Backend(describe_sdk_error(&err))),
        }
    }

    async fn health(&self) -> Result<(), StorageError> {
        self.client
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .map(|_| ())
            .map_err(|err| StorageError::Backend(describe_sdk_error(&err)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixed_key_trims_slashes() {
        assert_eq!(prefixed_key(Some("/lab/"), "a/b"), "lab/a/b");
        assert_eq!(prefixed_key(None, "a/b"), "a/b");
        assert_eq!(prefixed_key(Some(""), "a/b"), "a/b");
    }

    #[tokio::test]
    async fn memory_roundtrip() {
        let storage = MemoryStorage::new();
        let uri = storage
            .upload("a/b.bin", Bytes::from_static(b"hi"))
            .await
            .unwrap();
        assert_eq!(uri, "memory://a/b.bin");
        assert_eq!(storage.download("a/b.bin").await.unwrap().as_ref(), b"hi");
        assert!(storage.exists("a/b.bin").await.unwrap());
        assert_eq!(storage.head("a/b.bin").await.unwrap().size, 2);
        assert_eq!(
            storage.list("a/").await.unwrap(),
            vec!["a/b.bin".to_string()]
        );
        storage.delete("a/b.bin").await.unwrap();
        assert!(!storage.exists("a/b.bin").await.unwrap());
        assert!(storage.download("a/b.bin").await.is_err());
    }

    #[tokio::test]
    async fn memory_rejects_parent_segments() {
        let storage = MemoryStorage::new();
        assert!(storage
            .upload("../secret", Bytes::from_static(b"no"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn filesystem_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path());
        storage
            .upload("ckpt/w/obj.bin", Bytes::from_static(b"bytes"))
            .await
            .unwrap();
        let listed = storage.list("ckpt/").await.unwrap();
        assert_eq!(listed, vec!["ckpt/w/obj.bin".to_string()]);
        assert_eq!(
            storage.download("ckpt/w/obj.bin").await.unwrap().as_ref(),
            b"bytes"
        );
        storage.delete("ckpt/w/obj.bin").await.unwrap();
        assert!(matches!(
            storage.download("ckpt/w/obj.bin").await,
            Err(StorageError::NotFound(_))
        ));
        assert!(storage.health().await.is_ok());
    }
}
