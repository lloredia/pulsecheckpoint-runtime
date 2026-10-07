//! gRPC handlers for [`crate::pulse::v1::pulse_service_server::PulseService`].

use bytes::Bytes;
use chrono::Utc;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{error, info};

use crate::checkpoint::{CheckpointError, CheckpointManager};
use crate::metrics;
use crate::pulse::v1::pulse_service_server::PulseService;
use crate::pulse::v1::{
    save_checkpoint_stream_request, CheckpointStatus, ComponentHealth, DatasetInfo,
    DeleteCheckpointRequest, DeleteCheckpointResponse, DeregisterWorkerRequest,
    DeregisterWorkerResponse, GetCheckpointRequest, GetCheckpointResponse, HealthCheckRequest,
    HealthCheckResponse, HeartbeatRequest, HeartbeatResponse, ListCheckpointsRequest,
    ListCheckpointsResponse, ListDatasetsRequest, ListDatasetsResponse, ListWorkersRequest,
    ListWorkersResponse, Metadata, RegisterDatasetRequest, RegisterDatasetResponse,
    RegisterWorkerRequest, RegisterWorkerResponse, SaveCheckpointRequest, SaveCheckpointResponse,
    SaveCheckpointStreamHeader, SaveCheckpointStreamRequest, ServiceStatus, Timestamp,
    WorkerStatus,
};
use crate::worker::{validate_worker_id, WorkerError, WorkerRegistry};

/// Dataset names recorded by this process. Not persisted.
pub struct DatasetRegistry {
    datasets: DashMap<String, DatasetInfo>,
}

impl DatasetRegistry {
    pub fn new() -> Self {
        Self {
            datasets: DashMap::new(),
        }
    }

    pub fn register(
        &self,
        id: String,
        path: String,
        metadata: HashMap<String, String>,
    ) -> Result<DatasetInfo, Status> {
        validate_worker_id(&id).map_err(|err| Status::invalid_argument(err.to_string()))?;
        if path.is_empty() {
            return Err(Status::invalid_argument("dataset path cannot be empty"));
        }
        let info = DatasetInfo {
            dataset_id: id.clone(),
            path,
            metadata: Some(Metadata { labels: metadata }),
            registered_at: Some(now_timestamp()),
        };
        self.datasets.insert(id, info.clone());
        metrics::set_datasets(self.datasets.len());
        Ok(info)
    }

    pub fn list(&self) -> Vec<DatasetInfo> {
        let mut datasets: Vec<_> = self
            .datasets
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        datasets.sort_by(|left, right| left.dataset_id.cmp(&right.dataset_id));
        datasets
    }
}

impl Default for DatasetRegistry {
    fn default() -> Self {
        Self::new()
    }
}

pub struct PulseServiceImpl {
    worker_registry: Arc<WorkerRegistry>,
    checkpoint_manager: Arc<CheckpointManager>,
    dataset_registry: Arc<DatasetRegistry>,
    start_time: chrono::DateTime<Utc>,
}

impl PulseServiceImpl {
    pub fn new(
        worker_registry: Arc<WorkerRegistry>,
        checkpoint_manager: Arc<CheckpointManager>,
    ) -> Self {
        Self {
            worker_registry,
            checkpoint_manager,
            dataset_registry: Arc::new(DatasetRegistry::new()),
            start_time: Utc::now(),
        }
    }
}

#[tonic::async_trait]
impl PulseService for PulseServiceImpl {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let timer = metrics::GRPC_REQUEST_DURATION
            .with_label_values(&["register_worker"])
            .start_timer();
        let req = request.into_inner();
        let metadata = labels(req.metadata);
        let result = self.worker_registry.register(req.worker_id, metadata);
        timer.observe_duration();
        match result {
            Ok(worker) => {
                metrics::GRPC_REQUESTS_TOTAL
                    .with_label_values(&["register_worker", "success"])
                    .inc();
                metrics::WORKER_REGISTRATIONS_TOTAL.inc();
                Ok(Response::new(RegisterWorkerResponse {
                    success: true,
                    message: "worker registered".into(),
                    worker: Some(worker.to_proto()),
                }))
            }
            Err(WorkerError::AlreadyExists(id)) => {
                metrics::GRPC_REQUESTS_TOTAL
                    .with_label_values(&["register_worker", "already_exists"])
                    .inc();
                Err(Status::already_exists(format!(
                    "worker {id} already exists"
                )))
            }
            Err(WorkerError::InvalidWorkerId(id)) => {
                metrics::GRPC_REQUESTS_TOTAL
                    .with_label_values(&["register_worker", "invalid"])
                    .inc();
                Err(Status::invalid_argument(format!("invalid worker id {id}")))
            }
            Err(err) => {
                metrics::ERRORS_TOTAL
                    .with_label_values(&["worker_registration"])
                    .inc();
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn deregister_worker(
        &self,
        request: Request<DeregisterWorkerRequest>,
    ) -> Result<Response<DeregisterWorkerResponse>, Status> {
        let req = request.into_inner();
        match self.worker_registry.deregister(&req.worker_id) {
            Ok(()) => {
                metrics::GRPC_REQUESTS_TOTAL
                    .with_label_values(&["deregister_worker", "success"])
                    .inc();
                Ok(Response::new(DeregisterWorkerResponse {
                    success: true,
                    message: "worker deregistered".into(),
                }))
            }
            Err(WorkerError::NotFound(id)) => {
                Err(Status::not_found(format!("worker {id} not found")))
            }
            Err(err) => Err(Status::internal(err.to_string())),
        }
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let status = optional_worker_status(req.status)?;
        match self.worker_registry.heartbeat(&req.worker_id, status) {
            Ok(()) => {
                metrics::WORKER_HEARTBEATS_TOTAL.inc();
                metrics::GRPC_REQUESTS_TOTAL
                    .with_label_values(&["heartbeat", "success"])
                    .inc();
                Ok(Response::new(HeartbeatResponse {
                    success: true,
                    server_time: Some(now_timestamp()),
                }))
            }
            Err(WorkerError::NotFound(id)) => {
                Err(Status::not_found(format!("worker {id} not found")))
            }
            Err(err) => Err(Status::internal(err.to_string())),
        }
    }

    async fn list_workers(
        &self,
        request: Request<ListWorkersRequest>,
    ) -> Result<Response<ListWorkersResponse>, Status> {
        let req = request.into_inner();
        let status_filter = optional_worker_status(req.status_filter)?;
        let workers = self.worker_registry.list(status_filter);
        let (page, next, total) = paginate(workers, req.page_size, &req.page_token)?;
        Ok(Response::new(ListWorkersResponse {
            workers: page.into_iter().map(|worker| worker.to_proto()).collect(),
            next_page_token: next,
            total_count: total,
        }))
    }

    async fn register_dataset(
        &self,
        request: Request<RegisterDatasetRequest>,
    ) -> Result<Response<RegisterDatasetResponse>, Status> {
        let req = request.into_inner();
        let dataset =
            self.dataset_registry
                .register(req.dataset_id, req.path, labels(req.metadata))?;
        info!(dataset_id = %dataset.dataset_id, "dataset registered");
        Ok(Response::new(RegisterDatasetResponse {
            success: true,
            message: "dataset registered".into(),
            dataset: Some(dataset),
        }))
    }

    async fn list_datasets(
        &self,
        request: Request<ListDatasetsRequest>,
    ) -> Result<Response<ListDatasetsResponse>, Status> {
        let req = request.into_inner();
        let datasets = self.dataset_registry.list();
        let (page, next, total) = paginate(datasets, req.page_size, &req.page_token)?;
        Ok(Response::new(ListDatasetsResponse {
            datasets: page,
            next_page_token: next,
            total_count: total,
        }))
    }

    async fn save_checkpoint(
        &self,
        request: Request<SaveCheckpointRequest>,
    ) -> Result<Response<SaveCheckpointResponse>, Status> {
        let timer = metrics::GRPC_REQUEST_DURATION
            .with_label_values(&["save_checkpoint"])
            .start_timer();
        let req = request.into_inner();
        let result = self
            .store_checkpoint(
                req.worker_id,
                Bytes::from(req.data),
                labels(req.metadata),
                req.idempotency_key,
            )
            .await;
        timer.observe_duration();
        result
    }

    async fn save_checkpoint_stream(
        &self,
        request: Request<tonic::Streaming<SaveCheckpointStreamRequest>>,
    ) -> Result<Response<SaveCheckpointResponse>, Status> {
        use tokio_stream::StreamExt;

        let mut stream = request.into_inner();
        let mut header: Option<SaveCheckpointStreamHeader> = None;
        let mut data = Vec::new();
        while let Some(chunk) = stream.next().await {
            match chunk?.request {
                Some(save_checkpoint_stream_request::Request::Header(value)) => {
                    if header.is_some() {
                        return Err(Status::invalid_argument("duplicate stream header"));
                    }
                    header = Some(value);
                }
                Some(save_checkpoint_stream_request::Request::Chunk(chunk)) => {
                    data.extend_from_slice(&chunk);
                }
                None => {}
            }
        }
        let header = header.ok_or_else(|| Status::invalid_argument("missing stream header"))?;
        if header.total_size > 0 {
            let actual = i64::try_from(data.len()).unwrap_or(i64::MAX);
            if header.total_size != actual {
                return Err(Status::invalid_argument(format!(
                    "stream size {} does not match declared {}",
                    actual, header.total_size
                )));
            }
        }
        self.store_checkpoint(
            header.worker_id,
            Bytes::from(data),
            labels(header.metadata),
            header.idempotency_key,
        )
        .await
    }

    async fn get_checkpoint(
        &self,
        request: Request<GetCheckpointRequest>,
    ) -> Result<Response<GetCheckpointResponse>, Status> {
        let req = request.into_inner();
        if req.checkpoint_id.is_empty() {
            return Err(Status::invalid_argument("checkpoint id cannot be empty"));
        }
        let checkpoint = self
            .checkpoint_manager
            .get(&req.checkpoint_id)
            .ok_or_else(|| {
                Status::not_found(format!("checkpoint {} not found", req.checkpoint_id))
            })?;
        let data = if req.include_data {
            self.checkpoint_manager
                .get_data(&req.checkpoint_id)
                .await
                .map_err(checkpoint_status)?
                .to_vec()
        } else {
            Vec::new()
        };
        Ok(Response::new(GetCheckpointResponse {
            checkpoint: Some(checkpoint.to_proto()),
            data,
        }))
    }

    async fn list_checkpoints(
        &self,
        request: Request<ListCheckpointsRequest>,
    ) -> Result<Response<ListCheckpointsResponse>, Status> {
        let req = request.into_inner();
        let worker_id = if req.worker_id.is_empty() {
            None
        } else {
            Some(req.worker_id)
        };
        let status_filter = optional_checkpoint_status(req.status_filter)?;
        let checkpoints = self
            .checkpoint_manager
            .list(worker_id.as_deref(), status_filter);
        let (page, next, total) = paginate(checkpoints, req.page_size, &req.page_token)?;
        Ok(Response::new(ListCheckpointsResponse {
            checkpoints: page.into_iter().map(|item| item.to_proto()).collect(),
            next_page_token: next,
            total_count: total,
        }))
    }

    async fn delete_checkpoint(
        &self,
        request: Request<DeleteCheckpointRequest>,
    ) -> Result<Response<DeleteCheckpointResponse>, Status> {
        let req = request.into_inner();
        match self.checkpoint_manager.delete(&req.checkpoint_id).await {
            Ok(()) => Ok(Response::new(DeleteCheckpointResponse {
                success: true,
                message: "checkpoint deleted".into(),
            })),
            Err(err) => Err(checkpoint_status(err)),
        }
    }

    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let now = Utc::now();
        let uptime = now - self.start_time;
        let (storage_status, storage_message) = match self.checkpoint_manager.check_storage().await
        {
            Ok(()) => (ServiceStatus::Healthy, "storage reachable".to_string()),
            Err(err) => (ServiceStatus::Unhealthy, err.to_string()),
        };
        let mut components = HashMap::new();
        components.insert(
            "storage".to_string(),
            ComponentHealth {
                status: storage_status.into(),
                message: storage_message,
                last_check: Some(now_timestamp()),
            },
        );
        components.insert(
            "worker_registry".to_string(),
            ComponentHealth {
                status: ServiceStatus::Healthy.into(),
                message: format!(
                    "{} workers registered in memory",
                    self.worker_registry.total_count()
                ),
                last_check: Some(now_timestamp()),
            },
        );
        let overall = if storage_status == ServiceStatus::Healthy {
            ServiceStatus::Healthy
        } else {
            ServiceStatus::Degraded
        };
        Ok(Response::new(HealthCheckResponse {
            status: overall.into(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime: Some(Timestamp {
                seconds: uptime.num_seconds(),
                nanos: 0,
            }),
            components,
        }))
    }
}

impl PulseServiceImpl {
    async fn store_checkpoint(
        &self,
        worker_id: String,
        data: Bytes,
        metadata: HashMap<String, String>,
        idempotency_key: String,
    ) -> Result<Response<SaveCheckpointResponse>, Status> {
        if !self.worker_registry.exists(&worker_id) {
            metrics::GRPC_REQUESTS_TOTAL
                .with_label_values(&["save_checkpoint", "failed_precondition"])
                .inc();
            return Err(Status::failed_precondition(format!(
                "worker {worker_id} is not registered"
            )));
        }
        if data.is_empty() {
            return Err(Status::invalid_argument("checkpoint data cannot be empty"));
        }
        let key = if idempotency_key.is_empty() {
            None
        } else {
            Some(idempotency_key)
        };
        match self
            .checkpoint_manager
            .save(&worker_id, data, metadata, key)
            .await
        {
            Ok(checkpoint) => {
                metrics::GRPC_REQUESTS_TOTAL
                    .with_label_values(&["save_checkpoint", "success"])
                    .inc();
                Ok(Response::new(SaveCheckpointResponse {
                    success: true,
                    message: "checkpoint saved".into(),
                    checkpoint: Some(checkpoint.to_proto()),
                }))
            }
            Err(err) => {
                metrics::GRPC_REQUESTS_TOTAL
                    .with_label_values(&["save_checkpoint", "error"])
                    .inc();
                metrics::ERRORS_TOTAL
                    .with_label_values(&["checkpoint_save"])
                    .inc();
                error!(error = %err, "failed to save checkpoint");
                Err(checkpoint_status(err))
            }
        }
    }
}

fn labels(metadata: Option<Metadata>) -> HashMap<String, String> {
    metadata.map(|value| value.labels).unwrap_or_default()
}

fn now_timestamp() -> Timestamp {
    let now = Utc::now();
    Timestamp {
        seconds: now.timestamp(),
        nanos: i32::try_from(now.timestamp_subsec_nanos()).unwrap_or(0),
    }
}

fn optional_worker_status(code: i32) -> Result<Option<WorkerStatus>, Status> {
    if code == i32::from(WorkerStatus::Unspecified) {
        return Ok(None);
    }
    WorkerStatus::try_from(code)
        .map(Some)
        .map_err(|_| Status::invalid_argument(format!("unknown worker status {code}")))
}

fn optional_checkpoint_status(code: i32) -> Result<Option<CheckpointStatus>, Status> {
    if code == i32::from(CheckpointStatus::Unspecified) {
        return Ok(None);
    }
    CheckpointStatus::try_from(code)
        .map(Some)
        .map_err(|_| Status::invalid_argument(format!("unknown checkpoint status {code}")))
}

fn checkpoint_status(err: CheckpointError) -> Status {
    match err {
        CheckpointError::NotFound(id) => Status::not_found(format!("checkpoint {id} not found")),
        CheckpointError::InvalidData(message) => Status::failed_precondition(message),
        other => Status::internal(other.to_string()),
    }
}

fn paginate<T>(
    items: Vec<T>,
    page_size: i32,
    page_token: &str,
) -> Result<(Vec<T>, String, i32), Status> {
    if page_size < 0 {
        return Err(Status::invalid_argument("page_size must be >= 0"));
    }
    let offset = if page_token.is_empty() {
        0
    } else {
        page_token
            .parse::<usize>()
            .map_err(|_| Status::invalid_argument("invalid page_token"))?
    };
    let limit = if page_size == 0 {
        100
    } else {
        (page_size as usize).min(1000)
    };
    let total = i32::try_from(items.len()).unwrap_or(i32::MAX);
    let page: Vec<_> = items.into_iter().skip(offset).take(limit).collect();
    let next = offset.saturating_add(page.len());
    let next_token = if next < total as usize {
        next.to_string()
    } else {
        String::new()
    };
    Ok((page, next_token, total))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::CheckpointManager;
    use crate::config::RetryConfig;
    use crate::storage::MemoryStorage;

    fn service() -> PulseServiceImpl {
        let storage = Arc::new(MemoryStorage::new());
        let manager = Arc::new(CheckpointManager::new(
            storage,
            RetryConfig {
                max_attempts: 1,
                initial_delay_ms: 1,
                max_delay_ms: 1,
                multiplier: 1.0,
            },
        ));
        PulseServiceImpl::new(Arc::new(WorkerRegistry::new()), manager)
    }

    #[tokio::test]
    async fn worker_checkpoint_and_dataset_flow() {
        let svc = service();
        let registered = svc
            .register_worker(Request::new(RegisterWorkerRequest {
                worker_id: "worker-1".into(),
                metadata: Some(Metadata {
                    labels: HashMap::from([("gpu".into(), "none".into())]),
                }),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(registered.success);

        let duplicate = svc
            .register_worker(Request::new(RegisterWorkerRequest {
                worker_id: "worker-1".into(),
                metadata: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(duplicate.code(), tonic::Code::AlreadyExists);

        let saved = svc
            .save_checkpoint(Request::new(SaveCheckpointRequest {
                worker_id: "worker-1".into(),
                data: b"state".to_vec(),
                metadata: None,
                idempotency_key: "epoch-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        let checkpoint_id = saved.checkpoint.unwrap().checkpoint_id;

        let again = svc
            .save_checkpoint(Request::new(SaveCheckpointRequest {
                worker_id: "worker-1".into(),
                data: b"other".to_vec(),
                metadata: None,
                idempotency_key: "epoch-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(again.checkpoint.unwrap().checkpoint_id, checkpoint_id);

        let loaded = svc
            .get_checkpoint(Request::new(GetCheckpointRequest {
                checkpoint_id: checkpoint_id.clone(),
                include_data: true,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(loaded.data, b"state");

        let dataset = svc
            .register_dataset(Request::new(RegisterDatasetRequest {
                dataset_id: "train".into(),
                path: "s3://bucket/train".into(),
                metadata: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(dataset.dataset.unwrap().path, "s3://bucket/train");

        let health = svc
            .health_check(Request::new(HealthCheckRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(health.status, i32::from(ServiceStatus::Healthy));

        svc.delete_checkpoint(Request::new(DeleteCheckpointRequest { checkpoint_id }))
            .await
            .unwrap();
        svc.deregister_worker(Request::new(DeregisterWorkerRequest {
            worker_id: "worker-1".into(),
        }))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn save_requires_registered_worker() {
        let svc = service();
        let err = svc
            .save_checkpoint(Request::new(SaveCheckpointRequest {
                worker_id: "missing".into(),
                data: b"x".to_vec(),
                metadata: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn pagination_uses_offset_tokens() {
        let (page, next, total) = paginate(vec!["a", "b", "c"], 2, "").unwrap();
        assert_eq!(page, vec!["a", "b"]);
        assert_eq!(next, "2");
        assert_eq!(total, 3);
        let (page, next, _) = paginate(vec!["a", "b", "c"], 2, &next).unwrap();
        assert_eq!(page, vec!["c"]);
        assert!(next.is_empty());
    }
}
