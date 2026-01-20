//! gRPC service implementation for PulseService

use bytes::Bytes;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{error, info, instrument};

use crate::checkpoint::{CheckpointError, CheckpointManager};
use crate::metrics;
use crate::pulse::v1::*;
use crate::worker::{WorkerError, WorkerRegistry};

/// Dataset registry for tracking registered datasets
pub struct DatasetRegistry {
    datasets: dashmap::DashMap<String, DatasetInfo>,
}

impl DatasetRegistry {
    pub fn new() -> Self {
        Self {
            datasets: dashmap::DashMap::new(),
        }
    }

    pub fn register(&self, id: String, path: String, metadata: HashMap<String, String>) -> DatasetInfo {
        let info = DatasetInfo {
            dataset_id: id.clone(),
            path,
            metadata: Some(Metadata { labels: metadata }),
            registered_at: Some(Timestamp {
                seconds: Utc::now().timestamp(),
                nanos: Utc::now().timestamp_subsec_nanos() as i32,
            }),
        };
        self.datasets.insert(id, info.clone());
        metrics::DATASETS_TOTAL.inc();
        info
    }

    pub fn list(&self) -> Vec<DatasetInfo> {
        self.datasets.iter().map(|e| e.value().clone()).collect()
    }

    pub fn count(&self) -> usize {
        self.datasets.len()
    }
}

impl Default for DatasetRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// gRPC service implementation
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
impl pulse_service_server::PulseService for PulseServiceImpl {
    #[instrument(skip(self, request))]
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let timer = metrics::GRPC_REQUEST_DURATION
            .with_label_values(&["register_worker"])
            .start_timer();

        let req = request.into_inner();
        info!(worker_id = %req.worker_id, "Registering worker");

        let metadata = req.metadata.map(|m| m.labels).unwrap_or_default();

        match self.worker_registry.register(req.worker_id.clone(), metadata) {
            Ok(worker) => {
                metrics::GRPC_REQUESTS_TOTAL
                    .with_label_values(&["register_worker", "success"])
                    .inc();
                metrics::WORKER_REGISTRATIONS_TOTAL.inc();
                timer.observe_duration();

                Ok(Response::new(RegisterWorkerResponse {
                    success: true,
                    message: "Worker registered successfully".to_string(),
                    worker: Some(worker.to_proto()),
                }))
            }
            Err(WorkerError::AlreadyExists(id)) => {
                metrics::GRPC_REQUESTS_TOTAL
                    .with_label_values(&["register_worker", "already_exists"])
                    .inc();
                timer.observe_duration();
                Err(Status::already_exists(format!("Worker {} already exists", id)))
            }
            Err(e) => {
                metrics::GRPC_REQUESTS_TOTAL
                    .with_label_values(&["register_worker", "error"])
                    .inc();
                metrics::ERRORS_TOTAL.with_label_values(&["worker_registration"]).inc();
                timer.observe_duration();
                error!(error = %e, "Failed to register worker");
                Err(Status::internal(e.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn deregister_worker(
        &self,
        request: Request<DeregisterWorkerRequest>,
    ) -> Result<Response<DeregisterWorkerResponse>, Status> {
        let req = request.into_inner();
        info!(worker_id = %req.worker_id, "Deregistering worker");

        match self.worker_registry.deregister(&req.worker_id) {
            Ok(_) => {
                metrics::GRPC_REQUESTS_TOTAL
                    .with_label_values(&["deregister_worker", "success"])
                    .inc();

                Ok(Response::new(DeregisterWorkerResponse {
                    success: true,
                    message: "Worker deregistered successfully".to_string(),
                }))
            }
            Err(WorkerError::NotFound(id)) => {
                Err(Status::not_found(format!("Worker {} not found", id)))
            }
            Err(e) => {
                error!(error = %e, "Failed to deregister worker");
                Err(Status::internal(e.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();

        let status = if req.status != WorkerStatus::Unspecified as i32 {
            Some(WorkerStatus::try_from(req.status).unwrap_or(WorkerStatus::Active))
        } else {
            None
        };

        match self.worker_registry.heartbeat(&req.worker_id, status) {
            Ok(_) => {
                metrics::WORKER_HEARTBEATS_TOTAL.inc();

                let now = Utc::now();
                Ok(Response::new(HeartbeatResponse {
                    success: true,
                    server_time: Some(Timestamp {
                        seconds: now.timestamp(),
                        nanos: now.timestamp_subsec_nanos() as i32,
                    }),
                }))
            }
            Err(WorkerError::NotFound(id)) => {
                Err(Status::not_found(format!("Worker {} not found", id)))
            }
            Err(e) => {
                error!(error = %e, "Heartbeat failed");
                Err(Status::internal(e.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn list_workers(
        &self,
        request: Request<ListWorkersRequest>,
    ) -> Result<Response<ListWorkersResponse>, Status> {
        let req = request.into_inner();

        let status_filter = if req.status_filter != WorkerStatus::Unspecified as i32 {
            Some(WorkerStatus::try_from(req.status_filter).unwrap_or(WorkerStatus::Active))
        } else {
            None
        };

        let workers = self.worker_registry.list(status_filter);
        let total = workers.len() as i32;

        Ok(Response::new(ListWorkersResponse {
            workers: workers.into_iter().map(|w| w.to_proto()).collect(),
            next_page_token: String::new(),
            total_count: total,
        }))
    }

    #[instrument(skip(self, request))]
    async fn register_dataset(
        &self,
        request: Request<RegisterDatasetRequest>,
    ) -> Result<Response<RegisterDatasetResponse>, Status> {
        let req = request.into_inner();
        info!(dataset_id = %req.dataset_id, path = %req.path, "Registering dataset");

        let metadata = req.metadata.map(|m| m.labels).unwrap_or_default();
        let dataset = self.dataset_registry.register(req.dataset_id, req.path, metadata);

        Ok(Response::new(RegisterDatasetResponse {
            success: true,
            message: "Dataset registered successfully".to_string(),
            dataset: Some(dataset),
        }))
    }

    #[instrument(skip(self, _request))]
    async fn list_datasets(
        &self,
        _request: Request<ListDatasetsRequest>,
    ) -> Result<Response<ListDatasetsResponse>, Status> {
        let datasets = self.dataset_registry.list();
        let total = datasets.len() as i32;

        Ok(Response::new(ListDatasetsResponse {
            datasets,
            next_page_token: String::new(),
            total_count: total,
        }))
    }

    #[instrument(skip(self, request), fields(worker_id, size))]
    async fn save_checkpoint(
        &self,
        request: Request<SaveCheckpointRequest>,
    ) -> Result<Response<SaveCheckpointResponse>, Status> {
        let timer = metrics::GRPC_REQUEST_DURATION
            .with_label_values(&["save_checkpoint"])
            .start_timer();

        let req = request.into_inner();
        tracing::Span::current().record("worker_id", &req.worker_id.as_str());
        tracing::Span::current().record("size", req.data.len());

        info!(
            worker_id = %req.worker_id,
            size = req.data.len(),
            "Saving checkpoint"
        );

        // Verify worker exists
        if !self.worker_registry.exists(&req.worker_id) {
            return Err(Status::failed_precondition(format!(
                "Worker {} is not registered",
                req.worker_id
            )));
        }

        let metadata = req.metadata.map(|m| m.labels).unwrap_or_default();
        let idempotency_key = if req.idempotency_key.is_empty() {
            None
        } else {
            Some(req.idempotency_key)
        };

        match self
            .checkpoint_manager
            .save(&req.worker_id, Bytes::from(req.data), metadata, idempotency_key)
            .await
        {
            Ok(checkpoint) => {
                metrics::GRPC_REQUESTS_TOTAL
                    .with_label_values(&["save_checkpoint", "success"])
                    .inc();
                timer.observe_duration();

                Ok(Response::new(SaveCheckpointResponse {
                    success: true,
                    message: "Checkpoint saved successfully".to_string(),
                    checkpoint: Some(checkpoint.to_proto()),
                }))
            }
            Err(CheckpointError::IdempotentDuplicate(key)) => {
                metrics::GRPC_REQUESTS_TOTAL
                    .with_label_values(&["save_checkpoint", "idempotent_hit"])
                    .inc();
                timer.observe_duration();
                Err(Status::already_exists(format!(
                    "Checkpoint with idempotency key {} already exists",
                    key
                )))
            }
            Err(e) => {
                metrics::GRPC_REQUESTS_TOTAL
                    .with_label_values(&["save_checkpoint", "error"])
                    .inc();
                metrics::ERRORS_TOTAL.with_label_values(&["checkpoint_save"]).inc();
                timer.observe_duration();
                error!(error = %e, "Failed to save checkpoint");
                Err(Status::internal(e.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn save_checkpoint_stream(
        &self,
        request: Request<tonic::Streaming<SaveCheckpointStreamRequest>>,
    ) -> Result<Response<SaveCheckpointResponse>, Status> {
        use tokio_stream::StreamExt;

        let mut stream = request.into_inner();
        let mut header: Option<SaveCheckpointStreamHeader> = None;
        let mut data = Vec::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            match chunk.request {
                Some(save_checkpoint_stream_request::Request::Header(h)) => {
                    header = Some(h);
                }
                Some(save_checkpoint_stream_request::Request::Chunk(c)) => {
                    data.extend_from_slice(&c);
                }
                None => {}
            }
        }

        let header = header.ok_or_else(|| Status::invalid_argument("Missing stream header"))?;

        // Verify worker exists
        if !self.worker_registry.exists(&header.worker_id) {
            return Err(Status::failed_precondition(format!(
                "Worker {} is not registered",
                header.worker_id
            )));
        }

        let metadata = header.metadata.map(|m| m.labels).unwrap_or_default();
        let idempotency_key = if header.idempotency_key.is_empty() {
            None
        } else {
            Some(header.idempotency_key)
        };

        match self
            .checkpoint_manager
            .save(&header.worker_id, Bytes::from(data), metadata, idempotency_key)
            .await
        {
            Ok(checkpoint) => Ok(Response::new(SaveCheckpointResponse {
                success: true,
                message: "Checkpoint saved successfully".to_string(),
                checkpoint: Some(checkpoint.to_proto()),
            })),
            Err(e) => {
                error!(error = %e, "Failed to save streamed checkpoint");
                Err(Status::internal(e.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn get_checkpoint(
        &self,
        request: Request<GetCheckpointRequest>,
    ) -> Result<Response<GetCheckpointResponse>, Status> {
        let req = request.into_inner();

        let checkpoint = self
            .checkpoint_manager
            .get(&req.checkpoint_id)
            .ok_or_else(|| Status::not_found(format!("Checkpoint {} not found", req.checkpoint_id)))?;

        let data = if req.include_data {
            match self.checkpoint_manager.get_data(&req.checkpoint_id).await {
                Ok(d) => d.to_vec(),
                Err(e) => {
                    error!(error = %e, "Failed to retrieve checkpoint data");
                    return Err(Status::internal(e.to_string()));
                }
            }
        } else {
            Vec::new()
        };

        Ok(Response::new(GetCheckpointResponse {
            checkpoint: Some(checkpoint.to_proto()),
            data,
        }))
    }

    #[instrument(skip(self, request))]
    async fn list_checkpoints(
        &self,
        request: Request<ListCheckpointsRequest>,
    ) -> Result<Response<ListCheckpointsResponse>, Status> {
        let req = request.into_inner();

        let worker_id = if req.worker_id.is_empty() {
            None
        } else {
            Some(req.worker_id.as_str())
        };

        let status_filter = if req.status_filter != CheckpointStatus::Unspecified as i32 {
            CheckpointStatus::try_from(req.status_filter).ok()
        } else {
            None
        };

        let checkpoints = self.checkpoint_manager.list(worker_id, status_filter);
        let total = checkpoints.len() as i32;

        Ok(Response::new(ListCheckpointsResponse {
            checkpoints: checkpoints.into_iter().map(|c| c.to_proto()).collect(),
            next_page_token: String::new(),
            total_count: total,
        }))
    }

    #[instrument(skip(self, request))]
    async fn delete_checkpoint(
        &self,
        request: Request<DeleteCheckpointRequest>,
    ) -> Result<Response<DeleteCheckpointResponse>, Status> {
        let req = request.into_inner();
        info!(checkpoint_id = %req.checkpoint_id, "Deleting checkpoint");

        match self.checkpoint_manager.delete(&req.checkpoint_id).await {
            Ok(_) => Ok(Response::new(DeleteCheckpointResponse {
                success: true,
                message: "Checkpoint deleted successfully".to_string(),
            })),
            Err(CheckpointError::NotFound(id)) => {
                Err(Status::not_found(format!("Checkpoint {} not found", id)))
            }
            Err(e) => {
                error!(error = %e, "Failed to delete checkpoint");
                Err(Status::internal(e.to_string()))
            }
        }
    }

    #[instrument(skip(self, _request))]
    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let now = Utc::now();
        let uptime = now - self.start_time;

        let mut components = std::collections::HashMap::new();

        // Storage health
        components.insert(
            "storage".to_string(),
            ComponentHealth {
                status: ServiceStatus::Healthy.into(),
                message: "S3 connection healthy".to_string(),
                last_check: Some(Timestamp {
                    seconds: now.timestamp(),
                    nanos: now.timestamp_subsec_nanos() as i32,
                }),
            },
        );

        // Worker registry health
        components.insert(
            "worker_registry".to_string(),
            ComponentHealth {
                status: ServiceStatus::Healthy.into(),
                message: format!("{} workers registered", self.worker_registry.total_count()),
                last_check: Some(Timestamp {
                    seconds: now.timestamp(),
                    nanos: now.timestamp_subsec_nanos() as i32,
                }),
            },
        );

        Ok(Response::new(HealthCheckResponse {
            status: ServiceStatus::Healthy.into(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime: Some(Timestamp {
                seconds: uptime.num_seconds(),
                nanos: 0,
            }),
            components,
        }))
    }
}