//! End-to-end checkpoint save against MinIO or any S3-compatible endpoint.
//!
//! Set `PULSE_S3_ENDPOINT` (and credentials the AWS SDK can see) to run it.
//! In CI the workflow starts MinIO and exports those variables. Without the
//! endpoint, local `cargo test` skips this file unless `CI=true`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pulse_runtime::api::PulseServiceImpl;
use pulse_runtime::checkpoint::CheckpointManager;
use pulse_runtime::config::{RetryConfig, StorageConfig};
use pulse_runtime::pulse::v1::pulse_service_client::PulseServiceClient;
use pulse_runtime::pulse::v1::pulse_service_server::PulseServiceServer;
use pulse_runtime::pulse::v1::{
    GetCheckpointRequest, RegisterWorkerRequest, SaveCheckpointRequest,
};
use pulse_runtime::storage::{S3Storage, Storage};
use pulse_runtime::worker::WorkerRegistry;
use tonic::transport::Server;
use uuid::Uuid;

#[tokio::test]
async fn grpc_checkpoint_roundtrip_against_s3() {
    let endpoint = match std::env::var("PULSE_S3_ENDPOINT") {
        Ok(value) if !value.is_empty() => value,
        _ if std::env::var("CI").ok().as_deref() == Some("true") => {
            panic!("PULSE_S3_ENDPOINT must be set when CI=true");
        }
        _ => {
            eprintln!("skipping s3 integration: set PULSE_S3_ENDPOINT to run it");
            return;
        }
    };

    let bucket = std::env::var("PULSE_S3_BUCKET").unwrap_or_else(|_| "checkpoints".into());
    let region = std::env::var("PULSE_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
    let force_path_style = std::env::var("PULSE_S3_FORCE_PATH_STYLE")
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(true);
    let prefix = format!("it-{}", Uuid::new_v4().simple());

    let storage_config = StorageConfig {
        endpoint: Some(endpoint),
        bucket,
        region,
        force_path_style,
        path_prefix: Some(prefix),
        ..StorageConfig::default()
    };
    let storage = Arc::new(
        S3Storage::new(&storage_config)
            .await
            .expect("connect to s3 endpoint and ensure bucket"),
    );
    storage
        .health()
        .await
        .expect("bucket should respond to head");

    let manager = Arc::new(CheckpointManager::new(
        storage.clone(),
        RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 20,
            max_delay_ms: 200,
            multiplier: 2.0,
        },
    ));
    let workers = Arc::new(WorkerRegistry::new());
    let service = PulseServiceImpl::new(workers, manager);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(PulseServiceServer::new(service))
            .serve_with_incoming(incoming)
            .await
    });

    let url = format!("http://{addr}");
    let mut client = None;
    for _ in 0..40 {
        match PulseServiceClient::connect(url.clone()).await {
            Ok(connected) => {
                client = Some(connected);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    let mut client = client.expect("grpc client connected");

    client
        .register_worker(RegisterWorkerRequest {
            worker_id: "it-worker".into(),
            metadata: None,
        })
        .await
        .expect("register worker");

    let payload = b"integration-checkpoint".to_vec();
    let saved = client
        .save_checkpoint(SaveCheckpointRequest {
            worker_id: "it-worker".into(),
            data: payload.clone(),
            metadata: Some(pulse_runtime::pulse::v1::Metadata {
                labels: HashMap::from([("kind".into(), "integration".into())]),
            }),
            idempotency_key: "it-key".into(),
        })
        .await
        .expect("save checkpoint")
        .into_inner();
    let checkpoint_id = saved.checkpoint.expect("checkpoint info").checkpoint_id;

    let loaded = client
        .get_checkpoint(GetCheckpointRequest {
            checkpoint_id,
            include_data: true,
        })
        .await
        .expect("get checkpoint")
        .into_inner();
    assert_eq!(loaded.data, payload);

    server.abort();
}
