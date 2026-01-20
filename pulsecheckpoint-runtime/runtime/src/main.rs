//! PulseCheckpoint Runtime - High-performance distributed checkpoint system
//!
//! This runtime provides gRPC APIs for worker registration, dataset management,
//! and checkpoint persistence to S3-compatible storage.

use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::watch;
use tonic::transport::Server;
use tracing::{info, error};

mod api;
mod checkpoint;
mod config;
mod metrics;
mod storage;
mod worker;

use api::PulseServiceImpl;
use config::AppConfig;
use metrics::MetricsServer;
use storage::S3Storage;
use worker::WorkerRegistry;
use checkpoint::CheckpointManager;

/// PulseCheckpoint Runtime CLI arguments
#[derive(Parser, Debug)]
#[command(name = "pulse-runtime")]
#[command(author = "PulseCheckpoint Contributors")]
#[command(version = "0.1.0")]
#[command(about = "High-performance distributed checkpoint runtime")]
struct Args {
    /// Path to configuration file
    #[arg(short, long, env = "PULSE_CONFIG")]
    config: Option<String>,

    /// gRPC server address
    #[arg(long, env = "PULSE_GRPC_ADDR", default_value = "0.0.0.0:50051")]
    grpc_addr: SocketAddr,

    /// Metrics server address
    #[arg(long, env = "PULSE_METRICS_ADDR", default_value = "0.0.0.0:9090")]
    metrics_addr: SocketAddr,

    /// Log level
    #[arg(long, env = "PULSE_LOG_LEVEL", default_value = "info")]
    log_level: String,
}

// Include generated protobuf code
pub mod pulse {
    pub mod v1 {
        tonic::include_proto!("pulse.v1");
        
        pub const FILE_DESCRIPTOR_SET: &[u8] = 
            tonic::include_file_descriptor_set!("pulse_descriptor");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load .env file if present
    dotenvy::dotenv().ok();

    // Parse CLI arguments
    let args = Args::parse();

    // Initialize logging
    init_logging(&args.log_level);

    info!(
        version = env!("CARGO_PKG_VERSION"),
        grpc_addr = %args.grpc_addr,
        metrics_addr = %args.metrics_addr,
        "Starting PulseCheckpoint Runtime"
    );

    // Load configuration
    let config = if let Some(config_path) = &args.config {
        AppConfig::from_file(config_path)?
    } else {
        AppConfig::from_env()?
    };

    // Initialize components
    let storage = Arc::new(S3Storage::new(&config.storage).await?);
    let worker_registry = Arc::new(WorkerRegistry::new());
    let checkpoint_manager = Arc::new(CheckpointManager::new(
        storage.clone(),
        config.retry.clone(),
    ));

    // Create gRPC service
    let pulse_service = PulseServiceImpl::new(
        worker_registry.clone(),
        checkpoint_manager.clone(),
    );

    // Create reflection service for gRPC debugging
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(pulse::v1::FILE_DESCRIPTOR_SET)
        .build()?;

    // Setup graceful shutdown
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let shutdown_rx_metrics = shutdown_rx.clone();

    // Start metrics server
    let metrics_handle = tokio::spawn(async move {
        let metrics_server = MetricsServer::new(args.metrics_addr);
        if let Err(e) = metrics_server.run(shutdown_rx_metrics).await {
            error!(error = %e, "Metrics server error");
        }
    });

    // Start heartbeat monitor
    let worker_registry_clone = worker_registry.clone();
    let heartbeat_handle = tokio::spawn(async move {
        worker_registry_clone.start_heartbeat_monitor(shutdown_rx.clone()).await;
    });

    // Build gRPC server
    let grpc_server = Server::builder()
        .add_service(reflection_service)
        .add_service(pulse::v1::pulse_service_server::PulseServiceServer::new(pulse_service))
        .serve_with_shutdown(args.grpc_addr, async move {
            shutdown_signal().await;
            info!("Received shutdown signal, initiating graceful shutdown");
            let _ = shutdown_tx.send(true);
        });

    info!(addr = %args.grpc_addr, "gRPC server listening");
    info!(addr = %args.metrics_addr, "Metrics server listening");

    // Run server
    grpc_server.await?;

    // Wait for background tasks
    info!("Waiting for background tasks to complete");
    let _ = tokio::join!(metrics_handle, heartbeat_handle);

    info!("Shutdown complete");
    Ok(())
}

/// Initialize structured logging with tracing
fn init_logging(level: &str) {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(level));

    tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer()
                .json()
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
        )
        .init();
}

/// Wait for shutdown signal (Ctrl+C or SIGTERM)
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
