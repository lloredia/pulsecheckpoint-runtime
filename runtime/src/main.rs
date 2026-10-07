//! Process entrypoint for the PulseCheckpoint gRPC service.

use clap::Parser;
use pulse_runtime::api::PulseServiceImpl;
use pulse_runtime::checkpoint::CheckpointManager;
use pulse_runtime::config::{AppConfig, ConfigError};
use pulse_runtime::metrics::{self, MetricsServer};
use pulse_runtime::pulse::v1::pulse_service_server::PulseServiceServer;
use pulse_runtime::storage::{S3Storage, StorageError};
use pulse_runtime::worker::WorkerRegistry;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::watch;
use tonic::transport::Server;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(name = "pulse-runtime")]
#[command(version)]
#[command(about = "Single-process gRPC checkpoint service")]
struct Args {
    /// Path to a TOML config file.
    #[arg(short, long, env = "PULSE_CONFIG")]
    config: Option<String>,

    /// gRPC listen address. Overrides the config file when set.
    #[arg(long, env = "PULSE_GRPC_ADDR")]
    grpc_addr: Option<SocketAddr>,

    /// Metrics listen address. Overrides the config file when set.
    #[arg(long, env = "PULSE_METRICS_ADDR")]
    metrics_addr: Option<SocketAddr>,

    /// Log level passed to the tracing filter.
    #[arg(long, env = "PULSE_LOG_LEVEL")]
    log_level: Option<String>,

    /// Probe the local metrics `/health` endpoint and exit.
    #[arg(long)]
    healthcheck: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenvy::dotenv();
    let args = Args::parse();

    if args.healthcheck {
        let port = args
            .metrics_addr
            .unwrap_or_else(|| "0.0.0.0:9090".parse().expect("default metrics addr"))
            .port();
        metrics::probe_http_health(port)?;
        return Ok(());
    }

    let config = load_config(&args)?;
    let log_level = args
        .log_level
        .clone()
        .unwrap_or_else(|| config.logging.level.clone());
    init_logging(&log_level, &config.logging.format);

    let grpc_addr: SocketAddr = args.grpc_addr.unwrap_or(
        config
            .server
            .grpc_addr
            .parse()
            .map_err(|err| format!("invalid grpc address: {err}"))?,
    );
    let metrics_addr: SocketAddr = args.metrics_addr.unwrap_or(
        config
            .server
            .metrics_addr
            .parse()
            .map_err(|err| format!("invalid metrics address: {err}"))?,
    );

    info!(
        version = env!("CARGO_PKG_VERSION"),
        %grpc_addr,
        %metrics_addr,
        "starting pulse runtime"
    );

    let storage = Arc::new(connect_storage(&config).await?);
    let workers = Arc::new(WorkerRegistry::with_config(
        Duration::from_secs(config.server.heartbeat_timeout_secs),
        Duration::from_secs(config.server.heartbeat_interval_secs.max(1)),
    ));
    let checkpoints = Arc::new(CheckpointManager::new(storage, config.retry.clone()));
    let service = PulseServiceImpl::new(workers.clone(), checkpoints);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(pulse_runtime::pulse::v1::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let metrics_shutdown = shutdown_rx.clone();
    let (bound_metrics, listener) = MetricsServer::listen(metrics_addr).await?;
    let metrics_handle = tokio::spawn(async move {
        if let Err(err) = MetricsServer::serve(listener, metrics_shutdown).await {
            error!(error = %err, "metrics server stopped");
        }
    });

    let heartbeat_shutdown = shutdown_rx.clone();
    let heartbeat_workers = workers.clone();
    let heartbeat_handle = tokio::spawn(async move {
        heartbeat_workers
            .start_heartbeat_monitor(heartbeat_shutdown)
            .await;
    });

    info!(%bound_metrics, "metrics server listening");
    info!(%grpc_addr, "grpc server listening");

    Server::builder()
        .add_service(reflection)
        .add_service(PulseServiceServer::new(service))
        .serve_with_shutdown(grpc_addr, async move {
            shutdown_signal().await;
            info!("shutdown signal received");
            let _ = shutdown_tx.send(true);
        })
        .await?;

    let _ = tokio::join!(metrics_handle, heartbeat_handle);
    info!("shutdown complete");
    Ok(())
}

fn load_config(args: &Args) -> Result<AppConfig, ConfigError> {
    if let Some(path) = &args.config {
        AppConfig::from_file(path)
    } else {
        AppConfig::from_env()
    }
}

async fn connect_storage(config: &AppConfig) -> Result<S3Storage, StorageError> {
    let mut last = None;
    for attempt in 1..=5 {
        match S3Storage::new(&config.storage).await {
            Ok(storage) => return Ok(storage),
            Err(err) => {
                tracing::warn!(attempt, error = %err, "storage is not ready");
                last = Some(err);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    Err(last.unwrap_or_else(|| StorageError::Backend("storage connection failed".into())))
}

fn init_logging(level: &str, format: &str) {
    use tracing_subscriber::fmt;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    let registry = tracing_subscriber::registry().with(filter);
    if format.eq_ignore_ascii_case("pretty") {
        registry.with(fmt::layer()).init();
    } else {
        registry
            .with(fmt::layer().json().with_target(true).with_thread_ids(true))
            .init();
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
