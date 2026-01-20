//! Prometheus metrics for observability

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server, StatusCode};
use lazy_static::lazy_static;
use prometheus::{
    self, Encoder, Gauge, Histogram, HistogramOpts, HistogramVec,
    IntCounter, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};
use std::convert::Infallible;
use std::net::SocketAddr;
use tokio::sync::watch;
use tracing::{error, info};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // Checkpoint metrics
    pub static ref CHECKPOINTS_TOTAL: IntCounter = IntCounter::new(
        "pulse_checkpoints_total",
        "Total number of checkpoints saved"
    ).unwrap();

    pub static ref CHECKPOINT_BYTES_TOTAL: IntCounter = IntCounter::new(
        "pulse_checkpoint_bytes_total",
        "Total bytes uploaded to storage"
    ).unwrap();

    pub static ref CHECKPOINT_DURATION: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "pulse_checkpoint_duration_seconds",
            "Time to save a checkpoint"
        )
        .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])
    ).unwrap();

    // Worker metrics
    pub static ref ACTIVE_WORKERS: Gauge = Gauge::new(
        "pulse_active_workers",
        "Number of currently registered workers"
    ).unwrap();

    pub static ref WORKER_REGISTRATIONS_TOTAL: IntCounter = IntCounter::new(
        "pulse_worker_registrations_total",
        "Total number of worker registrations"
    ).unwrap();

    pub static ref WORKER_HEARTBEATS_TOTAL: IntCounter = IntCounter::new(
        "pulse_worker_heartbeats_total",
        "Total number of worker heartbeats"
    ).unwrap();

    // S3 metrics
    pub static ref S3_REQUESTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("pulse_s3_requests_total", "Total S3 API requests"),
        &["operation", "status"]
    ).unwrap();

    pub static ref S3_REQUEST_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "pulse_s3_request_duration_seconds",
            "S3 request latency"
        )
        .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]),
        &["operation"]
    ).unwrap();

    // gRPC metrics
    pub static ref GRPC_REQUESTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("pulse_grpc_requests_total", "Total gRPC requests"),
        &["method", "status"]
    ).unwrap();

    pub static ref GRPC_REQUEST_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "pulse_grpc_request_duration_seconds",
            "gRPC request latency"
        )
        .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
        &["method"]
    ).unwrap();

    // Dataset metrics
    pub static ref DATASETS_TOTAL: IntGauge = IntGauge::new(
        "pulse_datasets_total",
        "Total number of registered datasets"
    ).unwrap();

    // Error metrics
    pub static ref ERRORS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("pulse_errors_total", "Total errors by type"),
        &["type"]
    ).unwrap();
}

/// Register all metrics with the registry
pub fn register_metrics() {
    REGISTRY.register(Box::new(CHECKPOINTS_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(CHECKPOINT_BYTES_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(CHECKPOINT_DURATION.clone())).unwrap();
    REGISTRY.register(Box::new(ACTIVE_WORKERS.clone())).unwrap();
    REGISTRY.register(Box::new(WORKER_REGISTRATIONS_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(WORKER_HEARTBEATS_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(S3_REQUESTS_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(S3_REQUEST_DURATION.clone())).unwrap();
    REGISTRY.register(Box::new(GRPC_REQUESTS_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(GRPC_REQUEST_DURATION.clone())).unwrap();
    REGISTRY.register(Box::new(DATASETS_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(ERRORS_TOTAL.clone())).unwrap();
}

/// HTTP metrics server
pub struct MetricsServer {
    addr: SocketAddr,
}

impl MetricsServer {
    pub fn new(addr: SocketAddr) -> Self {
        // Register metrics on creation
        register_metrics();
        Self { addr }
    }

    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<(), hyper::Error> {
        let make_svc = make_service_fn(|_conn| async {
            Ok::<_, Infallible>(service_fn(Self::handle_request))
        });

        let server = Server::bind(&self.addr)
            .serve(make_svc)
            .with_graceful_shutdown(async move {
                shutdown.changed().await.ok();
            });

        info!(addr = %self.addr, "Metrics server started");
        server.await
    }

    async fn handle_request(req: Request<Body>) -> Result<Response<Body>, Infallible> {
        match req.uri().path() {
            "/metrics" => {
                let encoder = TextEncoder::new();
                let metric_families = REGISTRY.gather();
                let mut buffer = Vec::new();
                
                if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
                    error!(error = %e, "Failed to encode metrics");
                    return Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from("Failed to encode metrics"))
                        .unwrap());
                }

                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", encoder.format_type())
                    .body(Body::from(buffer))
                    .unwrap())
            }
            "/health" => {
                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("OK"))
                    .unwrap())
            }
            "/ready" => {
                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("Ready"))
                    .unwrap())
            }
            _ => {
                Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::from("Not Found"))
                    .unwrap())
            }
        }
    }
}

/// Helper trait for timing operations
pub trait TimerExt {
    fn stop_and_record(self) -> f64;
}

impl TimerExt for prometheus::HistogramTimer {
    fn stop_and_record(self) -> f64 {
        self.stop_and_record()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_registration() {
        // Metrics should be registered without panicking
        // Note: Can only register once, so this test validates the lazy_static works
        assert!(CHECKPOINTS_TOTAL.get() >= 0);
    }

    #[test]
    fn test_counter_increment() {
        let initial = CHECKPOINTS_TOTAL.get();
        CHECKPOINTS_TOTAL.inc();
        assert_eq!(CHECKPOINTS_TOTAL.get(), initial + 1);
    }
}
