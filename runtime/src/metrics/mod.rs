//! Prometheus metrics served over HTTP.

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::header::{HeaderValue, CONTENT_TYPE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use prometheus::{
    Encoder, Gauge, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    Opts, Registry, TextEncoder,
};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{LazyLock, Once};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{debug, error, info};

static REGISTERED: Once = Once::new();

pub static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

pub static CHECKPOINTS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pulse_checkpoints_total",
        "Checkpoint saves that finished writing bytes",
    ))
    .expect("checkpoint counter")
});

pub static CHECKPOINT_BYTES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pulse_checkpoint_bytes_total",
        "Bytes accepted by successful checkpoint saves",
    ))
    .expect("checkpoint bytes counter")
});

pub static CHECKPOINT_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::with_opts(
        HistogramOpts::new(
            "pulse_checkpoint_duration_seconds",
            "Time to save a checkpoint, including storage retries",
        )
        .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
    )
    .expect("checkpoint histogram")
});

pub static ACTIVE_WORKERS: LazyLock<Gauge> = LazyLock::new(|| {
    Gauge::with_opts(Opts::new(
        "pulse_active_workers",
        "Workers currently in the active state",
    ))
    .expect("active workers gauge")
});

pub static WORKER_REGISTRATIONS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pulse_worker_registrations_total",
        "Successful worker registrations",
    ))
    .expect("registration counter")
});

pub static WORKER_HEARTBEATS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pulse_worker_heartbeats_total",
        "Accepted worker heartbeats",
    ))
    .expect("heartbeat counter")
});

pub static S3_REQUESTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new("pulse_s3_requests_total", "S3 API requests"),
        &["operation", "status"],
    )
    .expect("s3 counter")
});

pub static S3_REQUEST_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new("pulse_s3_request_duration_seconds", "S3 request latency")
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]),
        &["operation"],
    )
    .expect("s3 histogram")
});

pub static GRPC_REQUESTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pulse_grpc_requests_total",
            "gRPC requests handled by this process",
        ),
        &["method", "status"],
    )
    .expect("grpc counter")
});

pub static GRPC_REQUEST_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "pulse_grpc_request_duration_seconds",
            "gRPC handler latency",
        )
        .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
        &["method"],
    )
    .expect("grpc histogram")
});

pub static DATASETS_TOTAL: LazyLock<IntGauge> = LazyLock::new(|| {
    IntGauge::with_opts(Opts::new(
        "pulse_datasets_total",
        "Datasets registered in this process",
    ))
    .expect("dataset gauge")
});

pub static ERRORS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new("pulse_errors_total", "Handler errors by type"),
        &["error_type"],
    )
    .expect("error counter")
});

pub fn register_metrics() {
    REGISTERED.call_once(|| {
        let registry = &*REGISTRY;
        registry
            .register(Box::new(CHECKPOINTS_TOTAL.clone()))
            .expect("register checkpoints");
        registry
            .register(Box::new(CHECKPOINT_BYTES_TOTAL.clone()))
            .expect("register bytes");
        registry
            .register(Box::new(CHECKPOINT_DURATION.clone()))
            .expect("register duration");
        registry
            .register(Box::new(ACTIVE_WORKERS.clone()))
            .expect("register workers");
        registry
            .register(Box::new(WORKER_REGISTRATIONS_TOTAL.clone()))
            .expect("register registrations");
        registry
            .register(Box::new(WORKER_HEARTBEATS_TOTAL.clone()))
            .expect("register heartbeats");
        registry
            .register(Box::new(S3_REQUESTS_TOTAL.clone()))
            .expect("register s3 requests");
        registry
            .register(Box::new(S3_REQUEST_DURATION.clone()))
            .expect("register s3 duration");
        registry
            .register(Box::new(GRPC_REQUESTS_TOTAL.clone()))
            .expect("register grpc requests");
        registry
            .register(Box::new(GRPC_REQUEST_DURATION.clone()))
            .expect("register grpc duration");
        registry
            .register(Box::new(DATASETS_TOTAL.clone()))
            .expect("register datasets");
        registry
            .register(Box::new(ERRORS_TOTAL.clone()))
            .expect("register errors");
    });
}

pub fn set_active_workers(count: usize) {
    ACTIVE_WORKERS.set(count as f64);
}

pub fn set_datasets(count: usize) {
    let value = i64::try_from(count).unwrap_or(i64::MAX);
    DATASETS_TOTAL.set(value);
}

struct Routed {
    status: StatusCode,
    content_type: &'static str,
    body: Bytes,
}

fn route(path: &str) -> Routed {
    match path {
        "/metrics" => {
            let encoder = TextEncoder::new();
            let families = REGISTRY.gather();
            let mut buffer = Vec::new();
            if encoder.encode(&families, &mut buffer).is_err() {
                return Routed {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    content_type: "text/plain; charset=utf-8",
                    body: Bytes::from_static(b"failed to encode metrics"),
                };
            }
            Routed {
                status: StatusCode::OK,
                content_type: "text/plain; version=0.0.4; charset=utf-8",
                body: Bytes::from(buffer),
            }
        }
        "/health" | "/ready" => Routed {
            status: StatusCode::OK,
            content_type: "text/plain; charset=utf-8",
            body: Bytes::from_static(b"ok"),
        },
        _ => Routed {
            status: StatusCode::NOT_FOUND,
            content_type: "text/plain; charset=utf-8",
            body: Bytes::from_static(b"not found"),
        },
    }
}

fn to_response(routed: Routed) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(routed.body));
    *response.status_mut() = routed.status;
    if let Ok(value) = HeaderValue::from_str(routed.content_type) {
        response.headers_mut().insert(CONTENT_TYPE, value);
    }
    response
}

async fn handle(req: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(to_response(route(req.uri().path())))
}

/// HTTP server for `/metrics`, `/health`, and `/ready`.
pub struct MetricsServer;

impl MetricsServer {
    pub async fn listen(addr: SocketAddr) -> std::io::Result<(SocketAddr, TcpListener)> {
        register_metrics();
        let listener = TcpListener::bind(addr).await?;
        let bound = listener.local_addr()?;
        Ok((bound, listener))
    }

    pub async fn serve(
        listener: TcpListener,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), std::io::Error> {
        info!(addr = %listener.local_addr().unwrap_or(SocketAddr::from(([0, 0, 0, 0], 0))), "metrics server started");
        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                accepted = listener.accept() => {
                    let (stream, _) = match accepted {
                        Ok(pair) => pair,
                        Err(err) => {
                            error!(error = %err, "metrics accept failed");
                            break;
                        }
                    };
                    let io = TokioIo::new(stream);
                    tokio::spawn(async move {
                        if let Err(err) = http1::Builder::new().serve_connection(io, service_fn(handle)).await {
                            debug!(error = %err, "metrics connection closed");
                        }
                    });
                }
            }
        }
        Ok(())
    }
}

/// TCP + HTTP probe used by the process healthcheck flag and container HEALTHCHECK.
pub fn probe_http_health(port: u16) -> std::io::Result<()> {
    use std::io::{Read, Write};
    use std::net::{Ipv4Addr, TcpStream};

    let target = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let mut stream = TcpStream::connect_timeout(&target, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    stream.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
    let mut buf = [0u8; 128];
    let n = stream.read(&mut buf)?;
    let text = String::from_utf8_lossy(&buf[..n]);
    if text.starts_with("HTTP/1.1 200") || text.starts_with("HTTP/1.0 200") {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "unexpected health response: {text}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_text_includes_checkpoint_counter() {
        register_metrics();
        let before = CHECKPOINTS_TOTAL.get();
        CHECKPOINTS_TOTAL.inc();
        let routed = route("/metrics");
        assert_eq!(routed.status, StatusCode::OK);
        let body = String::from_utf8(routed.body.to_vec()).unwrap();
        assert!(body.contains("pulse_checkpoints_total"));
        assert!(CHECKPOINTS_TOTAL.get() > before);
        assert_eq!(route("/health").status, StatusCode::OK);
        assert_eq!(route("/missing").status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn http_health_responds_on_bound_port() {
        let (addr, listener) = MetricsServer::listen(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let (tx, rx) = watch::channel(false);
        let port = addr.port();
        let handle = tokio::spawn(async move { MetricsServer::serve(listener, rx).await });
        tokio::task::spawn_blocking(move || probe_http_health(port))
            .await
            .expect("probe task")
            .expect("health response");
        tx.send(true).unwrap();
        let _ = handle.await;
    }
}
