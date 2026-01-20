# PulseCheckpoint Runtime

[![CI](https://github.com/your-org/pulsecheckpoint-runtime/actions/workflows/ci.yml/badge.svg)](https://github.com/your-org/pulsecheckpoint-runtime/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A distributed runtime providing a Python SDK for registering workers and saving checkpoints, backed by a high-performance Rust backend using gRPC. The system persists checkpoint artifacts into S3-compatible object storage with support for concurrent workers, retry logic, fault tolerance, and observability.

## 🏗️ Architecture

```
┌─────────────────┐     gRPC      ┌─────────────────────┐     S3 API    ┌─────────────────┐
│   Python SDK    │──────────────▶│    Rust Runtime     │──────────────▶│  Object Storage │
│                 │               │                     │               │  (MinIO / S3)   │
│  • register_    │               │  • Tokio async      │               │                 │
│    worker       │               │  • Tonic gRPC       │               │  • Checkpoints  │
│  • register_    │               │  • Retry logic      │               │  • Artifacts    │
│    dataset      │               │  • Metrics          │               │                 │
│  • save_        │               │  • Structured logs  │               │                 │
│    checkpoint   │               │                     │               │                 │
└─────────────────┘               └─────────────────────┘               └─────────────────┘
                                           │
                                           ▼
                                  ┌─────────────────┐
                                  │   Prometheus    │
                                  │   + Grafana     │
                                  └─────────────────┘
```

## ✨ Features

- **High-Performance Rust Backend**: Async I/O with Tokio, gRPC with Tonic
- **Python SDK**: Simple, developer-friendly API for ML/distributed workloads
- **S3-Compatible Storage**: MinIO for local development, AWS S3 for production
- **Fault Tolerance**: Retry logic, idempotent writes, graceful shutdown
- **Observability**: Prometheus metrics, structured logging, Grafana dashboards
- **Infrastructure as Code**: Terraform modules for AWS deployment
- **CI/CD**: GitHub Actions for build, test, lint, and Docker image publishing

## 🚀 Quick Start

### Prerequisites

- Rust 1.75+ (`rustup install stable`)
- Python 3.10+
- Docker & Docker Compose
- Make

### Local Development

```bash
# Clone the repository
git clone https://github.com/your-org/pulsecheckpoint-runtime.git
cd pulsecheckpoint-runtime

# Start local infrastructure (MinIO, Prometheus, Grafana)
make dev-up

# Build the Rust runtime
make build-runtime

# Install Python SDK
make install-sdk

# Run the runtime
make run-runtime

# In another terminal, run the example
make run-example
```

### Using the Python SDK

```python
from pulse import Runtime

# Connect to the runtime
rt = Runtime("grpc://localhost:50051")

# Register a worker
rt.register_worker("worker-1", metadata={"gpu": "A100", "memory": "80GB"})

# Register a dataset
rt.register_dataset("training-data", path="s3://bucket/data")

# Save a checkpoint (async upload to S3)
checkpoint_id = rt.save_checkpoint(
    worker_id="worker-1",
    data=b"model-state-bytes",
    metadata={"epoch": 10, "loss": 0.001}
)

# Send heartbeat
rt.heartbeat("worker-1")

# Graceful shutdown
rt.deregister_worker("worker-1")
```

## 📁 Repository Structure

```
pulsecheckpoint-runtime/
├── proto/                      # Protocol Buffer definitions
│   └── pulse.proto
├── runtime/                    # Rust runtime implementation
│   ├── Cargo.toml
│   ├── build.rs
│   └── src/
│       ├── main.rs
│       ├── api/                # gRPC service implementations
│       ├── storage/            # S3 client abstraction
│       ├── checkpoint/         # Checkpoint management
│       ├── worker/             # Worker registry
│       ├── metrics/            # Prometheus metrics
│       └── config/             # Configuration management
├── sdk/                        # Python SDK
│   ├── pulse/
│   │   ├── __init__.py
│   │   ├── runtime.py
│   │   └── generated/          # Generated protobuf code
│   ├── pyproject.toml
│   └── tests/
├── terraform/                  # Infrastructure as Code
│   ├── modules/
│   │   ├── ecs/
│   │   ├── s3/
│   │   ├── iam/
│   │   ├── vpc/
│   │   └── security-groups/
│   └── environments/
│       ├── dev/
│       └── prod/
├── docker/                     # Container configurations
│   ├── Dockerfile.runtime
│   └── Dockerfile.dev
├── docker-compose.yml          # Local development stack
├── .github/workflows/          # CI/CD pipelines
│   └── ci.yml
├── observability/              # Monitoring configuration
│   ├── prometheus/
│   └── grafana/
├── examples/                   # Example usage scripts
├── docs/                       # Documentation
├── Makefile                    # Task automation
└── README.md
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PULSE_GRPC_ADDR` | gRPC server address | `0.0.0.0:50051` |
| `PULSE_METRICS_ADDR` | Prometheus metrics address | `0.0.0.0:9090` |
| `PULSE_S3_ENDPOINT` | S3 endpoint URL | `http://localhost:9000` |
| `PULSE_S3_BUCKET` | S3 bucket name | `checkpoints` |
| `PULSE_S3_REGION` | AWS region | `us-east-1` |
| `AWS_ACCESS_KEY_ID` | AWS access key | - |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | - |
| `PULSE_LOG_LEVEL` | Log level (trace/debug/info/warn/error) | `info` |
| `PULSE_MAX_RETRIES` | Max retry attempts for S3 operations | `3` |
| `PULSE_RETRY_DELAY_MS` | Initial retry delay in milliseconds | `100` |

### Configuration File

```toml
# config.toml
[server]
grpc_addr = "0.0.0.0:50051"
metrics_addr = "0.0.0.0:9090"

[storage]
endpoint = "http://localhost:9000"
bucket = "checkpoints"
region = "us-east-1"

[retry]
max_attempts = 3
initial_delay_ms = 100
max_delay_ms = 5000

[logging]
level = "info"
format = "json"
```

## 📊 Observability

### Metrics

The runtime exposes Prometheus metrics at `/metrics`:

- `pulse_checkpoints_total` - Total checkpoints saved (counter)
- `pulse_checkpoint_bytes_total` - Total bytes uploaded (counter)
- `pulse_checkpoint_duration_seconds` - Checkpoint save latency (histogram)
- `pulse_active_workers` - Currently registered workers (gauge)
- `pulse_s3_requests_total` - S3 API requests (counter)
- `pulse_s3_request_duration_seconds` - S3 request latency (histogram)
- `pulse_grpc_requests_total` - gRPC requests by method (counter)
- `pulse_grpc_request_duration_seconds` - gRPC request latency (histogram)

### Grafana Dashboard

Access Grafana at `http://localhost:3000` (admin/admin) with pre-configured dashboards.

### Structured Logging

```json
{
  "timestamp": "2024-01-15T10:30:00.000Z",
  "level": "INFO",
  "target": "pulse::checkpoint",
  "message": "Checkpoint saved successfully",
  "worker_id": "worker-1",
  "checkpoint_id": "chk_abc123",
  "size_bytes": 104857600,
  "duration_ms": 250
}
```

## 🚢 Deployment

### AWS Deployment with Terraform

```bash
cd terraform/environments/prod

# Initialize Terraform
terraform init

# Review the plan
terraform plan -var-file="terraform.tfvars"

# Apply infrastructure
terraform apply -var-file="terraform.tfvars"
```

### Docker Deployment

```bash
# Build production image
docker build -f docker/Dockerfile.runtime -t pulsecheckpoint-runtime:latest .

# Run with environment variables
docker run -d \
  -p 50051:50051 \
  -p 9090:9090 \
  -e PULSE_S3_ENDPOINT=https://s3.us-east-1.amazonaws.com \
  -e PULSE_S3_BUCKET=my-checkpoints \
  -e AWS_ACCESS_KEY_ID=xxx \
  -e AWS_SECRET_ACCESS_KEY=xxx \
  pulsecheckpoint-runtime:latest
```

## 🧪 Testing

```bash
# Run Rust tests
make test-runtime

# Run Python SDK tests
make test-sdk

# Run integration tests (requires local stack)
make test-integration

# Run all tests
make test
```

## 🔒 Security

- **TLS**: gRPC supports TLS for encrypted communication
- **IAM**: Least-privilege IAM policies for S3 access
- **Network**: Security groups restrict access to required ports
- **Secrets**: AWS Secrets Manager integration for credentials

### Enabling TLS

```bash
# Generate certificates
make generate-certs

# Run with TLS enabled
PULSE_TLS_CERT=/path/to/cert.pem \
PULSE_TLS_KEY=/path/to/key.pem \
make run-runtime
```

## 📚 API Reference

See [docs/API.md](docs/API.md) for complete API documentation.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Tokio](https://tokio.rs/) - Async runtime for Rust
- [Tonic](https://github.com/hyperium/tonic) - gRPC implementation for Rust
- [MinIO](https://min.io/) - S3-compatible object storage
