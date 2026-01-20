# Build stage
FROM rust:latest AS builder

# Install protobuf compiler
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the entire project
COPY . .

# Build the runtime
WORKDIR /app/runtime
RUN cargo build --release

# Runtime stage - use same base as rust:latest (Debian Trixie)
FROM debian:trixie

# Install CA certificates for HTTPS and other runtime dependencies
RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder
COPY --from=builder /app/runtime/target/release/pulse-runtime /usr/local/bin/pulse-runtime

# Expose ports
EXPOSE 50051 9090

# Set default environment variables
ENV RUST_LOG=info
ENV GRPC_PORT=50051
ENV METRICS_PORT=9090
ENV S3_BUCKET=checkpoints
ENV S3_REGION=us-east-1

# Run the runtime
CMD ["pulse-runtime"]
