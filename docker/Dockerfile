# Multi-stage build. The runtime image has no shell and runs as the distroless nonroot user.
FROM rust:1.99-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY proto ./proto
COPY runtime ./runtime

WORKDIR /src/runtime
RUN cargo build --release --locked

FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=builder --chown=nonroot:nonroot /src/runtime/target/release/pulse-runtime /usr/local/bin/pulse-runtime

USER nonroot:nonroot
EXPOSE 50051 9090
ENV PULSE_GRPC_ADDR=0.0.0.0:50051 \
    PULSE_METRICS_ADDR=0.0.0.0:9090

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD ["/usr/local/bin/pulse-runtime", "--healthcheck"]

ENTRYPOINT ["/usr/local/bin/pulse-runtime"]
