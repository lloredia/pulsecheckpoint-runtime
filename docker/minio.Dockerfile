# Community MinIO is source-only. This image builds the pinned upstream release.
FROM golang:1.27-bookworm AS build

RUN go install github.com/minio/minio@RELEASE.2025-10-15T17-29-55Z

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 65532 --create-home --shell /usr/sbin/nologin minio \
    && mkdir -p /data \
    && chown minio:minio /data

COPY --from=build /go/bin/minio /usr/local/bin/minio

USER minio
EXPOSE 9000 9001
VOLUME ["/data"]
HEALTHCHECK --interval=5s --timeout=5s --start-period=5s --retries=20 \
    CMD ["curl", "-fsS", "http://127.0.0.1:9000/minio/health/live"]
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
