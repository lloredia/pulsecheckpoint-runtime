.PHONY: help dev-up dev-down dev-logs build run test fmt clippy audit generate-proto run-example docker-build clean

PYTHON ?= python3
export PYTHONPATH := $(CURDIR)/sdk$(if $(PYTHONPATH),:$(PYTHONPATH),)

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "%-18s %s\n", $$1, $$2}'

dev-up: ## Start MinIO, the runtime, Prometheus, and Grafana
	docker compose up -d --build

dev-down: ## Stop the local stack
	docker compose down

dev-logs: ## Follow local stack logs
	docker compose logs -f

build: ## Build the runtime binary
	cargo build --manifest-path runtime/Cargo.toml --locked

run: ## Run the runtime against env or config.toml
	cargo run --manifest-path runtime/Cargo.toml --locked

test: ## Run unit tests and the MinIO test when PULSE_S3_ENDPOINT is set
	cargo test --manifest-path runtime/Cargo.toml --locked --all-targets

fmt: ## Check rustfmt
	cargo fmt --manifest-path runtime/Cargo.toml --all -- --check

clippy: ## Run clippy with warnings denied
	cargo clippy --manifest-path runtime/Cargo.toml --locked --all-targets --all-features -- -D warnings

audit: ## Run cargo audit and cargo deny
	cargo audit --file runtime/Cargo.lock
	cargo deny --manifest-path runtime/Cargo.toml check

generate-proto: ## Regenerate Python stubs from proto/pulse.proto
	$(PYTHON) -m grpc_tools.protoc \
		-I proto \
		--python_out=sdk/pulse/generated \
		--grpc_python_out=sdk/pulse/generated \
		proto/pulse.proto
	$(PYTHON) scripts/fix_grpc_imports.py

run-example: ## Call the gRPC API with examples/basic_usage.py
	$(PYTHON) examples/basic_usage.py

docker-build: ## Build the runtime image
	docker build -f docker/Dockerfile -t pulse-runtime:local .

clean: ## Remove build output
	cargo clean --manifest-path runtime/Cargo.toml
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
