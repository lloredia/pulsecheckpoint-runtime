.PHONY: all build test clean dev-up dev-down help

# Default target
all: build

# ==============================================================================
# Development Environment
# ==============================================================================

dev-up: ## Start local development stack (MinIO, Prometheus, Grafana)
	docker-compose up -d
	@echo "MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
	@echo "Prometheus: http://localhost:9091"
	@echo "Grafana: http://localhost:3000 (admin/admin)"

dev-down: ## Stop local development stack
	docker-compose down

dev-logs: ## View development stack logs
	docker-compose logs -f

# ==============================================================================
# Rust Runtime
# ==============================================================================

build-runtime: ## Build Rust runtime
	cd runtime && cargo build --release

run-runtime: ## Run Rust runtime locally
	cd runtime && PULSE_S3_ENDPOINT=http://localhost:9000 \
		AWS_ACCESS_KEY_ID=minioadmin \
		AWS_SECRET_ACCESS_KEY=minioadmin \
		cargo run --release

test-runtime: ## Run Rust tests
	cd runtime && cargo test

lint-runtime: ## Lint Rust code
	cd runtime && cargo fmt --check && cargo clippy -- -D warnings

fmt-runtime: ## Format Rust code
	cd runtime && cargo fmt

# ==============================================================================
# Python SDK
# ==============================================================================

install-sdk: ## Install Python SDK in development mode
	pip install -e sdk[dev]

generate-proto: ## Generate Python protobuf code
	python -m grpc_tools.protoc \
		-I proto \
		--python_out=sdk/pulse/generated \
		--grpc_python_out=sdk/pulse/generated \
		proto/pulse.proto

test-sdk: ## Run Python SDK tests
	cd sdk && pytest tests/ -v

lint-sdk: ## Lint Python code
	cd sdk && ruff check pulse/ && black --check pulse/

fmt-sdk: ## Format Python code
	cd sdk && black pulse/

# ==============================================================================
# Docker
# ==============================================================================

docker-build: ## Build Docker image
	docker build -f docker/Dockerfile.runtime -t pulsecheckpoint-runtime:latest .

docker-run: ## Run Docker container
	docker run -p 50051:50051 -p 9090:9090 \
		-e PULSE_S3_ENDPOINT=http://host.docker.internal:9000 \
		-e AWS_ACCESS_KEY_ID=minioadmin \
		-e AWS_SECRET_ACCESS_KEY=minioadmin \
		pulsecheckpoint-runtime:latest

# ==============================================================================
# Testing
# ==============================================================================

test: test-runtime test-sdk ## Run all tests

test-integration: ## Run integration tests
	@echo "Running integration tests..."
	cd examples && python basic_usage.py

# ==============================================================================
# Infrastructure
# ==============================================================================

tf-init: ## Initialize Terraform
	cd terraform/environments/dev && terraform init

tf-plan: ## Plan Terraform changes
	cd terraform/environments/dev && terraform plan

tf-apply: ## Apply Terraform changes
	cd terraform/environments/dev && terraform apply

tf-destroy: ## Destroy Terraform infrastructure
	cd terraform/environments/dev && terraform destroy

# ==============================================================================
# Examples
# ==============================================================================

run-example: ## Run example Python script
	python examples/basic_usage.py

# ==============================================================================
# Cleanup
# ==============================================================================

clean: ## Clean build artifacts
	cd runtime && cargo clean
	rm -rf sdk/pulse/generated/*.py
	rm -rf sdk/*.egg-info
	rm -rf sdk/dist
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# ==============================================================================
# Help
# ==============================================================================

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
