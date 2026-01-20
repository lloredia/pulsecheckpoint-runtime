"""
PulseCheckpoint Runtime client implementation.

This module provides the main Runtime class for interacting with the
PulseCheckpoint gRPC service.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.parse import urlparse

import grpc

# Import generated protobuf code
from pulse.generated import pulse_pb2, pulse_pb2_grpc

logger = logging.getLogger(__name__)


class PulseError(Exception):
    """Base exception for all Pulse SDK errors."""

    pass


class ConnectionError(PulseError):
    """Raised when connection to the runtime fails."""

    pass


class WorkerError(PulseError):
    """Raised for worker-related errors."""

    pass


class CheckpointError(PulseError):
    """Raised for checkpoint-related errors."""

    pass


@dataclass
class WorkerInfo:
    """Information about a registered worker."""

    worker_id: str
    metadata: dict[str, str]
    status: str
    registered_at: float
    last_heartbeat: float


@dataclass
class DatasetInfo:
    """Information about a registered dataset."""

    dataset_id: str
    path: str
    metadata: dict[str, str]
    registered_at: float


@dataclass
class CheckpointInfo:
    """Information about a saved checkpoint."""

    checkpoint_id: str
    worker_id: str
    storage_path: str
    size_bytes: int
    checksum: str
    metadata: dict[str, str]
    created_at: float
    status: str


@dataclass
class RuntimeConfig:
    """Configuration for the Runtime client."""

    address: str = "localhost:50051"
    timeout: float = 30.0
    max_retries: int = 3
    retry_delay: float = 0.5
    heartbeat_interval: float = 30.0
    use_tls: bool = False
    tls_cert_path: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)


class Runtime:
    """
    Client for the PulseCheckpoint Runtime service.

    Provides methods for worker registration, dataset management,
    and checkpoint persistence.

    Example:
        >>> rt = Runtime("grpc://localhost:50051")
        >>> rt.register_worker("worker-1", metadata={"gpu": "A100"})
        >>> checkpoint_id = rt.save_checkpoint("worker-1", b"model-state")
        >>> rt.deregister_worker("worker-1")
    """

    def __init__(
        self,
        address: str = "grpc://localhost:50051",
        *,
        timeout: float = 30.0,
        max_retries: int = 3,
        use_tls: bool = False,
        tls_cert_path: str | None = None,
    ):
        """
        Initialize the Runtime client.

        Args:
            address: gRPC server address (e.g., "grpc://localhost:50051")
            timeout: Default timeout for RPC calls in seconds
            max_retries: Maximum number of retry attempts
            use_tls: Whether to use TLS for the connection
            tls_cert_path: Path to TLS certificate (if using TLS)
        """
        self._config = RuntimeConfig(
            address=self._parse_address(address),
            timeout=timeout,
            max_retries=max_retries,
            use_tls=use_tls,
            tls_cert_path=tls_cert_path,
        )
        self._channel: grpc.Channel | None = None
        self._stub: pulse_pb2_grpc.PulseServiceStub | None = None
        self._heartbeat_threads: dict[str, threading.Thread] = {}
        self._heartbeat_stop_events: dict[str, threading.Event] = {}
        self._lock = threading.Lock()

        self._connect()

    def _parse_address(self, address: str) -> str:
        """Parse the address string and extract host:port."""
        if address.startswith("grpc://"):
            parsed = urlparse(address)
            return f"{parsed.hostname}:{parsed.port or 50051}"
        return address

    def _connect(self) -> None:
        """Establish connection to the gRPC server."""
        try:
            if self._config.use_tls:
                if self._config.tls_cert_path:
                    with open(self._config.tls_cert_path, "rb") as f:
                        credentials = grpc.ssl_channel_credentials(f.read())
                else:
                    credentials = grpc.ssl_channel_credentials()
                self._channel = grpc.secure_channel(
                    self._config.address, credentials
                )
            else:
                self._channel = grpc.insecure_channel(self._config.address)

            self._stub = pulse_pb2_grpc.PulseServiceStub(self._channel)
            logger.info(f"Connected to PulseCheckpoint Runtime at {self._config.address}")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to {self._config.address}: {e}") from e

    def _retry_call(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute a gRPC call with retry logic."""
        last_error: Exception | None = None
        
        for attempt in range(self._config.max_retries):
            try:
                return func(*args, timeout=self._config.timeout, **kwargs)
            except grpc.RpcError as e:
                last_error = e
                status_code = e.code()
                
                # Don't retry on certain errors
                if status_code in (
                    grpc.StatusCode.INVALID_ARGUMENT,
                    grpc.StatusCode.NOT_FOUND,
                    grpc.StatusCode.ALREADY_EXISTS,
                    grpc.StatusCode.PERMISSION_DENIED,
                    grpc.StatusCode.UNAUTHENTICATED,
                ):
                    raise
                
                if attempt < self._config.max_retries - 1:
                    delay = self._config.retry_delay * (2 ** attempt)
                    logger.warning(
                        f"RPC call failed (attempt {attempt + 1}/{self._config.max_retries}), "
                        f"retrying in {delay}s: {e}"
                    )
                    time.sleep(delay)
        
        raise last_error or PulseError("Unknown error during retry")

    def close(self) -> None:
        """Close the connection and stop all heartbeat threads."""
        # Stop all heartbeat threads
        for worker_id in list(self._heartbeat_stop_events.keys()):
            self._stop_heartbeat(worker_id)
        
        if self._channel:
            self._channel.close()
            self._channel = None
            self._stub = None
            logger.info("Disconnected from PulseCheckpoint Runtime")

    def __enter__(self) -> Runtime:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    # =========================================================================
    # Worker Management
    # =========================================================================

    def register_worker(
        self,
        worker_id: str,
        *,
        metadata: dict[str, str] | None = None,
        auto_heartbeat: bool = True,
    ) -> WorkerInfo:
        """
        Register a worker with the runtime.

        Args:
            worker_id: Unique identifier for the worker
            metadata: Optional metadata labels for the worker
            auto_heartbeat: Whether to start automatic heartbeat thread

        Returns:
            WorkerInfo with registration details

        Raises:
            WorkerError: If registration fails or worker already exists
        """
        if not self._stub:
            raise ConnectionError("Not connected to runtime")

        request = pulse_pb2.RegisterWorkerRequest(
            worker_id=worker_id,
            metadata=pulse_pb2.Metadata(labels=metadata or {}),
        )

        try:
            response = self._retry_call(self._stub.RegisterWorker, request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.ALREADY_EXISTS:
                raise WorkerError(f"Worker {worker_id} already exists") from e
            raise WorkerError(f"Failed to register worker: {e.details()}") from e

        if not response.success:
            raise WorkerError(f"Failed to register worker: {response.message}")

        worker_info = self._proto_to_worker_info(response.worker)
        logger.info(f"Registered worker: {worker_id}")

        if auto_heartbeat:
            self._start_heartbeat(worker_id)

        return worker_info

    def deregister_worker(self, worker_id: str) -> None:
        """
        Deregister a worker from the runtime.

        Args:
            worker_id: ID of the worker to deregister

        Raises:
            WorkerError: If deregistration fails
        """
        if not self._stub:
            raise ConnectionError("Not connected to runtime")

        # Stop heartbeat first
        self._stop_heartbeat(worker_id)

        request = pulse_pb2.DeregisterWorkerRequest(worker_id=worker_id)

        try:
            response = self._retry_call(self._stub.DeregisterWorker, request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                raise WorkerError(f"Worker {worker_id} not found") from e
            raise WorkerError(f"Failed to deregister worker: {e.details()}") from e

        if not response.success:
            raise WorkerError(f"Failed to deregister worker: {response.message}")

        logger.info(f"Deregistered worker: {worker_id}")

    def heartbeat(self, worker_id: str, status: str = "ACTIVE") -> None:
        """
        Send a heartbeat for a worker.

        Args:
            worker_id: ID of the worker
            status: Worker status (ACTIVE, IDLE, etc.)
        """
        if not self._stub:
            raise ConnectionError("Not connected to runtime")

        status_map = {
            "ACTIVE": pulse_pb2.WORKER_STATUS_ACTIVE,
            "IDLE": pulse_pb2.WORKER_STATUS_IDLE,
            "UNHEALTHY": pulse_pb2.WORKER_STATUS_UNHEALTHY,
        }

        request = pulse_pb2.HeartbeatRequest(
            worker_id=worker_id,
            status=status_map.get(status.upper(), pulse_pb2.WORKER_STATUS_ACTIVE),
        )

        try:
            self._stub.Heartbeat(request, timeout=self._config.timeout)
        except grpc.RpcError as e:
            logger.warning(f"Heartbeat failed for worker {worker_id}: {e}")

    def _start_heartbeat(self, worker_id: str) -> None:
        """Start automatic heartbeat thread for a worker."""
        with self._lock:
            if worker_id in self._heartbeat_threads:
                return

            stop_event = threading.Event()
            self._heartbeat_stop_events[worker_id] = stop_event

            def heartbeat_loop() -> None:
                while not stop_event.wait(self._config.heartbeat_interval):
                    try:
                        self.heartbeat(worker_id)
                    except Exception as e:
                        logger.error(f"Heartbeat error for {worker_id}: {e}")

            thread = threading.Thread(
                target=heartbeat_loop,
                name=f"heartbeat-{worker_id}",
                daemon=True,
            )
            thread.start()
            self._heartbeat_threads[worker_id] = thread
            logger.debug(f"Started heartbeat thread for worker {worker_id}")

    def _stop_heartbeat(self, worker_id: str) -> None:
        """Stop automatic heartbeat thread for a worker."""
        with self._lock:
            if worker_id in self._heartbeat_stop_events:
                self._heartbeat_stop_events[worker_id].set()
                del self._heartbeat_stop_events[worker_id]
            
            if worker_id in self._heartbeat_threads:
                thread = self._heartbeat_threads[worker_id]
                thread.join(timeout=2.0)
                del self._heartbeat_threads[worker_id]
                logger.debug(f"Stopped heartbeat thread for worker {worker_id}")

    def list_workers(self, status_filter: str | None = None) -> list[WorkerInfo]:
        """
        List all registered workers.

        Args:
            status_filter: Optional status filter (ACTIVE, IDLE, UNHEALTHY)

        Returns:
            List of WorkerInfo objects
        """
        if not self._stub:
            raise ConnectionError("Not connected to runtime")

        status_map = {
            "ACTIVE": pulse_pb2.WORKER_STATUS_ACTIVE,
            "IDLE": pulse_pb2.WORKER_STATUS_IDLE,
            "UNHEALTHY": pulse_pb2.WORKER_STATUS_UNHEALTHY,
        }

        request = pulse_pb2.ListWorkersRequest(
            status_filter=status_map.get(
                (status_filter or "").upper(),
                pulse_pb2.WORKER_STATUS_UNSPECIFIED,
            ),
        )

        response = self._retry_call(self._stub.ListWorkers, request)
        return [self._proto_to_worker_info(w) for w in response.workers]

    # =========================================================================
    # Dataset Management
    # =========================================================================

    def register_dataset(
        self,
        dataset_id: str,
        path: str,
        *,
        metadata: dict[str, str] | None = None,
    ) -> DatasetInfo:
        """
        Register a dataset with the runtime.

        Args:
            dataset_id: Unique identifier for the dataset
            path: Storage path for the dataset
            metadata: Optional metadata labels

        Returns:
            DatasetInfo with registration details
        """
        if not self._stub:
            raise ConnectionError("Not connected to runtime")

        request = pulse_pb2.RegisterDatasetRequest(
            dataset_id=dataset_id,
            path=path,
            metadata=pulse_pb2.Metadata(labels=metadata or {}),
        )

        response = self._retry_call(self._stub.RegisterDataset, request)

        if not response.success:
            raise PulseError(f"Failed to register dataset: {response.message}")

        logger.info(f"Registered dataset: {dataset_id}")
        return self._proto_to_dataset_info(response.dataset)

    def list_datasets(self) -> list[DatasetInfo]:
        """
        List all registered datasets.

        Returns:
            List of DatasetInfo objects
        """
        if not self._stub:
            raise ConnectionError("Not connected to runtime")

        request = pulse_pb2.ListDatasetsRequest()
        response = self._retry_call(self._stub.ListDatasets, request)
        return [self._proto_to_dataset_info(d) for d in response.datasets]

    # =========================================================================
    # Checkpoint Management
    # =========================================================================

    def save_checkpoint(
        self,
        worker_id: str,
        data: bytes,
        *,
        metadata: dict[str, str] | None = None,
        idempotency_key: str | None = None,
    ) -> str:
        """
        Save a checkpoint for a worker.

        Args:
            worker_id: ID of the worker saving the checkpoint
            data: Checkpoint data bytes
            metadata: Optional metadata labels
            idempotency_key: Optional key for idempotent writes

        Returns:
            Checkpoint ID

        Raises:
            CheckpointError: If checkpoint save fails
        """
        if not self._stub:
            raise ConnectionError("Not connected to runtime")

        request = pulse_pb2.SaveCheckpointRequest(
            worker_id=worker_id,
            data=data,
            metadata=pulse_pb2.Metadata(labels=metadata or {}),
            idempotency_key=idempotency_key or str(uuid.uuid4()),
        )

        try:
            response = self._retry_call(self._stub.SaveCheckpoint, request)
        except grpc.RpcError as e:
            raise CheckpointError(f"Failed to save checkpoint: {e.details()}") from e

        if not response.success:
            raise CheckpointError(f"Failed to save checkpoint: {response.message}")

        checkpoint_id = response.checkpoint.checkpoint_id
        logger.info(
            f"Saved checkpoint {checkpoint_id} for worker {worker_id} "
            f"({len(data)} bytes)"
        )
        return checkpoint_id

    def get_checkpoint(
        self,
        checkpoint_id: str,
        *,
        include_data: bool = False,
    ) -> CheckpointInfo:
        """
        Get checkpoint information.

        Args:
            checkpoint_id: ID of the checkpoint
            include_data: Whether to include checkpoint data

        Returns:
            CheckpointInfo object
        """
        if not self._stub:
            raise ConnectionError("Not connected to runtime")

        request = pulse_pb2.GetCheckpointRequest(
            checkpoint_id=checkpoint_id,
            include_data=include_data,
        )

        try:
            response = self._retry_call(self._stub.GetCheckpoint, request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                raise CheckpointError(f"Checkpoint {checkpoint_id} not found") from e
            raise

        return self._proto_to_checkpoint_info(response.checkpoint)

    def get_checkpoint_data(self, checkpoint_id: str) -> bytes:
        """
        Download checkpoint data.

        Args:
            checkpoint_id: ID of the checkpoint

        Returns:
            Checkpoint data bytes
        """
        if not self._stub:
            raise ConnectionError("Not connected to runtime")

        request = pulse_pb2.GetCheckpointRequest(
            checkpoint_id=checkpoint_id,
            include_data=True,
        )

        try:
            response = self._retry_call(self._stub.GetCheckpoint, request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                raise CheckpointError(f"Checkpoint {checkpoint_id} not found") from e
            raise

        return response.data

    def list_checkpoints(
        self,
        worker_id: str | None = None,
        status_filter: str | None = None,
    ) -> list[CheckpointInfo]:
        """
        List checkpoints.

        Args:
            worker_id: Optional filter by worker ID
            status_filter: Optional status filter

        Returns:
            List of CheckpointInfo objects
        """
        if not self._stub:
            raise ConnectionError("Not connected to runtime")

        status_map = {
            "PENDING": pulse_pb2.CHECKPOINT_STATUS_PENDING,
            "UPLOADING": pulse_pb2.CHECKPOINT_STATUS_UPLOADING,
            "COMPLETED": pulse_pb2.CHECKPOINT_STATUS_COMPLETED,
            "FAILED": pulse_pb2.CHECKPOINT_STATUS_FAILED,
        }

        request = pulse_pb2.ListCheckpointsRequest(
            worker_id=worker_id or "",
            status_filter=status_map.get(
                (status_filter or "").upper(),
                pulse_pb2.CHECKPOINT_STATUS_UNSPECIFIED,
            ),
        )

        response = self._retry_call(self._stub.ListCheckpoints, request)
        return [self._proto_to_checkpoint_info(c) for c in response.checkpoints]

    def delete_checkpoint(self, checkpoint_id: str) -> None:
        """
        Delete a checkpoint.

        Args:
            checkpoint_id: ID of the checkpoint to delete
        """
        if not self._stub:
            raise ConnectionError("Not connected to runtime")

        request = pulse_pb2.DeleteCheckpointRequest(checkpoint_id=checkpoint_id)

        try:
            response = self._retry_call(self._stub.DeleteCheckpoint, request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                raise CheckpointError(f"Checkpoint {checkpoint_id} not found") from e
            raise

        if not response.success:
            raise CheckpointError(f"Failed to delete checkpoint: {response.message}")

        logger.info(f"Deleted checkpoint: {checkpoint_id}")

    # =========================================================================
    # Health Check
    # =========================================================================

    def health_check(self) -> dict[str, Any]:
        """
        Check the health of the runtime service.

        Returns:
            Dictionary with health status information
        """
        if not self._stub:
            raise ConnectionError("Not connected to runtime")

        request = pulse_pb2.HealthCheckRequest()
        response = self._retry_call(self._stub.HealthCheck, request)

        status_map = {
            pulse_pb2.SERVICE_STATUS_HEALTHY: "HEALTHY",
            pulse_pb2.SERVICE_STATUS_DEGRADED: "DEGRADED",
            pulse_pb2.SERVICE_STATUS_UNHEALTHY: "UNHEALTHY",
        }

        return {
            "status": status_map.get(response.status, "UNKNOWN"),
            "version": response.version,
            "uptime_seconds": response.uptime.seconds if response.uptime else 0,
            "components": {
                name: {
                    "status": status_map.get(comp.status, "UNKNOWN"),
                    "message": comp.message,
                }
                for name, comp in response.components.items()
            },
        }

    # =========================================================================
    # Helper Methods
    # =========================================================================

    @staticmethod
    def _proto_to_worker_info(proto: pulse_pb2.WorkerInfo) -> WorkerInfo:
        """Convert protobuf WorkerInfo to dataclass."""
        status_map = {
            pulse_pb2.WORKER_STATUS_ACTIVE: "ACTIVE",
            pulse_pb2.WORKER_STATUS_IDLE: "IDLE",
            pulse_pb2.WORKER_STATUS_UNHEALTHY: "UNHEALTHY",
            pulse_pb2.WORKER_STATUS_TERMINATED: "TERMINATED",
        }
        return WorkerInfo(
            worker_id=proto.worker_id,
            metadata=dict(proto.metadata.labels) if proto.metadata else {},
            status=status_map.get(proto.status, "UNKNOWN"),
            registered_at=proto.registered_at.seconds if proto.registered_at else 0,
            last_heartbeat=proto.last_heartbeat.seconds if proto.last_heartbeat else 0,
        )

    @staticmethod
    def _proto_to_dataset_info(proto: pulse_pb2.DatasetInfo) -> DatasetInfo:
        """Convert protobuf DatasetInfo to dataclass."""
        return DatasetInfo(
            dataset_id=proto.dataset_id,
            path=proto.path,
            metadata=dict(proto.metadata.labels) if proto.metadata else {},
            registered_at=proto.registered_at.seconds if proto.registered_at else 0,
        )

    @staticmethod
    def _proto_to_checkpoint_info(proto: pulse_pb2.CheckpointInfo) -> CheckpointInfo:
        """Convert protobuf CheckpointInfo to dataclass."""
        status_map = {
            pulse_pb2.CHECKPOINT_STATUS_PENDING: "PENDING",
            pulse_pb2.CHECKPOINT_STATUS_UPLOADING: "UPLOADING",
            pulse_pb2.CHECKPOINT_STATUS_COMPLETED: "COMPLETED",
            pulse_pb2.CHECKPOINT_STATUS_FAILED: "FAILED",
        }
        return CheckpointInfo(
            checkpoint_id=proto.checkpoint_id,
            worker_id=proto.worker_id,
            storage_path=proto.storage_path,
            size_bytes=proto.size_bytes,
            checksum=proto.checksum,
            metadata=dict(proto.metadata.labels) if proto.metadata else {},
            created_at=proto.created_at.seconds if proto.created_at else 0,
            status=status_map.get(proto.status, "UNKNOWN"),
        )
