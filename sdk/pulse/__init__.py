"""
PulseCheckpoint SDK - Python client for the PulseCheckpoint Runtime.

Example usage:
    from pulse import Runtime

    rt = Runtime("grpc://localhost:50051")
    rt.register_worker("worker-1", metadata={"gpu": "A100"})
    rt.register_dataset("training-data", path="s3://bucket/data")
    checkpoint_id = rt.save_checkpoint("worker-1", b"model-state")
"""

from pulse.runtime import Runtime, PulseError, ConnectionError, WorkerError, CheckpointError

__version__ = "0.1.0"
__all__ = [
    "Runtime",
    "PulseError",
    "ConnectionError",
    "WorkerError",
    "CheckpointError",
]
