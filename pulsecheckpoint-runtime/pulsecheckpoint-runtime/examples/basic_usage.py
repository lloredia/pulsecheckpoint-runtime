#!/usr/bin/env python3
"""
PulseCheckpoint SDK - Basic Usage Example

This example demonstrates how to use the PulseCheckpoint SDK to:
1. Connect to the runtime
2. Register workers
3. Register datasets
4. Save and retrieve checkpoints

Prerequisites:
    - PulseCheckpoint runtime running (make run-runtime)
    - MinIO running (make dev-up)
    - SDK installed (make install-sdk)

Usage:
    python examples/basic_usage.py
"""

import time
import json
from pulse import Runtime, PulseError

def main():
    print("=" * 60)
    print("PulseCheckpoint SDK - Basic Usage Example")
    print("=" * 60)

    # Connect to the runtime
    print("\n1. Connecting to runtime...")
    try:
        rt = Runtime("grpc://localhost:50051", timeout=10.0)
        print("   ✓ Connected successfully")
    except Exception as e:
        print(f"   ✗ Failed to connect: {e}")
        print("   Make sure the runtime is running (make run-runtime)")
        return

    # Health check
    print("\n2. Health check...")
    try:
        health = rt.health_check()
        print(f"   ✓ Status: {health['status']}")
        print(f"   ✓ Version: {health['version']}")
        print(f"   ✓ Uptime: {health['uptime_seconds']}s")
    except Exception as e:
        print(f"   ✗ Health check failed: {e}")

    # Register a worker
    print("\n3. Registering worker...")
    worker_id = "example-worker-1"
    try:
        worker = rt.register_worker(
            worker_id,
            metadata={
                "gpu": "NVIDIA A100",
                "memory": "80GB",
                "framework": "pytorch",
            },
            auto_heartbeat=True,
        )
        print(f"   ✓ Registered worker: {worker.worker_id}")
        print(f"   ✓ Status: {worker.status}")
    except PulseError as e:
        print(f"   ✗ Failed to register worker: {e}")
        # Worker might already exist, continue anyway
        pass

    # Register a dataset
    print("\n4. Registering dataset...")
    try:
        dataset = rt.register_dataset(
            "training-data-v1",
            path="s3://my-bucket/datasets/training",
            metadata={
                "version": "1.0",
                "samples": "1000000",
                "format": "parquet",
            },
        )
        print(f"   ✓ Registered dataset: {dataset.dataset_id}")
        print(f"   ✓ Path: {dataset.path}")
    except PulseError as e:
        print(f"   ✗ Failed to register dataset: {e}")

    # Save a checkpoint
    print("\n5. Saving checkpoint...")
    try:
        # Simulate model state as JSON
        model_state = {
            "epoch": 10,
            "loss": 0.0523,
            "accuracy": 0.9847,
            "learning_rate": 0.001,
            "weights_shape": [1024, 512, 256],
        }
        checkpoint_data = json.dumps(model_state).encode("utf-8")
        
        checkpoint_id = rt.save_checkpoint(
            worker_id,
            checkpoint_data,
            metadata={
                "epoch": "10",
                "type": "full",
                "framework": "pytorch",
            },
            idempotency_key=f"epoch-10-{worker_id}",
        )
        print(f"   ✓ Saved checkpoint: {checkpoint_id}")
        print(f"   ✓ Size: {len(checkpoint_data)} bytes")
    except PulseError as e:
        print(f"   ✗ Failed to save checkpoint: {e}")
        checkpoint_id = None

    # List checkpoints
    print("\n6. Listing checkpoints...")
    try:
        checkpoints = rt.list_checkpoints(worker_id=worker_id)
        print(f"   ✓ Found {len(checkpoints)} checkpoint(s)")
        for cp in checkpoints[:3]:  # Show first 3
            print(f"      - {cp.checkpoint_id}: {cp.size_bytes} bytes ({cp.status})")
    except PulseError as e:
        print(f"   ✗ Failed to list checkpoints: {e}")

    # Retrieve checkpoint data
    if checkpoint_id:
        print("\n7. Retrieving checkpoint data...")
        try:
            data = rt.get_checkpoint_data(checkpoint_id)
            retrieved_state = json.loads(data.decode("utf-8"))
            print(f"   ✓ Retrieved checkpoint data:")
            print(f"      - Epoch: {retrieved_state['epoch']}")
            print(f"      - Loss: {retrieved_state['loss']}")
            print(f"      - Accuracy: {retrieved_state['accuracy']}")
        except PulseError as e:
            print(f"   ✗ Failed to retrieve checkpoint: {e}")

    # List workers
    print("\n8. Listing workers...")
    try:
        workers = rt.list_workers()
        print(f"   ✓ Found {len(workers)} worker(s)")
        for w in workers:
            print(f"      - {w.worker_id}: {w.status}")
    except PulseError as e:
        print(f"   ✗ Failed to list workers: {e}")

    # Cleanup
    print("\n9. Cleaning up...")
    try:
        rt.deregister_worker(worker_id)
        print(f"   ✓ Deregistered worker: {worker_id}")
    except PulseError as e:
        print(f"   ✗ Failed to deregister worker: {e}")

    # Close connection
    rt.close()
    print("\n   ✓ Connection closed")

    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
