#!/usr/bin/env python3
"""
PulseCheckpoint Runtime - Quick Test Script

Run this in your pulsecheckpoint-runtime directory after docker-compose up:
    pip install grpcio grpcio-tools protobuf
    python test_runtime.py

Or if you have the SDK installed:
    cd sdk && pip install -e . && cd ..
    python test_runtime.py
"""

import sys
import uuid
from datetime import datetime

def test_grpc_connectivity():
    """Test basic gRPC connectivity to the runtime."""
    import grpc
    
    print("=" * 60)
    print("PulseCheckpoint Runtime - Connectivity Test")
    print("=" * 60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Test gRPC connection
    print("1. Testing gRPC connection to localhost:50051...")
    channel = grpc.insecure_channel('localhost:50051')
    
    try:
        grpc.channel_ready_future(channel).result(timeout=5)
        print("   ✅ Connected successfully!")
        connected = True
    except grpc.FutureTimeoutError:
        print("   ❌ Connection timeout")
        print("   → Is docker-compose running?")
        connected = False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        connected = False
    
    if not connected:
        channel.close()
        return False
    
    # Try gRPC reflection to discover services
    print("\n2. Discovering gRPC services...")
    try:
        from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc
        stub = reflection_pb2_grpc.ServerReflectionStub(channel)
        
        def make_request():
            yield reflection_pb2.ServerReflectionRequest(list_services="")
        
        responses = list(stub.ServerReflectionInfo(make_request()))
        for resp in responses:
            if resp.HasField('list_services_response'):
                services = resp.list_services_response.service
                print(f"   ✅ Found {len(services)} service(s):")
                for svc in services:
                    print(f"      • {svc.name}")
    except ImportError:
        print("   ⚠️  grpcio-reflection not installed")
        print("   → pip install grpcio-reflection")
    except grpc.RpcError as e:
        print(f"   ⚠️  Reflection not enabled on server")
    except Exception as e:
        print(f"   ⚠️  {type(e).__name__}: {e}")
    
    channel.close()
    return True


def test_http_endpoints():
    """Test HTTP endpoints (metrics, MinIO, Grafana, Prometheus)."""
    import urllib.request
    import urllib.error
    
    print("\n3. Testing HTTP endpoints...")
    
    endpoints = [
        ("MinIO Console", "http://localhost:9001", "MinIO"),
        ("MinIO API", "http://localhost:9000/minio/health/live", None),
        ("Prometheus", "http://localhost:9092", "Prometheus"),
        ("Grafana", "http://localhost:3001", "Grafana"),
        ("Runtime Metrics", "http://localhost:9091/metrics", "pulse_"),
    ]
    
    for name, url, check_text in endpoints:
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'pulse-test'})
            with urllib.request.urlopen(req, timeout=3) as resp:
                data = resp.read().decode('utf-8', errors='ignore')[:500]
                if check_text and check_text not in data:
                    print(f"   ⚠️  {name}: {url} - unexpected response")
                else:
                    print(f"   ✅ {name}: {url}")
        except urllib.error.HTTPError as e:
            if e.code == 403:
                print(f"   ✅ {name}: {url} (auth required)")
            else:
                print(f"   ⚠️  {name}: {url} - HTTP {e.code}")
        except urllib.error.URLError as e:
            print(f"   ❌ {name}: {url} - {e.reason}")
        except Exception as e:
            print(f"   ❌ {name}: {url} - {e}")


def test_with_sdk():
    """Test using the Python SDK if available."""
    print("\n4. Testing Python SDK...")
    
    try:
        # Try importing the SDK
        sys.path.insert(0, 'sdk')
        from pulse import Runtime, PulseError
        print("   ✅ SDK imported successfully")
    except ImportError as e:
        print(f"   ⚠️  SDK not found: {e}")
        print("   → cd sdk && pip install -e .")
        return False
    
    test_id = str(uuid.uuid4())[:8]
    worker_id = f"test-worker-{test_id}"
    
    try:
        # Connect
        rt = Runtime("localhost:50051")
        print("   ✅ Runtime client connected")
        
        # Register worker
        rt.register_worker(worker_id, metadata={"test": "true"})
        print(f"   ✅ Registered worker: {worker_id}")
        
        # Heartbeat
        rt.heartbeat(worker_id)
        print("   ✅ Heartbeat sent")
        
        # Save checkpoint
        test_data = f"checkpoint-{test_id}".encode()
        cp_id = rt.save_checkpoint(worker_id, test_data, metadata={"epoch": "1"})
        print(f"   ✅ Saved checkpoint: {cp_id}")
        
        # Load checkpoint
        loaded = rt.load_checkpoint(cp_id)
        if loaded == test_data:
            print("   ✅ Loaded checkpoint - data verified!")
        else:
            print("   ⚠️  Loaded checkpoint - data mismatch")
        
        # List checkpoints
        checkpoints = rt.list_checkpoints(worker_id)
        print(f"   ✅ Listed {len(checkpoints)} checkpoint(s)")
        
        # Cleanup
        rt.deregister_worker(worker_id)
        print(f"   ✅ Deregistered worker")
        
        rt.close()
        print("   ✅ Connection closed")
        
        return True
        
    except PulseError as e:
        print(f"   ❌ SDK Error: {e}")
        return False
    except Exception as e:
        print(f"   ❌ Error: {type(e).__name__}: {e}")
        return False


def main():
    print("""
╔══════════════════════════════════════════════════════════╗
║         PulseCheckpoint Runtime - Test Suite             ║
╚══════════════════════════════════════════════════════════╝
""")
    
    # Check gRPC connectivity
    grpc_ok = test_grpc_connectivity()
    
    # Check HTTP endpoints
    test_http_endpoints()
    
    # Test SDK if gRPC is working
    if grpc_ok:
        test_with_sdk()
    else:
        print("\n4. Skipping SDK test (gRPC not available)")
    
    print("\n" + "=" * 60)
    print("Test complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
