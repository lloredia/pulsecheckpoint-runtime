//! PulseCheckpoint runtime.
//!
//! One process keeps the worker, dataset, and checkpoint index in memory and
//! writes checkpoint bytes to an S3-compatible backend. Restarting the process
//! drops that index even if the objects are still in the bucket.

pub mod api;
pub mod checkpoint;
pub mod config;
pub mod metrics;
pub mod storage;
pub mod worker;

/// Generated gRPC types for `proto/pulse.proto`.
#[allow(clippy::all, dead_code, unused_imports)]
pub mod pulse {
    pub mod v1 {
        tonic::include_proto!("pulse.v1");

        pub const FILE_DESCRIPTOR_SET: &[u8] =
            tonic::include_file_descriptor_set!("pulse_descriptor");
    }
}
