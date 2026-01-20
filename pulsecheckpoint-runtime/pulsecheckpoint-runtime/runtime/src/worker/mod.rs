//! Worker registry for tracking registered workers and their heartbeats

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::metrics;
use crate::pulse::v1::{Metadata, WorkerInfo, WorkerStatus};

/// Worker data stored in the registry
#[derive(Debug, Clone)]
pub struct Worker {
    pub id: String,
    pub metadata: HashMap<String, String>,
    pub status: WorkerStatus,
    pub registered_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
}

impl Worker {
    /// Create a new worker
    pub fn new(id: String, metadata: HashMap<String, String>) -> Self {
        let now = Utc::now();
        Self {
            id,
            metadata,
            status: WorkerStatus::Active,
            registered_at: now,
            last_heartbeat: now,
        }
    }

    /// Convert to protobuf WorkerInfo
    pub fn to_proto(&self) -> WorkerInfo {
        WorkerInfo {
            worker_id: self.id.clone(),
            metadata: Some(Metadata {
                labels: self.metadata.clone(),
            }),
            status: self.status.into(),
            registered_at: Some(datetime_to_proto(self.registered_at)),
            last_heartbeat: Some(datetime_to_proto(self.last_heartbeat)),
        }
    }
}

/// Thread-safe worker registry
pub struct WorkerRegistry {
    workers: DashMap<String, Worker>,
    heartbeat_timeout: Duration,
    check_interval: Duration,
}

impl WorkerRegistry {
    /// Create a new worker registry with default settings
    pub fn new() -> Self {
        Self::with_config(Duration::from_secs(90), Duration::from_secs(30))
    }

    /// Create a new worker registry with custom timeouts
    pub fn with_config(heartbeat_timeout: Duration, check_interval: Duration) -> Self {
        Self {
            workers: DashMap::new(),
            heartbeat_timeout,
            check_interval,
        }
    }

    /// Register a new worker
    pub fn register(&self, worker_id: String, metadata: HashMap<String, String>) -> Result<Worker, WorkerError> {
        if worker_id.is_empty() {
            return Err(WorkerError::InvalidWorkerId("Worker ID cannot be empty".into()));
        }

        let worker = Worker::new(worker_id.clone(), metadata);
        
        // Check if worker already exists
        if self.workers.contains_key(&worker_id) {
            return Err(WorkerError::AlreadyExists(worker_id));
        }

        self.workers.insert(worker_id.clone(), worker.clone());
        metrics::ACTIVE_WORKERS.inc();
        
        info!(worker_id = %worker_id, "Worker registered");
        Ok(worker)
    }

    /// Deregister a worker
    pub fn deregister(&self, worker_id: &str) -> Result<(), WorkerError> {
        match self.workers.remove(worker_id) {
            Some(_) => {
                metrics::ACTIVE_WORKERS.dec();
                info!(worker_id = %worker_id, "Worker deregistered");
                Ok(())
            }
            None => Err(WorkerError::NotFound(worker_id.to_string())),
        }
    }

    /// Update worker heartbeat
    pub fn heartbeat(&self, worker_id: &str, status: Option<WorkerStatus>) -> Result<(), WorkerError> {
        match self.workers.get_mut(worker_id) {
            Some(mut worker) => {
                worker.last_heartbeat = Utc::now();
                if let Some(s) = status {
                    worker.status = s;
                }
                debug!(worker_id = %worker_id, "Heartbeat received");
                Ok(())
            }
            None => Err(WorkerError::NotFound(worker_id.to_string())),
        }
    }

    /// Get a worker by ID
    pub fn get(&self, worker_id: &str) -> Option<Worker> {
        self.workers.get(worker_id).map(|w| w.clone())
    }

    /// List all workers, optionally filtered by status
    pub fn list(&self, status_filter: Option<WorkerStatus>) -> Vec<Worker> {
        self.workers
            .iter()
            .filter(|entry| {
                status_filter.map_or(true, |s| entry.status == s)
            })
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get the count of active workers
    pub fn active_count(&self) -> usize {
        self.workers
            .iter()
            .filter(|entry| entry.status == WorkerStatus::Active)
            .count()
    }

    /// Get total worker count
    pub fn total_count(&self) -> usize {
        self.workers.len()
    }

    /// Check if a worker exists
    pub fn exists(&self, worker_id: &str) -> bool {
        self.workers.contains_key(worker_id)
    }

    /// Start the heartbeat monitor task
    pub async fn start_heartbeat_monitor(&self, mut shutdown: watch::Receiver<bool>) {
        let mut check_interval = interval(self.check_interval);
        
        info!(
            timeout_secs = self.heartbeat_timeout.as_secs(),
            interval_secs = self.check_interval.as_secs(),
            "Starting heartbeat monitor"
        );

        loop {
            tokio::select! {
                _ = check_interval.tick() => {
                    self.check_heartbeats();
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("Heartbeat monitor shutting down");
                        break;
                    }
                }
            }
        }
    }

    /// Check all workers for stale heartbeats
    fn check_heartbeats(&self) {
        let now = Utc::now();
        let timeout = chrono::Duration::from_std(self.heartbeat_timeout).unwrap();
        
        let mut unhealthy_count = 0;
        
        for mut entry in self.workers.iter_mut() {
            let worker = entry.value_mut();
            let elapsed = now - worker.last_heartbeat;
            
            if elapsed > timeout && worker.status == WorkerStatus::Active {
                warn!(
                    worker_id = %worker.id,
                    elapsed_secs = elapsed.num_seconds(),
                    "Worker heartbeat timeout, marking as unhealthy"
                );
                worker.status = WorkerStatus::Unhealthy;
                unhealthy_count += 1;
            }
        }

        if unhealthy_count > 0 {
            metrics::ACTIVE_WORKERS.sub(unhealthy_count as f64);
        }
    }
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Worker-related errors
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("Worker not found: {0}")]
    NotFound(String),
    
    #[error("Worker already exists: {0}")]
    AlreadyExists(String),
    
    #[error("Invalid worker ID: {0}")]
    InvalidWorkerId(String),
}

/// Convert DateTime to protobuf Timestamp
fn datetime_to_proto(dt: DateTime<Utc>) -> crate::pulse::v1::Timestamp {
    crate::pulse::v1::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_worker() {
        let registry = WorkerRegistry::new();
        let result = registry.register("worker-1".to_string(), HashMap::new());
        assert!(result.is_ok());
        assert!(registry.exists("worker-1"));
    }

    #[test]
    fn test_register_duplicate_worker() {
        let registry = WorkerRegistry::new();
        registry.register("worker-1".to_string(), HashMap::new()).unwrap();
        let result = registry.register("worker-1".to_string(), HashMap::new());
        assert!(matches!(result, Err(WorkerError::AlreadyExists(_))));
    }

    #[test]
    fn test_deregister_worker() {
        let registry = WorkerRegistry::new();
        registry.register("worker-1".to_string(), HashMap::new()).unwrap();
        assert!(registry.deregister("worker-1").is_ok());
        assert!(!registry.exists("worker-1"));
    }

    #[test]
    fn test_heartbeat() {
        let registry = WorkerRegistry::new();
        registry.register("worker-1".to_string(), HashMap::new()).unwrap();
        
        let before = registry.get("worker-1").unwrap().last_heartbeat;
        std::thread::sleep(std::time::Duration::from_millis(10));
        registry.heartbeat("worker-1", None).unwrap();
        let after = registry.get("worker-1").unwrap().last_heartbeat;
        
        assert!(after > before);
    }

    #[test]
    fn test_list_workers() {
        let registry = WorkerRegistry::new();
        registry.register("worker-1".to_string(), HashMap::new()).unwrap();
        registry.register("worker-2".to_string(), HashMap::new()).unwrap();
        
        let workers = registry.list(None);
        assert_eq!(workers.len(), 2);
    }
}
