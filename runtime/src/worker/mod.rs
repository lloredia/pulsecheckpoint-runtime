//! In-memory worker registry. Registrations are lost on process restart.

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::watch;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::metrics;
use crate::pulse::v1::{Metadata, WorkerInfo, WorkerStatus};

#[derive(Debug, Clone)]
pub struct Worker {
    pub id: String,
    pub metadata: HashMap<String, String>,
    pub status: WorkerStatus,
    pub registered_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
}

impl Worker {
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

pub struct WorkerRegistry {
    workers: DashMap<String, Worker>,
    heartbeat_timeout: Duration,
    check_interval: Duration,
}

impl WorkerRegistry {
    pub fn new() -> Self {
        Self::with_config(Duration::from_secs(90), Duration::from_secs(30))
    }

    pub fn with_config(heartbeat_timeout: Duration, check_interval: Duration) -> Self {
        Self {
            workers: DashMap::new(),
            heartbeat_timeout,
            check_interval,
        }
    }

    pub fn register(
        &self,
        worker_id: String,
        metadata: HashMap<String, String>,
    ) -> Result<Worker, WorkerError> {
        validate_worker_id(&worker_id)?;
        if self.workers.contains_key(&worker_id) {
            return Err(WorkerError::AlreadyExists(worker_id));
        }
        let worker = Worker::new(worker_id.clone(), metadata);
        self.workers.insert(worker_id.clone(), worker.clone());
        self.refresh_gauge();
        info!(worker_id = %worker_id, "worker registered");
        Ok(worker)
    }

    pub fn deregister(&self, worker_id: &str) -> Result<(), WorkerError> {
        if self.workers.remove(worker_id).is_none() {
            return Err(WorkerError::NotFound(worker_id.to_string()));
        }
        self.refresh_gauge();
        info!(worker_id, "worker deregistered");
        Ok(())
    }

    pub fn heartbeat(
        &self,
        worker_id: &str,
        status: Option<WorkerStatus>,
    ) -> Result<(), WorkerError> {
        let mut worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| WorkerError::NotFound(worker_id.to_string()))?;
        worker.last_heartbeat = Utc::now();
        if let Some(status) = status {
            worker.status = status;
        }
        drop(worker);
        self.refresh_gauge();
        debug!(worker_id, "heartbeat received");
        Ok(())
    }

    pub fn get(&self, worker_id: &str) -> Option<Worker> {
        self.workers.get(worker_id).map(|worker| worker.clone())
    }

    pub fn list(&self, status_filter: Option<WorkerStatus>) -> Vec<Worker> {
        let mut workers: Vec<_> = self
            .workers
            .iter()
            .filter(|entry| status_filter.is_none_or(|status| entry.status == status))
            .map(|entry| entry.value().clone())
            .collect();
        workers.sort_by(|left, right| left.id.cmp(&right.id));
        workers
    }

    pub fn active_count(&self) -> usize {
        self.workers
            .iter()
            .filter(|entry| entry.status == WorkerStatus::Active)
            .count()
    }

    pub fn total_count(&self) -> usize {
        self.workers.len()
    }

    pub fn exists(&self, worker_id: &str) -> bool {
        self.workers.contains_key(worker_id)
    }

    pub async fn start_heartbeat_monitor(&self, mut shutdown: watch::Receiver<bool>) {
        let mut ticker = interval(self.check_interval);
        info!(
            timeout_secs = self.heartbeat_timeout.as_secs(),
            interval_secs = self.check_interval.as_secs(),
            "starting heartbeat monitor"
        );
        loop {
            tokio::select! {
                _ = ticker.tick() => self.check_heartbeats(),
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        info!("heartbeat monitor shutting down");
                        break;
                    }
                }
            }
        }
    }

    fn check_heartbeats(&self) {
        let now = Utc::now();
        let Ok(timeout) = chrono::Duration::from_std(self.heartbeat_timeout) else {
            return;
        };
        for mut entry in self.workers.iter_mut() {
            let worker = entry.value_mut();
            if worker.status == WorkerStatus::Active && now - worker.last_heartbeat > timeout {
                warn!(worker_id = %worker.id, "heartbeat timed out");
                worker.status = WorkerStatus::Unhealthy;
            }
        }
        self.refresh_gauge();
    }

    fn refresh_gauge(&self) {
        metrics::set_active_workers(self.active_count());
    }
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WorkerError {
    #[error("worker not found: {0}")]
    NotFound(String),

    #[error("worker already exists: {0}")]
    AlreadyExists(String),

    #[error("invalid worker id: {0}")]
    InvalidWorkerId(String),
}

pub fn validate_worker_id(id: &str) -> Result<(), WorkerError> {
    if id.is_empty()
        || id.contains('/')
        || id.contains('\\')
        || id.contains('\0')
        || id.split('.').any(|part| part.is_empty())
        || id.contains("..")
    {
        return Err(WorkerError::InvalidWorkerId(id.to_string()));
    }
    Ok(())
}

fn datetime_to_proto(dt: DateTime<Utc>) -> crate::pulse::v1::Timestamp {
    crate::pulse::v1::Timestamp {
        seconds: dt.timestamp(),
        nanos: i32::try_from(dt.timestamp_subsec_nanos()).unwrap_or(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_reject_duplicates() {
        let registry = WorkerRegistry::new();
        registry
            .register("worker-1".into(), HashMap::new())
            .unwrap();
        assert!(registry.exists("worker-1"));
        assert!(matches!(
            registry.register("worker-1".into(), HashMap::new()),
            Err(WorkerError::AlreadyExists(_))
        ));
        assert!(matches!(
            registry.register("".into(), HashMap::new()),
            Err(WorkerError::InvalidWorkerId(_))
        ));
        assert!(matches!(
            registry.register("../x".into(), HashMap::new()),
            Err(WorkerError::InvalidWorkerId(_))
        ));
    }

    #[test]
    fn deregister_missing_worker() {
        let registry = WorkerRegistry::new();
        assert!(matches!(
            registry.deregister("missing"),
            Err(WorkerError::NotFound(_))
        ));
    }

    #[test]
    fn heartbeat_updates_timestamp_and_status() {
        let registry = WorkerRegistry::new();
        registry
            .register("worker-1".into(), HashMap::new())
            .unwrap();
        {
            let mut worker = registry.workers.get_mut("worker-1").unwrap();
            worker.last_heartbeat = DateTime::<Utc>::from_timestamp(1, 0).unwrap();
        }
        registry
            .heartbeat("worker-1", Some(WorkerStatus::Idle))
            .unwrap();
        let worker = registry.get("worker-1").unwrap();
        assert!(worker.last_heartbeat.timestamp() > 1);
        assert_eq!(worker.status, WorkerStatus::Idle);
    }

    #[test]
    fn stale_heartbeat_marks_worker_unhealthy() {
        let registry =
            WorkerRegistry::with_config(Duration::from_secs(30), Duration::from_secs(30));
        registry.register("fresh".into(), HashMap::new()).unwrap();
        registry.register("stale".into(), HashMap::new()).unwrap();
        {
            let mut worker = registry.workers.get_mut("stale").unwrap();
            worker.last_heartbeat = Utc::now() - chrono::Duration::seconds(120);
        }
        registry.check_heartbeats();
        assert_eq!(registry.get("fresh").unwrap().status, WorkerStatus::Active);
        assert_eq!(
            registry.get("stale").unwrap().status,
            WorkerStatus::Unhealthy
        );
        assert_eq!(registry.active_count(), 1);
        assert_eq!(registry.list(Some(WorkerStatus::Unhealthy)).len(), 1);
    }

    #[test]
    fn list_is_sorted() {
        let registry = WorkerRegistry::new();
        registry.register("b".into(), HashMap::new()).unwrap();
        registry.register("a".into(), HashMap::new()).unwrap();
        let ids: Vec<_> = registry.list(None).into_iter().map(|w| w.id).collect();
        assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
    }
}
