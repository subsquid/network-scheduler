use serde::Serialize;

use super::Dataset;
use super::Worker;

#[derive(Debug, Clone, Serialize)]
pub struct SchedulingStatus {
    pub config: SchedulingStatusConfig,
    pub workers: Vec<Worker>,
    pub datasets: Vec<Dataset>,
    pub assignment_timestamp_sec: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SchedulingStatusConfig {
    pub supported_worker_versions: String,
    pub recommended_worker_versions: String,
}
