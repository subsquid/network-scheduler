use semver::VersionReq;
use serde::Serialize;

use super::Worker;

#[derive(Debug, Clone, Serialize)]
pub struct SchedulingStatus {
    pub config: SchedulingStatusConfig,
    pub workers: Vec<Worker>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SchedulingStatusConfig {
    pub supported_worker_versions: VersionReq,
    pub recommended_worker_versions: VersionReq,
}
