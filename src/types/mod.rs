mod assignment;
mod chunk;
mod dataset;
mod status;
mod summary;
mod worker;

use std::sync::Arc;

pub type WorkerIndex = u16;
pub type ChunkIndex = u32;
pub type ChunkWeight = u16;
pub type ReplicationFactor = u16;
pub type BlockNumber = u64;

/// Scheduling knobs shared by the legacy and multistep schedulers, embedded verbatim in the
/// config file (flattened into `cli::Config`) — the serde attrs pin the deployed key names.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SchedulingConfig {
    /// Per-worker capacity budget; the config-file key predates the field name.
    #[serde(rename = "worker_storage_bytes")]
    pub worker_capacity: u64,
    /// The fraction of the worker storage that is actually filled (on average).
    /// The closer it gets to 1, the less consistent the distribution is.
    /// Corresponds to `1 / (1 + epsilon)` from this paper:
    /// https://research.google/blog/consistent-hashing-with-bounded-loads/
    pub saturation: f64,
    pub min_replication: ReplicationFactor,
    #[serde(default)]
    pub ignore_reliability: bool,
}

/// A dataset's id, e.g. `s3://ethereum-mainnet-1`. Shared (`Arc`) — the same id rides along with
/// every chunk of the dataset.
pub type DatasetId = Arc<String>;

/// A chunk's id, e.g. `0018197829/0018246541-0018248424-c7ed95c9`. Shared (`Arc`) for the same
/// reason as [`DatasetId`].
pub type ChunkId = Arc<String>;

pub use assignment::{Assignment, FbVersion};
pub use chunk::Chunk;
pub use dataset::{Dataset, DatasetWatermark};
#[cfg(feature = "mvcc-chunks")]
pub use scheduler_metadata::{DatasetSchema, TableSchema};
pub use status::{SchedulingStatus, SchedulingStatusConfig};
pub use summary::ChunkSummary;
pub use worker::{Worker, WorkerStatus};
