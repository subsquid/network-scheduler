mod assignment;
mod chunk;
mod dataset;
mod dataset_schema;
mod status;
mod summary;
mod worker;

use std::sync::Arc;

pub type WorkerIndex = u16;
pub type ChunkIndex = u32;
pub type ChunkWeight = u16;
pub type ReplicationFactor = u16;
pub type BlockNumber = u64;

/// A dataset's id, e.g. `s3://ethereum-mainnet-1`. Shared (`Arc`) — the same id rides along with
/// every chunk of the dataset.
pub type DatasetId = Arc<String>;

/// A chunk's id, e.g. `0018197829/0018246541-0018248424-c7ed95c9`. Shared (`Arc`) for the same
/// reason as [`DatasetId`].
pub type ChunkId = Arc<String>;

pub use assignment::{Assignment, FbVersion};
pub use chunk::Chunk;
pub use dataset::{Dataset, DatasetWatermark};
pub use dataset_schema::{DatasetSchema, TableSchema};
pub use status::{SchedulingStatus, SchedulingStatusConfig};
pub use summary::ChunkSummary;
pub use worker::{Worker, WorkerStatus};
