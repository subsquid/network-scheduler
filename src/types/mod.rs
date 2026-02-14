mod assignment;
mod chunk;
mod dataset;
mod status;
mod summary;
mod worker;

pub type WorkerIndex = u16;
pub type ChunkIndex = u32;
pub type ChunkWeight = u16;
pub type ReplicationFactor = u16;
pub type BlockNumber = u64;

pub use assignment::{Assignment, FbVersion};
pub use chunk::Chunk;
pub use dataset::Dataset;
pub use status::{SchedulingStatus, SchedulingStatusConfig};
pub use summary::ChunkSummary;
pub use worker::{Worker, WorkerStatus};
