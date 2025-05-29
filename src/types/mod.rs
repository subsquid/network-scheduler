mod assignment;
mod chunk;
mod status;
mod worker;

pub type WorkerIndex = u16;
pub type ChunkIndex = u32;
pub type ChunkWeight = u16;

pub type BlockNumber = u64;

pub use assignment::Assignment;
pub use chunk::Chunk;
pub use status::{SchedulingStatus, SchedulingStatusConfig};
pub use worker::{Worker, WorkerStatus};
