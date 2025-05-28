use libp2p_identity::PeerId;

pub type WorkerIndex = u16;
pub type ChunkIndex = u32;
pub type ChunkWeight = u16;

pub type BlockNumber = u64;

mod assignment;
mod chunk;
mod status;

pub use assignment::Assignment;
pub use chunk::Chunk;
use serde::Serialize;
pub use status::{SchedulingStatus, SchedulingStatusConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStatus {
    Online,
    Stale,
    Offline,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Worker {
    #[serde(rename = "peer_id")]
    pub id: PeerId,
    pub status: WorkerStatus,
}

impl Worker {
    pub fn reliable(&self) -> bool {
        matches!(self.status, WorkerStatus::Online)
    }
}
