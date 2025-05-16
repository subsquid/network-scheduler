use std::collections::BTreeMap;

use libp2p_identity::PeerId;

pub type WorkerIndex = u16;
pub type ChunkIndex = u32;
pub type ChunkWeight = u16;

pub type BlockNumber = u64;

mod chunk;

pub use chunk::Chunk;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Worker {
    pub id: PeerId,
    pub reliable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    // chunk indexes are sorted
    pub workers: BTreeMap<PeerId, Vec<ChunkIndex>>,
}
