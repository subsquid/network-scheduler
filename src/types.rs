use std::collections::BTreeMap;

use libp2p_identity::PeerId;

pub type WorkerIndex = u16;
pub type ChunkIndex = u32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub workers: BTreeMap<PeerId, Vec<ChunkIndex>>,
}
