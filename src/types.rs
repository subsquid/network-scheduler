use libp2p_identity::PeerId;

pub type WorkerIndex = u16;
pub type ChunkIndex = u32;
pub type ChunkWeight = u16;

pub type BlockNumber = u64;

mod assignment;
mod chunk;

pub use assignment::Assignment;
pub use chunk::Chunk;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Worker {
    pub id: PeerId,
    pub reliable: bool,
}
