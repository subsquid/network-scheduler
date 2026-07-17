use super::{ChunkId, DatasetId};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Dataset {
    pub id: DatasetId,
    pub height: Option<u64>,
}

/// A dataset paired with where S3 discovery should resume for it: `dataset.height` is the last
/// indexed block (the chain-continuity anchor) and `last_chunk_id` is that chunk's id (the resume
/// key). Both are absent for a dataset with no chunks yet — discovery then lists from the start.
#[derive(Debug, Clone)]
pub struct DatasetWatermark {
    pub dataset: Dataset,
    pub last_chunk_id: Option<ChunkId>,
}
