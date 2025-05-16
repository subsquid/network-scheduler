use std::{ops::RangeInclusive, str::FromStr, sync::Arc};

use crate::pool;

use super::BlockNumber;

#[derive(Debug, Clone)]
pub struct Chunk {
    pub dataset: Arc<str>,
    pub id: String,
    pub size: u32,
    pub blocks: RangeInclusive<BlockNumber>,
    pub files: Arc<Vec<String>>,
    pub summary: Option<sqd_messages::assignments::ChunkSummary>,
}

impl Chunk {
    pub fn new(
        dataset: Arc<str>,
        id: String,
        size: u32,
        mut files: Vec<String>,
    ) -> anyhow::Result<Self> {
        files.sort_unstable();
        let files = pool::intern(files);
        let chunk = sqd_messages::data_chunk::DataChunk::from_str(&id)
            .map_err(|()| anyhow::anyhow!("Can't parse chunk id: {id}"))?;
        Ok(Self {
            dataset,
            id,
            files,
            blocks: chunk.first_block()..=chunk.last_block(),
            size,
            summary: None,
        })
    }
}

impl std::fmt::Display for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.dataset, self.id)
    }
}
