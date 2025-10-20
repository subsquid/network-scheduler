use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ChunkSummary {
    pub last_block_hash: String,
    pub last_block_timestamp: u64,
}
