//! Input type for chunk ingestion (inserts and correction replacements).

use std::ops::RangeInclusive;
use std::sync::Arc;

use crate::ids::SchemaId;

/// A chunk to insert (via `insert_new_chunks` / corrections).
#[derive(Debug, Clone, PartialEq)]
pub struct NewChunk {
    pub dataset: Arc<String>,
    pub id: Arc<String>,
    pub size: u32,
    pub blocks: RangeInclusive<u64>,
    pub schema_id: SchemaId,
    pub tables_present: Option<bit_vec::BitVec>,
    pub last_block_hash: Option<String>,
    pub last_block_timestamp: Option<i64>,
}
