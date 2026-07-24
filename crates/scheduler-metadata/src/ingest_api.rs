//! The ingest API contract: the wire/result types of [`PgIngest`](crate::pg::PgIngest).
//! [`IngestChunk`] and [`Correction`] are the literal HTTP request bodies; no SQL here.

use crate::dataset_schema::DatasetSchema;
use crate::ids::SchemaId;

/// One chunk to append (wire shape): the caller names tables and pins the schema; the server resolves
/// names → the positional bitmap. Distinct from [`NewChunk`](crate::NewChunk), the lowered row with
/// the bitmap resolved.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestChunk {
    pub id: String,
    pub first_block: u64,
    pub last_block: u64,
    pub size: u32,
    /// Explicit — never COALESCE-stamped. A superseded id is legal.
    pub schema_id: SchemaId,
    /// Table *names* present, resolved against `schema_id`'s tables in sorted-name order (unknown →
    /// `UnknownTable`). `None` or a full list = all present (NULL bitmap); `Some([])` = none (all-zero).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_block_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_block_timestamp: Option<i64>,
}

/// One same-range 1:1 replacement; names the old chunk by `id` (resolved to a `ChunkPk` in-tx).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Correction {
    pub old_chunk_id: String,
    pub replacement: IngestChunk,
}

/// A dataset's resume point: the highest `last_block` over chunks not terminally rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct Head {
    pub stored_head: Option<u64>,
}

/// Result of [`PgIngest::create_dataset`](crate::pg::PgIngest::create_dataset).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum DatasetOutcome {
    Created,
    AlreadyExists,
}

/// Lifecycle of one chunk as seen by an ingester.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum ChunkStatus {
    Pending,
    Admitted,
    Rejected,
    NotFound,
}

/// One of a dataset's WRITE schemas, as returned by
/// [`PgIngest::list_write_schemas`](crate::pg::PgIngest::list_write_schemas).
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaInfo {
    pub schema_id: SchemaId,
    pub schema: DatasetSchema,
    /// Referenced by at least one live chunk — a reader must still be able to decode it.
    pub active: bool,
}
