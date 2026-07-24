//! Service-owned wire DTOs and their `From` conversions to/from the PgIngest types. Input bodies
//! reuse crate types directly where those derive serde — `DatasetSchema`, and now `IngestChunk` /
//! `Correction` (with `SchemaId` serializing transparently as its `i32`); the request envelopes and
//! response DTOs below stay service-owned.

use scheduler_metadata::{ChunkStatus, Correction, Head, IngestChunk, SchemaInfo};
use scheduler_metadata::{DatasetSchema, NewDataset, ReadSchemaId, SchemaId};
use serde::{Deserialize, Serialize};

// ---- POST /datasets ----
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateDatasetRequest {
    pub location: String,
    #[serde(default)] // omitted ⇒ name = location with any scheme stripped (NewDataset::new)
    pub name: Option<String>,
    pub schema: DatasetSchema,
}

impl From<CreateDatasetRequest> for NewDataset {
    fn from(r: CreateDatasetRequest) -> Self {
        match r.name {
            Some(name) => NewDataset::with_name(name, r.location, r.schema),
            None => NewDataset::new(r.location, r.schema),
        }
    }
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct CreateDatasetResponse {
    pub name: String,
    pub created: bool,
}

// ---- POST write-schema (request body IS DatasetSchema) → {schema_id} ----
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct WriteSchemaResponse {
    pub schema_id: i32,
}
impl From<SchemaId> for WriteSchemaResponse {
    fn from(id: SchemaId) -> Self {
        Self { schema_id: id.0 }
    }
}

// ---- PUT read-schema (request body IS DatasetSchema) → {read_schema_id} ----
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ReadSchemaResponse {
    pub read_schema_id: i32,
}
impl From<ReadSchemaId> for ReadSchemaResponse {
    fn from(id: ReadSchemaId) -> Self {
        Self {
            read_schema_id: id.0,
        }
    }
}

// ---- GET read-schema (current) ----
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct CurrentReadSchemaResponse {
    pub read_schema_id: i32,
    pub schema: DatasetSchema,
}

// ---- GET write-schemas/{id} (one) ----
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct WriteSchemaViewResponse {
    pub schema_id: i32,
    pub schema: DatasetSchema,
}

// ---- GET write-schemas (list) ----
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SchemaInfoDto {
    pub schema_id: i32,
    pub active: bool,
    pub schema: DatasetSchema,
}
impl From<SchemaInfo> for SchemaInfoDto {
    fn from(s: SchemaInfo) -> Self {
        Self {
            schema_id: s.schema_id.0,
            active: s.active,
            schema: s.schema,
        }
    }
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SchemaListResponse {
    pub schemas: Vec<SchemaInfoDto>,
}

// ---- POST chunks (the wire chunk IS the crate's `IngestChunk`, deserialized directly) ----
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct InsertChunksRequest {
    pub chunks: Vec<IngestChunk>,
}

// ---- POST corrections (the wire correction IS the crate's `Correction`) ----
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CorrectionsRequest {
    pub corrections: Vec<Correction>,
}
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct CorrectionsResponse {
    /// Count of corrections registered (the swap itself happens later, scheduler-side).
    pub corrected: usize,
}

// ---- GET head (the resume point; null = no data yet) ----
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct HeadResponse {
    pub stored_head: Option<u64>,
}
impl From<Head> for HeadResponse {
    fn from(h: Head) -> Self {
        Self {
            stored_head: h.stored_head,
        }
    }
}

// ---- GET status ----
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChunkStatusWire {
    Pending,
    Admitted,
    Rejected,
    NotFound,
}
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ChunkStatusResponse {
    pub status: ChunkStatusWire,
}
impl From<ChunkStatus> for ChunkStatusWire {
    fn from(s: ChunkStatus) -> Self {
        match s {
            ChunkStatus::Pending => Self::Pending,
            ChunkStatus::Admitted => Self::Admitted,
            ChunkStatus::Rejected => Self::Rejected,
            ChunkStatus::NotFound => Self::NotFound,
        }
    }
}
