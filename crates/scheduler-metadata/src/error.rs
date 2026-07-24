//! Error types returned across the storage/ingest boundary.

use crate::ids::SchemaId;

/// Errors returned across the storage boundary (SchedulerStorage trait and ingest SQL).
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Nothing is mutated; adding workers or removing chunks clears it next cycle.
    #[error("scheduling shortage: current worker capacity cannot satisfy all replication floors")]
    Shortage,

    /// Another instance holds the DB advisory lock (raised by connect).
    #[error("another scheduler instance is already running for this database")]
    AlreadyRunning,

    #[error("database error: {0:#}")]
    Database(anyhow::Error),

    /// Postgres serialization failure (SQLSTATE 40001); distinct from Database so callers map it to a retryable 409, not a 500.
    #[error("serialization failure: a concurrent transaction conflicted")]
    Serialization,

    /// Raised by register_correction: invalid input or incompatible chunk state.
    #[error("correction rejected: {reason}")]
    CorrectionRejected { reason: String },

    /// Distinct from Database so callers treat a re-insert as a no-op; carries no identity (the batched insert can't isolate which chunk collided).
    #[error("chunk already exists")]
    ChunkAlreadyExists,

    /// The per-dataset lock wait exceeded `lock_timeout` (SQLSTATE 55P03); retryable, not a 500.
    #[error("timed out waiting for the per-dataset lock")]
    LockTimeout,
}

impl From<anyhow::Error> for StorageError {
    fn from(err: anyhow::Error) -> Self {
        StorageError::Database(err)
    }
}

/// This crate stays HTTP-agnostic; deliberately NOT `#[non_exhaustive]` so the service's status-mapping match stays exhaustive.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("dataset {name:?} not found")]
    DatasetNotFound { name: String },

    #[error("schema {schema_id} does not exist for this dataset")]
    SchemaNotFound { schema_id: SchemaId },

    #[error("schema {schema_id} has no table {table:?}")]
    UnknownTable { schema_id: SchemaId, table: String },

    /// The range/size can't be stored (`last < first`, span or size exceeds `i32::MAX`).
    #[error(
        "chunk {chunk_id:?} has an unstorable range/size \
         (first_block={first_block}, last_block={last_block}, size={size})"
    )]
    InvalidChunkRange {
        chunk_id: String,
        first_block: u64,
        last_block: u64,
        size: u32,
    },

    /// Overlaps a live chunk — existing or another in the same batch.
    #[error("chunk {chunk_id:?} in {dataset:?} overlaps live chunk(s): {conflicts_with:?}")]
    RangeOverlap {
        dataset: String,
        chunk_id: String,
        conflicts_with: Vec<String>,
    },

    /// Rejected wholesale: ON CONFLICT DO NOTHING would otherwise silently keep an arbitrary payload.
    #[error("chunk {chunk_id:?} appears more than once in the batch")]
    DuplicateInBatch { chunk_id: String },

    #[error("correction rejected: {reason}")]
    CorrectionRejected { reason: String },

    /// Concurrent writer changed the read schema; the promotion was not applied — re-read and retry.
    #[error("dataset {dataset:?} schema was changed concurrently; retry")]
    ConcurrentSchemaChange { dataset: String },

    /// The per-dataset lock wait timed out behind a wedged writer; nothing was inserted — retry.
    #[error("timed out waiting for the dataset's ingest lock; retry")]
    DatasetBusy,

    #[error("database error: {0:#}")]
    Database(#[from] anyhow::Error),
}

impl From<StorageError> for IngestError {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::CorrectionRejected { reason } => Self::CorrectionRejected { reason },
            StorageError::Database(err) => Self::Database(err),
            StorageError::LockTimeout => Self::DatasetBusy,
            // None of these arise on ingest paths: PgIngest runs no cycle, takes no singleton lock,
            // inserts ON CONFLICT DO NOTHING, and Serialization is intercepted as ConcurrentSchemaChange
            // in promote_read_schema's REPEATABLE READ tx. Folded to Database, but listed explicitly
            // (not `_`) so a new StorageError variant breaks the build instead of silently 500ing.
            e @ (StorageError::Shortage
            | StorageError::AlreadyRunning
            | StorageError::ChunkAlreadyExists
            | StorageError::Serialization) => Self::Database(anyhow::Error::new(e)),
        }
    }
}
