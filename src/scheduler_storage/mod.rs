use libp2p_identity::PeerId;
use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::sync::Arc;

use crate::types::{BlockNumber, ChunkId, DatasetId, DatasetSchema, Worker};
use crate::weight::SchedulingChunk;

/// Logical integer timestamp; the caller supplies the clock (test ticks, or a monotonic clock
/// such as wall-clock seconds in production).
pub type Tick = u64;

/// Monotonic assignment version (matches sched_worker_assignments.id / sched_portal_assignments.id).
pub type AssignmentId = i64;

/// Errors returned across the [`SchedulerStorage`] boundary.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Capacity is too tight to place every chunk at its replication floor. Nothing is mutated;
    /// adding workers or removing chunks clears it next cycle.
    #[error("scheduling shortage: current worker capacity cannot satisfy all replication floors")]
    Shortage,

    /// Another scheduler instance already holds the database advisory lock (raised by `connect`).
    #[error("another scheduler instance is already running for this database")]
    AlreadyRunning,

    #[error("database error: {0:#}")]
    Database(anyhow::Error),

    /// `register_correction` rejected invalid inputs or chunks in an incompatible state.
    #[error("correction rejected: {reason}")]
    CorrectionRejected { reason: String },

    /// `insert_new_chunks` refused a chunk whose `(dataset, id)` already exists. Distinct from
    /// `Database` so callers can treat a re-insert as a no-op. Carries no identity: the batched
    /// Postgres insert can't isolate which chunk collided.
    #[error("chunk already exists")]
    ChunkAlreadyExists,
}

impl From<anyhow::Error> for StorageError {
    fn from(err: anyhow::Error) -> Self {
        StorageError::Database(err)
    }
}

pub mod algorithm;
pub mod ids;
pub use ids::{ChunkPk, DatasetPk, SchemaId, WorkerPk};
#[cfg(test)]
pub(crate) mod in_memory;
pub mod postgres;

#[cfg(any(test, feature = "pg-testkit"))]
pub mod test_harness;

/// The scheduling algorithms' input view of a chunk.
#[derive(Debug, Clone, PartialEq)]
pub struct AlgoChunk {
    pub dataset: DatasetId,
    pub id: ChunkId,
    pub size: u32,
    pub blocks: RangeInclusive<BlockNumber>,
}

impl SchedulingChunk for AlgoChunk {
    fn dataset(&self) -> &Arc<String> {
        &self.dataset
    }
    fn id(&self) -> &Arc<String> {
        &self.id
    }
    fn blocks(&self) -> &RangeInclusive<BlockNumber> {
        &self.blocks
    }
    fn size(&self) -> u32 {
        self.size
    }
}

/// A chunk to insert ([`SchedulerStorage::insert_new_chunks`] / corrections).
#[derive(Debug, Clone, PartialEq)]
pub struct NewChunk {
    pub dataset: DatasetId,
    pub id: ChunkId,
    pub size: u32,
    pub blocks: RangeInclusive<BlockNumber>,
    /// `None` = stamp with the dataset's current schema.
    pub schema_id: Option<SchemaId>,
    /// Table-presence bitmap over `schema_id`'s tables in sorted-name order; `None` = all present.
    pub tables_present: Option<bit_vec::BitVec>,
}

/// A chunk as published in an assignment; its file set derives from the schema
/// (`schema_id` + `tables_present`) at assignment construction.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerAssignmentChunk {
    pub dataset: DatasetId,
    pub id: ChunkId,
    pub size: u32,
    pub blocks: RangeInclusive<BlockNumber>,
    /// The schema the chunk was written under (stamped at insert).
    pub schema_id: SchemaId,
    /// Table-presence bitmap over `schema_id`'s tables in sorted-name order; `None` = all present.
    pub tables_present: Option<bit_vec::BitVec>,
}

impl WorkerAssignmentChunk {
    /// Project the chunk down to what the scheduling algorithms may see.
    pub fn algo_view(&self) -> AlgoChunk {
        AlgoChunk {
            dataset: self.dataset.clone(),
            id: self.id.clone(),
            size: self.size,
            blocks: self.blocks.clone(),
        }
    }
}

/// A worker entry as it appears in a published assignment.
#[derive(Debug, Clone)]
pub struct AssignmentWorker {
    pub peer_id: PeerId,
    pub status: crate::types::WorkerStatus,
}

/// The published worker assignment: ideal ∪ stale mappings.
#[derive(Debug, Clone)]
pub struct WorkerAssignment {
    pub id: AssignmentId,
    pub chunk_workers: BTreeMap<ChunkPk, Vec<WorkerPk>>,
    pub chunks: BTreeMap<ChunkPk, WorkerAssignmentChunk>,
    pub workers: BTreeMap<WorkerPk, AssignmentWorker>,
    /// Replication factor the scheduler chose per chunk weight (ideal placement; excludes draining
    /// copies in `chunk_workers`).
    pub replication_by_weight: BTreeMap<u16, u16>,
}

/// The published portal assignment: confirmed routing for portal-visible chunks.
#[derive(Debug, Clone)]
pub struct PortalAssignment {
    pub id: AssignmentId,
    pub chunk_workers: BTreeMap<ChunkPk, Vec<WorkerPk>>,
    pub chunks: BTreeMap<ChunkPk, WorkerAssignmentChunk>,
    pub workers: BTreeMap<WorkerPk, AssignmentWorker>,
}

/// Storage backend for the MVCC scheduler lifecycle.
///
/// The scheduler orchestrates the order in which these methods run; each
/// implementor encapsulates its own execution (DB transactions for Postgres,
/// in-place mutation for InMemoryStorage).
pub trait SchedulerStorage {
    /// Create scheduling metadata for new chunks, and make them eligible for workers
    fn register_new_chunks(&mut self) -> Result<Vec<ChunkPk>, StorageError>;

    /// Update the current view on the active workers
    fn update_worker_set(
        &mut self,
        active_workers: &[Worker],
        now: Tick,
        gc_ticks: u64,
    ) -> Result<(), StorageError>;

    /// Run one full scheduling cycle: tombstone expired chunks, expire stale
    /// mappings, run `algorithm` in-process, diff + commit results.
    /// Returns the published WorkerAssignment (ideal ∪ stale).
    fn run_scheduling_cycle<Algo>(
        &mut self,
        algorithm: &Algo,
        config: &Algo::Config,
        now: Tick,
        m_ticks: u64,
    ) -> Result<WorkerAssignment, StorageError>
    where
        Algo: crate::scheduler_storage::algorithm::SchedulingAlgorithm + Send + Sync;

    /// Advance the confirmation watermark and replay pending routing diffs
    fn confirm_worker_assignment(
        &mut self,
        assignment_id: AssignmentId,
        now: Tick,
    ) -> Result<(), StorageError>;

    /// Atomically promote eligible chunks and drop marked-for-removal chunks.
    /// Mark stale mappings that are no longer needed for removal.
    fn run_visibility_cycle(&mut self, now: Tick) -> Result<PortalAssignment, StorageError>;

    /// Mark a chunk for removal. The replacement chunk must already have a
    /// confirmed worker assignment.
    fn mark_for_removal(&mut self, chunk_pk: ChunkPk, now: Tick) -> Result<(), StorageError>;

    // Seeding/ingestion entry points (production ingestion and the offline tools). Each dataset
    // carries its read schema; use `DatasetSchema::default()` when unknown.
    fn insert_new_datasets(
        &mut self,
        datasets: Vec<(String, DatasetSchema)>,
    ) -> Result<(), StorageError>;
    fn insert_new_chunks(&mut self, chunks: Vec<NewChunk>) -> Result<(), StorageError>;

    /// Make `schema` the dataset's current read schema (idempotent for identical content).
    /// Affects chunks inserted afterwards; existing chunks keep the schema they were stamped with.
    fn set_dataset_schema(
        &mut self,
        dataset: &str,
        schema: DatasetSchema,
    ) -> Result<(), StorageError>;

    /// Decode schemas for assignment construction: all of them, or those in `schema_ids`.
    /// Missing ids are omitted.
    fn load_schemas(
        &self,
        schema_ids: Option<&[SchemaId]>,
    ) -> Result<BTreeMap<SchemaId, DatasetSchema>, StorageError>;

    /// Register new chunk replacemet for an old chunk. New chunk must have the same block range as
    /// the old chunk. Also enabled by `pg-testkit` for offline tools (reshuffle-sim).
    #[cfg(any(test, feature = "pg-testkit"))]
    fn register_correction(
        &mut self,
        old_pk: ChunkPk,
        new_chunk: NewChunk,
        now: Tick,
    ) -> Result<ChunkPk, StorageError>;
}
