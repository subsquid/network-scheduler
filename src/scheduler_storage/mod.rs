use libp2p_identity::PeerId;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;
use std::sync::Arc;

use crate::types::{BlockNumber, ChunkId, DatasetId, DatasetSchema, Worker};
use crate::weight::SchedulingChunk;

/// The id newtypes, ingest input types, and the error enum now live in `scheduler-metadata`; the
/// scheduler keeps its historical `scheduler_storage::{…}` surface via this re-export.
pub use scheduler_metadata::{
    ChunkPk, DatasetPk, NewChunk, NewDataset, ReadSchemaId, SchemaId, StorageError, WorkerPk,
};

/// Lower a discovered [`Chunk`](crate::types::Chunk) to a [`NewChunk`] pinned to `schema_id`.
/// Discovery (the S3 listing) carries no schema, bitmap, or block-hash metadata, so the caller
/// resolves the dataset's seeded schema id; the metadata fields stay `None`.
pub fn discovered_chunk(c: crate::types::Chunk, schema_id: SchemaId) -> NewChunk {
    NewChunk {
        dataset: c.dataset,
        id: c.id,
        size: c.size,
        blocks: c.blocks,
        schema_id,
        tables_present: None,
        last_block_hash: None,
        last_block_timestamp: None,
    }
}

/// Logical integer timestamp; the caller supplies the clock (test ticks, or a monotonic clock
/// such as wall-clock seconds in production).
pub type Tick = u64;

/// Monotonic assignment version (matches sched_worker_assignments.id / sched_portal_assignments.id).
pub type AssignmentId = i32;

pub mod algorithm;
#[cfg(test)]
pub(crate) mod in_memory;
pub mod postgres;
pub mod schema_bundle;
pub use schema_bundle::{BundleId, SchemaBundle};

#[cfg(any(test, feature = "pg-testkit"))]
pub mod test_harness;

/// The scheduling algorithms' input view of a chunk.
#[derive(Debug, Clone, PartialEq)]
pub struct AlgoChunk {
    pub dataset: DatasetId,
    pub id: ChunkId,
    pub size: u32,
    pub blocks: RangeInclusive<BlockNumber>,
    pub is_portal_visible: bool,
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

impl AlgoChunk {
    /// The algorithm's view of a stored chunk. Portal visibility lives in the backend's lifecycle
    /// metadata, not in the chunk, so the caller supplies it.
    pub fn new(chunk: &WorkerAssignmentChunk, is_portal_visible: bool) -> Self {
        AlgoChunk {
            dataset: chunk.dataset.clone(),
            id: chunk.id.clone(),
            size: chunk.size,
            blocks: chunk.blocks.clone(),
            is_portal_visible,
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
    /// The read-schema id a portal validates each dataset's queries under; resolved against the
    /// [`SchemaBundle`] published with the assignment.
    ///
    /// **Total over the datasets `chunks` names, and no wider** — so it retires with the last
    /// visible chunk. Keying it off the `datasets` registry would grow forever, since nothing
    /// deletes a dataset row.
    ///
    /// `None` = never promoted, the seeded state of every dataset the scheduler creates. Distinct
    /// from a missing key, which would be a resolution failure rather than an answer.
    pub read_schemas: BTreeMap<DatasetId, Option<ReadSchemaId>>,
}

impl PortalAssignment {
    /// The scope `read_schemas` is total over.
    pub(crate) fn named_datasets(&self) -> BTreeSet<DatasetId> {
        self.chunks
            .values()
            .map(|chunk| chunk.dataset.clone())
            .collect()
    }
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
    ///
    /// Returns the published `WorkerAssignment` (ideal ∪ stale). The bundle no longer travels with
    /// it — call [`Self::generate_schema_bundle`] after every cycle, success or `Shortage`. On
    /// success the cycle also persists the round's write-schema ids, atomically with the
    /// assignment, which is what keeps a shortage-round bundle covering the frozen assignment.
    fn run_scheduling_cycle<Algo>(
        &mut self,
        algorithm: &Algo,
        config: &Algo::Config,
        now: Tick,
        m_ticks: u64,
    ) -> Result<WorkerAssignment, StorageError>
    where
        Algo: crate::scheduler_storage::algorithm::SchedulingAlgorithm + Send + Sync;

    /// Build the schema bundle from committed rows alone — identical under success, shortage, and
    /// after a restart. Write section: routable window (ADR 0002) ∪ the persisted set of the last
    /// successful assignment, so nothing the frozen published assignment names drops out
    /// mid-shortage. Read section: the CURRENT read schema of every referenced dataset, so a
    /// promote reaches the very next bundle.
    fn generate_schema_bundle(&self) -> Result<SchemaBundle, StorageError>;

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
    // carries its identity and storage location plus an initial WRITE schema — deliberately no read
    // pointer, so the read registry keeps exactly one writer (see `promote_read_schema`).
    // `NewDataset::new` derives the name from the location (scheme stripped), or `with_name` sets
    // an explicit one.
    fn insert_new_datasets(&mut self, datasets: Vec<NewDataset>) -> Result<(), StorageError>;
    fn insert_new_chunks(&mut self, chunks: Vec<NewChunk>) -> Result<(), StorageError>;

    /// Register `schema` in the dataset's WRITE registry (idempotent for identical content); no read
    /// pointer and no compatibility gate — those are ingest-service concerns. New chunks may pin it;
    /// existing chunks keep the schema they were stamped with.
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

    /// Make `schema` the dataset's current read schema, returning its content-deduped id.
    ///
    /// For tests and the simulation only. In production the read registry has one writer, the
    /// metadata service; the read path's concurrency argument rests on there being no second.
    fn promote_read_schema(
        &mut self,
        dataset: &str,
        schema: DatasetSchema,
    ) -> Result<ReadSchemaId, StorageError>;

    /// Register a new chunk replacement for an old chunk. New chunk must have the same block range
    /// as the old chunk. A production path now — reorgs drive it.
    fn register_correction(
        &mut self,
        old_pk: ChunkPk,
        new_chunk: NewChunk,
        now: Tick,
    ) -> Result<ChunkPk, StorageError>;
}
