use libp2p_identity::PeerId;
use std::collections::BTreeMap;

use crate::types::{Chunk, Worker};

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

    /// Test-only: `register_correction` rejected invalid inputs or chunks in an
    /// incompatible state.
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
pub use ids::{ChunkPk, DatasetId, WorkerPk};
#[cfg(test)]
pub(crate) mod in_memory;
pub mod postgres;

#[cfg(test)]
pub mod test_harness;

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
    pub chunks: BTreeMap<ChunkPk, Chunk>,
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
    pub chunks: BTreeMap<ChunkPk, Chunk>,
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

    // Seeding/ingestion. In production chunks and datasets arrive through other ingestion paths;
    // these are also the entry points the offline tools (e.g. `reshuffle-sim`) drive directly.
    fn insert_new_datasets(&mut self, datasets: Vec<String>) -> Result<(), StorageError>;
    fn insert_new_chunks(&mut self, chunks: Vec<Chunk>) -> Result<(), StorageError>;
    /// Register new chunk replacemet for an old chunk. New chunk must have the same block range as
    /// the old chunk.
    #[cfg(test)]
    fn register_correction(
        &mut self,
        old_pk: ChunkPk,
        new_chunk: Chunk,
        now: Tick,
    ) -> Result<ChunkPk, StorageError>;
}
