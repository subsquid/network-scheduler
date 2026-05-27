use std::collections::BTreeMap;

use anyhow::Result;
use libp2p_identity::PeerId;

/// Opaque chunk primary key, matches `chunks.chunk_pk` in Postgres.
pub type ChunkPk = i64;

/// Monotonic assignment version, matches `sched_assignments.id`.
pub type AssignmentId = i64;

/// Monotonic worker identifier within the scheduler, matches `sched_workers.id`.
pub type WorkerId = i64;

/// Logical time unit for property-based testing.
/// In production, this maps to wall-clock time; in tests, it's an integer tick.
pub type Tick = u64;

pub mod in_memory;

// ---------------------------------------------------------------------------
// Domain types (storage-layer view — no PeerId, no Arc<String>, no S3 URLs)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkRecord {
    pub chunk_pk: ChunkPk,
    pub dataset_id: i16,
    pub chunk_id: String,
    pub size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkLifecycle {
    pub chunk_pk: ChunkPk,
    pub applied_at_worker: Option<AssignmentId>,
    pub applied_at_portal: Option<AssignmentId>,
    pub marked_for_removal: Option<Tick>,
    pub dropped_at_portal: Option<AssignmentId>,
    pub dropped_at_worker: Option<AssignmentId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleMapping {
    pub chunk_pk: ChunkPk,
    pub worker_id: WorkerId,
    pub detected_at: Tick,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerRecord {
    pub worker_id: WorkerId,
    pub peer_id: String,
    pub inactive_since: Option<Tick>,
}

/// Result of a diff between the new ideal assignment and the previous ideal
/// in `sched_chunk_workers`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingDiff {
    /// Workers that gained a chunk they didn't have before.
    pub added: Vec<(ChunkPk, WorkerId)>,
    /// Workers that lost a chunk assignment.
    pub removed: Vec<(ChunkPk, WorkerId)>,
}

/// Storage backend for the MVCC scheduler lifecycle.
///
/// Each method corresponds to one atomic step in the scheduling cycle.
/// The scheduler orchestrates the order; the storage just executes mutations.
///
/// For property-based tests, `now` is a `Tick` counter.
/// For Postgres, `now` maps to `now()` (the parameter is ignored and
/// the DB clock is used instead).
pub trait SchedulerStorage {
    // -- Chunk registration --------------------------------------------------

    /// Find all chunks that have no lifecycle row yet, create one.
    /// Returns the PKs of newly registered chunks.
    fn register_new_chunks(&mut self) -> Result<Vec<ChunkPk>>;

    // -- Worker assignment cycle ---------------------------------------------

    /// Drop chunks from the worker assignment where:
    /// - they were already dropped from the portal assignment, AND
    /// - at least `m_ticks` have elapsed since that portal drop.
    ///
    /// Returns the PKs of chunks dropped this cycle (needed for capacity math).
    fn drop_expired_worker_chunks(
        &mut self,
        assignment_id: AssignmentId,
        now: Tick,
        m_ticks: u64,
    ) -> Result<Vec<ChunkPk>>;

    /// Fetch all chunks eligible for worker assignment:
    /// - no `applied_at_worker` yet
    /// - not `marked_for_removal`
    /// Ordered by registration time (FIFO).
    fn get_unassigned_chunks(&self) -> Result<Vec<ChunkRecord>>;

    /// Mark the given chunks as assigned to workers in the given assignment.
    fn mark_assigned_to_workers(
        &mut self,
        chunk_pks: &[ChunkPk],
        assignment_id: AssignmentId,
    ) -> Result<()>;

    /// Return all chunks currently in the worker assignment
    /// (applied_at_worker is set, dropped_at_worker is not).
    fn get_worker_assignment_chunks(&self) -> Result<Vec<ChunkRecord>>;

    // -- Worker confirmation -------------------------------------------------

    /// Record that workers have confirmed applying up to this assignment.
    fn confirm_worker_assignment(&mut self, assignment_id: AssignmentId, at: Tick) -> Result<()>;

    /// Get the latest confirmed worker assignment ID, if any.
    fn get_latest_confirmed_assignment(&self) -> Result<Option<AssignmentId>>;

    // -- Portal assignment cycle ---------------------------------------------

    /// Promote chunks to the portal assignment. A chunk is eligible when:
    /// - it has `applied_at_worker` set
    /// - that assignment is <= `confirmed_up_to`
    /// - it has no `applied_at_portal` yet
    /// - it has no `dropped_at_portal`
    ///
    /// AND drop chunks marked for removal from the portal.
    ///
    /// These two operations MUST be atomic (same transaction / same mutation)
    /// to uphold invariant 5 (no overlapping portal chunks).
    ///
    /// Returns (promoted_pks, dropped_pks).
    fn apply_portal_assignment(
        &mut self,
        assignment_id: AssignmentId,
        confirmed_up_to: AssignmentId,
    ) -> Result<(Vec<ChunkPk>, Vec<ChunkPk>)>;

    /// Return all chunks currently in the portal assignment.
    fn get_portal_assignment_chunks(&self) -> Result<Vec<ChunkRecord>>;

    // -- Backfill ------------------------------------------------------------

    /// Mark a chunk for removal. Precondition: the replacement chunk must
    /// already have `applied_at_worker` with a confirmed assignment.
    fn mark_for_removal(&mut self, chunk_pk: ChunkPk, now: Tick) -> Result<()>;

    // -- Chunk-to-worker mappings (stale mapping layer) ----------------------

    /// Get the ideal chunk-to-worker mapping (without stale holdovers).
    fn get_chunk_worker_mappings(&self) -> Result<BTreeMap<ChunkPk, Vec<WorkerId>>>;

    /// Apply a mapping diff: record new stale mappings for removals
    /// and update `sched_chunk_workers` with changed ideal mappings.
    fn apply_mapping_diff(&mut self, diff: &MappingDiff, now: Tick) -> Result<()>;

    /// Remove stale mappings where the algorithm re-assigned the chunk
    /// back to the same worker (flip-flop resolution).
    fn resolve_flipflops(&mut self, readded: &[(ChunkPk, WorkerId)]) -> Result<()>;

    /// Expire stale mappings older than `m_ticks`. The old worker drops
    /// off the published assignment automatically. Returns expired entries.
    fn expire_stale_mappings(&mut self, now: Tick, m_ticks: u64) -> Result<Vec<StaleMapping>>;

    // -- Workers -------------------------------------------------------------

    /// Return all workers currently in the registry, including inactive ones
    /// that haven't been garbage-collected yet.
    fn get_workers(&self) -> Result<Vec<WorkerRecord>>;

    /// Sync the worker registry with the current active set.
    /// - New workers are inserted.
    /// - Returned workers have `inactive_since` cleared.
    /// - Departed workers get `inactive_since` set and their `sched_stale_mappings` rows deleted.
    /// - Workers stale longer than `gc_ticks` are removed from the registry.
    /// Returns (added_worker_ids, departed_worker_ids).
    fn update_worker_set(
        &mut self,
        peer_ids: &[String],
        now: Tick,
        gc_ticks: u64,
    ) -> Result<(Vec<WorkerId>, Vec<WorkerId>)>;

    // -- Assignment bookkeeping ----------------------------------------------

    /// Create a new assignment version, returns its ID.
    fn create_assignment(&mut self, at: Tick) -> Result<AssignmentId>;

    // -- Replication reservations --------------------------------------------

    /// Create a reservation for capacity being freed by a replication decrease.
    fn create_replication_reservation(
        &mut self,
        reserved_bytes: u64,
        portal_assignment_id: AssignmentId,
    ) -> Result<()>;

    /// Get unfulfilled reservations whose portal assignment is older than `m_ticks`.
    fn get_fulfillable_reservations(
        &mut self,
        now: Tick,
        m_ticks: u64,
    ) -> Result<Vec<ReplicationReservation>>;

    /// Mark a reservation as fulfilled.
    fn fulfill_reservation(
        &mut self,
        reservation_id: i64,
        worker_assignment_id: AssignmentId,
        at: Tick,
    ) -> Result<()>;

    // Update workers list based on the Clickhouse information
    fn update_workers_list(&mut self, peer_id: PeerId) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationReservation {
    pub id: i64,
    pub reserved_bytes: u64,
    pub portal_assignment_id: AssignmentId,
}
