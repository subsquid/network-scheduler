//! In-memory reference model of [`SchedulerStorage`](crate::scheduler_storage::SchedulerStorage) —
//! test-only. Serves as the simulation's oracle and a fast backend for the storage suites; it is
//! not a production storage path.

use crate::scheduler_storage::algorithm::{CurrentPlacement, ScheduleOutput, SchedulingAlgorithm};
use crate::scheduler_storage::{
    AlgoChunk, AssignmentId, AssignmentWorker, ChunkPk, NewChunk, PortalAssignment,
    SchedulerStorage, SchemaBundle, SchemaId, StorageError, Tick, WorkerAssignment,
    WorkerAssignmentChunk, WorkerPk,
};
use crate::types::{BlockNumber, DatasetSchema, Worker, WorkerStatus};
use anyhow::{Context, Result};
use libp2p_identity::PeerId;
use semver::Version;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::RangeInclusive;
use thiserror::Error;

mod nonoverlap;
#[cfg(test)]
mod tests;

type Dataset = String;
type ChunkId = String;

type WorkerAssignmentId = u64;
type PortalAssignmentId = u64;
type TimeUnit = u64;

type PublishedWorkerAssignment = WorkerAssignment;

/// One cycle's routing changes, not yet applied to the confirmed routing. An
/// empty `Vec` means the chunk was removed. Replayed in order and dropped by
/// `confirm_worker_assignment`, so total memory is bounded by the confirmation lag.
#[derive(Debug, Clone)]
struct ChunkRoutingDiff {
    changes: BTreeMap<ChunkPk, Vec<WorkerPk>>,
}

#[derive(Default, Debug)]
struct SchedulerChunkMetadata {
    applied_at_worker_assignment_id: Option<WorkerAssignmentId>,
    applied_at_portal_assignment_id: Option<PortalAssignmentId>,
    marked_for_removal: bool,
    /// True if refused at registration for overlapping a live chunk in its dataset; never scheduled
    /// or reconsidered
    rejected: bool,
    dropped_at_portal_assignment_id: Option<PortalAssignmentId>,
    /// Tick at which the chunk was tombstoned; NULL = not tombstoned.
    dropped_from_worker_assignment_at: Option<TimeUnit>,
}

/// Lifecycle-state predicates over the `sched_chunk_metadata` columns, named after the states in
/// `docs/mvcc-storage.md`. (`discovered` — no metadata row — and the bare `rejected`/
/// `marked_for_removal` flags aren't methods: the former has no row, the latter read for themselves.)
///
/// The stamps are cumulative, so each milestone has two readings. The predicates split into two
/// families and the name tells you which at a glance:
///
/// * **Threshold reached** — "has the chunk ever passed milestone X?". True from the milestone
///   onward, *including* after the chunk has moved further along (an `entered_portal_assignment`
///   chunk stays `entered_*` even once dropped). These mirror the column verb in the past tense:
///   `entered_*` for the `applied_at_*` stamps, `confirmed_by` for the derived watermark threshold.
///   They read as "has passed this point", never as a present condition.
/// * **Current state** — "is the chunk in state X *right now*?". True only in the window between one
///   stamp and the next, and it closes when the next stamp lands. These read as a present
///   adjectival condition: `portal_visible`, `tombstoned`.
///
/// `in_removal` is the union of the removal-tail current states; `live` / `worker_servable` are
/// derived roles composed from the two families.
impl SchedulerChunkMetadata {
    // --- Threshold-reached: monotone, "has the chunk ever passed milestone X?" ---

    /// Threshold: has ever entered a worker assignment (workers were told to download it). Stays
    /// true thereafter, even once the chunk is dropped or tombstoned.
    fn entered_worker_assignment(&self) -> bool {
        self.applied_at_worker_assignment_id.is_some()
    }

    /// Threshold: has ever been promoted into a portal assignment. Stays true even after the chunk
    /// is dropped from the portal — for "promoted *and still there*", use `portal_visible`.
    fn entered_portal_assignment(&self) -> bool {
        self.applied_at_portal_assignment_id.is_some()
    }

    /// Threshold: its worker assignment is at or below the confirmation `watermark` (an X% quorum of
    /// workers reported holding it). Derived, so it takes the watermark rather than reading a column;
    /// the `_by` arg is the threshold-family tell. Monotone in the watermark, which only advances.
    fn confirmed_by(&self, watermark: WorkerAssignmentId) -> bool {
        self.applied_at_worker_assignment_id
            .is_some_and(|worker_aid| worker_aid <= watermark)
    }

    // --- Current state: momentary, "is the chunk in state X right now?" ---

    /// Current: promoted into a portal assignment and not yet dropped from one — the window between
    /// the two portal stamps. (Threshold counterpart: `entered_portal_assignment`.)
    fn portal_visible(&self) -> bool {
        self.applied_at_portal_assignment_id.is_some()
            && self.dropped_at_portal_assignment_id.is_none()
    }

    /// Current/terminal: dropped from the worker assignment. The row is retained as an audit marker
    /// and the chunk leaves every future cycle, so this state never closes.
    fn tombstoned(&self) -> bool {
        self.dropped_from_worker_assignment_at.is_some()
    }

    /// Current: in the removal tail by any of the three removal stamps — marked for removal, or
    /// already dropped at the portal or worker level. The union of the tail's current states.
    fn in_removal(&self) -> bool {
        self.marked_for_removal
            || self.dropped_at_portal_assignment_id.is_some()
            || self.dropped_from_worker_assignment_at.is_some()
    }

    // --- Derived roles: composed from both families ---

    /// Role: admitted and not leaving — neither rejected nor anywhere in the removal tail. A live
    /// chunk still claims its block range (the registration comparison set). Equivalent to
    /// `worker_servable() && !marked_for_removal && dropped_at_portal.is_none()`, the condition
    /// `live_admitted_chunks` filters on.
    fn live(&self) -> bool {
        !self.rejected && !self.in_removal()
    }

    /// Role: not rejected and not tombstoned — eligible to appear in a worker assignment. Unlike
    /// `live`, a draining chunk (marked / portal-dropped but not yet tombstoned) is still served by
    /// workers through the M-tick window.
    fn worker_servable(&self) -> bool {
        !self.rejected && !self.tombstoned()
    }
}

/// 1:1 replacement of an old chunk, keyed by `old_chunk_pk` (at most one per
/// source chunk). Completed rows are kept as an inert audit trail.
#[derive(Debug, Clone)]
struct ChunkCorrection {
    new_chunk_pk: ChunkPk,
    dataset: Dataset,
    created_at: TimeUnit,
    applied_at_portal_assignment_id: Option<PortalAssignmentId>,
}

/// Reasons `register_correction` rejects a request.
#[derive(Debug, Error)]
pub enum CorrectionRejected {
    #[error("old chunk {old_pk:?} not found")]
    OldChunkNotFound { old_pk: ChunkPk },
    #[error("a correction for old chunk {old_pk:?} already exists")]
    DuplicatePending { old_pk: ChunkPk },
    #[error("old chunk {old_pk:?} is already being removed")]
    OldChunkBeingRemoved { old_pk: ChunkPk },
    #[error("old chunk {old_pk:?} was rejected at registration (terminal — nothing to supersede)")]
    OldChunkRejected { old_pk: ChunkPk },
    #[error("replacement chunk {dataset}/{chunk_id} already exists")]
    ReplacementChunkExists { dataset: Dataset, chunk_id: ChunkId },
    #[error("replacement dataset {new_dataset} does not match old chunk dataset {old_dataset}")]
    DatasetMismatch {
        old_dataset: Dataset,
        new_dataset: Dataset,
    },
    #[error("replacement range {new:?} does not match old chunk {old_pk:?} range {old:?}")]
    RangeChanged {
        old_pk: ChunkPk,
        old: RangeInclusive<BlockNumber>,
        new: RangeInclusive<BlockNumber>,
    },
}

/// One visibility cycle: when it ran, and the confirmation watermark it saw (drain derivation).
#[derive(Debug, Clone, Copy)]
struct PortalAssignmentEntry {
    created_at: TimeUnit,
    confirmed_up_to: WorkerAssignmentId,
}

/// Grace-period holdover for a `(chunk, worker)` pair removed from the ideal assignment
/// while the chunk is still portal-visible. Pending vs draining is derived: draining once a
/// portal assignment's `confirmed_up_to` covers `superseded_at_worker_assignment_id`,
/// anchored at the first such assignment.
#[derive(Debug, Clone)]
struct StaleMapping {
    /// First worker assignment whose ideal no longer routes the chunk to this worker.
    superseded_at_worker_assignment_id: WorkerAssignmentId,
}

#[derive(Debug, Clone)]
struct WorkerEntry {
    peer_id: PeerId,
    version: Option<Version>,
    inactive_since: Option<TimeUnit>,
}

impl WorkerEntry {
    fn status(&self) -> WorkerStatus {
        match self.inactive_since {
            None => WorkerStatus::Online,
            Some(_) => WorkerStatus::Stale,
        }
    }
}

mod counter {
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default, Debug)]
    pub struct Counter(AtomicU64);

    impl Counter {
        /// Fresh monotonically increasing id, starting at 1; 0 is reserved as
        /// the "no id allocated yet" sentinel for defaulted watermarks.
        pub fn next(&self) -> u64 {
            self.0.fetch_add(1, Ordering::Relaxed) + 1
        }
    }
}
use counter::Counter;

#[derive(Default, Debug)]
struct Counters {
    chunk_pk: Counter,
    worker_id: Counter,
    worker_assignment_id: Counter,
    portal_assignment_id: Counter,
    schema_id: Counter,
}

#[derive(Default)]
pub(crate) struct InMemoryStorage {
    datasets: HashSet<Dataset>,
    /// Per-dataset current read schema id (mirrors the `schemas` row with `superseded_at IS NULL`)
    dataset_schemas: HashMap<Dataset, SchemaId>,
    /// Schema payloads by id, first-seen content kept (mirrors the `schemas` table)
    schemas: HashMap<SchemaId, DatasetSchema>,
    /// `(dataset, canonical json)` → id: re-applying a seen schema reuses its id (mirrors
    /// `UNIQUE (dataset_id, hash)` + reactivation)
    schema_ids: HashMap<(Dataset, String), SchemaId>,
    chunks: BTreeMap<ChunkPk, WorkerAssignmentChunk>,
    /// Natural identity → surrogate pk; a duplicate `(dataset, id)` insert is rejected
    /// (mirrors Postgres `UNIQUE (dataset_id, chunk_id)`). Never pruned — chunks
    /// are tombstoned, not deleted.
    chunk_index: HashMap<(Dataset, ChunkId), ChunkPk>,

    sched_chunk_metadata: BTreeMap<ChunkPk, SchedulerChunkMetadata>,
    sched_worker_assignments: BTreeMap<WorkerAssignmentId, TimeUnit>,
    sched_portal_assignments: BTreeMap<PortalAssignmentId, PortalAssignmentEntry>,

    // The highest worker assignment confirmed by the workers
    sched_worker_assignment_confirmation: WorkerAssignmentId,

    sched_stale_mappings: BTreeMap<(ChunkPk, WorkerPk), StaleMapping>,
    sched_workers: BTreeMap<WorkerPk, WorkerEntry>,
    // Active chunk -> workers mapping (latest scheduled ideal).
    sched_ideal_chunk_workers: BTreeMap<ChunkPk, Vec<WorkerPk>>,
    // Routing as of the highest confirmed worker assignment. What portals route by.
    sched_confirmed_chunk_workers: BTreeMap<ChunkPk, Vec<WorkerPk>>,
    // Per-cycle routing changes waiting to be applied to the confirmed routing.
    sched_worker_assignment_diffs: BTreeMap<WorkerAssignmentId, ChunkRoutingDiff>,

    // Keyed by old_chunk_pk.
    chunk_corrections: BTreeMap<ChunkPk, ChunkCorrection>,

    counters: Counters,
}

#[derive(Debug, Error)]
pub enum InsertChunkError {
    #[error("dataset {dataset} not found")]
    NoDatasetFound { dataset: Dataset },
    #[error("chunk {dataset}/{chunk_id} already exists")]
    ChunkAlreadyExists { dataset: Dataset, chunk_id: ChunkId },
}

#[derive(Debug, Error)]
pub enum InsertDatasetError {
    #[error("dataset {dataset} already exists")]
    UniqueConstraintViolation { dataset: Dataset },
}

impl InMemoryStorage {
    fn assignment_workers(&self) -> BTreeMap<WorkerPk, AssignmentWorker> {
        self.sched_workers
            .iter()
            .map(|(id, entry)| {
                (
                    *id,
                    AssignmentWorker {
                        peer_id: entry.peer_id,
                        status: entry.status(),
                    },
                )
            })
            .collect()
    }

    /// Per-chunk holders physically on disk now: `ideal ∪ stale`, deduped. What a
    /// placement-aware [`SchedulingAlgorithm`](crate::scheduler_storage::algorithm::SchedulingAlgorithm)
    /// reconciles against.
    fn current_placement(&self) -> BTreeMap<ChunkPk, Vec<WorkerPk>> {
        let mut out: BTreeMap<ChunkPk, BTreeSet<WorkerPk>> = BTreeMap::new();
        for (pk, workers) in &self.sched_ideal_chunk_workers {
            out.entry(*pk).or_default().extend(workers.iter().copied());
        }
        for (pk, worker_id) in self.sched_stale_mappings.keys() {
            out.entry(*pk).or_default().insert(*worker_id);
        }
        out.into_iter()
            .map(|(pk, set)| (pk, set.into_iter().collect()))
            .collect()
    }

    /// Used by the ingester or operator to include datasets in the system. Every dataset gets a
    /// current schema (the default one), matching Postgres — chunk inserts stamp against it.
    pub fn insert_new_datasets(
        &mut self,
        datasets: Vec<Dataset>,
    ) -> Result<(), InsertDatasetError> {
        for dataset in datasets {
            if !self.datasets.insert(dataset.clone()) {
                return Err(InsertDatasetError::UniqueConstraintViolation { dataset });
            }
            self.ensure_current_schema(&dataset, DatasetSchema::default());
        }
        Ok(())
    }

    /// Ingester entry point. Errors if a chunk names an unregistered dataset or duplicates an
    /// existing `(dataset, id)` (mirroring Postgres' UNIQUE constraint). Returns the number
    /// inserted, which on success is all of them.
    pub fn insert_new_chunks(&mut self, chunks: Vec<NewChunk>) -> Result<usize, InsertChunkError> {
        let mut added = 0;
        for chunk in chunks {
            let dataset = (*chunk.dataset).clone();
            if !self.datasets.contains(&dataset) {
                return Err(InsertChunkError::NoDatasetFound { dataset });
            }
            let chunk_id = (*chunk.id).clone();
            if self.insert_one_chunk(chunk).is_none() {
                return Err(InsertChunkError::ChunkAlreadyExists { dataset, chunk_id });
            }
            added += 1;
        }
        Ok(added)
    }

    /// Shared index/pk bookkeeping behind `insert_new_chunks` and a correction's replacement insert;
    /// callers own the dataset-registered check and the typed error. Returns `None` on a duplicate
    /// `(dataset, id)`.
    fn insert_one_chunk(&mut self, chunk: NewChunk) -> Option<ChunkPk> {
        use std::collections::hash_map::Entry;
        // Mirror the Postgres insert: no explicit pin -> stamp the dataset's current schema
        // (registered datasets always have one, like the NOT NULL column).
        let schema_id = chunk
            .schema_id
            .or_else(|| self.dataset_schemas.get(&*chunk.dataset).copied())
            .expect("registered dataset has a current schema");
        let chunk = WorkerAssignmentChunk {
            dataset: chunk.dataset,
            id: chunk.id,
            size: chunk.size,
            blocks: chunk.blocks,
            schema_id,
            tables_present: chunk.tables_present,
        };
        match self
            .chunk_index
            .entry(((*chunk.dataset).clone(), (*chunk.id).clone()))
        {
            Entry::Vacant(vac) => {
                let pk = ChunkPk(self.counters.chunk_pk.next() as i64);
                vac.insert(pk);
                self.chunks.insert(pk, chunk);
                Some(pk)
            }
            Entry::Occupied(_) => None,
        }
    }

    /// Typed-error registration backing the `SchedulerStorage::register_correction` wrapper.
    /// Named distinctly so the trait method isn't shadowed: tests that assert a specific
    /// `CorrectionRejected` variant call this directly, while the happy path goes through the trait.
    fn register_correction_int(
        &mut self,
        old_pk: ChunkPk,
        new_chunk: NewChunk,
        now: TimeUnit,
    ) -> Result<ChunkPk, CorrectionRejected> {
        let Some(old_chunk) = self.chunks.get(&old_pk) else {
            return Err(CorrectionRejected::OldChunkNotFound { old_pk });
        };
        let dataset = (*old_chunk.dataset).clone();
        let old_blocks = old_chunk.blocks.clone();

        // The replacement must already belong to the old chunk's dataset; reject rather than
        // silently rewrite it.
        if *new_chunk.dataset != dataset {
            return Err(CorrectionRejected::DatasetMismatch {
                old_dataset: dataset,
                new_dataset: (*new_chunk.dataset).clone(),
            });
        }

        if self.chunk_corrections.contains_key(&old_pk) {
            return Err(CorrectionRejected::DuplicatePending { old_pk });
        }

        if let Some(meta) = self.sched_chunk_metadata.get(&old_pk) {
            // A rejected old chunk is terminal — never admitted, nothing portal-visible to supersede;
            // reviving it through a correction would also defeat the no-self-heal rule.
            if meta.rejected {
                return Err(CorrectionRejected::OldChunkRejected { old_pk });
            }
            if meta.in_removal() {
                return Err(CorrectionRejected::OldChunkBeingRemoved { old_pk });
            }
        }

        // A correction is a same-range 1-to-1 swap. Refuse a range-changing replacement before it is
        // inserted or a correction row is created — admitting it would either leave a dangling
        // pending correction (blocking future corrections of this chunk) or publish an overlap.
        if new_chunk.blocks != old_blocks {
            return Err(CorrectionRejected::RangeChanged {
                old_pk,
                old: old_blocks,
                new: new_chunk.blocks.clone(),
            });
        }

        // Insert the replacement (same-range 1-to-1, same dataset as checked above).
        let chunk_id = (*new_chunk.id).clone();
        let Some(new_pk) = self.insert_one_chunk(new_chunk) else {
            return Err(CorrectionRejected::ReplacementChunkExists { dataset, chunk_id });
        };
        // The replacement's sched_chunk_metadata row is created by register_new_chunks (the
        // standard addition flow), not here.

        self.chunk_corrections.insert(
            old_pk,
            ChunkCorrection {
                new_chunk_pk: new_pk,
                dataset,
                created_at: now,
                applied_at_portal_assignment_id: None,
            },
        );
        Ok(new_pk)
    }

    /// Apply ready corrections, re-evaluating after each firing so chains collapse in one cycle.
    /// A correction fires once its replacement is confirmed and no *pending* correction still has
    /// to produce its old chunk (i.e. no pending `X → old`). The only real dependency is a
    /// correction chain (`B → C` waits for `A → B` because `B` is `A → B`'s replacement);
    /// independent corrections in the same dataset fire in the same cycle. `created_at` is not
    /// consulted. Returns the old chunk PKs that fired.
    fn apply_ready_corrections(&mut self, portal_aid: PortalAssignmentId) -> Vec<ChunkPk> {
        let confirmed_up_to = self.sched_worker_assignment_confirmation;
        let mut fired: Vec<ChunkPk> = Vec::new();

        loop {
            // Next ready correction: replacement confirmed, and this chunk is not still being
            // produced by another pending correction.
            let next_ready: Option<ChunkPk> = self
                .chunk_corrections
                .iter()
                .filter(|(_, c)| c.applied_at_portal_assignment_id.is_none())
                .filter(|(_, c)| {
                    self.sched_chunk_metadata
                        .get(&c.new_chunk_pk)
                        .is_some_and(|m| m.confirmed_by(confirmed_up_to))
                })
                .find(|(old_pk, _)| {
                    !self.chunk_corrections.values().any(|other| {
                        other.applied_at_portal_assignment_id.is_none()
                            && other.new_chunk_pk == **old_pk
                    })
                })
                .map(|(pk, _)| *pk);

            let Some(old_pk) = next_ready else {
                break;
            };

            // The removal mark is picked up by the drop pass later this cycle.
            if let Some(meta) = self.sched_chunk_metadata.get_mut(&old_pk) {
                meta.marked_for_removal = true;
            }

            let entry = self
                .chunk_corrections
                .get_mut(&old_pk)
                .expect("old_pk came from chunk_corrections");
            entry.applied_at_portal_assignment_id = Some(portal_aid);

            fired.push(old_pk);
        }

        fired
    }

    /// Evict workers that went stale strictly before `stale_cutoff`, taking their stale mappings
    /// along (the Postgres FK does this via `ON DELETE CASCADE`).
    fn evict_stale_workers(&mut self, stale_cutoff: TimeUnit) {
        let evicted: HashSet<WorkerPk> = self
            .sched_workers
            .iter()
            .filter(|(_, entry)| matches!(entry.inactive_since, Some(ts) if ts < stale_cutoff))
            .map(|(id, _)| *id)
            .collect();
        if evicted.is_empty() {
            return;
        }
        self.sched_workers.retain(|id, _| !evicted.contains(id));
        self.sched_stale_mappings
            .retain(|(_, worker_id), _| !evicted.contains(worker_id));
    }

    /// True if `dropped_at_portal` names a portal assignment created at or before `cutoff` — i.e.
    /// its M-tick grace has elapsed. `None` (never dropped) is not elapsed.
    fn portal_drop_elapsed(
        &self,
        dropped_at_portal: Option<PortalAssignmentId>,
        cutoff: TimeUnit,
    ) -> bool {
        dropped_at_portal
            .and_then(|aid| self.sched_portal_assignments.get(&aid))
            .is_some_and(|entry| entry.created_at <= cutoff)
    }

    /// Tombstone chunks whose portal-removal happened at least `m_ticks` ago.
    /// One-way: the row stays as a permanent tombstone and the chunk leaves all
    /// future cycles. Also clears the chunk's stale mappings — the portal-level
    /// grace has already elapsed, so no worker-level drain is needed.
    fn tombstone_expired_chunks(&mut self, now: TimeUnit, m_ticks: TimeUnit) {
        let Some(cutoff) = now.checked_sub(m_ticks) else {
            return;
        };
        let to_tombstone: Vec<ChunkPk> = self
            .sched_chunk_metadata
            .iter()
            .filter_map(|(pk, meta)| {
                (!meta.tombstoned()
                    && self.portal_drop_elapsed(meta.dropped_at_portal_assignment_id, cutoff))
                .then_some(*pk)
            })
            .collect();
        for pk in &to_tombstone {
            let meta = self
                .sched_chunk_metadata
                .get_mut(pk)
                .expect("pk came from sched_chunk_metadata");
            meta.dropped_from_worker_assignment_at = Some(now);
            self.sched_stale_mappings
                .retain(|(stale_pk, _), _| stale_pk != pk);
        }
    }

    /// Delete draining stale mappings whose dropping portal assignment is at
    /// least `m_ticks` old. Pending mappings never expire here — their M-tick
    /// clock hasn't started.
    fn expire_drained_stale_mappings(&mut self, now: TimeUnit, m_ticks: TimeUnit) {
        let Some(cutoff) = now.checked_sub(m_ticks) else {
            return;
        };
        // Newest watermark among aged portal assignments; pairs at/under it are exactly those
        // whose derived drain anchor aged out (both columns monotone over assignment ids).
        let Some(watermark) = self
            .sched_portal_assignments
            .values()
            .filter(|e| e.created_at <= cutoff)
            .map(|e| e.confirmed_up_to)
            .max()
        else {
            return;
        };
        self.sched_stale_mappings
            .retain(|_, m| m.superseded_at_worker_assignment_id > watermark);
    }

    /// Mint a pending stale mapping for each `(chunk, worker)` pair dropped from
    /// the ideal, anchored on `new_worker_aid`. Deliberately not gated on portal
    /// visibility: a confirmed-but-unpromoted chunk can still lose a pair, which
    /// must enter `sched_stale_mappings` so `ideal ∪ stale` stays a superset of
    /// the confirmed routing. Chunks already leaving the portal are skipped — the
    /// whole-chunk removal handles them.
    fn record_reshuffle_stale_mappings(
        &mut self,
        ideal_mappings: &BTreeMap<ChunkPk, HashSet<WorkerPk>>,
        evicted: &HashSet<(ChunkPk, WorkerPk)>,
        new_worker_aid: WorkerAssignmentId,
    ) {
        for (pk, current_workers) in &self.sched_ideal_chunk_workers {
            let being_removed = self
                .sched_chunk_metadata
                .get(pk)
                .is_some_and(|m| m.dropped_at_portal_assignment_id.is_some());
            if being_removed {
                continue;
            }
            let ideal_workers = ideal_mappings.get(pk);
            for &worker_id in current_workers {
                let still_assigned = ideal_workers.is_some_and(|iw| iw.contains(&worker_id));
                // Only an active worker drains: a departed one serves nothing, so its copies
                // vanish. Mirrors the Postgres mint's `inactive_since IS NULL` filter.
                let worker_active = self
                    .sched_workers
                    .get(&worker_id)
                    .is_some_and(|entry| entry.inactive_since.is_none());
                // An evicted pair is deleted this cycle, not drained: never mint it as stale.
                if !still_assigned && worker_active && !evicted.contains(&(*pk, worker_id)) {
                    self.sched_stale_mappings
                        .entry((*pk, worker_id))
                        .or_insert(StaleMapping {
                            superseded_at_worker_assignment_id: new_worker_aid,
                        });
                }
            }
        }
    }

    /// Record the diff between the new ideal and the current one, for later
    /// replay by `confirm_worker_assignment`. Must run before
    /// `commit_ideal_mappings` overwrites `sched_ideal_chunk_workers`.
    fn record_worker_assignment_diff(
        &mut self,
        ideal_mappings: &BTreeMap<ChunkPk, HashSet<WorkerPk>>,
        new_worker_aid: WorkerAssignmentId,
    ) {
        let mut changes: BTreeMap<ChunkPk, Vec<WorkerPk>> = BTreeMap::new();

        for (pk, new_workers) in ideal_mappings {
            let old_set: HashSet<WorkerPk> = self
                .sched_ideal_chunk_workers
                .get(pk)
                .map(|workers| workers.iter().copied().collect())
                .unwrap_or_default();
            if &old_set != new_workers {
                changes.insert(*pk, new_workers.iter().copied().collect());
            }
        }

        // Chunks dropped from the ideal: empty `Vec` = removal.
        for pk in self.sched_ideal_chunk_workers.keys() {
            if !ideal_mappings.contains_key(pk) {
                changes.insert(*pk, Vec::new());
            }
        }

        if !changes.is_empty() {
            self.sched_worker_assignment_diffs
                .insert(new_worker_aid, ChunkRoutingDiff { changes });
        }
    }

    fn commit_ideal_mappings(&mut self, ideal_mappings: BTreeMap<ChunkPk, HashSet<WorkerPk>>) {
        // Flip-flop: a pair back in the ideal is no longer stale.
        for (pk, ideal_workers) in &ideal_mappings {
            for &worker_id in ideal_workers {
                self.sched_stale_mappings.remove(&(*pk, worker_id));
            }
        }

        self.sched_ideal_chunk_workers = ideal_mappings
            .into_iter()
            .map(|(pk, workers)| (pk, workers.into_iter().collect()))
            .collect();
    }

    /// Set `dataset`'s current read schema and return its id, reusing the id of previously seen
    /// identical content — mirrors Postgres' `ensure_current_schema`.
    fn ensure_current_schema(&mut self, dataset: &str, schema: DatasetSchema) -> SchemaId {
        use std::collections::hash_map::Entry;
        let canon = serde_json::to_string(&schema.canonicalized()).expect("schema serializes");
        let id = match self.schema_ids.entry((dataset.to_owned(), canon)) {
            Entry::Occupied(occ) => *occ.get(),
            Entry::Vacant(vac) => {
                let id = SchemaId(self.counters.schema_id.next() as i32);
                self.schemas.insert(id, schema);
                *vac.insert(id)
            }
        };
        self.dataset_schemas.insert(dataset.to_owned(), id);
        id
    }

    /// Published worker assignment: `ideal ∪ stale` (pending and draining), with
    /// tombstoned chunks excluded and worker lists deduped. Because a stale row
    /// is minted the moment a pair leaves the ideal, this is always a superset of
    /// the confirmed routing — workers never drop data a portal still routes to.
    /// The caller allocates and registers `id`.
    fn build_worker_assignment(
        &self,
        id: WorkerAssignmentId,
        replication_by_weight: BTreeMap<u16, u16>,
    ) -> PublishedWorkerAssignment {
        // `current_placement` is the same `ideal ∪ stale` union, sorted and deduped; keep only the
        // chunks workers may still serve.
        let mut chunk_workers = self.current_placement();
        chunk_workers.retain(|pk, _| {
            self.sched_chunk_metadata
                .get(pk)
                .is_none_or(|m| m.worker_servable())
        });

        let chunks: BTreeMap<ChunkPk, WorkerAssignmentChunk> = chunk_workers
            .keys()
            .filter_map(|pk| self.chunks.get(pk).map(|c| (*pk, c.clone())))
            .collect();

        WorkerAssignment {
            id: id as AssignmentId,
            chunk_workers,
            chunks,
            workers: self.assignment_workers(),
            replication_by_weight,
        }
    }

    /// Apply the non-overlap promotion gate (`docs/nonoverlap-promotion-gate.md`): mark each chunk
    /// chosen by [`nonoverlap::chunks_to_promote`] portal-visible under `portal_aid`. The gating
    /// decision is read-only and lives in [`nonoverlap`].
    fn promote_through_overlap_gate(
        &mut self,
        portal_aid: PortalAssignmentId,
        confirmed_up_to: WorkerAssignmentId,
    ) {
        for pk in nonoverlap::chunks_to_promote(self, confirmed_up_to) {
            let meta = self
                .sched_chunk_metadata
                .get_mut(&pk)
                .expect("promoted pk must have metadata");
            meta.applied_at_portal_assignment_id = Some(portal_aid);
        }
    }
}

impl SchedulerStorage for InMemoryStorage {
    fn insert_new_datasets(
        &mut self,
        datasets: Vec<(String, DatasetSchema)>,
    ) -> Result<(), StorageError> {
        for (name, schema) in &datasets {
            schema
                .validate()
                .with_context(|| format!("invalid schema for dataset {name}"))?;
        }
        // All-or-nothing like the Postgres transaction: reject the whole batch up front, so a
        // duplicate mid-batch can't leave earlier names registered without a schema.
        let mut incoming = HashSet::new();
        for (name, _) in &datasets {
            if self.datasets.contains(name) || !incoming.insert(name.as_str()) {
                return Err(
                    anyhow::Error::from(InsertDatasetError::UniqueConstraintViolation {
                        dataset: name.clone(),
                    })
                    .into(),
                );
            }
        }
        for (name, schema) in datasets {
            self.datasets.insert(name.clone());
            self.ensure_current_schema(&name, schema);
        }
        Ok(())
    }

    fn set_dataset_schema(
        &mut self,
        dataset: &str,
        schema: DatasetSchema,
    ) -> Result<(), StorageError> {
        if !self.datasets.contains(dataset) {
            return Err(anyhow::anyhow!("set_dataset_schema: dataset {dataset} not found").into());
        }
        schema.validate().context("invalid dataset schema")?;
        self.ensure_current_schema(dataset, schema);
        Ok(())
    }

    fn load_schemas(
        &self,
        schema_ids: Option<&[SchemaId]>,
    ) -> Result<BTreeMap<SchemaId, DatasetSchema>, StorageError> {
        Ok(match schema_ids {
            None => self
                .schemas
                .iter()
                .map(|(id, s)| (*id, s.clone()))
                .collect(),
            Some(ids) => ids
                .iter()
                .filter_map(|id| self.schemas.get(id).cloned().map(|schema| (*id, schema)))
                .collect(),
        })
    }

    fn active_schema_bundle(&self) -> Result<BTreeMap<SchemaId, DatasetSchema>, StorageError> {
        // Every worker-servable chunk's schema: entered a worker assignment, not yet tombstoned —
        // the postgres predicate verbatim. Deriving from `current_placement` instead would drop a
        // chunk reshuffled out of the ideal but still served through its drain window.
        let mut ids: BTreeSet<SchemaId> = BTreeSet::new();
        for (pk, meta) in &self.sched_chunk_metadata {
            if meta.entered_worker_assignment()
                && !meta.tombstoned()
                && let Some(chunk) = self.chunks.get(pk)
            {
                ids.insert(chunk.schema_id);
            }
        }
        Ok(ids
            .into_iter()
            .filter_map(|id| self.schemas.get(&id).map(|s| (id, s.clone())))
            .collect())
    }

    fn insert_new_chunks(&mut self, chunks: Vec<NewChunk>) -> Result<(), StorageError> {
        match self.insert_new_chunks(chunks) {
            Ok(_added) => Ok(()),
            // Typed so callers can treat a re-insert as a no-op; a missing dataset stays a DB error.
            Err(InsertChunkError::ChunkAlreadyExists { .. }) => {
                Err(StorageError::ChunkAlreadyExists)
            }
            Err(err @ InsertChunkError::NoDatasetFound { .. }) => {
                Err(anyhow::Error::from(err).into())
            }
        }
    }

    /// Create a default lifecycle row for any chunk lacking one. Returns the new PKs.
    fn register_new_chunks(&mut self) -> Result<Vec<ChunkPk>, StorageError> {
        let decision = nonoverlap::admission_decision(self);
        for &pk in &decision.admitted {
            let insert_new = self
                .sched_chunk_metadata
                .insert(pk, SchedulerChunkMetadata::default());
            assert!(insert_new.is_none());
        }
        // A rejected chunk gets a terminal row so the scan never reconsiders it and it stays
        // unscheduled.
        for &pk in &decision.rejected {
            let insert_new = self.sched_chunk_metadata.insert(
                pk,
                SchedulerChunkMetadata {
                    rejected: true,
                    ..Default::default()
                },
            );
            assert!(insert_new.is_none());
        }
        Ok(decision.admitted)
    }

    fn update_worker_set(
        &mut self,
        active_workers: &[Worker],
        now: Tick,
        gc_ticks: u64,
    ) -> Result<(), StorageError> {
        let mut remaining: HashMap<PeerId, &Worker> =
            active_workers.iter().map(|w| (w.id, w)).collect();

        // Workers that go inactive this call, collected to drop their stale mappings below.
        let mut departed: Vec<WorkerPk> = Vec::new();

        // Matched peers are drained from `remaining`; leftovers are new.
        for (id, entry) in self.sched_workers.iter_mut() {
            if let Some(worker) = remaining.remove(&entry.peer_id) {
                entry.inactive_since = None;
                entry.version = worker.version.clone();
            } else if entry.inactive_since.is_none() {
                entry.inactive_since = Some(now);
                departed.push(*id);
            }
        }

        for (peer_id, worker) in remaining {
            let id = WorkerPk(self.counters.worker_id.next() as i32);
            self.sched_workers.insert(
                id,
                WorkerEntry {
                    peer_id,
                    version: worker.version.clone(),
                    inactive_since: None,
                },
            );
        }

        if !departed.is_empty() {
            let departed_set: HashSet<WorkerPk> = departed.iter().copied().collect();
            self.sched_stale_mappings
                .retain(|(_, worker_id), _| !departed_set.contains(worker_id));

            // Give a drain back its committed-holder status when every worker it was handing
            // off to has departed. Confirmation is a quorum of *active* workers, so a
            // recipient's departure lets the handoff count as "confirmed" without any download
            // (a vacuous confirmation — Invariant 2, docs/mvcc-storage.md); the drain's expiry
            // clock would then delete the fleet's last real copy. Promotion takes the copy off
            // the expiry clock and under the retention floor. Excess copies become ordinary
            // drains in later cycles; departed ideal rows fall out with the next cycle's diff.
            let handoff_void: HashSet<ChunkPk> = self
                .sched_ideal_chunk_workers
                .iter()
                .filter(|(_, holders)| {
                    !holders.is_empty()
                        && holders.iter().all(|w| {
                            self.sched_workers
                                .get(w)
                                .is_none_or(|entry| entry.inactive_since.is_some())
                        })
                })
                .map(|(pk, _)| *pk)
                .collect();
            let promoted: Vec<(ChunkPk, WorkerPk)> = self
                .sched_stale_mappings
                .keys()
                .filter(|(pk, worker_id)| {
                    handoff_void.contains(pk)
                        && self
                            .sched_workers
                            .get(worker_id)
                            .is_some_and(|entry| entry.inactive_since.is_none())
                })
                .copied()
                .collect();
            for (pk, worker_id) in promoted {
                self.sched_stale_mappings.remove(&(pk, worker_id));
                let holders = self
                    .sched_ideal_chunk_workers
                    .get_mut(&pk)
                    .expect("promoted chunk came from the ideal");
                if !holders.contains(&worker_id) {
                    holders.push(worker_id);
                }
            }
        }

        self.evict_stale_workers(now.saturating_sub(gc_ticks));

        Ok(())
    }

    /// Run one scheduling cycle: tombstone expired chunks, expire stale mappings, run `algorithm`,
    /// then diff + commit the result. Returns the published worker assignment.
    fn run_scheduling_cycle<Algo>(
        &mut self,
        algorithm: &Algo,
        config: &Algo::Config,
        now: Tick,
        m_ticks: u64,
    ) -> Result<(WorkerAssignment, SchemaBundle), StorageError>
    where
        Algo: SchedulingAlgorithm + Send + Sync,
    {
        // Clock-driven GC runs first; it persists even through the shortage below.
        self.tombstone_expired_chunks(now, m_ticks);
        self.expire_drained_stale_mappings(now, m_ticks);

        // Snapshot after expiry so the algorithm sees post-expiry placement. The algorithm only
        // does keyed lookups against it, so hand it a `HashMap` (O(1)) rather than the ordered
        // `BTreeMap` the snapshot is built as.
        let current_placement: CurrentPlacement = self.current_placement().into_iter().collect();

        // Split-role eviction inputs (roles documented on `SchedulingAlgorithm::schedule`). The
        // routing is filtered to portal-visible chunks, mirroring the Postgres
        // `fetch_confirmed_routing` predicate, so both backends see the same routed set. No
        // active-worker filter: departed holders are dropped downstream by the same position
        // mapping as `current_placement`.
        let committed: CurrentPlacement = self
            .sched_ideal_chunk_workers
            .iter()
            .map(|(pk, workers)| (*pk, workers.clone()))
            .collect();
        let confirmed_routing: CurrentPlacement = self
            .sched_confirmed_chunk_workers
            .iter()
            .filter(|(pk, _)| {
                self.sched_chunk_metadata
                    .get(pk)
                    .is_some_and(|meta| meta.portal_visible())
            })
            .map(|(pk, workers)| (*pk, workers.clone()))
            .collect();

        let chunks: Vec<(ChunkPk, AlgoChunk)> = self
            .chunks
            .iter()
            .filter(|(pk, _)| {
                self.sched_chunk_metadata
                    .get(pk)
                    .is_none_or(|m| m.worker_servable())
            })
            .map(|(pk, chunk)| {
                let is_portal_visible = self
                    .sched_chunk_metadata
                    .get(pk)
                    .is_some_and(|meta| meta.portal_visible());
                (*pk, AlgoChunk::new(chunk, is_portal_visible))
            })
            .collect();
        // Departed workers are not capacity. Mirrors the Postgres `decode_workers` filter.
        let workers: Vec<(WorkerPk, Worker)> = self
            .sched_workers
            .iter()
            .filter(|(_, entry)| entry.inactive_since.is_none())
            .map(|(id, entry)| {
                (
                    *id,
                    Worker {
                        id: entry.peer_id,
                        status: entry.status(),
                        version: entry.version.clone(),
                    },
                )
            })
            .collect();

        // A shortage is surfaced with nothing committed, so a degraded run can be driven through it.
        let ScheduleOutput {
            mapping,
            replication_by_weight,
            evicted,
        } = algorithm
            .schedule(
                chunks,
                workers,
                &current_placement,
                &committed,
                &confirmed_routing,
                config,
            )
            .map_err(|_| StorageError::Shortage)?;

        // This oracle keeps set-based bookkeeping, so index the returned `Vec` into a map once.
        let ideal_mappings: BTreeMap<ChunkPk, HashSet<WorkerPk>> = mapping
            .into_iter()
            .map(|(pk, holders)| {
                let set: HashSet<WorkerPk> = holders.iter().copied().collect();
                debug_assert_eq!(
                    set.len(),
                    holders.len(),
                    "duplicate holders for chunk {pk:?}"
                );
                (pk, set)
            })
            .collect();

        // Open the assignment the records below are written under.
        let new_worker_aid = self.counters.worker_assignment_id.next();
        self.sched_worker_assignments.insert(new_worker_aid, now);

        // Evicted copies are DELETED this cycle, not drained. Drop any pre-existing stale row so the
        // pair is in neither ideal nor stale -> the worker assignment omits it -> the worker deletes
        // it. The mint below skips these too, covering a pair that was in the OLD ideal.
        let evicted: HashSet<(ChunkPk, WorkerPk)> = evicted.into_iter().collect();
        for pair in &evicted {
            self.sched_stale_mappings.remove(pair);
        }

        self.record_reshuffle_stale_mappings(&ideal_mappings, &evicted, new_worker_aid);

        // Must precede commit_ideal_mappings, which overwrites the old ideal.
        self.record_worker_assignment_diff(&ideal_mappings, new_worker_aid);

        self.commit_ideal_mappings(ideal_mappings);

        let assignment = self.build_worker_assignment(new_worker_aid, replication_by_weight);

        // Stamp first-time entry into a worker assignment.
        for pk in self.sched_ideal_chunk_workers.keys() {
            let meta = self.sched_chunk_metadata.entry(*pk).or_default();
            if !meta.entered_worker_assignment() {
                meta.applied_at_worker_assignment_id = Some(new_worker_aid);
            }
        }

        // Bundle covers every chunk in its routable window — entered a worker assignment, not yet
        // tombstoned — not just this cycle's placement (ADR 0002): the portal can still route a
        // chunk the latest assignment dropped (promoted off an earlier confirmed entry, draining
        // for M ticks), so keying on current placement would yank a schema still being read.
        let mut schema_ids: BTreeSet<SchemaId> = assignment.schema_ids();
        schema_ids.extend(
            self.sched_chunk_metadata
                .iter()
                .filter(|(_, meta)| meta.entered_worker_assignment() && !meta.tombstoned())
                .filter_map(|(pk, _)| self.chunks.get(pk).map(|c| c.schema_id)),
        );
        let schemas: BTreeMap<SchemaId, DatasetSchema> = schema_ids
            .into_iter()
            .filter_map(|id| self.schemas.get(&id).map(|schema| (id, schema.clone())))
            .collect();
        let bundle = SchemaBundle::from_schemas(schemas);
        Ok((assignment, bundle))
    }

    /// Record that workers have confirmed up to this assignment: advances the
    /// watermark and replays the intervening routing diffs into
    /// `sched_confirmed_chunk_workers`, dropping them as applied. The in-memory
    /// model does not record confirmation time, so `now` is unused.
    fn confirm_worker_assignment(
        &mut self,
        assignment_id: AssignmentId,
        _now: Tick,
    ) -> Result<(), StorageError> {
        let assignment_id = assignment_id as WorkerAssignmentId;
        let previous = self.sched_worker_assignment_confirmation;
        let new_watermark = previous.max(assignment_id);
        if new_watermark == previous {
            return Ok(());
        }

        let to_apply: Vec<WorkerAssignmentId> = self
            .sched_worker_assignment_diffs
            .range(previous + 1..=new_watermark)
            .map(|(id, _)| *id)
            .collect();
        for id in to_apply {
            let diff = self
                .sched_worker_assignment_diffs
                .remove(&id)
                .expect("diff id came from the range above");
            for (pk, workers) in diff.changes {
                if workers.is_empty() {
                    self.sched_confirmed_chunk_workers.remove(&pk);
                } else {
                    self.sched_confirmed_chunk_workers.insert(pk, workers);
                }
            }
        }

        self.sched_worker_assignment_confirmation = new_watermark;
        Ok(())
    }

    /// Produce a new portal assignment. Atomically: apply ready corrections,
    /// promote confirmed chunks (skipping pending correction targets), and drop
    /// chunks marked for removal. Recording the watermark on the assignment is
    /// what activates drains (derived; see [`StaleMapping`]). Returns the
    /// portal-visible chunks routed by the confirmed routing.
    fn run_visibility_cycle(&mut self, now: Tick) -> Result<PortalAssignment, StorageError> {
        let confirmed_up_to = self.sched_worker_assignment_confirmation;
        let portal_assignment_id = self.counters.portal_assignment_id.next();
        self.sched_portal_assignments.insert(
            portal_assignment_id,
            PortalAssignmentEntry {
                created_at: now,
                confirmed_up_to,
            },
        );

        // Before the promote/drop pass so corrections fired this cycle take
        // effect immediately.
        self.apply_ready_corrections(portal_assignment_id);

        // Promote before drop so a correction's atomic swap (old marked, new promoted) is never
        // observed as both.
        self.promote_through_overlap_gate(portal_assignment_id, confirmed_up_to);
        for meta in self.sched_chunk_metadata.values_mut() {
            if meta.marked_for_removal && meta.dropped_at_portal_assignment_id.is_none() {
                meta.dropped_at_portal_assignment_id = Some(portal_assignment_id);
            }
        }

        let portal_chunks: BTreeMap<ChunkPk, WorkerAssignmentChunk> = self
            .sched_chunk_metadata
            .iter()
            .filter(|(_, meta)| meta.portal_visible())
            .filter_map(|(pk, _)| self.chunks.get(pk).map(|chunk| (*pk, chunk.clone())))
            .collect();

        // Route by the confirmed routing — the ideal may be ahead of the
        // watermark and point at workers that haven't downloaded yet.
        let chunk_workers: BTreeMap<ChunkPk, Vec<WorkerPk>> = portal_chunks
            .keys()
            .filter_map(|pk| {
                self.sched_confirmed_chunk_workers
                    .get(pk)
                    .map(|workers| (*pk, workers.clone()))
            })
            .collect();

        let workers = self.assignment_workers();

        Ok(PortalAssignment {
            id: portal_assignment_id as AssignmentId,
            chunk_workers,
            chunks: portal_chunks,
            workers,
        })
    }

    /// Mark a chunk for removal in a later visibility cycle. The in-memory model does not record
    /// removal time, so `now` is unused.
    fn mark_for_removal(&mut self, chunk_pk: ChunkPk, _now: Tick) -> Result<(), StorageError> {
        if let Some(meta) = self.sched_chunk_metadata.get_mut(&chunk_pk) {
            meta.marked_for_removal = true;
        }
        Ok(())
    }

    fn register_correction(
        &mut self,
        old_pk: ChunkPk,
        new_chunk: NewChunk,
        now: Tick,
    ) -> Result<ChunkPk, StorageError> {
        self.register_correction_int(old_pk, new_chunk, now)
            .map_err(|e| StorageError::CorrectionRejected {
                reason: e.to_string(),
            })
    }
}
