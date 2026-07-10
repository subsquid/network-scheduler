//! Read-only inspection of scheduler storage for tests.
//!
//! Test-grade API: methods take filter closures and scan storage in full; backends
//! are not expected to optimize them. Production hot paths should use direct,
//! named queries.

use crate::scheduler_storage::NewChunk;
use crate::scheduler_storage::{ChunkPk, Tick, WorkerPk};
use crate::types::Worker;
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkView {
    pub chunk_pk: ChunkPk,
    pub dataset: String,
    pub chunk_id: String,
    pub size: u32,
    /// Inclusive block range as persisted, so tests can verify the round-trip.
    pub blocks: std::ops::RangeInclusive<crate::types::BlockNumber>,
}

/// One row of `sched_chunk_metadata`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkMetadataView {
    pub chunk_pk: ChunkPk,
    pub applied_at_worker_assignment_id: Option<u64>,
    pub applied_at_portal_assignment_id: Option<u64>,
    pub marked_for_removal: bool,
    pub rejected: bool,
    pub dropped_at_portal_assignment_id: Option<u64>,
    pub dropped_from_worker_assignment_at: Option<u64>,
}

/// One row of `sched_stale_mappings`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleMappingView {
    pub chunk_pk: ChunkPk,
    pub worker_id: WorkerPk,
    /// First worker assignment whose ideal dropped the pair (activation gate).
    pub superseded_at_worker_assignment_id: u64,
    /// `None` while pending; the portal assignment anchoring the drain once draining.
    pub dropped_at_portal_assignment_id: Option<u64>,
}

/// One row of `sched_worker_assignment_diffs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerAssignmentDiffView {
    pub worker_assignment_id: u64,
    pub chunk_pk: ChunkPk,
    /// The routing for this chunk as of this assignment; empty = "remove from confirmed routing".
    pub worker_ids: Vec<WorkerPk>,
}

/// One row of `chunk_corrections`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrectionView {
    pub old_chunk_pk: ChunkPk,
    pub new_chunk_pk: ChunkPk,
    /// Denormalized at registration; orders corrections within a dataset.
    pub dataset: String,
    pub created_at: Tick,
    /// `None` while pending; the portal assignment that applied the swap once completed.
    pub applied_at_portal_assignment_id: Option<u64>,
}

impl CorrectionView {
    /// Whether the swap was applied by a visibility cycle.
    pub fn is_completed(&self) -> bool {
        self.applied_at_portal_assignment_id.is_some()
    }
}

/// One row of `sched_workers`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerView {
    pub worker_id: WorkerPk,
    pub peer_id: String,
    pub version: Option<String>,
    pub inactive_since: Option<Tick>,
}

/// Read-only inspector for scheduler storage. See module docs.
pub trait StorageInspect {
    fn get_chunks_metadata<F>(&self, filter: F) -> Vec<ChunkMetadataView>
    where
        F: FnMut(&ChunkMetadataView) -> bool;

    fn get_stale_mappings<F>(&self, filter: F) -> Vec<StaleMappingView>
    where
        F: FnMut(&StaleMappingView) -> bool;

    fn get_workers<F>(&self, filter: F) -> Vec<WorkerView>
    where
        F: FnMut(&WorkerView) -> bool;

    /// Rows of `sched_ideal_chunk_workers` — the ideal mapping only.
    fn get_chunk_workers<F>(&self, filter: F) -> Vec<(ChunkPk, Vec<WorkerPk>)>
    where
        F: FnMut(&ChunkPk, &[WorkerPk]) -> bool;

    fn get_worker_assignment_diffs<F>(&self, filter: F) -> Vec<WorkerAssignmentDiffView>
    where
        F: FnMut(&WorkerAssignmentDiffView) -> bool;

    fn get_corrections<F>(&self, filter: F) -> Vec<CorrectionView>
    where
        F: FnMut(&CorrectionView) -> bool;

    fn get_portal_assignment_created_at(&self, id: u64) -> Option<Tick>;

    /// The current worker-assignment confirmation watermark.
    fn get_worker_assignment_confirmation(&self) -> u64;

    fn get_chunks<F>(&self, filter: F) -> Vec<ChunkView>
    where
        F: FnMut(&ChunkView) -> bool;

    fn get_datasets<F>(&self, filter: F) -> Vec<String>
    where
        F: FnMut(&str) -> bool;

    /// The storage pk of an inserted chunk, looked up by its `(dataset, id)`. Test convenience
    /// over [`get_chunks`](Self::get_chunks).
    fn pk_of(&self, chunk: &NewChunk) -> ChunkPk {
        self.get_chunks(|view| view.dataset == *chunk.dataset && view.chunk_id == *chunk.id)
            .into_iter()
            .next()
            .expect("chunk not inserted")
            .chunk_pk
    }

    /// The `sched_chunk_metadata` row for `pk`. Test convenience over
    /// [`get_chunks_metadata`](Self::get_chunks_metadata).
    fn get_chunk_metadata_by_pk(&self, pk: ChunkPk) -> ChunkMetadataView {
        self.get_chunks_metadata(|view| view.chunk_pk == pk)
            .into_iter()
            .next()
            .expect("metadata not found for chunk")
    }

    /// Build a pk-ordered [`Snapshot`] of the backend's chunks and placement.
    fn snapshot(&self) -> Snapshot {
        let peer_ids: HashMap<WorkerPk, String> = self
            .get_workers(|_| true)
            .into_iter()
            .map(|w| (w.worker_id, w.peer_id))
            .collect();
        let mut ideal_by_pk: HashMap<ChunkPk, BTreeSet<WorkerPk>> = self
            .get_chunk_workers(|_, _| true)
            .into_iter()
            .map(|(pk, wids)| (pk, wids.into_iter().collect()))
            .collect();
        let mut stale_by_pk: HashMap<ChunkPk, BTreeSet<WorkerPk>> = HashMap::new();
        for view in self.get_stale_mappings(|_| true) {
            stale_by_pk
                .entry(view.chunk_pk)
                .or_default()
                .insert(view.worker_id);
        }
        let mut snap = Snapshot {
            chunk_pks: Vec::new(),
            chunk_ids: Vec::new(),
            chunk_sizes: Vec::new(),
            ideal: Vec::new(),
            stale: Vec::new(),
            peer_ids,
        };
        for view in self.get_chunks(|_| true) {
            // Pks are unique, so move the holder sets out rather than clone.
            snap.ideal
                .push(ideal_by_pk.remove(&view.chunk_pk).unwrap_or_default());
            snap.stale
                .push(stale_by_pk.remove(&view.chunk_pk).unwrap_or_default());
            snap.chunk_sizes.push(view.size);
            snap.chunk_ids.push(view.chunk_id);
            snap.chunk_pks.push(view.chunk_pk);
        }
        snap
    }

    /// Pks anywhere in the removal tail — `marked_for_removal` or either `dropped_*` stamp (not just
    /// the `marked_for_removal` state) — excluded from floor obligations: a whole-chunk removal
    /// legitimately goes to zero copies.
    fn chunks_in_removal(&self) -> BTreeSet<ChunkPk> {
        self.get_chunks_metadata(|meta| {
            meta.marked_for_removal
                || meta.dropped_at_portal_assignment_id.is_some()
                || meta.dropped_from_worker_assignment_at.is_some()
        })
        .into_iter()
        .map(|meta| meta.chunk_pk)
        .collect()
    }

    /// Pks rejected at registration for overlapping another chunk's range — never schedulable, so
    /// they carry no floor obligation either.
    fn rejected_chunks(&self) -> BTreeSet<ChunkPk> {
        self.get_chunks_metadata(|meta| meta.rejected)
            .into_iter()
            .map(|meta| meta.chunk_pk)
            .collect()
    }

    /// Gate-A visible chunks: promoted (`applied_at_portal_assignment_id` set) and not yet dropped.
    fn visible_chunks(&self) -> BTreeSet<ChunkPk> {
        self.get_chunks_metadata(|meta| {
            meta.applied_at_portal_assignment_id.is_some()
                && meta.dropped_at_portal_assignment_id.is_none()
        })
        .into_iter()
        .map(|meta| meta.chunk_pk)
        .collect()
    }
}

/// Point-in-time view of a backend's chunks and placement, chunk-pk-ordered. Holders are
/// opaque `WorkerPk`s; `peer_ids` maps them back to peer-id strings. Indices are positional
/// within one snapshot only; cross-snapshot comparisons key by pk.
pub struct Snapshot {
    pub chunk_pks: Vec<ChunkPk>,
    /// Client-assigned `chunk_id` per chunk, parallel to `chunk_pks`.
    pub chunk_ids: Vec<String>,
    pub chunk_sizes: Vec<u32>,
    /// Established (ideal) holders per chunk.
    pub ideal: Vec<BTreeSet<WorkerPk>>,
    /// Draining (stale) holders per chunk.
    pub stale: Vec<BTreeSet<WorkerPk>>,
    /// `WorkerPk` → peer-id string for every worker the backend knows.
    pub peer_ids: HashMap<WorkerPk, String>,
}

impl Snapshot {
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        debug_assert_eq!(
            self.chunk_ids.len(),
            self.chunk_pks.len(),
            "snapshot parallel vectors diverged",
        );
        self.chunk_pks.len()
    }

    /// Physically resident holders (`ideal ∪ stale`) of chunk `i`.
    pub fn physical_holders(&self, i: usize) -> BTreeSet<WorkerPk> {
        self.ideal[i].union(&self.stale[i]).copied().collect()
    }

    /// Physically resident holders (`ideal ∪ stale`) per chunk.
    pub fn physical_holder_sets(&self) -> Vec<BTreeSet<WorkerPk>> {
        (0..self.len()).map(|i| self.physical_holders(i)).collect()
    }

    /// Physically resident holders (`ideal ∪ stale`) keyed by chunk pk.
    pub fn physical_holders_by_pk(&self) -> BTreeMap<ChunkPk, BTreeSet<WorkerPk>> {
        (0..self.len())
            .map(|i| (self.chunk_pks[i], self.physical_holders(i)))
            .collect()
    }

    /// Drop chunks for which `keep` returns false, keeping the parallel vectors aligned.
    /// Lets oracles exclude removing chunks, which legitimately go to zero copies.
    pub fn retain_chunks(&mut self, mut keep: impl FnMut(&ChunkPk) -> bool) {
        let kept: Vec<usize> = (0..self.len())
            .filter(|&i| keep(&self.chunk_pks[i]))
            .collect();
        self.chunk_pks = kept.iter().map(|&i| self.chunk_pks[i]).collect();
        self.chunk_ids = kept.iter().map(|&i| self.chunk_ids[i].clone()).collect();
        self.chunk_sizes = kept.iter().map(|&i| self.chunk_sizes[i]).collect();
        self.ideal = kept.iter().map(|&i| self.ideal[i].clone()).collect();
        self.stale = kept.iter().map(|&i| self.stale[i].clone()).collect();
    }

    /// Total bytes each worker holds across `holders_per_chunk` (parallel to this snapshot).
    pub fn byte_load(&self, holders_per_chunk: &[BTreeSet<WorkerPk>]) -> HashMap<WorkerPk, u64> {
        let mut load = HashMap::new();
        for (chunk, holders) in holders_per_chunk.iter().enumerate() {
            let size = u64::from(self.chunk_sizes[chunk]);
            for &worker in holders {
                *load.entry(worker).or_default() += size;
            }
        }
        load
    }

    /// Pks of `workers` looked up by peer id; workers this snapshot doesn't know are skipped.
    pub fn worker_pks(&self, workers: &[&Worker]) -> Vec<WorkerPk> {
        let pk_of_peer: HashMap<&str, WorkerPk> = self
            .peer_ids
            .iter()
            .map(|(pk, id)| (id.as_str(), *pk))
            .collect();
        workers
            .iter()
            .filter_map(|w| pk_of_peer.get(w.id.to_string().as_str()).copied())
            .collect()
    }
}
