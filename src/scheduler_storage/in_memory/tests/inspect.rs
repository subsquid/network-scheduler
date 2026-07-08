//! Read-only [`StorageInspect`] view over [`InMemoryStorage`] internals. Test-support only,
//! consumed by the storage tests and the multistep simulation harness.

use super::super::{ChunkPk, InMemoryStorage, WorkerPk};
use crate::scheduler_storage::Tick;
use crate::scheduler_storage::test_harness::inspect::{
    ChunkMetadataView, ChunkView, CorrectionView, StaleMappingView, StorageInspect,
    WorkerAssignmentDiffView, WorkerView,
};

impl StorageInspect for InMemoryStorage {
    fn get_chunks_metadata<F>(&self, mut filter: F) -> Vec<ChunkMetadataView>
    where
        F: FnMut(&ChunkMetadataView) -> bool,
    {
        self.sched_chunk_metadata
            .iter()
            .filter_map(|(pk, meta)| {
                let view = ChunkMetadataView {
                    chunk_pk: *pk,
                    applied_at_worker_assignment_id: meta.applied_at_worker_assignment_id,
                    applied_at_portal_assignment_id: meta.applied_at_portal_assignment_id,
                    marked_for_removal: meta.marked_for_removal,
                    rejected: meta.rejected,
                    dropped_at_portal_assignment_id: meta.dropped_at_portal_assignment_id,
                    dropped_from_worker_assignment_at: meta.dropped_from_worker_assignment_at,
                };
                filter(&view).then_some(view)
            })
            .collect()
    }

    fn get_stale_mappings<F>(&self, mut filter: F) -> Vec<StaleMappingView>
    where
        F: FnMut(&StaleMappingView) -> bool,
    {
        self.sched_stale_mappings
            .iter()
            .filter_map(|((pk, worker_id), mapping)| {
                let view = StaleMappingView {
                    chunk_pk: *pk,
                    worker_id: *worker_id,
                    superseded_at_worker_assignment_id: mapping.superseded_at_worker_assignment_id,
                    dropped_at_portal_assignment_id: mapping.dropped_at_portal_assignment_id,
                };
                filter(&view).then_some(view)
            })
            .collect()
    }

    fn get_workers<F>(&self, mut filter: F) -> Vec<WorkerView>
    where
        F: FnMut(&WorkerView) -> bool,
    {
        self.sched_workers
            .iter()
            .filter_map(|(id, entry)| {
                let view = WorkerView {
                    worker_id: *id,
                    peer_id: entry.peer_id.to_string(),
                    version: entry.version.as_ref().map(|v| v.to_string()),
                    inactive_since: entry.inactive_since,
                };
                filter(&view).then_some(view)
            })
            .collect()
    }

    fn get_chunk_workers<F>(&self, mut filter: F) -> Vec<(ChunkPk, Vec<WorkerPk>)>
    where
        F: FnMut(&ChunkPk, &[WorkerPk]) -> bool,
    {
        self.sched_ideal_chunk_workers
            .iter()
            .filter(|&(pk, workers)| filter(pk, workers))
            .map(|(pk, workers)| (*pk, workers.clone()))
            .collect()
    }

    fn get_worker_assignment_diffs<F>(&self, mut filter: F) -> Vec<WorkerAssignmentDiffView>
    where
        F: FnMut(&WorkerAssignmentDiffView) -> bool,
    {
        self.sched_worker_assignment_diffs
            .iter()
            .flat_map(|(wa_id, diff)| {
                diff.changes
                    .iter()
                    .map(move |(chunk_pk, worker_ids)| WorkerAssignmentDiffView {
                        worker_assignment_id: *wa_id,
                        chunk_pk: *chunk_pk,
                        worker_ids: worker_ids.clone(),
                    })
            })
            .filter(|view| filter(view))
            .collect()
    }

    fn get_corrections<F>(&self, mut filter: F) -> Vec<CorrectionView>
    where
        F: FnMut(&CorrectionView) -> bool,
    {
        self.chunk_corrections
            .iter()
            .filter_map(|(old_pk, correction)| {
                let view = CorrectionView {
                    old_chunk_pk: *old_pk,
                    new_chunk_pk: correction.new_chunk_pk,
                    dataset: correction.dataset.clone(),
                    created_at: correction.created_at,
                    applied_at_portal_assignment_id: correction.applied_at_portal_assignment_id,
                };
                filter(&view).then_some(view)
            })
            .collect()
    }

    fn get_portal_assignment_created_at(&self, id: u64) -> Option<Tick> {
        self.sched_portal_assignments.get(&id).copied()
    }

    fn get_worker_assignment_confirmation(&self) -> u64 {
        self.sched_worker_assignment_confirmation
    }

    fn get_chunks<F>(&self, mut filter: F) -> Vec<ChunkView>
    where
        F: FnMut(&ChunkView) -> bool,
    {
        self.chunks
            .iter()
            .filter_map(|(pk, c)| {
                let view = ChunkView {
                    chunk_pk: *pk,
                    dataset: (*c.dataset).clone(),
                    chunk_id: (*c.id).clone(),
                    size: c.size,
                    blocks: c.blocks.clone(),
                };
                filter(&view).then_some(view)
            })
            .collect()
    }

    fn get_datasets<F>(&self, mut filter: F) -> Vec<String>
    where
        F: FnMut(&str) -> bool,
    {
        self.datasets
            .iter()
            .filter(|&d| filter(d))
            .cloned()
            .collect()
    }
}
