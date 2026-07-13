//! Test-only read-only inspection of [`PostgresStorage`] via [`StorageInspect`].

use crate::scheduler_storage::test_harness::inspect::{
    ChunkMetadataView, ChunkView, CorrectionView, StaleMappingView, StorageInspect,
    WorkerAssignmentDiffView, WorkerView,
};
use crate::scheduler_storage::{ChunkPk, Tick, WorkerPk};

use super::super::PostgresStorage;
use super::super::rows::block_range_from_columns;

impl StorageInspect for PostgresStorage {
    fn get_chunks<F>(&self, mut filter: F) -> Vec<ChunkView>
    where
        F: FnMut(&ChunkView) -> bool,
    {
        let rows: Vec<(i64, String, String, i32, i64, i32)> = self
            .with_conn_ref(async move |conn| {
                sqlx::query_as(
                    "SELECT c.chunk_pk, d.name, c.chunk_id, c.size, c.first_block, c.last_block_delta \
                     FROM chunks c JOIN datasets d ON d.id = c.dataset_id \
                     ORDER BY c.chunk_pk",
                )
                .fetch_all(conn)
                .await
            })
            .expect("get_chunks query");
        rows.into_iter()
            .map(
                |(chunk_pk, dataset, chunk_id, size, first_block, last_block_delta)| ChunkView {
                    chunk_pk: ChunkPk(chunk_pk),
                    dataset,
                    chunk_id,
                    size: size as u32,
                    blocks: block_range_from_columns(first_block, last_block_delta),
                },
            )
            .filter(|view| filter(view))
            .collect()
    }

    fn get_chunks_metadata<F>(&self, mut filter: F) -> Vec<ChunkMetadataView>
    where
        F: FnMut(&ChunkMetadataView) -> bool,
    {
        // `sqlx::query_as` needs the full row tuple type.
        #[allow(clippy::type_complexity)]
        let rows: Vec<(
            i64,
            Option<i64>,
            Option<i64>,
            bool,
            Option<i64>,
            Option<i64>,
            bool,
        )> = self
            .with_conn_ref(async move |conn| {
                sqlx::query_as(
                    "SELECT chunk_pk, \
                            applied_at_worker_assignment_id, \
                            applied_at_portal_assignment_id, \
                            (marked_for_removal IS NOT NULL), \
                            dropped_at_portal_assignment_id, \
                            dropped_from_worker_assignment_at, \
                            rejected \
                     FROM sched_chunk_metadata \
                     ORDER BY chunk_pk",
                )
                .fetch_all(conn)
                .await
            })
            .expect("get_chunks_metadata query");
        rows.into_iter()
            .map(
                |(
                    chunk_pk,
                    applied_at_worker_assignment_id,
                    applied_at_portal_assignment_id,
                    marked_for_removal,
                    dropped_at_portal_assignment_id,
                    dropped_from_worker_assignment_at,
                    rejected,
                )| ChunkMetadataView {
                    chunk_pk: ChunkPk(chunk_pk),
                    applied_at_worker_assignment_id: applied_at_worker_assignment_id
                        .map(|v| v as u64),
                    applied_at_portal_assignment_id: applied_at_portal_assignment_id
                        .map(|v| v as u64),
                    marked_for_removal,
                    rejected,
                    dropped_at_portal_assignment_id: dropped_at_portal_assignment_id
                        .map(|v| v as u64),
                    dropped_from_worker_assignment_at: dropped_from_worker_assignment_at
                        .map(|v| v as u64),
                },
            )
            .filter(|view| filter(view))
            .collect()
    }

    fn get_stale_mappings<F>(&self, mut filter: F) -> Vec<StaleMappingView>
    where
        F: FnMut(&StaleMappingView) -> bool,
    {
        // The drain anchor is derived, not stored (see the migration): the first portal
        // assignment whose recorded watermark covered the mapping's superseding assignment —
        // exactly the id the per-row stamp used to hold, since no assignment can cover a mapping
        // before its superseding assignment is confirmed.
        let rows: Vec<(i64, i64, i64, Option<i64>)> = self
            .with_conn_ref(async move |conn| {
                sqlx::query_as(
                    "SELECT s.chunk_pk, s.worker_id, \
                            s.superseded_at_worker_assignment_id, \
                            (SELECT MIN(p.id) FROM sched_portal_assignments p \
                             WHERE p.confirmed_up_to >= s.superseded_at_worker_assignment_id) \
                     FROM sched_stale_mappings s \
                     ORDER BY s.chunk_pk, s.worker_id",
                )
                .fetch_all(conn)
                .await
            })
            .expect("get_stale_mappings query");
        rows.into_iter()
            .map(
                |(
                    chunk_pk,
                    worker_id,
                    superseded_at_worker_assignment_id,
                    dropped_at_portal_assignment_id,
                )| {
                    StaleMappingView {
                        chunk_pk: ChunkPk(chunk_pk),
                        worker_id: WorkerPk(worker_id),
                        superseded_at_worker_assignment_id: superseded_at_worker_assignment_id
                            as u64,
                        dropped_at_portal_assignment_id: dropped_at_portal_assignment_id
                            .map(|v| v as u64),
                    }
                },
            )
            .filter(|view| filter(view))
            .collect()
    }

    fn get_workers<F>(&self, mut filter: F) -> Vec<WorkerView>
    where
        F: FnMut(&WorkerView) -> bool,
    {
        let rows: Vec<(i64, String, Option<String>, Option<i64>)> = self
            .with_conn_ref(async move |conn| {
                sqlx::query_as(
                    "SELECT id, peer_id, version, inactive_since \
                     FROM sched_workers \
                     ORDER BY id",
                )
                .fetch_all(conn)
                .await
            })
            .expect("get_workers query");
        rows.into_iter()
            .map(|(worker_id, peer_id, version, inactive_since)| WorkerView {
                worker_id: WorkerPk(worker_id),
                peer_id,
                version,
                inactive_since: inactive_since.map(|v| v as Tick),
            })
            .filter(|view| filter(view))
            .collect()
    }

    fn get_chunk_workers<F>(&self, mut filter: F) -> Vec<(ChunkPk, Vec<WorkerPk>)>
    where
        F: FnMut(&ChunkPk, &[WorkerPk]) -> bool,
    {
        let rows: Vec<(i64, Vec<i64>)> = self
            .with_conn_ref(async move |conn| {
                sqlx::query_as(
                    "SELECT chunk_pk, worker_ids \
                     FROM sched_ideal_chunk_workers \
                     ORDER BY chunk_pk",
                )
                .fetch_all(conn)
                .await
            })
            .expect("get_chunk_workers query");
        rows.into_iter()
            .map(|(chunk_pk, worker_ids)| {
                (
                    ChunkPk(chunk_pk),
                    worker_ids.into_iter().map(WorkerPk).collect::<Vec<_>>(),
                )
            })
            .filter(|(chunk_pk, worker_ids)| filter(chunk_pk, worker_ids))
            .collect()
    }

    fn get_worker_assignment_diffs<F>(&self, mut filter: F) -> Vec<WorkerAssignmentDiffView>
    where
        F: FnMut(&WorkerAssignmentDiffView) -> bool,
    {
        let rows: Vec<(i64, i64, Vec<i64>)> = self
            .with_conn_ref(async move |conn| {
                sqlx::query_as(
                    "SELECT worker_assignment_id, chunk_pk, worker_ids \
                     FROM sched_worker_assignment_diffs \
                     ORDER BY worker_assignment_id, chunk_pk",
                )
                .fetch_all(&mut *conn)
                .await
            })
            .expect("get_worker_assignment_diffs query");
        rows.into_iter()
            .map(
                |(worker_assignment_id, chunk_pk, worker_ids)| WorkerAssignmentDiffView {
                    worker_assignment_id: worker_assignment_id as u64,
                    chunk_pk: ChunkPk(chunk_pk),
                    worker_ids: worker_ids.into_iter().map(WorkerPk).collect(),
                },
            )
            .filter(|view| filter(view))
            .collect()
    }

    fn get_corrections<F>(&self, mut filter: F) -> Vec<CorrectionView>
    where
        F: FnMut(&CorrectionView) -> bool,
    {
        let rows: Vec<(i64, i64, String, i64, Option<i64>)> = self
            .with_conn_ref(async move |conn| {
                sqlx::query_as(
                    "SELECT cc.old_chunk_pk, cc.new_chunk_pk, d.name, cc.created_at, \
                            cc.applied_at_portal_assignment_id \
                     FROM chunk_corrections cc JOIN datasets d ON d.id = cc.dataset_id \
                     ORDER BY cc.old_chunk_pk",
                )
                .fetch_all(conn)
                .await
            })
            .expect("get_corrections query");
        rows.into_iter()
            .map(
                |(old_chunk_pk, new_chunk_pk, dataset, created_at, applied)| CorrectionView {
                    old_chunk_pk: ChunkPk(old_chunk_pk),
                    new_chunk_pk: ChunkPk(new_chunk_pk),
                    dataset,
                    created_at: created_at as Tick,
                    applied_at_portal_assignment_id: applied.map(|id| id as u64),
                },
            )
            .filter(|view| filter(view))
            .collect()
    }

    fn get_portal_assignment_created_at(&self, id: u64) -> Option<Tick> {
        let result: Option<i64> = self
            .with_conn_ref(async move |conn| {
                sqlx::query_scalar("SELECT created_at FROM sched_portal_assignments WHERE id = $1")
                    .bind(i64::try_from(id).expect("portal assignment id overflows i64"))
                    .fetch_optional(conn)
                    .await
            })
            .expect("get_portal_assignment_created_at query");
        result.map(|v| v as Tick)
    }

    fn get_worker_assignment_confirmation(&self) -> u64 {
        let result: i64 = self
            .with_conn_ref(async move |conn| {
                sqlx::query_scalar(
                    "SELECT COALESCE(MAX(assignment_id), 0) FROM sched_worker_confirmations",
                )
                .fetch_one(conn)
                .await
            })
            .expect("get_worker_assignment_confirmation query");
        result as u64
    }

    fn get_datasets<F>(&self, mut filter: F) -> Vec<String>
    where
        F: FnMut(&str) -> bool,
    {
        let rows: Vec<(String,)> = self
            .with_conn_ref(async move |conn| {
                sqlx::query_as("SELECT name FROM datasets ORDER BY name")
                    .fetch_all(conn)
                    .await
            })
            .expect("get_datasets query");
        rows.into_iter()
            .map(|(name,)| name)
            .filter(|dataset| filter(dataset))
            .collect()
    }
}
