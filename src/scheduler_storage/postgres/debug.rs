use anyhow::Context;

use crate::scheduler_storage::{ChunkPk, StorageError, WorkerPk};

use super::PostgresStorage;

const SCHEDULER_TABLES: &[&str] = &[
    "datasets",
    "chunks",
    "sched_worker_assignments",
    "sched_portal_assignments",
    "sched_workers",
    "sched_worker_confirmations",
    "sched_chunk_metadata",
    "sched_ideal_chunk_workers",
    "sched_confirmed_chunk_workers",
    "sched_worker_assignment_diffs",
    "sched_stale_mappings",
    "chunk_corrections",
];

impl PostgresStorage {
    pub fn table_sizes(&self) -> Result<Vec<(&'static str, i64)>, StorageError> {
        self.with_conn_ref(async move |conn| {
            let mut sizes = Vec::with_capacity(SCHEDULER_TABLES.len());
            for &table in SCHEDULER_TABLES {
                let sql = sqlx::AssertSqlSafe(format!("SELECT count(*) FROM {table}"));
                let count: i64 = sqlx::query_scalar(sql)
                    .fetch_one(&mut *conn)
                    .await
                    .with_context(|| format!("count rows in {table}"))?;
                sizes.push((table, count));
            }
            Ok::<_, StorageError>(sizes)
        })
    }

    /// The stale (draining) `(chunk, worker)` mappings in the current published placement, under the
    /// same servable filter `build_worker_assignment` applies. For offline tooling (reshuffle-sim),
    /// which recovers the ideal placement as published `chunk_workers` minus these.
    pub fn stale_mappings(&self) -> Result<Vec<(ChunkPk, WorkerPk)>, StorageError> {
        self.with_conn_ref(async move |conn| {
            let rows: Vec<(ChunkPk, WorkerPk)> = sqlx::query_as(
                r#"
                SELECT s.chunk_pk, s.worker_id
                FROM sched_stale_mappings s
                JOIN sched_chunk_metadata m ON m.chunk_pk = s.chunk_pk
                WHERE m.dropped_from_worker_assignment_at IS NULL AND NOT m.rejected
                "#,
            )
            .fetch_all(&mut *conn)
            .await
            .context("fetch stale mappings")?;
            Ok::<_, StorageError>(rows)
        })
    }
}
