use anyhow::Context;
use sqlx::postgres::PgConnection;

use crate::scheduler_storage::{ChunkPk, StorageError, WorkerPk};

use super::{PostgresStorage, explain};

/// Run `run`. With `SIM_SQL_EXPLAIN` set, wrap it in `auto_explain` so the plans of the queries it
/// issues (with ANALYZE timings) reach the postgres log — read via `docker logs` — without each call
/// site writing the `SET`/`RESET` itself. Needs `auto_explain` loaded server-side (the sim harness
/// does that). The `RESET` runs even if `run` fails, so it can't leak onto later queries. Off by
/// default `run` runs untouched.
pub(super) async fn with_explain<R>(
    conn: &mut PgConnection,
    run: impl AsyncFnOnce(&mut PgConnection) -> sqlx::Result<R>,
) -> sqlx::Result<R> {
    if !explain::enabled() {
        return run(conn).await;
    }
    sqlx::raw_sql("SET auto_explain.log_analyze = on; SET auto_explain.log_min_duration = 0")
        .execute(&mut *conn)
        .await?;
    let result = run(&mut *conn).await;
    let _ = sqlx::raw_sql("RESET auto_explain.log_min_duration; RESET auto_explain.log_analyze")
        .execute(&mut *conn)
        .await;
    result
}

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

    /// The stale `(chunk, worker)` copies a cycle at `(now, m_ticks)` would expire — the same
    /// predicate [`expire_drained_stale_mappings`](super::scheduling_cycle) runs, as a read. Offline
    /// tooling (reshuffle-sim) queries it before the cycle to attribute a same-worker drain→refetch
    /// that a step-boundary placement diff can't see.
    pub fn expiring_stale_mappings(
        &self,
        now: u64,
        m_ticks: u64,
    ) -> Result<Vec<(ChunkPk, WorkerPk)>, StorageError> {
        use super::rows::tick_to_i64;
        self.with_conn_ref(async move |conn| {
            let rows: Vec<(ChunkPk, WorkerPk)> = sqlx::query_as(
                r#"
                SELECT chunk_pk, worker_id
                FROM sched_stale_mappings
                WHERE dropped_at_portal_assignment_id IS NOT NULL
                  AND dropped_at_portal_assignment_id IN (
                      SELECT id FROM sched_portal_assignments WHERE created_at <= $1 - $2
                  )
                "#,
            )
            .bind(tick_to_i64(now))
            .bind(tick_to_i64(m_ticks))
            .fetch_all(&mut *conn)
            .await
            .context("fetch expiring stale mappings")?;
            Ok::<_, StorageError>(rows)
        })
    }
}
