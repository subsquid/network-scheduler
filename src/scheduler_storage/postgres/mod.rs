//! PostgreSQL-backed implementation of [`SchedulerStorage`].
//!
//! Single [`PgConnection`], no pool — the scheduler runs cycles sequentially.
//! The connection holds an advisory lock for the struct's lifetime, so only one
//! scheduler instance runs at a time; dropping the struct releases it.
//!
//! `Tick` values are logical integer timestamps stored in `BIGINT` columns;
//! `m_ticks`/`gc_ticks` are raw tick counts used in integer arithmetic.

// Not yet wired into a production caller; remove once the scheduler loop uses it.
mod nonoverlap;
mod registration;
mod rows;
mod scheduling_cycle;
mod visibility;
mod workers;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use anyhow::Context;
use sqlx::Connection;
use sqlx::postgres::PgConnection;

use crate::metrics::{PhaseTimer, Timer};
use crate::scheduler_storage::algorithm::{ScheduleOutput, SchedulingAlgorithm};
use crate::scheduler_storage::{
    AssignmentId, ChunkPk, PortalAssignment, SchedulerStorage, StorageError, Tick,
    WorkerAssignment, WorkerPk,
};
use crate::types::{Chunk, Worker};

use nonoverlap::Candidate;
use rows::{chunk_from_row, tick_to_i64};

/// Rows per batched write. Caps the jsonb payload and keeps a single statement under Postgres'
/// per-value / message-size limits. The cycle's writes also need whole chunks to stay within one
/// batch, or `array_agg` would split a chunk's holder set across statements.
const DEFAULT_BATCH_SIZE: usize = 10_000;

/// Synchronous facade over a Postgres connection: owns a current-thread tokio
/// runtime and drives all sqlx queries via `block_on`.
///
/// # Panics
///
/// `block_on` panics inside an already-running tokio runtime; an async caller
/// must drive this from a dedicated blocking thread, not an async task.
pub struct PostgresStorage {
    rt: tokio::runtime::Runtime,
    conn: std::cell::RefCell<PgConnection>,
    batch_size: usize,
}

impl PostgresStorage {
    /// Connect and acquire the scheduler advisory lock. Errors if another
    /// scheduler already holds the lock.
    pub fn connect(database_url: &str) -> Result<Self, StorageError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build current-thread runtime")?;
        let (conn, locked) = rt.block_on(async {
            let mut conn = PgConnection::connect(database_url)
                .await
                .context("failed to connect to Postgres")?;
            // Advisory locks are cluster-wide, not per-database; scope the key to the
            // current database, or per-test databases on one cluster collide on the lock.
            let locked: bool = sqlx::query_scalar(
                "SELECT pg_try_advisory_lock(hashtext('network-scheduler:' || current_database()))",
            )
            .fetch_one(&mut conn)
            .await
            .context("failed to acquire advisory lock")?;
            Ok::<_, anyhow::Error>((conn, locked))
        })?;
        if !locked {
            return Err(StorageError::AlreadyRunning);
        }
        Ok(Self {
            rt,
            conn: std::cell::RefCell::new(conn),
            batch_size: DEFAULT_BATCH_SIZE,
        })
    }

    /// Run pending sqlx migrations. Call once on startup before the scheduling loop.
    pub fn migrate(&mut self) -> Result<(), StorageError> {
        self.with_conn(async move |conn| {
            sqlx::migrate!("./migrations")
                .run(&mut *conn)
                .await
                .context("migration failed")?;
            Ok::<_, StorageError>(())
        })
    }

    /// Run an async query closure on the owned runtime with exclusive
    /// connection access. The `AsyncFnOnce` bound lets the future borrow the
    /// `&mut PgConnection` argument, which `FnOnce(_) -> Fut` cannot express.
    fn with_conn<T>(&mut self, f: impl AsyncFnOnce(&mut PgConnection) -> T) -> T {
        let Self { rt, conn, .. } = self;
        rt.block_on(f(conn.get_mut()))
    }

    /// `&self` variant of [`Self::with_conn`] for the test-only
    /// [`StorageInspect`](crate::scheduler_storage::test_harness::inspect::StorageInspect)
    /// reads. The `RefCell` borrow is taken in this sync frame and held across
    /// `block_on`; no guard crosses an `.await`.
    #[cfg(test)]
    fn with_conn_ref<T>(&self, f: impl AsyncFnOnce(&mut PgConnection) -> T) -> T {
        let mut conn = self.conn.borrow_mut();
        self.rt.block_on(f(&mut conn))
    }
}

impl SchedulerStorage for PostgresStorage {
    fn insert_new_datasets(&mut self, datasets: Vec<String>) -> Result<(), StorageError> {
        self.with_conn(async move |conn| -> Result<(), StorageError> {
            let mut timer = PhaseTimer::new("insert_new_datasets");
            for name in &datasets {
                // A duplicate name is rejected by the UNIQUE(name) constraint — surfaced as the
                // database's error, not pre-checked here.
                let res = sqlx::query("INSERT INTO datasets (name) VALUES ($1)")
                    .bind(name)
                    .execute(&mut *conn)
                    .await
                    .context("insert_new_datasets")?;
                timer.stmt(res.rows_affected());
            }
            Ok(())
        })
    }

    fn insert_new_chunks(&mut self, chunks: Vec<Chunk>) -> Result<(), StorageError> {
        let batch_size = self.batch_size;
        self.with_conn(async move |conn| {
            registration::insert_chunks(conn, &chunks, batch_size).await
        })
    }

    fn register_new_chunks(&mut self) -> Result<Vec<ChunkPk>, StorageError> {
        self.with_conn(async move |conn| {
            let _timer = Timer::new("register_new_chunks");

            let candidate_rows = registration::fetch_candidates(conn).await?;
            let registration::Classified {
                exempt,
                candidates,
                range_rejected,
            } = registration::classify(&candidate_rows);

            // Reject candidates overlapping a live chunk in their dataset (indexed SQL probe), then
            // settle overlaps within the batch (two new chunks covering the same range).
            let conflicting = nonoverlap::overlapping_live(&mut *conn, &candidates).await?;
            let (clear, mut rejected): (Vec<Candidate>, Vec<Candidate>) = candidates
                .into_iter()
                .partition(|c| !conflicting.contains(&c.pk));
            let (accepted, batch_rejected) = nonoverlap::settle_within_batch(clear);
            rejected.extend(batch_rejected);
            nonoverlap::report_registration_rejected(&rejected);
            registration::warn_range_rejected(&range_rejected);

            let mut admitted = exempt;
            admitted.extend(&accepted);
            registration::persist_admitted(conn, &admitted).await?;
            let rejected_pks: Vec<ChunkPk> = rejected
                .iter()
                .chain(&range_rejected)
                .map(|c| c.pk)
                .collect();
            registration::persist_rejected(conn, &rejected_pks).await?;

            Ok::<_, StorageError>(admitted)
        })
    }

    fn update_worker_set(
        &mut self,
        active_workers: &[Worker],
        now: Tick,
        gc_ticks: u64,
    ) -> Result<(), StorageError> {
        self.with_conn(async move |conn| {
            let _timer = Timer::new("update_worker_set");
            let peer_ids: Vec<String> = active_workers.iter().map(|w| w.id.to_string()).collect();
            let versions: Vec<Option<String>> = active_workers
                .iter()
                .map(|w| w.version.as_ref().map(|v| v.to_string()))
                .collect();

            let mut tx = conn.begin().await.context("update_worker_set: begin")?;
            workers::upsert_active(&mut tx, &peer_ids, &versions).await?;
            let departed = workers::mark_departed(&mut tx, &peer_ids, now).await?;
            workers::delete_stale_mappings(&mut tx, &departed).await?;
            workers::gc_inactive(&mut tx, now, gc_ticks).await?;
            tx.commit().await.context("update_worker_set: commit")?;

            Ok::<_, StorageError>(())
        })
    }

    fn run_scheduling_cycle<A>(
        &mut self,
        algorithm: &A,
        config: &A::Config,
        now: Tick,
        m_ticks: u64,
    ) -> Result<WorkerAssignment, StorageError>
    where
        A: SchedulingAlgorithm + Send + Sync,
    {
        use scheduling_cycle as phase;

        let batch_size = self.batch_size;
        self.with_conn(async move |conn| {
            let _timer = Timer::new("run_scheduling_cycle");
            // Phase A — clock-driven GC, committed up front so it survives a Phase B
            // shortage rollback; otherwise stale never drains under a sustained shortage.
            let mut gc_tx = conn
                .begin()
                .await
                .context("run_scheduling_cycle: begin gc")?;
            let new_wa_id = phase::open_worker_assignment(&mut gc_tx, now).await?;
            phase::tombstone_expired_chunks(&mut gc_tx, new_wa_id, now, m_ticks).await?;
            phase::expire_drained_stale_mappings(&mut gc_tx, now, m_ticks).await?;
            gc_tx
                .commit()
                .await
                .context("run_scheduling_cycle: commit gc")?;

            // Phase B — placement reconcile; rolls back on shortage, leaving Phase A committed.
            let mut tx = conn.begin().await.context("run_scheduling_cycle: begin")?;

            // Ideal rows are reused below for prev-ideal diffing (same transaction).
            let ideal_placement_rows = phase::fetch_ideal_placement_rows(&mut tx).await?;
            let stale_placement_rows = phase::fetch_stale_placement_rows(&mut tx).await?;
            // The algorithm and the storage helpers now share the surrogate newtypes
            // (ChunkPk/WorkerPk), so no conversion is needed at this boundary.
            let current_placement =
                phase::merge_current_placement(&ideal_placement_rows, &stale_placement_rows);

            let chunk_rows = phase::fetch_active_chunks(&mut tx).await?;
            let worker_rows = phase::fetch_workers(&mut tx).await?;

            // Decode the active chunks for the algorithm. build_worker_assignment re-fetches the
            // (much smaller) placed subset itself, so we don't keep a full chunk map around.
            let mut datasets = rows::DatasetInterner::new();
            let mut chunks_for_algo: Vec<(ChunkPk, Chunk)> = Vec::with_capacity(chunk_rows.len());
            for row in &chunk_rows {
                let chunk = chunk_from_row(row, datasets.intern(&row.dataset_name))?;
                chunks_for_algo.push((row.chunk_pk, chunk));
            }

            let phase::DecodedWorkers {
                published: workers_map,
                for_algo: workers_for_algo,
            } = phase::decode_workers(&worker_rows)?;

            // The algorithm's only `Err` is infeasibility, i.e. a shortage.
            let ScheduleOutput {
                mapping,
                replication_by_weight,
            } = algorithm
                .schedule(
                    chunks_for_algo,
                    workers_for_algo,
                    &current_placement,
                    config,
                )
                .map_err(|_| StorageError::Shortage)?;

            let ideal_mappings = mapping;

            let mut prev_ideal: BTreeMap<ChunkPk, std::collections::HashSet<WorkerPk>> =
                BTreeMap::new();
            for row in &ideal_placement_rows {
                prev_ideal.insert(row.chunk_pk, row.worker_ids.iter().copied().collect());
            }

            let being_removed = phase::fetch_being_removed_chunks(&mut tx).await?;

            phase::mint_superseded_stale_mappings(
                &mut tx,
                new_wa_id,
                &prev_ideal,
                &ideal_mappings,
                &workers_map,
                &being_removed,
                batch_size,
            )
            .await?;
            phase::record_routing_diffs(
                &mut tx,
                new_wa_id,
                &prev_ideal,
                &ideal_mappings,
                batch_size,
            )
            .await?;
            phase::commit_new_ideal(&mut tx, &prev_ideal, &ideal_mappings, batch_size).await?;
            phase::resolve_flip_flops(&mut tx, &ideal_mappings, batch_size).await?;
            phase::stamp_entered_chunks(&mut tx, new_wa_id, &ideal_mappings).await?;

            tx.commit().await.context("run_scheduling_cycle: commit")?;

            let wa = phase::build_worker_assignment(
                conn,
                new_wa_id,
                &workers_map,
                replication_by_weight,
            )
            .await?;
            Ok::<_, StorageError>(wa)
        })
    }

    fn confirm_worker_assignment(
        &mut self,
        assignment_id: AssignmentId,
        now: Tick,
    ) -> Result<(), StorageError> {
        use visibility as phase;

        self.with_conn(async move |conn| {
            let _timer = Timer::new("confirm_worker_assignment");
            let mut tx = conn
                .begin()
                .await
                .context("confirm_worker_assignment: begin")?;

            let prev = phase::confirmation_watermark(&mut tx).await?;

            if assignment_id <= prev {
                tx.commit()
                    .await
                    .context("confirm_worker_assignment: commit (no-op)")?;
                return Ok(());
            }

            phase::advance_confirmation_watermark(&mut tx, assignment_id, now).await?;
            phase::replay_confirmed_diffs(&mut tx, prev, assignment_id).await?;
            phase::drop_replayed_diffs(&mut tx, prev, assignment_id).await?;

            tx.commit()
                .await
                .context("confirm_worker_assignment: commit")?;
            Ok::<_, StorageError>(())
        })
    }

    fn run_visibility_cycle(&mut self, now: Tick) -> Result<PortalAssignment, StorageError> {
        use visibility as phase;

        self.with_conn(async move |conn| {
            let _timer = Timer::new("run_visibility_cycle");
            let mut tx = conn.begin().await.context("run_visibility_cycle: begin")?;

            let new_pa_id = phase::open_portal_assignment(&mut tx, now).await?;
            let confirmed_up_to = phase::confirmation_watermark(&mut tx).await?;

            phase::apply_ready_corrections(&mut tx, new_pa_id, confirmed_up_to, now).await?;
            phase::promote_eligible_chunks(&mut tx, new_pa_id, confirmed_up_to).await?;
            phase::drop_marked_chunks(&mut tx, new_pa_id).await?;
            phase::activate_confirmed_drains(&mut tx, new_pa_id, confirmed_up_to).await?;

            tx.commit().await.context("run_visibility_cycle: commit")?;

            let chunks = phase::fetch_portal_visible_chunks(conn).await?;
            let portal_pks: Vec<ChunkPk> = chunks.keys().copied().collect();
            let chunk_workers = phase::fetch_confirmed_routing(conn, &portal_pks).await?;
            let workers = phase::fetch_portal_workers(conn).await?;

            Ok::<_, StorageError>(phase::assemble_portal_assignment(
                new_pa_id,
                chunks,
                chunk_workers,
                workers,
            ))
        })
    }

    fn mark_for_removal(&mut self, chunk_pk: ChunkPk, now: Tick) -> Result<(), StorageError> {
        self.with_conn(async move |conn| {
            let mut timer = PhaseTimer::new("mark_for_removal");
            let res = sqlx::query(
                "UPDATE sched_chunk_metadata SET marked_for_removal = $2 WHERE chunk_pk = $1",
            )
            .bind(chunk_pk)
            .bind(tick_to_i64(now))
            .execute(&mut *conn)
            .await
            .context("mark_for_removal")?;
            timer.stmt(res.rows_affected());
            Ok::<_, StorageError>(())
        })
    }

    #[cfg(test)]
    fn register_correction(
        &mut self,
        old_pk: ChunkPk,
        new_chunk: Chunk,
        now: Tick,
    ) -> Result<ChunkPk, StorageError> {
        // Implemented in `tests::correction`, alongside the other postgres test-only code.
        self.register_correction_impl(old_pk, new_chunk, now)
    }
}
