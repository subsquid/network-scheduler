//! PostgreSQL-backed implementation of [`SchedulerStorage`].
//!
//! Single [`PgConnection`], no pool — the scheduler runs cycles sequentially.
//! The connection holds an advisory lock for the struct's lifetime, so only one
//! scheduler instance runs at a time; dropping the struct releases it.
//!
//! `Tick` values are logical integer timestamps stored in `BIGINT` columns;
//! `m_ticks`/`gc_ticks` are raw tick counts used in integer arithmetic.

// `register_correction`; also enabled by `pg-testkit` for offline tools (reshuffle-sim).
#[cfg(any(test, feature = "pg-testkit"))]
mod correction;
mod debug;
pub mod explain;
// Not yet wired into a production caller; remove once the scheduler loop uses it.
mod nonoverlap;
mod registration;
mod rows;
mod scheduling_cycle;
mod schema;
#[cfg(any(test, feature = "pg-testkit"))]
mod testkit;
mod visibility;
mod workers;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context;
use sqlx::Connection;
use sqlx::postgres::PgConnection;

use crate::metrics::{PhaseTimer, Timer};
use crate::scheduler_storage::algorithm::{CurrentPlacement, ScheduleOutput, SchedulingAlgorithm};
use crate::scheduler_storage::{
    AssignmentId, ChunkPk, DatasetPk, NewChunk, PortalAssignment, SchedulerStorage, SchemaBundle,
    SchemaId, StorageError, Tick, WorkerAssignment,
};
use crate::types::{Dataset, DatasetSchema, DatasetWatermark, Worker};

use nonoverlap::Candidate;
use rows::tick_to_i64;

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
            // Session-scoped GUCs, held for this connection's lifetime: raise sort/hash
            // memory for the cycle's heavy routing and visibility queries.
            for stmt in [
                "SET work_mem = '512MB'",
                "SET maintenance_work_mem = '512MB'",
            ] {
                sqlx::query(stmt)
                    .execute(&mut conn)
                    .await
                    .with_context(|| format!("failed to run {stmt}"))?;
            }
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

    /// Existing datasets, each with the S3-discovery watermark for its last chunk (its id and end
    /// block), or `None` when the dataset has no chunks yet. A dataset absent from the result has no
    /// row yet and must be created before its chunks can be registered.
    pub fn datasets_with_last_chunk(&self) -> Result<Vec<DatasetWatermark>, StorageError> {
        self.with_conn_ref(async move |conn| {
            let rows: Vec<(String, Option<String>, Option<i64>)> = sqlx::query_as(
                "SELECT d.name, lc.chunk_id, lc.last_block
                 FROM datasets d
                 LEFT JOIN LATERAL (
                     SELECT c.chunk_id, c.first_block + c.last_block_delta AS last_block
                     FROM chunks c
                     LEFT JOIN sched_chunk_metadata m ON m.chunk_pk = c.chunk_pk
                     WHERE c.dataset_id = d.id AND m.rejected IS NOT TRUE
                     ORDER BY c.first_block DESC
                     LIMIT 1
                 ) lc ON true",
            )
            .fetch_all(&mut *conn)
            .await
            .context("fetch dataset watermarks")?;
            Ok(rows
                .into_iter()
                .map(|(name, chunk_id, last_block)| DatasetWatermark {
                    dataset: Dataset {
                        id: std::sync::Arc::new(name),
                        height: last_block.map(|b| b as u64),
                    },
                    last_chunk_id: chunk_id.map(std::sync::Arc::new),
                })
                .collect())
        })
    }

    /// Run an async query closure on the owned runtime with exclusive
    /// connection access. The `AsyncFnOnce` bound lets the future borrow the
    /// `&mut PgConnection` argument, which `FnOnce(_) -> Fut` cannot express.
    fn with_conn<T>(&mut self, f: impl AsyncFnOnce(&mut PgConnection) -> T) -> T {
        let Self { rt, conn, .. } = self;
        rt.block_on(f(conn.get_mut()))
    }

    /// `&self` variant of [`Self::with_conn`] for read-only callers: the test-only
    /// [`StorageInspect`](crate::scheduler_storage::test_harness::inspect::StorageInspect)
    /// reads and [`Self::table_sizes`]. The `RefCell` borrow is taken in this sync frame
    /// and held across `block_on`; no guard crosses an `.await`.
    fn with_conn_ref<T>(&self, f: impl AsyncFnOnce(&mut PgConnection) -> T) -> T {
        let mut conn = self.conn.borrow_mut();
        self.rt.block_on(f(&mut conn))
    }

    /// Register same-range corrections in bulk: one transaction, all-or-nothing. Returns the
    /// replacement pks in input order. The trait's `register_correction` is a batch of one.
    #[cfg(any(test, feature = "pg-testkit"))]
    pub fn register_corrections(
        &mut self,
        corrections: Vec<(ChunkPk, NewChunk)>,
        now: Tick,
    ) -> Result<Vec<ChunkPk>, StorageError> {
        if corrections.is_empty() {
            return Ok(Vec::new());
        }
        let batch_size = self.batch_size;
        self.with_conn(async move |conn| {
            correction::register_corrections(conn, &corrections, now, batch_size).await
        })
    }

    /// Clone every live chunk of dataset `src` into dataset `dst`, server-side; the next
    /// `register_new_chunks` admits the clones. Returns the number of clones.
    #[cfg(any(test, feature = "pg-testkit"))]
    pub fn copy_dataset_chunks(&mut self, src: &str, dst: &str) -> Result<u64, StorageError> {
        self.with_conn(async move |conn| testkit::copy_dataset_chunks(conn, src, dst).await)
    }
}

impl SchedulerStorage for PostgresStorage {
    fn insert_new_datasets(
        &mut self,
        datasets: Vec<(String, DatasetSchema)>,
    ) -> Result<(), StorageError> {
        self.with_conn(async move |conn| -> Result<(), StorageError> {
            let mut timer = PhaseTimer::new("insert_new_datasets");
            let mut tx = conn.begin().await.context("insert_new_datasets: begin")?;
            for (name, dataset_schema) in &datasets {
                let dataset_id: DatasetPk =
                    sqlx::query_scalar("INSERT INTO datasets (name) VALUES ($1) RETURNING id")
                        .bind(name)
                        .fetch_one(&mut *tx)
                        .await
                        .context("insert_new_datasets")?;
                schema::ensure_current_schema(&mut tx, dataset_id, dataset_schema).await?;
                timer.stmt(3); // dataset insert + schema supersede + activate
            }
            tx.commit().await.context("insert_new_datasets: commit")?;
            Ok(())
        })
    }

    fn set_dataset_schema(
        &mut self,
        dataset: &str,
        dataset_schema: DatasetSchema,
    ) -> Result<(), StorageError> {
        self.with_conn(async move |conn| -> Result<(), StorageError> {
            let mut tx = conn.begin().await.context("set_dataset_schema: begin")?;
            let dataset_id: Option<DatasetPk> =
                sqlx::query_scalar("SELECT id FROM datasets WHERE name = $1")
                    .bind(dataset)
                    .fetch_optional(&mut *tx)
                    .await
                    .context("set_dataset_schema: lookup")?;
            let Some(dataset_id) = dataset_id else {
                return Err(
                    anyhow::anyhow!("set_dataset_schema: dataset {dataset} not found").into(),
                );
            };
            schema::ensure_current_schema(&mut tx, dataset_id, &dataset_schema).await?;
            tx.commit().await.context("set_dataset_schema: commit")?;
            Ok(())
        })
    }

    fn load_schemas(
        &self,
        schema_ids: Option<&[crate::scheduler_storage::SchemaId]>,
    ) -> Result<BTreeMap<crate::scheduler_storage::SchemaId, DatasetSchema>, StorageError> {
        self.with_conn_ref(async move |conn| {
            schema::load_schemas(conn, schema_ids)
                .await
                .map_err(StorageError::from)
        })
    }

    fn active_schema_bundle(
        &self,
    ) -> Result<BTreeMap<crate::scheduler_storage::SchemaId, DatasetSchema>, StorageError> {
        self.with_conn_ref(async move |conn| {
            schema::active_schema_bundle(conn)
                .await
                .map_err(StorageError::from)
        })
    }

    fn insert_new_chunks(&mut self, chunks: Vec<NewChunk>) -> Result<(), StorageError> {
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
            workers::promote_orphaned_drains(&mut tx, &departed).await?;
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
    ) -> Result<(WorkerAssignment, SchemaBundle), StorageError>
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
            phase::tombstone_expired_chunks(&mut gc_tx, now, m_ticks).await?;
            phase::expire_drained_stale_mappings(&mut gc_tx, now, m_ticks).await?;
            gc_tx
                .commit()
                .await
                .context("run_scheduling_cycle: commit gc")?;

            // Phase B — placement reconcile; rolls back on shortage, leaving Phase A committed.
            let mut tx = conn.begin().await.context("run_scheduling_cycle: begin")?;

            // One streamed round-trip decoding the algorithm's inputs and the published chunk
            // columns together, so the post-commit assignment build needn't re-read them.
            let phase::ActiveChunks {
                for_algo: chunks_for_algo,
                current_placement,
                committed_placement,
                published: published_chunks,
                bundle_schema_ids,
            } = phase::fetch_active_chunks_with_placement(&mut tx).await?;

            // Confirmed routing for the eviction victim-ordering hint (best-effort). Read in the same
            // tx as the placement so both see a consistent snapshot; filtered to portal-visible chunks
            // server-side, matching the in-memory backend's routed set.
            let confirmed_routing: CurrentPlacement = visibility::fetch_confirmed_routing(&mut tx)
                .await?
                .into_iter()
                .collect();

            let worker_rows = phase::fetch_workers(&mut tx).await?;
            let phase::DecodedWorkers {
                published: workers_map,
                for_algo: workers_for_algo,
            } = phase::decode_workers(&worker_rows)?;

            let ScheduleOutput {
                mapping: ideal_mappings,
                replication_by_weight,
                evicted,
            } = {
                let mut timer = PhaseTimer::new("run_scheduling_cycle:schedule");
                let chunk_count = chunks_for_algo.len() as u64;
                let out = algorithm
                    .schedule(
                        chunks_for_algo,
                        workers_for_algo,
                        &current_placement,
                        &committed_placement,
                        &confirmed_routing,
                        config,
                    )
                    .map_err(|_| StorageError::Shortage)?;
                timer.items(chunk_count);
                out
            };

            let new_wa_id = phase::open_worker_assignment(&mut tx, now).await?;

            // Stage the new ideal into the future twin, then diff it against the live ideal to
            // derive the cycle's deltas and swap the twins.
            phase::write_future_ideal(&mut tx, &ideal_mappings, batch_size).await?;
            phase::apply_deltas_and_swap(&mut tx, new_wa_id, &evicted).await?;

            tx.commit().await.context("run_scheduling_cycle: commit")?;

            let wa = phase::build_worker_assignment(
                conn,
                new_wa_id,
                workers_map,
                replication_by_weight,
                ideal_mappings,
                published_chunks,
            )
            .await?;

            // Bundle covers the assignment's chunks (ideal ∪ stale) plus every chunk in its
            // routable window — entered a worker assignment, not yet tombstoned — so a chunk the
            // latest assignment dropped but an earlier confirmed entry still routes to keeps its
            // schema (ADR 0002). Only the content load hits the DB, and schema content is
            // immutable per id, so the by-id read is safe under concurrent writers.
            let mut schema_ids: BTreeSet<SchemaId> = wa.schema_ids();
            schema_ids.extend(bundle_schema_ids);
            let schema_ids: Vec<SchemaId> = schema_ids.into_iter().collect();
            let bundle =
                SchemaBundle::from_schemas(schema::load_schemas(conn, Some(&schema_ids)).await?);
            Ok::<_, StorageError>((wa, bundle))
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

            // Watermark read first so the portal assignment records it (activates drains).
            let confirmed_up_to = phase::confirmation_watermark(&mut tx).await?;
            let new_pa_id = phase::open_portal_assignment(&mut tx, now, confirmed_up_to).await?;

            phase::apply_ready_corrections(&mut tx, new_pa_id, confirmed_up_to, now).await?;
            phase::promote_eligible_chunks(&mut tx, new_pa_id, confirmed_up_to).await?;
            phase::drop_marked_chunks(&mut tx, new_pa_id).await?;

            tx.commit().await.context("run_visibility_cycle: commit")?;

            let mut chunks = phase::fetch_portal_visible_chunks(conn).await?;
            // Settle overlaps in memory over the visible set we just fetched (see
            // `evict_portal_overlaps`), keeping the assignment disjoint without a per-promotion probe.
            // The eviction's un-promotes are written back before the routing fetch, so its
            // server-side visibility predicate sees the same set as `chunks`.
            phase::evict_portal_overlaps(conn, &mut chunks).await?;
            let chunk_workers = phase::fetch_confirmed_routing(conn).await?;
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

    #[cfg(any(test, feature = "pg-testkit"))]
    fn register_correction(
        &mut self,
        old_pk: ChunkPk,
        new_chunk: NewChunk,
        now: Tick,
    ) -> Result<ChunkPk, StorageError> {
        // A batch of one; the batch API in the sibling `correction` module is the implementation.
        let pks = self.register_corrections(vec![(old_pk, new_chunk)], now)?;
        Ok(pks[0])
    }
}
