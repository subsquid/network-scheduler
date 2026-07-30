//! PostgreSQL-backed implementation of [`SchedulerStorage`].
//!
//! Single [`PgConnection`], no pool — each instance serves one loop, which runs
//! its calls sequentially. [`PostgresStorage::connect`] is the leader: it holds a session advisory
//! lock for the struct's lifetime (admission control, so a second instance fails fast), migrates,
//! then claims the next leadership [`Epoch`]. [`PostgresStorage::connect_follower`] needs that
//! epoch and covers the instance's secondary loops. Every mutating path re-reads the epoch inside
//! its own transaction and fails with [`StorageError::FencedOut`] once a newer leader claimed one,
//! so a demoted instance cannot commit no matter what its connections are still doing.
//!
//! # Ownership map
//!
//! The instance's connections interleave without any cross-connection lock because each table has
//! exactly one writing connection:
//!
//! * **worker connection** — `sched_workers` status columns only, plus the confirmation tables it
//!   solely owns (its diff replay consumes ids strictly below those a running cycle mints).
//! * **ingest writers** — the chunk-discovery connection and metadata-service: append-only,
//!   new-key rows. They never UPDATE/DELETE a row the cycle writes.
//! * **scheduling connection** — every UPDATE/DELETE of existing `sched_*` rows: the scheduling
//!   cycle (departed-worker cleanup included), the visibility cycle, worker GC, and — once wired —
//!   the corrections consumer.
//!
//! No two connections write the same row, so the cycle tables need no advisory lock and the only
//! 40001 reachable is the fence's own row lock, which maps to `FencedOut`. The cycle's transactions
//! run at REPEATABLE READ only to give their reads one snapshot.
//!
//! `Tick` values are logical integer timestamps stored in `BIGINT` columns;
//! `m_ticks`/`gc_ticks` are raw tick counts used in integer arithmetic.

mod admission;
mod auto_explain;
mod debug;
mod nonoverlap;
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
use std::time::Duration;

use anyhow::Context;
use sqlx::Connection;
use sqlx::postgres::PgConnection;

use crate::metrics::{PhaseTimer, Timer};
use crate::scheduler_storage::algorithm::{CurrentPlacement, ScheduleOutput, SchedulingAlgorithm};
use crate::scheduler_storage::{
    AssignmentId, ChunkPk, DatasetPk, NewChunk, NewDataset, PortalAssignment, SchedulerStorage,
    SchemaBundle, SchemaId, StorageError, Tick, WorkerAssignment,
};
use crate::types::{Dataset, DatasetSchema, DatasetWatermark, Worker};

use nonoverlap::Candidate;
use rows::tick_to_i64;

/// Rows per batched write, the default the CLI's `--batch-size` takes. Caps the jsonb payload and
/// keeps a single statement under Postgres' per-value / message-size limits; the cycle's writes also
/// need whole chunks to stay within one batch, or `array_agg` would split a chunk's holder set
/// across statements.
pub const DEFAULT_BATCH_SIZE: usize = 10_000;

/// Cap on a leadership claim's wait for an in-flight fenced transaction (a scheduling cycle holds
/// one for minutes) — the default the CLI's `--leadership-claim-timeout` takes, and what the tests
/// connect with. Expiry surfaces as `AlreadyRunning`, i.e. "retry as a candidate".
pub const DEFAULT_CLAIM_LOCK_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Session memory GUCs (`work_mem`/`maintenance_work_mem`) for one connection.
#[derive(Clone, Copy, Debug)]
pub enum SessionMemory {
    /// 512MB, for the scheduling cycle's routing reads and the visibility queries.
    Raised,
    /// Server default, for connections running only small statements (chunk ingest).
    ServerDefault,
}

/// Leadership fencing token. Only the claim in [`PostgresStorage::connect`] mints one, so a
/// follower connection cannot be constructed without a leader's epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Epoch(i64);

impl Epoch {
    /// The raw counter, for reporting it. Not a constructor: an epoch still only comes from a
    /// successful claim.
    pub fn get(self) -> i64 {
        self.0
    }
}

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
    fence: Epoch,
    /// Declared last so it drops after `conn`: the harness can only drop a database once this
    /// storage's connection to it is gone.
    #[cfg(any(test, feature = "pg-testkit"))]
    case_db: Option<pg_testkit::CaseDb>,
}

impl PostgresStorage {
    /// Connect as the leader: take the scheduler advisory lock, run the migrations, then claim the
    /// next leadership epoch — the fence every write of this instance carries. Errors with
    /// [`StorageError::AlreadyRunning`] if another scheduler holds the lock. `claim_lock_timeout`
    /// caps how long the claim waits behind an in-flight fenced transaction before giving up as a
    /// candidate; `batch_size` is the rows-per-batch cap this instance's batched writes use.
    pub fn connect(
        database_url: &str,
        claim_lock_timeout: Duration,
        batch_size: usize,
    ) -> Result<(Self, Epoch), StorageError> {
        let (rt, mut conn) = open(database_url, SessionMemory::Raised)?;
        let epoch = rt.block_on(async {
            if !try_acquire_scheduler_lock(&mut conn).await? {
                return Err(StorageError::AlreadyRunning);
            }
            // Before the claim, since the claim needs the leadership row: a real schema change
            // therefore runs while a demoted instance may still be writing, and serializes against
            // it on the table locks its DDL takes.
            scheduler_metadata::pg::MIGRATOR
                .run(&mut conn)
                .await
                .context("migration failed")?;
            claim_leadership(&mut conn, claim_lock_timeout).await
        })?;
        Ok((Self::fenced_by(rt, conn, epoch, batch_size), epoch))
    }

    /// An extra connection of the *same* instance (the service's secondary loops), fenced by the
    /// leader's `epoch`: no advisory lock, no migrations. Its writes fail with
    /// [`StorageError::FencedOut`] once a newer leader claims the epoch.
    pub fn connect_follower(
        database_url: &str,
        memory: SessionMemory,
        epoch: Epoch,
        batch_size: usize,
    ) -> Result<Self, StorageError> {
        let (rt, conn) = open(database_url, memory)?;
        Ok(Self::fenced_by(rt, conn, epoch, batch_size))
    }

    fn fenced_by(
        rt: tokio::runtime::Runtime,
        conn: PgConnection,
        fence: Epoch,
        batch_size: usize,
    ) -> Self {
        Self {
            rt,
            conn: std::cell::RefCell::new(conn),
            batch_size,
            fence,
            #[cfg(any(test, feature = "pg-testkit"))]
            case_db: None,
        }
    }

    /// Tie a harness database to this storage, so the case's database goes when the storage does.
    #[cfg(any(test, feature = "pg-testkit"))]
    pub(crate) fn owning(mut self, db: pg_testkit::CaseDb) -> Self {
        self.case_db = Some(db);
        self
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

    /// Each dataset's seeded write schema id (the one created with the dataset), by name.
    /// The S3-discovery path stamps these on discovered chunks, which carry no schema info.
    pub fn seeded_schema_ids(&self) -> Result<BTreeMap<String, SchemaId>, StorageError> {
        self.with_conn_ref(async move |conn| {
            scheduler_metadata::pg::schema::seeded_schema_ids(conn).await
        })
    }

    /// Current confirmation watermark, 0 when nothing is confirmed — seeds the service's gate
    /// across restarts.
    pub fn worker_confirmation_watermark(&mut self) -> Result<AssignmentId, StorageError> {
        self.with_conn(async move |conn| {
            let watermark = sqlx::query_scalar(
                "SELECT COALESCE(MAX(assignment_id), 0) FROM sched_worker_confirmations",
            )
            .fetch_one(&mut *conn)
            .await
            .context("fetch confirmation watermark")?;
            Ok(watermark)
        })
    }

    /// Liveness probe: round-trips the connection. An error means the connection (and with it the
    /// advisory lock) is gone — this storage is unusable and the process must not keep retrying.
    ///
    /// `timeout` is required rather than defaulted: the failure this exists to catch includes a socket
    /// that accepts writes and never answers: an unbounded probe would hang exactly where the
    /// operation it is vouching for already hung. A timed-out probe leaves a request in flight, so
    /// its verdict has to be final — which it is, since every caller treats a failure as fatal.
    pub fn ping(&mut self, timeout: Duration) -> Result<(), StorageError> {
        self.with_conn(async move |conn| {
            tokio::time::timeout(timeout, conn.ping())
                .await
                .map_err(|_| anyhow::anyhow!("timed out after {timeout:?}"))?
                .context("ping Postgres connection")?;
            Ok(())
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
    pub fn register_corrections(
        &mut self,
        corrections: Vec<(ChunkPk, NewChunk)>,
        now: Tick,
    ) -> Result<Vec<ChunkPk>, StorageError> {
        if corrections.is_empty() {
            return Ok(Vec::new());
        }
        let batch_size = self.batch_size;
        let fence = self.fence;
        self.with_conn(async move |conn| {
            // The shared helper opens its own transaction, which nests as a savepoint here.
            let mut tx = begin_fenced(conn, fence, Isolation::ServerDefault).await?;
            let pks = scheduler_metadata::pg::correction::register_corrections(
                &mut tx,
                &corrections,
                now,
                batch_size,
            )
            .await?;
            tx.commit().await.context("register_corrections: commit")?;
            Ok(pks)
        })
    }

    /// Clone every live chunk of dataset `src` into dataset `dst`, server-side; the next
    /// `register_new_chunks` admits the clones. Returns the number of clones.
    #[cfg(any(test, feature = "pg-testkit"))]
    pub fn copy_dataset_chunks(&mut self, src: &str, dst: &str) -> Result<u64, StorageError> {
        let fence = self.fence;
        self.with_conn(async move |conn| {
            let mut tx = begin_fenced(conn, fence, Isolation::ServerDefault).await?;
            let cloned = testkit::copy_dataset_chunks(&mut tx, src, dst).await?;
            tx.commit().await.context("copy_dataset_chunks: commit")?;
            Ok(cloned)
        })
    }
}

/// A connection and the runtime driving it, with this connection's session GUCs applied.
fn open(
    database_url: &str,
    memory: SessionMemory,
) -> Result<(tokio::runtime::Runtime, PgConnection), StorageError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build current-thread runtime")?;
    let conn = rt.block_on(async {
        let mut conn = PgConnection::connect(database_url)
            .await
            .context("failed to connect to Postgres")?;
        if matches!(memory, SessionMemory::Raised) {
            // Session-scoped GUCs, held for this connection's lifetime.
            for stmt in [
                "SET work_mem = '512MB'",
                "SET maintenance_work_mem = '512MB'",
            ] {
                sqlx::query(stmt)
                    .execute(&mut conn)
                    .await
                    .with_context(|| format!("failed to run {stmt}"))?;
            }
        }
        Ok::<_, anyhow::Error>(conn)
    })?;
    Ok((rt, conn))
}

/// Take the singleton scheduler session lock, held for the connection's lifetime. `false` means
/// another instance holds it — cheap admission control, so a second instance fails fast instead of
/// stealing leadership from a healthy one. Advisory locks are cluster-wide, not per-database; the
/// key is scoped to the current database, or per-test databases on one cluster would collide on it.
async fn try_acquire_scheduler_lock(conn: &mut PgConnection) -> anyhow::Result<bool> {
    sqlx::query_scalar(
        "SELECT pg_try_advisory_lock(hashtext('network-scheduler:' || current_database()))",
    )
    .fetch_one(&mut *conn)
    .await
    .context("failed to acquire advisory lock")
}

/// Claim leadership by bumping the epoch. Blocks behind any in-flight fenced transaction's
/// `FOR SHARE`, so the previous leader's last write either commits before this claim or is refused
/// after it — bounded by `lock_timeout`, because a hung startup hides worse than a retryable
/// `AlreadyRunning`.
async fn claim_leadership(
    conn: &mut PgConnection,
    lock_timeout: Duration,
) -> Result<Epoch, StorageError> {
    let mut tx = conn.begin().await.context("begin leadership claim")?;
    // `SET LOCAL`: the cap belongs to this claim, not to every later statement on the connection.
    // A bare integer is milliseconds to Postgres' `lock_timeout`.
    let lock_timeout_ms = lock_timeout.as_millis();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SET LOCAL lock_timeout = '{lock_timeout_ms}'"
    )))
    .execute(&mut *tx)
    .await
    .context("cap the leadership claim's wait")?;
    let claim = sqlx::query_scalar(
        "UPDATE sched_leadership SET epoch = epoch + 1, leader_pid = pg_backend_pid() \
         WHERE only_row RETURNING epoch",
    )
    .fetch_one(&mut *tx)
    .await;
    let epoch: i64 = match claim {
        Ok(epoch) => epoch,
        // Someone else's write is still in flight; the caller retries as a fresh candidate.
        Err(e) if scheduler_metadata::pg::rows::is_lock_timeout(&e) => {
            return Err(StorageError::AlreadyRunning);
        }
        Err(e) => {
            return Err(anyhow::Error::new(e)
                .context("claim leadership epoch")
                .into());
        }
    };
    tx.commit().await.context("commit leadership claim")?;
    Ok(Epoch(epoch))
}

/// Begin a transaction and check the fence, in one place because both matter to every write.
///
/// The `FOR SHARE` holds the leadership row for the transaction's lifetime, so a concurrent claim
/// parks until we finish: no fenced write can commit after a new leader believes it is exclusive.
/// It is also the transaction's first read, so it fixes the REPEATABLE READ snapshot — for which
/// the isolation level must already be set (`SET TRANSACTION` only accepts a virgin transaction).
async fn begin_fenced(
    conn: &mut PgConnection,
    fence: Epoch,
    isolation: Isolation,
) -> Result<sqlx::Transaction<'_, sqlx::Postgres>, StorageError> {
    let mut tx = conn.begin().await.context("begin transaction")?;
    if matches!(isolation, Isolation::RepeatableRead) {
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *tx)
            .await
            .context("set transaction isolation level")?;
    }
    let read = sqlx::query_scalar("SELECT epoch FROM sched_leadership WHERE only_row FOR SHARE")
        .fetch_one(&mut *tx)
        .await;
    let current: i64 = match read {
        Ok(epoch) => epoch,
        // Under REPEATABLE READ, parking on a claim that then commits raises 40001 — the same event
        // as a plain mismatch. The row lock is already released with the aborted transaction.
        Err(e) if scheduler_metadata::pg::rows::is_serialization_failure(&e) => {
            return Err(StorageError::FencedOut);
        }
        Err(e) => {
            return Err(anyhow::Error::new(e)
                .context("read leadership epoch")
                .into());
        }
    };
    if Epoch(current) != fence {
        // Roll back explicitly: dropping a transaction only queues the `ROLLBACK` until the
        // connection is next used, and a fenced-out caller never uses it again — the `FOR SHARE`
        // would go on parking the new leader's claim.
        tx.rollback().await.context("release the fence")?;
        return Err(StorageError::FencedOut);
    }
    Ok(tx)
}

/// Isolation level for a fenced transaction.
#[derive(Clone, Copy, Debug)]
enum Isolation {
    /// No `SET`: the session default (READ COMMITTED), which re-snapshots per statement.
    ServerDefault,
    /// One snapshot for the whole transaction, so the cycle's placement read, its stale-mint worker
    /// filter, and its cleanup predicates see the same worker set.
    RepeatableRead,
}

impl SchedulerStorage for PostgresStorage {
    fn insert_new_datasets(&mut self, datasets: Vec<NewDataset>) -> Result<(), StorageError> {
        let fence = self.fence;
        self.with_conn(async move |conn| -> Result<(), StorageError> {
            let mut timer = PhaseTimer::new("insert_new_datasets");
            let mut tx = begin_fenced(conn, fence, Isolation::ServerDefault).await?;
            for NewDataset {
                name,
                location,
                schema,
            } in &datasets
            {
                let dataset_id: DatasetPk = sqlx::query_scalar(
                    "INSERT INTO datasets (name, location) VALUES ($1, $2) RETURNING id",
                )
                .bind(name)
                .bind(location)
                .fetch_one(&mut *tx)
                .await
                .context("insert_new_datasets")?;
                // The scheduler seeds only the WRITE registry; the read pointer is an
                // ingest-service concern (PgIngest), not a scheduler one.
                scheduler_metadata::pg::schema::insert_write_schema(&mut tx, dataset_id, schema)
                    .await?;
                timer.stmt(2); // dataset insert + write-schema insert
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
        let fence = self.fence;
        self.with_conn(async move |conn| -> Result<(), StorageError> {
            let mut tx = begin_fenced(conn, fence, Isolation::ServerDefault).await?;
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
            // "Register the dataset's WRITE schema": add it to the (immutable, deduped) write
            // registry. No read pointer, no compatibility gate — those live in PgIngest.
            scheduler_metadata::pg::schema::insert_write_schema(
                &mut tx,
                dataset_id,
                &dataset_schema,
            )
            .await?;
            tx.commit().await.context("set_dataset_schema: commit")?;
            Ok(())
        })
    }

    #[cfg(test)]
    fn load_schemas(
        &self,
        schema_ids: Option<&[crate::scheduler_storage::SchemaId]>,
    ) -> Result<BTreeMap<crate::scheduler_storage::SchemaId, DatasetSchema>, StorageError> {
        self.with_conn_ref(async move |conn| {
            scheduler_metadata::pg::schema::load_schemas(conn, schema_ids).await
        })
    }

    fn generate_schema_bundle(&self) -> Result<SchemaBundle, StorageError> {
        self.with_conn_ref(async move |conn| {
            schema::generate_bundle(conn)
                .await
                .map_err(StorageError::from)
        })
    }

    #[cfg(test)]
    fn promote_read_schema(
        &mut self,
        dataset: &str,
        schema: DatasetSchema,
    ) -> Result<crate::scheduler_storage::ReadSchemaId, StorageError> {
        let dataset = dataset.to_owned();
        self.with_conn(async move |conn| {
            let mut tx = conn.begin().await.context("promote_read_schema: begin")?;
            let dataset_id: crate::scheduler_storage::DatasetPk =
                sqlx::query_scalar("SELECT id FROM datasets WHERE name = $1")
                    .bind(&dataset)
                    .fetch_optional(&mut *tx)
                    .await
                    .context("promote_read_schema: resolve dataset")?
                    .ok_or_else(|| {
                        StorageError::Database(anyhow::anyhow!(
                            "promote_read_schema: dataset {dataset} not found"
                        ))
                    })?;
            let id =
                scheduler_metadata::pg::schema::promote_read_schema(&mut tx, dataset_id, &schema)
                    .await?;
            tx.commit().await.context("promote_read_schema: commit")?;
            Ok(id)
        })
    }

    fn insert_new_chunks(&mut self, chunks: Vec<NewChunk>) -> Result<(), StorageError> {
        let batch_size = self.batch_size;
        let fence = self.fence;
        self.with_conn(async move |conn| {
            // The shared helper opens its own transaction, which nests as a savepoint here.
            let mut tx = begin_fenced(conn, fence, Isolation::ServerDefault).await?;
            scheduler_metadata::pg::registration::insert_chunks(&mut tx, &chunks, batch_size)
                .await?;
            tx.commit().await.context("insert_new_chunks: commit")?;
            Ok(())
        })
    }

    fn register_new_chunks(&mut self) -> Result<Vec<ChunkPk>, StorageError> {
        let fence = self.fence;
        self.with_conn(async move |conn| {
            let _timer = Timer::new("register_new_chunks");
            // One transaction, so admission is atomic as well as fenced.
            let mut tx = begin_fenced(conn, fence, Isolation::ServerDefault).await?;

            let candidate_rows = admission::fetch_candidates(&mut tx).await?;
            let admission::Classified {
                exempt,
                candidates,
                range_rejected,
            } = admission::classify(&candidate_rows);

            // Reject candidates overlapping a live chunk in their dataset (indexed SQL probe), then
            // settle overlaps within the batch (two new chunks covering the same range).
            let conflicting = nonoverlap::overlapping_live(&mut tx, &candidates).await?;
            let (clear, mut rejected): (Vec<Candidate>, Vec<Candidate>) = candidates
                .into_iter()
                .partition(|c| !conflicting.contains(&c.pk));
            let (accepted, batch_rejected) = nonoverlap::settle_within_batch(clear);
            rejected.extend(batch_rejected);
            nonoverlap::report_registration_rejected(&rejected);
            admission::warn_range_rejected(&range_rejected);

            let mut admitted = exempt;
            admitted.extend(&accepted);
            admission::persist_admitted(&mut tx, &admitted).await?;
            let rejected_pks: Vec<ChunkPk> = rejected
                .iter()
                .chain(&range_rejected)
                .map(|c| c.pk)
                .collect();
            admission::persist_rejected(&mut tx, &rejected_pks).await?;
            tx.commit().await.context("register_new_chunks: commit")?;

            Ok::<_, StorageError>(admitted)
        })
    }

    fn update_worker_set(
        &mut self,
        active_workers: &[Worker],
        now: Tick,
    ) -> Result<(), StorageError> {
        let fence = self.fence;
        self.with_conn(async move |conn| {
            let _timer = Timer::new("update_worker_set");
            let peer_ids: Vec<String> = active_workers.iter().map(|w| w.id.to_string()).collect();
            let versions: Vec<Option<String>> = active_workers
                .iter()
                .map(|w| w.version.as_ref().map(|v| v.to_string()))
                .collect();

            // Status columns only (see the module's ownership map); a departure's mapping-table
            // consequences are settled by the scheduling cycle, keyed on `inactive_since`.
            let mut tx = begin_fenced(conn, fence, Isolation::ServerDefault).await?;
            workers::upsert_active(&mut tx, &peer_ids, &versions).await?;
            workers::mark_departed(&mut tx, &peer_ids, now).await?;
            tx.commit().await.context("update_worker_set: commit")?;

            Ok::<_, StorageError>(())
        })
    }

    fn gc_inactive_workers(&mut self, now: Tick, gc_ticks: u64) -> Result<(), StorageError> {
        let fence = self.fence;
        self.with_conn(async move |conn| {
            // Server default, not RR: the DELETE must re-snapshot (see `workers::gc_inactive_workers`).
            let mut tx = begin_fenced(conn, fence, Isolation::ServerDefault).await?;
            workers::gc_inactive_workers(&mut tx, now, gc_ticks).await?;
            tx.commit().await.context("gc_inactive_workers: commit")?;
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
        let fence = self.fence;
        self.with_conn(async move |conn| {
            let _timer = Timer::new("run_scheduling_cycle");
            // Phase A — departed-worker cleanup, then clock-driven GC, committed up front so it
            // survives a Phase B shortage rollback; otherwise stale never drains under a
            // sustained shortage. The promotion must run before the expiry — rescue before reaper.
            let mut gc_tx = begin_fenced(conn, fence, Isolation::RepeatableRead).await?;
            workers::delete_inactive_stale_mappings(&mut gc_tx).await?;
            workers::promote_orphaned_drains(&mut gc_tx).await?;
            phase::tombstone_expired_chunks(&mut gc_tx, now, m_ticks).await?;
            phase::expire_drained_stale_mappings(&mut gc_tx, now, m_ticks).await?;
            gc_tx
                .commit()
                .await
                .context("run_scheduling_cycle: commit gc")?;

            // Phase B — placement reconcile; rolls back on shortage, leaving Phase A committed.
            let mut tx = begin_fenced(conn, fence, Isolation::RepeatableRead).await?;

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

            // The round's bundle write ids — the routable window as of this commit, which is what
            // the generator publishes until the next success. Scanned window plus the new ideal's
            // chunks: the scan ran before this cycle stamped first-placed chunks, so those come
            // from the in-memory mapping. Deliberately wider than the new assignment — a chunk the
            // ideal just dropped is still routable while draining (ADR 0002), so its schema stays
            // until it tombstones and the next success's scan sheds it.
            let published_schema_ids: Vec<SchemaId> =
                {
                    let mut ids = bundle_schema_ids;
                    ids.extend(ideal_mappings.iter().filter_map(|(pk, _)| {
                        published_chunks.get(pk).map(|chunk| chunk.schema_id)
                    }));
                    let mut ids: Vec<SchemaId> = ids.into_iter().collect();
                    // Sorted for a deterministic bind and sequential inserts into the PK index.
                    ids.sort_unstable();
                    ids
                };
            phase::persist_assignment_schemas(&mut tx, &published_schema_ids).await?;

            // Read before commit: the tx sees its own writes, so the published assignment is
            // exactly what this commit makes live; a post-commit read takes a fresh snapshot.
            let stale_holders = phase::fetch_stale_holders(&mut tx).await?;

            tx.commit().await.context("run_scheduling_cycle: commit")?;

            let wa = phase::build_worker_assignment(
                new_wa_id,
                workers_map,
                replication_by_weight,
                ideal_mappings,
                published_chunks,
                stale_holders,
            );
            Ok::<_, StorageError>(wa)
        })
    }

    fn confirm_worker_assignment(
        &mut self,
        assignment_id: AssignmentId,
        now: Tick,
    ) -> Result<(), StorageError> {
        use visibility as phase;

        let fence = self.fence;
        self.with_conn(async move |conn| {
            let _timer = Timer::new("confirm_worker_assignment");
            let mut tx = begin_fenced(conn, fence, Isolation::ServerDefault).await?;

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

        let fence = self.fence;
        self.with_conn(async move |conn| {
            let _timer = Timer::new("run_visibility_cycle");
            let mut tx = begin_fenced(conn, fence, Isolation::ServerDefault).await?;

            // Watermark read first so the portal assignment records it (activates drains).
            let confirmed_up_to = phase::confirmation_watermark(&mut tx).await?;
            let new_pa_id = phase::open_portal_assignment(&mut tx, now, confirmed_up_to).await?;

            phase::apply_ready_corrections(&mut tx, new_pa_id, confirmed_up_to, now).await?;
            phase::promote_eligible_chunks(&mut tx, new_pa_id, confirmed_up_to).await?;
            phase::drop_marked_chunks(&mut tx, new_pa_id).await?;

            // Assembled inside the transaction so the chunk set, the eviction that trims it, its
            // routing and its read references share one commit — which also makes the eviction's
            // un-promotes transactional, closing a crash window that left chunks promoted after the
            // assignment dropped them.
            let mut chunks = phase::fetch_portal_visible_chunks(&mut tx).await?;
            // Settle overlaps in memory over the visible set we just fetched (see
            // `evict_portal_overlaps`), keeping the assignment disjoint without a per-promotion probe.
            phase::evict_portal_overlaps(&mut tx, &mut chunks).await?;
            // From the post-eviction set, so the reference needs no pruning to match what is named.
            let named: Vec<&str> = chunks
                .values()
                .map(|chunk| chunk.dataset.as_str())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let read_schemas = schema::read_schema_ids_by_dataset(&mut tx, &named).await?;
            let chunk_workers = phase::fetch_confirmed_routing(&mut tx).await?;
            let workers = phase::fetch_portal_workers(&mut tx).await?;

            tx.commit().await.context("run_visibility_cycle: commit")?;

            Ok::<_, StorageError>(phase::assemble_portal_assignment(
                new_pa_id,
                chunks,
                chunk_workers,
                workers,
                read_schemas,
            ))
        })
    }

    fn mark_for_removal(&mut self, chunk_pk: ChunkPk, now: Tick) -> Result<(), StorageError> {
        let fence = self.fence;
        self.with_conn(async move |conn| {
            let mut timer = PhaseTimer::new("mark_for_removal");
            let mut tx = begin_fenced(conn, fence, Isolation::ServerDefault).await?;
            let res = sqlx::query(
                "UPDATE sched_chunk_metadata SET marked_for_removal = $2 WHERE chunk_pk = $1",
            )
            .bind(chunk_pk)
            .bind(tick_to_i64(now))
            .execute(&mut *tx)
            .await
            .context("mark_for_removal")?;
            tx.commit().await.context("mark_for_removal: commit")?;
            timer.stmt(res.rows_affected());
            Ok::<_, StorageError>(())
        })
    }

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
