//! Phase helpers for [`PostgresStorage::run_scheduling_cycle`], run inside the
//! cycle's transaction, plus the post-commit [`build_worker_assignment`] read.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::Arc;

use anyhow::{Context, Result};
use futures::TryStreamExt;
use itertools::Itertools as _;
use rustc_hash::FxHashMap;
use semver::Version;
use sqlx::postgres::PgConnection;
use sqlx::{Postgres, Transaction};

use crate::metrics::{PhaseTimer, Timer};
use crate::scheduler_storage::algorithm::CurrentPlacement;
use crate::scheduler_storage::{
    AlgoChunk, AssignmentId, AssignmentWorker, ChunkPk, SchemaId, WorkerAssignment,
    WorkerAssignmentChunk, WorkerPk,
};
use crate::types::Worker;

use super::rows::{
    ActiveChunkRow, WorkerRow, assignment_worker_from_row, chunk_from_active_row, tick_to_i64,
};

/// Per-chunk stale holders aggregated once, for `LEFT JOIN stale_agg st` — pairs with
/// [`PLACEMENT_MERGE`].
const STALE_AGG_CTE: &str = "\
        WITH stale_agg AS (
            SELECT chunk_pk, array_agg(worker_id) AS extra
            FROM sched_stale_mappings
            GROUP BY chunk_pk
        )";

/// Placement = ideal ∪ stale. The ideal already stores each chunk's holders as a (sorted) array,
/// so it passes through untouched; only chunks that actually have a stale row pay the unnest+merge
/// — the whole ideal isn't decomposed and re-aggregated every read.
const PLACEMENT_MERGE: &str = "\
               CASE
                   WHEN st.extra IS NULL THEN i.worker_ids
                   WHEN i.worker_ids IS NULL
                       THEN (SELECT array_agg(DISTINCT v ORDER BY v) FROM unnest(st.extra) AS v)
                   ELSE (SELECT array_agg(DISTINCT v ORDER BY v) FROM unnest(i.worker_ids || st.extra) AS v)
               END AS worker_ids";

pub(super) async fn open_worker_assignment(
    tx: &mut Transaction<'_, Postgres>,
    now: u64,
) -> Result<AssignmentId> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:open_worker_assignment");
    let id = sqlx::query_scalar(
        "INSERT INTO sched_worker_assignments (created_at) VALUES ($1) RETURNING id",
    )
    .bind(tick_to_i64(now))
    .fetch_one(&mut **tx)
    .await
    .context("run_scheduling_cycle: insert worker assignment")?;
    timer.stmt(1);
    Ok(id)
}

/// Tombstone chunks whose portal-drop landed at least `m_ticks` ago, stamping the drop tick.
pub(super) async fn tombstone_expired_chunks(
    tx: &mut Transaction<'_, Postgres>,
    now: u64,
    m_ticks: u64,
) -> Result<()> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:tombstone_expired_chunks");
    let res = sqlx::query(
        r#"
        UPDATE sched_chunk_metadata
        SET dropped_from_worker_assignment_at = $1
        WHERE dropped_at_portal_assignment_id IS NOT NULL
          AND dropped_from_worker_assignment_at IS NULL
          AND dropped_at_portal_assignment_id IN (
              SELECT id FROM sched_portal_assignments
              WHERE created_at <= $1 - $2
          )
        "#,
    )
    .bind(tick_to_i64(now))
    .bind(tick_to_i64(m_ticks))
    .execute(&mut **tx)
    .await
    .context("run_scheduling_cycle: tombstone chunks")?;
    timer.stmt(res.rows_affected());

    // Drop tombstoned chunks' stale mappings. A removed chunk's stale never drains (its
    // superseded_at_worker assignment is never confirmed, so it's never dropped_at_portal/expired);
    // left behind it counts in `ideal ∪ stale` and overcommits a worker. Mirrors the in-memory oracle.
    // Scoped to chunks tombstoned *this* cycle (the UPDATE above stamped them with $1): prior
    // cycles already dropped their tombstoned chunks' stale in this same transaction, so the
    // all-time `IS NOT NULL` set would re-scan every previously tombstoned chunk, every cycle.
    let res = sqlx::query(
        "DELETE FROM sched_stale_mappings WHERE chunk_pk IN \
         (SELECT chunk_pk FROM sched_chunk_metadata WHERE dropped_from_worker_assignment_at = $1)",
    )
    .bind(tick_to_i64(now))
    .execute(&mut **tx)
    .await
    .context("run_scheduling_cycle: drop tombstoned stale mappings")?;
    timer.stmt(res.rows_affected());
    Ok(())
}

/// Expire stale mappings whose drain activated at least `m_ticks` ago: those at/under the newest
/// watermark among aged portal assignments (exact: both monotone over assignment ids).
pub(super) async fn expire_drained_stale_mappings(
    tx: &mut Transaction<'_, Postgres>,
    now: u64,
    m_ticks: u64,
) -> Result<()> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:expire_drained_stale_mappings");
    let res = sqlx::query(
        r#"
        DELETE FROM sched_stale_mappings
        WHERE superseded_at_worker_assignment_id <= (
            SELECT COALESCE(MAX(confirmed_up_to), 0)
            FROM sched_portal_assignments
            WHERE created_at <= $1 - $2
        )
        "#,
    )
    .bind(tick_to_i64(now))
    .bind(tick_to_i64(m_ticks))
    .execute(&mut **tx)
    .await
    .context("run_scheduling_cycle: expire stale mappings")?;
    timer.stmt(res.rows_affected());
    Ok(())
}

/// The scheduling cycle's one streamed read of the active (not tombstoned/rejected) chunk set,
/// decoded into every in-memory form the rest of the cycle needs.
pub(super) struct ActiveChunks {
    /// The algorithm's chunk input, in (dataset name, block) order.
    pub(super) for_algo: Vec<(ChunkPk, AlgoChunk)>,
    /// ideal ∪ stale; entries only for placed chunks.
    pub(super) current_placement: CurrentPlacement,
    /// ideal alone
    pub(super) committed_placement: CurrentPlacement,
    /// Confirmed routing of portal-visible chunks — the eviction victim-ordering hint.
    pub(super) confirmed_routing: CurrentPlacement,
    /// Full-column decode of the active rows, reused by the post-commit
    /// [`build_worker_assignment`] so it needn't re-read the chunks; shares `for_algo`'s `Arc`s.
    pub(super) published: FxHashMap<ChunkPk, WorkerAssignmentChunk>,
    /// schema_ids of every chunk that has entered a worker assignment and is not yet tombstoned (ADR 0002)
    pub(super) bundle_schema_ids: BTreeSet<SchemaId>,
}

/// Reads every active chunk — registered, and neither `rejected` nor removed (tombstoned) — spanning
/// the lifecycle from **pending** (registered but not yet placed) through **marked-for-removal**.
/// Pending chunks are included so the algorithm can place them; they carry no `current_placement` entry.
/// `capacity_hint` pre-sizes the decode maps — the previous cycle's active-chunk count.
pub(super) async fn fetch_active_chunks_with_placement(
    tx: &mut Transaction<'_, Postgres>,
    capacity_hint: usize,
) -> Result<ActiveChunks> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:fetch_active_chunks_with_placement");

    // The dataset list in the server's collation order; the sort below uses its ranks, so the
    // big query neither joins `datasets` (6M+ name texts stay off the wire) nor sorts. `id`
    // tiebreaks names a non-C collation may compare equal.
    let dataset_rows: Vec<(i16, String)> =
        sqlx::query_as("SELECT id, name FROM datasets ORDER BY name, id")
            .fetch_all(&mut **tx)
            .await
            .context("run_scheduling_cycle: fetch datasets")?;
    let mut name_by_id: FxHashMap<i16, Arc<String>> = FxHashMap::default();
    let mut rank_of: FxHashMap<Arc<String>, u32> = FxHashMap::default();
    for (rank, (id, name)) in dataset_rows.into_iter().enumerate() {
        let name = Arc::new(name);
        name_by_id.insert(id, name.clone());
        rank_of.insert(name, rank as u32);
    }

    let sql = format!(
        r#"
        {STALE_AGG_CTE}
        SELECT c.chunk_pk, c.dataset_id, c.chunk_id, c.size,
               c.schema_id, c.tables_present,
               c.first_block, c.last_block_delta,
               (s.applied_at_portal_assignment_id IS NOT NULL
                AND s.dropped_at_portal_assignment_id IS NULL) AS is_portal_visible,
               (s.applied_at_worker_assignment_id IS NOT NULL) AS entered_worker_assignment,
               i.worker_ids AS ideal_worker_ids,
               CASE WHEN s.applied_at_portal_assignment_id IS NOT NULL
                     AND s.dropped_at_portal_assignment_id IS NULL
                    THEN ccw.worker_ids END AS routed_worker_ids,
               {PLACEMENT_MERGE}
        FROM chunks c
        JOIN sched_chunk_metadata s ON s.chunk_pk = c.chunk_pk
        LEFT JOIN sched_ideal_chunk_workers i ON i.chunk_pk = c.chunk_pk
        LEFT JOIN sched_confirmed_chunk_workers ccw ON ccw.chunk_pk = c.chunk_pk
        LEFT JOIN stale_agg st ON st.chunk_pk = c.chunk_pk
        WHERE s.dropped_from_worker_assignment_at IS NULL
          AND NOT s.rejected
        "#
    );
    let mut stream = sqlx::query_as::<_, ActiveChunkRow>(sqlx::AssertSqlSafe(sql)).fetch(&mut **tx);

    let with_capacity =
        || CurrentPlacement::with_capacity_and_hasher(capacity_hint, Default::default());
    let mut out = ActiveChunks {
        for_algo: Vec::with_capacity(capacity_hint),
        current_placement: with_capacity(),
        committed_placement: with_capacity(),
        confirmed_routing: with_capacity(),
        published: FxHashMap::with_capacity_and_hasher(capacity_hint, Default::default()),
        bundle_schema_ids: BTreeSet::new(),
    };
    let mut count = 0u64;
    while let Some(mut row) = stream
        .try_next()
        .await
        .context("run_scheduling_cycle: fetch active chunks with placement")?
    {
        let dataset = name_by_id
            .get(&row.dataset_id)
            .expect("chunks.dataset_id references a dataset row read in the same snapshot")
            .clone();
        let pk = row.chunk_pk;
        let is_portal_visible = row.is_portal_visible;
        if let Some(holders) = row.worker_ids.take() {
            out.current_placement.insert(pk, holders);
        }
        if let Some(committed) = row.ideal_worker_ids.take() {
            out.committed_placement.insert(pk, committed);
        }
        if let Some(routed) = row.routed_worker_ids.take() {
            out.confirmed_routing.insert(pk, routed);
        }
        if row.entered_worker_assignment {
            out.bundle_schema_ids.insert(row.schema_id);
        }
        let chunk = chunk_from_active_row(row, dataset);
        out.for_algo
            .push((pk, AlgoChunk::new(&chunk, is_portal_visible)));
        out.published.insert(pk, chunk);
        count += 1;
    }
    // The exact order `ORDER BY d.name, c.first_block` would produce — stage-1 packing is
    // order-sensitive, so the feed order is observable behaviour — as an integer-key sort.
    out.for_algo
        .sort_by_cached_key(|(_, chunk)| (rank_of[&*chunk.dataset], *chunk.blocks.start()));
    timer.stmt(count);
    Ok(out)
}

pub(super) async fn fetch_workers(tx: &mut Transaction<'_, Postgres>) -> Result<Vec<WorkerRow>> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:fetch_workers");
    let rows: Vec<WorkerRow> =
        sqlx::query_as("SELECT id, peer_id, version, inactive_since FROM sched_workers")
            .fetch_all(&mut **tx)
            .await
            .context("run_scheduling_cycle: fetch workers")?;
    timer.stmt(rows.len() as u64);
    Ok(rows)
}

/// Apply the post-write deltas and promote the new ideal as a single simple-query batch — one
/// round-trip instead of one per statement. The statements run sequentially inside the cycle transaction (each
/// sees the previous one's effects, exactly as when they were separate calls), and the swap is
/// textually last so the diff/stale statements still read the live (pre-swap) ideal. The phases are
/// mutually independent — disjoint rows, different target tables — so batching them changes nothing
/// but the round-trip count; see [`SQL_DELTAS_AND_SWAP`] for the per-section semantics.
///
/// The simple protocol takes no bind parameters, so `new_wa_id` (a value we mint, never user input)
/// is substituted in as a literal — `AssertSqlSafe` acknowledges that. `execute` sums `rows_affected`
/// across the batch, so the timer still reports the rows touched, just not split per phase.
pub(super) async fn apply_deltas_and_swap(
    tx: &mut Transaction<'_, Postgres>,
    new_wa_id: AssignmentId,
    evicted: &[(ChunkPk, WorkerPk)],
) -> Result<()> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:apply_deltas_and_swap");
    // Integer surrogate pks we mint/own — literal substitution is as safe as `$WA`. Empty `evicted`
    // yields `'{}'`, a valid empty array, so the eviction DELETE is a no-op and always present.
    let evicted_chunks = evicted.iter().map(|(c, _)| c.0).format(",").to_string();
    let evicted_workers = evicted.iter().map(|(_, w)| w.0).format(",").to_string();
    let sql = SQL_DELTAS_AND_SWAP
        .replace("$WA", &new_wa_id.to_string())
        .replace("$EVICTED_CHUNKS", &evicted_chunks)
        .replace("$EVICTED_WORKERS", &evicted_workers);
    let res = sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(&mut **tx)
        .await
        .context("run_scheduling_cycle: apply deltas and swap")?;
    timer.stmt(res.rows_affected());
    Ok(())
}

/// The batch run by [`apply_deltas_and_swap`], one round-trip via the simple query protocol. `$WA`
/// is the new worker-assignment id, substituted before execution.
///
/// One cycle's placement change has to land as a unit: every copy the new ideal abandons must keep
/// serving through its grace window, every deletion the reconcile decided must actually free its
/// bytes, and portals must learn exactly what moved. Sections, in execution order:
///   1. Start the grace window for abandoned copies (Invariant 4): holders the new ideal drops
///      keep serving as drains. Active workers only — a departed worker's copies left with it —
///      and not chunks mid whole-chunk drop, which runs on its own clock.
///   2. Make floor preemption real: a reclaimed copy must free its bytes *now* — deleting instead
///      of draining is what actually produces the room the starved floor was promised (ADR 0001).
///   3. Tell portals what moved: one diff row per chunk whose holder set changed, appeared, or
///      vanished, so the routing can replay the change once workers confirm the assignment.
///   4. Cancel self-resolved removals: a pair the new ideal takes back was never really leaving,
///      so its drain (and the clock on it) must not outlive the decision.
///   5. Start the lifecycle of newcomers: a chunk's first appearance in an ideal is the anchor
///      that later gates its portal promotion and schema-bundle membership (ADR 0002).
///   6. Make the new ideal live: swap the staged twin in, leaving the old table empty and ready
///      to stage the next cycle.
const SQL_DELTAS_AND_SWAP: &str = r#"
INSERT INTO sched_stale_mappings (chunk_pk, worker_id, superseded_at_worker_assignment_id)
SELECT c.chunk_pk, u.worker_id, $WA
FROM sched_ideal_chunk_workers c
LEFT JOIN sched_future_ideal_chunk_workers f ON f.chunk_pk = c.chunk_pk
CROSS JOIN LATERAL UNNEST(c.worker_ids) AS u(worker_id)
WHERE u.worker_id <> ALL (COALESCE(f.worker_ids, '{}'::int[]))
  AND EXISTS (
      SELECT 1 FROM sched_workers sw
      WHERE sw.id = u.worker_id AND sw.inactive_since IS NULL
  )
  AND NOT EXISTS (
      SELECT 1 FROM sched_chunk_metadata m
      WHERE m.chunk_pk = c.chunk_pk
        AND m.dropped_at_portal_assignment_id IS NOT NULL
  )
ON CONFLICT (chunk_pk, worker_id) DO NOTHING;

DELETE FROM sched_stale_mappings s
USING unnest('{$EVICTED_CHUNKS}'::bigint[], '{$EVICTED_WORKERS}'::int[]) AS ev(chunk_pk, worker_id)
WHERE s.chunk_pk = ev.chunk_pk AND s.worker_id = ev.worker_id;

INSERT INTO sched_worker_assignment_diffs (worker_assignment_id, chunk_pk, worker_ids)
SELECT $WA, COALESCE(f.chunk_pk, c.chunk_pk), COALESCE(f.worker_ids, '{}'::int[])
FROM sched_future_ideal_chunk_workers f
FULL JOIN sched_ideal_chunk_workers c ON c.chunk_pk = f.chunk_pk
WHERE f.worker_ids IS DISTINCT FROM c.worker_ids;

DELETE FROM sched_stale_mappings s
USING sched_future_ideal_chunk_workers f
WHERE s.chunk_pk = f.chunk_pk
  AND s.worker_id = ANY (f.worker_ids);

UPDATE sched_chunk_metadata m
SET applied_at_worker_assignment_id = $WA
FROM sched_future_ideal_chunk_workers f
WHERE m.chunk_pk = f.chunk_pk
  AND m.applied_at_worker_assignment_id IS NULL;

TRUNCATE sched_ideal_chunk_workers;
ALTER TABLE sched_ideal_chunk_workers RENAME TO sched_ideal_chunk_workers__swap;
ALTER TABLE sched_future_ideal_chunk_workers RENAME TO sched_ideal_chunk_workers;
ALTER TABLE sched_ideal_chunk_workers__swap RENAME TO sched_future_ideal_chunk_workers;
"#;

/// Stage the freshly computed ideal into the empty future twin via one `COPY … FROM STDIN`. The PK
/// is dropped before the COPY and rebuilt after, so the load does a single sorted index build rather
/// than a per-row btree insert per chunk.
pub(super) async fn write_future_ideal(
    tx: &mut Transaction<'_, Postgres>,
    ideal_mappings: &[(ChunkPk, Vec<WorkerPk>)],
    batch_size: usize,
) -> Result<()> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:write_future_ideal");
    // Drop the PK by catalog lookup, not by name: a swap renames the twins and carries their
    // constraint names along, so this table's PK name isn't predictable. (The rebuild re-adds it
    // unnamed for the same reason.)
    sqlx::raw_sql(
        r#"
        DO $$
        DECLARE c name;
        BEGIN
            SELECT conname INTO c FROM pg_constraint
            WHERE conrelid = 'sched_future_ideal_chunk_workers'::regclass AND contype = 'p';
            IF c IS NOT NULL THEN
                EXECUTE format('ALTER TABLE sched_future_ideal_chunk_workers DROP CONSTRAINT %I', c);
            END IF;
        END $$;
        "#,
    )
    .execute(&mut **tx)
    .await
    .context("run_scheduling_cycle: drop future ideal pk")?;
    let mut copy = tx
        .copy_in_raw("COPY sched_future_ideal_chunk_workers (chunk_pk, worker_ids) FROM STDIN")
        .await
        .context("run_scheduling_cycle: begin COPY future ideal")?;

    // Text COPY in `batch_size`-row batches to bound memory: `chunk_pk \t {w1,w2,…}` per chunk.
    // Holders arrive canonical (sorted by worker_pk, distinct) from `invert_worker_chunks`. No field
    // holds the tab/newline/backslash that text COPY escapes, so they go out raw.
    let mut buf = String::new();
    for batch in &ideal_mappings.iter().chunks(batch_size) {
        buf.clear();
        for (chunk_pk, holders) in batch {
            debug_assert!(
                holders.windows(2).all(|w| w[0].0 < w[1].0),
                "non-canonical holders for chunk {}",
                chunk_pk.0
            );
            writeln!(
                buf,
                "{}\t{{{}}}",
                chunk_pk.0,
                holders.iter().map(|w| w.0).format(",")
            )
            .expect("writing to a String is infallible");
        }
        copy.send(buf.as_bytes())
            .await
            .context("run_scheduling_cycle: send COPY batch")?;
    }

    let rows = copy
        .finish()
        .await
        .context("run_scheduling_cycle: finish COPY future ideal")?;
    sqlx::raw_sql("ALTER TABLE sched_future_ideal_chunk_workers ADD PRIMARY KEY (chunk_pk)")
        .execute(&mut **tx)
        .await
        .context("run_scheduling_cycle: rebuild future ideal pk")?;
    timer.stmt(rows);
    Ok(())
}

/// Decoded workers: `published` is what the assignment exposes; `for_algo` is what the scheduling
/// algorithm consumes (keyed the same, paired with the parsed `Worker`).
///
/// Departed workers are left out of `for_algo` — absence from the active set means a full
/// detection window without a sighting, so they are not capacity. They stay in `published`, which
/// is the assignment's metadata lookup.
pub(super) struct DecodedWorkers {
    pub(super) published: BTreeMap<WorkerPk, AssignmentWorker>,
    pub(super) for_algo: Vec<(WorkerPk, Worker)>,
}

pub(super) fn decode_workers(worker_rows: &[WorkerRow]) -> Result<DecodedWorkers> {
    let _timer = Timer::new("run_scheduling_cycle:decode_workers");
    let mut published: BTreeMap<WorkerPk, AssignmentWorker> = BTreeMap::new();
    let mut for_algo: Vec<(WorkerPk, Worker)> = Vec::with_capacity(worker_rows.len());
    for row in worker_rows {
        let (id, worker) = assignment_worker_from_row(row)?;
        if row.inactive_since.is_none() {
            let version = row
                .version
                .as_deref()
                .map(Version::parse)
                .transpose()
                .with_context(|| format!("invalid version for worker {}", row.id))?;
            for_algo.push((
                id,
                Worker {
                    id: worker.peer_id,
                    status: worker.status,
                    version,
                },
            ));
        }
        published.insert(id, worker);
    }
    Ok(DecodedWorkers {
        published,
        for_algo,
    })
}

/// Post-commit assembly of the published WorkerAssignment: ideal ∪ stale, excluding tombstoned
/// chunks.
pub(super) async fn build_worker_assignment(
    conn: &mut PgConnection,
    id: AssignmentId,
    workers: BTreeMap<WorkerPk, AssignmentWorker>,
    replication_by_weight: BTreeMap<u16, u16>,
    ideal_mappings: Vec<(ChunkPk, Vec<WorkerPk>)>,
    mut active_chunks: FxHashMap<ChunkPk, WorkerAssignmentChunk>,
) -> Result<WorkerAssignment> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:build_worker_assignment");
    // Post-delta stale holders: pre-existing rows plus this cycle's mint, minus resolved
    // flip-flops and phase-A drops — i.e. exactly what the committed table now holds.
    let stale: Vec<(ChunkPk, Vec<WorkerPk>)> = sqlx::query_as(
        "SELECT chunk_pk, array_agg(worker_id) FROM sched_stale_mappings GROUP BY chunk_pk",
    )
    .fetch_all(&mut *conn)
    .await
    .context("build_worker_assignment: fetch stale holders")?;
    timer.stmt(stale.len() as u64);

    let mut chunk_workers: BTreeMap<ChunkPk, Vec<WorkerPk>> = ideal_mappings.into_iter().collect();
    for (pk, extra) in stale {
        // Keep holder sets canonical (sorted, distinct), as the SQL merge did.
        let holders = chunk_workers.entry(pk).or_default();
        holders.extend(extra);
        holders.sort_unstable();
        holders.dedup();
    }

    // Publish only placed chunks (an active chunk the algorithm left unplaced is not published).
    // A placed pk absent from the active set is a stale row for a chunk tombstoned in an earlier
    // GC — its holders must not be published either.
    let mut chunks_out: BTreeMap<ChunkPk, WorkerAssignmentChunk> = BTreeMap::new();
    chunk_workers.retain(|pk, _| match active_chunks.remove(pk) {
        Some(chunk) => {
            chunks_out.insert(*pk, chunk);
            true
        }
        None => false,
    });

    Ok(WorkerAssignment {
        id,
        chunk_workers,
        chunks: chunks_out,
        workers,
        replication_by_weight,
    })
}
