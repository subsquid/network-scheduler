//! Phase helpers for [`PostgresStorage::run_scheduling_cycle`], run inside the
//! cycle's transaction, plus the post-commit [`build_worker_assignment`] read.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use futures::TryStreamExt;
use semver::Version;
use sqlx::postgres::PgConnection;
use sqlx::{Postgres, Transaction};

use crate::metrics::{PhaseTimer, Timer};
use crate::scheduler_storage::algorithm::CurrentPlacement;
use crate::scheduler_storage::{
    AlgoChunk, AssignmentId, AssignmentWorker, ChunkPk, WorkerAssignment, WorkerAssignmentChunk,
    WorkerPk,
};
use crate::types::Worker;

use super::rows::{
    ActiveChunkRow, DatasetInterner, WorkerRow, algo_chunk_from_assignment_chunk,
    assignment_worker_from_row, chunk_from_row, tick_to_i64,
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

/// Expire stale mappings whose portal-drop landed at least `m_ticks` ago.
pub(super) async fn expire_drained_stale_mappings(
    tx: &mut Transaction<'_, Postgres>,
    now: u64,
    m_ticks: u64,
) -> Result<()> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:expire_drained_stale_mappings");
    let res = sqlx::query(
        r#"
        DELETE FROM sched_stale_mappings
        WHERE dropped_at_portal_assignment_id IS NOT NULL
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
    .context("run_scheduling_cycle: expire stale mappings")?;
    timer.stmt(res.rows_affected());
    Ok(())
}

/// The scheduling cycle's one streamed read of the active (not tombstoned/rejected) chunk set,
/// decoded into every in-memory form the rest of the cycle needs.
pub(super) struct ActiveChunks {
    /// The algorithm's chunk input, in (dataset, block) order.
    pub(super) for_algo: Vec<(ChunkPk, AlgoChunk)>,
    /// ideal ∪ stale; entries only for placed chunks.
    pub(super) current_placement: CurrentPlacement,
    /// Full-column decode of the same rows, for the post-commit
    /// [`build_worker_assignment`] — which therefore needn't re-read the ~6M chunk
    /// rows. Shares the dataset/id `Arc`s with `for_algo`, so the extra memory is
    /// the struct + map node per chunk, not a second copy of the strings.
    pub(super) published: BTreeMap<ChunkPk, WorkerAssignmentChunk>,
}

/// Active chunks decoded straight into the cycle's in-memory forms ([`ActiveChunks`]).
/// Streamed — a buffered row Vec was ~1 GB at 6M chunks.
pub(super) async fn fetch_active_chunks_with_placement(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<ActiveChunks> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:fetch_active_chunks_with_placement");
    let sql = format!(
        r#"
        {STALE_AGG_CTE}
        SELECT c.chunk_pk, d.name AS dataset_name, c.chunk_id, c.size,
               c.schema_id, c.tables_present,
               c.first_block, c.last_block_delta,
               {PLACEMENT_MERGE}
        FROM chunks c
        JOIN datasets d ON d.id = c.dataset_id
        JOIN sched_chunk_metadata s ON s.chunk_pk = c.chunk_pk
        LEFT JOIN sched_ideal_chunk_workers i ON i.chunk_pk = c.chunk_pk
        LEFT JOIN stale_agg st ON st.chunk_pk = c.chunk_pk
        WHERE s.dropped_from_worker_assignment_at IS NULL
          AND NOT s.rejected
        -- Two scalar sort keys, not a ROW() constructor: `(a, b)` would sort on one record-typed
        -- key via record_cmp and materialize an extra ROW column through the 6M-row sort
        -- (measured ~2.5x slower, spilling to disk). The row order is identical.
        ORDER BY d.name, c.first_block
        "#
    );
    let mut stream = sqlx::query_as::<_, ActiveChunkRow>(sqlx::AssertSqlSafe(sql)).fetch(&mut **tx);

    let mut datasets = DatasetInterner::new();
    let mut out = ActiveChunks {
        for_algo: Vec::new(),
        current_placement: CurrentPlacement::default(),
        published: BTreeMap::new(),
    };
    let mut count = 0u64;
    while let Some(mut row) = stream
        .try_next()
        .await
        .context("run_scheduling_cycle: fetch active chunks with placement")?
    {
        let dataset = datasets.intern(&row.chunk.dataset_name);
        let pk = row.chunk.chunk_pk;
        if let Some(holders) = row.worker_ids.take() {
            out.current_placement.insert(pk, holders);
        }
        let chunk = chunk_from_row(row.chunk, dataset);
        out.for_algo
            .push((pk, algo_chunk_from_assignment_chunk(&chunk)));
        out.published.insert(pk, chunk);
        count += 1;
    }
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

/// Apply all four post-write deltas and promote the new ideal as a single simple-query batch — one
/// round-trip instead of eight. The statements run sequentially inside the cycle transaction (each
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
) -> Result<()> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:apply_deltas_and_swap");
    let sql = SQL_DELTAS_AND_SWAP.replace("$WA", &new_wa_id.to_string());
    let res = sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(&mut **tx)
        .await
        .context("run_scheduling_cycle: apply deltas and swap")?;
    timer.stmt(res.rows_affected());
    Ok(())
}

/// The batch run by [`apply_deltas_and_swap`], one round-trip via the simple query protocol. `$WA`
/// is the new worker-assignment id, substituted before execution. Sections, in execution order:
///   1. Mint superseded stale: holders the future ideal drops that still hold a draining copy —
///      known workers only (a GC'd worker holds nothing, and its stale row would break the FK), and
///      not chunks mid portal-drop. `DO NOTHING` keeps any pre-existing stale row.
///   2. Record routing diffs in one FULL JOIN pass (both PKs merge-join): chunks whose holder set
///      changed or entered (emit future holders) or were dropped (emit `'{}'` via the COALESCE).
///      Canonical arrays make `IS DISTINCT FROM` an exact set test, and it also covers the
///      one-sided rows: a NULL side is distinct from any holder set.
///   3. Resolve flip-flops: a pair back in the new ideal is no longer stale.
///   4. Stamp entered chunks: first-touch the assignment id onto chunks new to the ideal.
///   5. Swap: truncate the old ideal, then 3-way rename so the future twin becomes live and the old
///      one is left empty for the next cycle.
const SQL_DELTAS_AND_SWAP: &str = r#"
INSERT INTO sched_stale_mappings (chunk_pk, worker_id, superseded_at_worker_assignment_id)
SELECT c.chunk_pk, u.worker_id, $WA
FROM sched_ideal_chunk_workers c
LEFT JOIN sched_future_ideal_chunk_workers f ON f.chunk_pk = c.chunk_pk
CROSS JOIN LATERAL UNNEST(c.worker_ids) AS u(worker_id)
WHERE u.worker_id <> ALL (COALESCE(f.worker_ids, '{}'::bigint[]))
  AND EXISTS (SELECT 1 FROM sched_workers sw WHERE sw.id = u.worker_id)
  AND NOT EXISTS (
      SELECT 1 FROM sched_chunk_metadata m
      WHERE m.chunk_pk = c.chunk_pk
        AND m.dropped_at_portal_assignment_id IS NOT NULL
  )
ON CONFLICT (chunk_pk, worker_id) DO NOTHING;

INSERT INTO sched_worker_assignment_diffs (worker_assignment_id, chunk_pk, worker_ids)
SELECT $WA, COALESCE(f.chunk_pk, c.chunk_pk), COALESCE(f.worker_ids, '{}'::bigint[])
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

/// Stage the freshly computed ideal into the (empty) future twin via one `COPY … FROM STDIN`: one
/// text row per chunk. Holders arrive canonical from `invert_worker_chunks` (sorted by worker_pk,
/// distinct), so each stored array stays canonical for next cycle's `IS DISTINCT FROM` diff against
/// the live ideal. (The algorithm emits a chunk only once a worker holds it, so there are no
/// empty-holder rows.)
///
/// The PK is dropped before the COPY and rebuilt after: one sorted index build beats ~6M
/// incremental btree inserts. Rebuilt here, before [`apply_deltas_and_swap`], so its diff joins
/// still see both PKs. The constraint is looked up by catalog, not name: table renames in the swap
/// carry constraint names along, so after a swap the twin holds the other table's `_pkey` name.
pub(super) async fn write_future_ideal(
    tx: &mut Transaction<'_, Postgres>,
    ideal_mappings: &[(ChunkPk, Vec<WorkerPk>)],
    batch_size: usize,
) -> Result<()> {
    use itertools::Itertools as _;
    use std::fmt::Write as _;

    let mut timer = PhaseTimer::new("run_scheduling_cycle:write_future_ideal");
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
    // Auto-named: the preferred `_pkey` name may be held by the live twin after a swap, in which
    // case Postgres picks a suffixed one — the names ping-pong, nothing depends on them.
    sqlx::raw_sql("ALTER TABLE sched_future_ideal_chunk_workers ADD PRIMARY KEY (chunk_pk)")
        .execute(&mut **tx)
        .await
        .context("run_scheduling_cycle: rebuild future ideal pk")?;
    timer.stmt(rows);
    Ok(())
}

/// Decoded workers: `published` is what the assignment exposes; `for_algo` is what the scheduling
/// algorithm consumes (keyed the same, paired with the parsed `Worker`).
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
        published.insert(id, worker);
    }
    Ok(DecodedWorkers {
        published,
        for_algo,
    })
}

/// Post-commit assembly of the published WorkerAssignment: ideal ∪ stale, excluding tombstoned
/// chunks. The ideal is the algorithm output we just committed (byte-for-byte what
/// [`write_future_ideal`] staged), and the chunk columns were already decoded by the cycle's
/// active-chunks read — so the only query left is the (churn-sized) post-delta stale set, not a
/// second ~6M-row pass over chunks.
pub(super) async fn build_worker_assignment(
    conn: &mut PgConnection,
    id: AssignmentId,
    workers: &BTreeMap<WorkerPk, AssignmentWorker>,
    replication_by_weight: BTreeMap<u16, u16>,
    ideal_mappings: Vec<(ChunkPk, Vec<WorkerPk>)>,
    active_chunks: BTreeMap<ChunkPk, WorkerAssignmentChunk>,
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
    let mut active_chunks = active_chunks;
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
        workers: workers.clone(),
        replication_by_weight,
    })
}
