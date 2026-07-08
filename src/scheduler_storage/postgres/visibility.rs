//! Phase helpers for [`PostgresStorage::run_visibility_cycle`] and
//! [`PostgresStorage::confirm_worker_assignment`].

use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use sqlx::postgres::PgConnection;
use sqlx::{Postgres, Row, Transaction};

use crate::metrics::PhaseTimer;
use crate::scheduler_storage::{
    AssignmentId, AssignmentWorker, ChunkPk, PortalAssignment, WorkerAssignmentChunk, WorkerPk,
};
use crate::types::BlockNumber;

use super::rows::{
    ChunkRow, DatasetInterner, WorkerRow, assignment_worker_from_row, chunk_from_row, tick_to_i64,
};

// ---------------------------------------------------------------------------
// confirm_worker_assignment phases
// ---------------------------------------------------------------------------

/// Current confirmation watermark (max confirmed assignment_id).
pub(super) async fn confirmation_watermark(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<AssignmentId> {
    let mut timer = PhaseTimer::new("visibility:confirmation_watermark");
    let watermark = sqlx::query_scalar(
        "SELECT COALESCE(MAX(assignment_id), 0) FROM sched_worker_confirmations",
    )
    .fetch_one(&mut **tx)
    .await?;
    timer.stmt(1);
    Ok(watermark)
}

pub(super) async fn advance_confirmation_watermark(
    tx: &mut Transaction<'_, Postgres>,
    assignment_id: AssignmentId,
    now: u64,
) -> Result<()> {
    let mut timer = PhaseTimer::new("confirm_worker_assignment:advance_confirmation_watermark");
    let res = sqlx::query(
        "INSERT INTO sched_worker_confirmations (assignment_id, confirmed_at) VALUES ($1, $2) ON CONFLICT DO NOTHING"
    )
    .bind(assignment_id)
    .bind(tick_to_i64(now))
    .execute(&mut **tx)
    .await?;
    timer.stmt(res.rows_affected());
    Ok(())
}

/// Replay diffs in `(prev, assignment_id]` into `sched_confirmed_chunk_workers`.
pub(super) async fn replay_confirmed_diffs(
    tx: &mut Transaction<'_, Postgres>,
    prev: AssignmentId,
    assignment_id: AssignmentId,
) -> Result<()> {
    // The watermark can advance several assignments at once, so a chunk may have multiple diff rows
    // in (prev, assignment_id] (one per cycle it changed). Keep the newest per chunk — both because
    // it's the current routing and because one INSERT can't hit the same ON CONFLICT row twice:
    // `DISTINCT ON (chunk_pk) ... ORDER BY worker_assignment_id DESC`. Empty final set = removed.
    let mut timer = PhaseTimer::new("confirm_worker_assignment:replay_confirmed_diffs");
    let upserted = sqlx::query(
        r#"
        INSERT INTO sched_confirmed_chunk_workers (chunk_pk, worker_ids)
        SELECT chunk_pk, worker_ids
        FROM (
            SELECT DISTINCT ON (chunk_pk) chunk_pk, worker_ids
            FROM sched_worker_assignment_diffs
            WHERE worker_assignment_id > $1
              AND worker_assignment_id <= $2
            ORDER BY chunk_pk, worker_assignment_id DESC
        ) final
        WHERE cardinality(worker_ids) > 0
        ON CONFLICT (chunk_pk) DO UPDATE SET worker_ids = EXCLUDED.worker_ids
        "#,
    )
    .bind(prev)
    .bind(assignment_id)
    .execute(&mut **tx)
    .await?;
    timer.stmt(upserted.rows_affected());

    let deleted = sqlx::query(
        r#"
        DELETE FROM sched_confirmed_chunk_workers
        WHERE chunk_pk IN (
            SELECT chunk_pk FROM (
                SELECT DISTINCT ON (chunk_pk) chunk_pk, worker_ids
                FROM sched_worker_assignment_diffs
                WHERE worker_assignment_id > $1
                  AND worker_assignment_id <= $2
                ORDER BY chunk_pk, worker_assignment_id DESC
            ) final
            WHERE cardinality(worker_ids) = 0
        )
        "#,
    )
    .bind(prev)
    .bind(assignment_id)
    .execute(&mut **tx)
    .await?;
    timer.stmt(deleted.rows_affected());
    Ok(())
}

/// Drop the replayed diffs in `(prev, assignment_id]`.
pub(super) async fn drop_replayed_diffs(
    tx: &mut Transaction<'_, Postgres>,
    prev: AssignmentId,
    assignment_id: AssignmentId,
) -> Result<()> {
    let mut timer = PhaseTimer::new("confirm_worker_assignment:drop_replayed_diffs");
    let res = sqlx::query(
        r#"
        DELETE FROM sched_worker_assignment_diffs
        WHERE worker_assignment_id > $1 AND worker_assignment_id <= $2
        "#,
    )
    .bind(prev)
    .bind(assignment_id)
    .execute(&mut **tx)
    .await?;
    timer.stmt(res.rows_affected());
    Ok(())
}

// ---------------------------------------------------------------------------
// run_visibility_cycle phases
// ---------------------------------------------------------------------------

pub(super) async fn open_portal_assignment(
    tx: &mut Transaction<'_, Postgres>,
    now: u64,
) -> Result<AssignmentId> {
    let mut timer = PhaseTimer::new("run_visibility_cycle:open_portal_assignment");
    let id = sqlx::query_scalar(
        "INSERT INTO sched_portal_assignments (created_at) VALUES ($1) RETURNING id",
    )
    .bind(tick_to_i64(now))
    .fetch_one(&mut **tx)
    .await
    .context("run_visibility_cycle: insert portal assignment")?;
    timer.stmt(1);
    Ok(id)
}

/// Apply all ready corrections (must run before promote/drop). A correction fires once its
/// replacement is confirmed and no *pending* correction
/// still has to produce its old chunk (no pending `X → old`). The only dependency is a correction
/// chain (`B → C` waits for `A → B`); independent corrections fire together. We fetch the pending
/// set and resolve it with a Rust fixpoint — re-scanning until no new firing — so the result is
/// independent of row order and `created_at` (pure audit metadata), and chains still collapse in
/// one pass. Then bulk-write.
pub(super) async fn apply_ready_corrections(
    tx: &mut Transaction<'_, Postgres>,
    new_pa_id: AssignmentId,
    confirmed_up_to: AssignmentId,
    now: u64,
) -> Result<()> {
    let mut timer = PhaseTimer::new("run_visibility_cycle:apply_ready_corrections");
    let rows = sqlx::query(
        r#"
        SELECT cc.old_chunk_pk, cc.new_chunk_pk,
               s.applied_at_worker_assignment_id AS new_chunk_confirmed_at
        FROM chunk_corrections cc
        JOIN sched_chunk_metadata s ON s.chunk_pk = cc.new_chunk_pk
        WHERE cc.applied_at_portal_assignment_id IS NULL
        "#,
    )
    .fetch_all(&mut **tx)
    .await?;
    timer.stmt(rows.len() as u64);

    if rows.is_empty() {
        return Ok(());
    }

    // (old, new, replacement-confirmed) for every pending correction.
    let pending: Vec<(ChunkPk, ChunkPk, bool)> = rows
        .iter()
        .map(|r| {
            let confirmed = r
                .get::<Option<AssignmentId>, _>("new_chunk_confirmed_at")
                .is_some_and(|at| at <= confirmed_up_to);
            (
                r.get::<ChunkPk, _>("old_chunk_pk"),
                r.get::<ChunkPk, _>("new_chunk_pk"),
                confirmed,
            )
        })
        .collect();

    // Structural fixpoint: fire a confirmed correction unless a still-pending (unfired) correction
    // produces its old chunk. Re-scan until stable so a chain collapses in one pass whatever the
    // row order. `unfired_producers` holds every unfired correction's new_chunk_pk (replacements
    // are freshly inserted rows, so those pks are distinct); a correction is blocked exactly while
    // its old chunk is in that set. The O(1) lookup keeps a P-correction batch at O(P) per pass —
    // the per-candidate rescan it replaces was O(P²), tens of seconds at a 147K-chunk replacement.
    let mut fired: HashSet<ChunkPk> = HashSet::new();
    let mut unfired_producers: HashSet<ChunkPk> = pending.iter().map(|&(_, new, _)| new).collect();
    loop {
        let mut progressed = false;
        for &(old, new, confirmed) in &pending {
            if !confirmed || fired.contains(&old) || unfired_producers.contains(&old) {
                continue;
            }
            fired.insert(old);
            unfired_producers.remove(&new);
            progressed = true;
        }
        if !progressed {
            break;
        }
    }

    let fired_old_pks: Vec<ChunkPk> = fired.into_iter().collect();

    if fired_old_pks.is_empty() {
        return Ok(());
    }

    let res = sqlx::query(
        "UPDATE sched_chunk_metadata SET marked_for_removal = $1 WHERE chunk_pk = ANY($2)",
    )
    .bind(tick_to_i64(now))
    .bind(&fired_old_pks)
    .execute(&mut **tx)
    .await?;
    timer.stmt(res.rows_affected());

    let res = sqlx::query(
        "UPDATE chunk_corrections SET applied_at_portal_assignment_id = $1 WHERE old_chunk_pk = ANY($2)",
    )
    .bind(new_pa_id)
    .bind(&fired_old_pks)
    .execute(&mut **tx)
    .await?;
    timer.stmt(res.rows_affected());

    Ok(())
}

/// Promote every eligible chunk to portal-visible in one UPDATE: confirmed by a worker assignment
/// at/under the watermark, not yet visible, not dropped, not marked for removal, and not a pending
/// correction's replacement (those promote via the correction path). No `rejected` check — rejected
/// chunks never reach a worker assignment, so the first condition already excludes them. The
/// non-overlap gate now lives in [`evict_portal_overlaps`], which settles overlaps in memory over the
/// visible set we fetch anyway — so this neither fetches the candidates nor probes per chunk.
pub(super) async fn promote_eligible_chunks(
    tx: &mut Transaction<'_, Postgres>,
    new_pa_id: AssignmentId,
    confirmed_up_to: AssignmentId,
) -> Result<()> {
    let mut timer = PhaseTimer::new("run_visibility_cycle:promote_eligible_chunks");
    let res = sqlx::query(
        r#"
        UPDATE sched_chunk_metadata SET applied_at_portal_assignment_id = $1
        WHERE applied_at_worker_assignment_id IS NOT NULL
          AND applied_at_worker_assignment_id <= $2
          AND applied_at_portal_assignment_id IS NULL
          AND dropped_at_portal_assignment_id IS NULL
          AND marked_for_removal IS NULL
          AND chunk_pk NOT IN (
              SELECT new_chunk_pk FROM chunk_corrections
              WHERE applied_at_portal_assignment_id IS NULL
          )
        "#,
    )
    .bind(new_pa_id)
    .bind(confirmed_up_to)
    .execute(&mut **tx)
    .await
    .context("run_visibility_cycle: promote eligible chunks")?;
    timer.stmt(res.rows_affected());
    Ok(())
}

/// Portal non-overlap gate, in memory over the visible set the assignment build already holds: one
/// sorted pass covers what the old SQL probe + in-batch settle did (new-vs-existing and new-vs-new).
/// Overlappers — lowest `(first_block, chunk_pk)` wins — are dropped from `chunks` and un-promoted, to
/// be reconsidered next cycle. The held-back gauge is emitted every cycle and should always read 0:
/// registration keeps admitted chunks disjoint, so a non-zero count means that invariant broke — hence
/// the `warn`.
pub(super) async fn evict_portal_overlaps(
    conn: &mut PgConnection,
    chunks: &mut BTreeMap<ChunkPk, WorkerAssignmentChunk>,
) -> Result<()> {
    // Time the in-memory sort + sweep as a CPU phase: swept count as items, no statements.
    let losers = {
        let mut timer = PhaseTimer::new("run_visibility_cycle:portal_overlap_sweep");
        let losers = portal_overlap_losers(chunks);
        timer.items(chunks.len() as u64);
        losers
    };
    crate::metrics::report_promotion_held_back(held_back_by_dataset(chunks, &losers));
    if losers.is_empty() {
        return Ok(());
    }
    tracing::warn!(
        count = losers.len(),
        "chunks evicted from portal assignment: block range overlaps another visible chunk \
         (registration should have prevented this)"
    );
    let mut timer = PhaseTimer::new("run_visibility_cycle:evict_portal_overlaps");
    let res = sqlx::query(
        "UPDATE sched_chunk_metadata SET applied_at_portal_assignment_id = NULL WHERE chunk_pk = ANY($1)",
    )
    .bind(&losers)
    .execute(conn)
    .await
    .context("run_visibility_cycle: un-promote overlapping chunks")?;
    timer.stmt(res.rows_affected());
    for pk in &losers {
        chunks.remove(pk);
    }
    Ok(())
}

/// pks of visible chunks that overlap an earlier-ranked chunk in the same dataset. Single sorted
/// sweep: chunks are interned, so group by the dataset `Arc`'s identity; within a dataset keep the
/// lowest `(first_block, chunk_pk)` and reject anything whose start falls at/before the last kept end
/// (same predicate as [`super::nonoverlap::settle_within_batch`], over the whole visible set).
fn portal_overlap_losers(chunks: &BTreeMap<ChunkPk, WorkerAssignmentChunk>) -> Vec<ChunkPk> {
    let mut items: Vec<(usize, BlockNumber, BlockNumber, ChunkPk)> = chunks
        .iter()
        .map(|(&pk, c)| {
            (
                std::sync::Arc::as_ptr(&c.dataset) as usize,
                *c.blocks.start(),
                *c.blocks.end(),
                pk,
            )
        })
        .collect();
    items.sort_unstable_by_key(|&(dataset, first, _last, pk)| (dataset, first, pk));
    let mut losers = Vec::new();
    let mut last: Option<(usize, BlockNumber)> = None; // (dataset, last_block of last kept)
    for (dataset, first, last_block, pk) in items {
        match last {
            Some((kept_dataset, kept_end)) if kept_dataset == dataset && first <= kept_end => {
                losers.push(pk);
            }
            _ => last = Some((dataset, last_block)),
        }
    }
    losers
}

/// Per-dataset counts of evicted chunks, for the held-back gauge.
fn held_back_by_dataset(
    chunks: &BTreeMap<ChunkPk, WorkerAssignmentChunk>,
    losers: &[ChunkPk],
) -> BTreeMap<String, i64> {
    let mut counts: BTreeMap<String, i64> = BTreeMap::new();
    for pk in losers {
        if let Some(chunk) = chunks.get(pk) {
            *counts.entry(chunk.dataset.to_string()).or_default() += 1;
        }
    }
    counts
}

pub(super) async fn drop_marked_chunks(
    tx: &mut Transaction<'_, Postgres>,
    new_pa_id: AssignmentId,
) -> Result<()> {
    let mut timer = PhaseTimer::new("run_visibility_cycle:drop_marked_chunks");
    let res = sqlx::query(
        r#"
        UPDATE sched_chunk_metadata
        SET dropped_at_portal_assignment_id = $1
        WHERE marked_for_removal IS NOT NULL
          AND dropped_at_portal_assignment_id IS NULL
        "#,
    )
    .bind(new_pa_id)
    .execute(&mut **tx)
    .await?;
    timer.stmt(res.rows_affected());
    Ok(())
}

/// Activate drains: stamp pending stale mappings whose superseding worker
/// assignment is now confirmed.
pub(super) async fn activate_confirmed_drains(
    tx: &mut Transaction<'_, Postgres>,
    new_pa_id: AssignmentId,
    confirmed_up_to: AssignmentId,
) -> Result<()> {
    let mut timer = PhaseTimer::new("run_visibility_cycle:activate_confirmed_drains");
    let res = sqlx::query(
        r#"
        UPDATE sched_stale_mappings
        SET dropped_at_portal_assignment_id = $1
        WHERE dropped_at_portal_assignment_id IS NULL
          AND superseded_at_worker_assignment_id <= $2
        "#,
    )
    .bind(new_pa_id)
    .bind(confirmed_up_to)
    .execute(&mut **tx)
    .await?;
    timer.stmt(res.rows_affected());
    Ok(())
}

/// Portal-visible = applied to a portal assignment and not dropped.
pub(super) async fn fetch_portal_visible_chunks(
    conn: &mut PgConnection,
) -> Result<BTreeMap<ChunkPk, WorkerAssignmentChunk>> {
    let mut timer = PhaseTimer::new("run_visibility_cycle:fetch_portal_visible_chunks");
    let chunk_rows: Vec<ChunkRow> = sqlx::query_as(
        r#"
        SELECT c.chunk_pk, d.name AS dataset_name, c.chunk_id, c.size,
               c.schema_id, c.tables_present,
               c.first_block, c.last_block_delta
        FROM chunks c
        JOIN datasets d ON d.id = c.dataset_id
        JOIN sched_chunk_metadata s ON s.chunk_pk = c.chunk_pk
        WHERE s.applied_at_portal_assignment_id IS NOT NULL
          AND s.dropped_at_portal_assignment_id IS NULL
        "#,
    )
    .fetch_all(&mut *conn)
    .await?;
    timer.stmt(chunk_rows.len() as u64);

    let mut datasets = DatasetInterner::new();
    let mut out: BTreeMap<ChunkPk, WorkerAssignmentChunk> = BTreeMap::new();
    for row in chunk_rows {
        let dataset = datasets.intern(&row.dataset_name);
        out.insert(row.chunk_pk, chunk_from_row(row, dataset));
    }
    Ok(out)
}

/// Confirmed routing for the portal-visible chunks. The visible set is re-derived server-side —
/// the same predicate `fetch_portal_visible_chunks` reads and `evict_portal_overlaps` has already
/// written its un-promotions to — instead of shipping the ~6M-pk array back as a parameter
/// (~50 MB serialized and a server-side hash of every element, ~3x the join's cost).
pub(super) async fn fetch_confirmed_routing(
    conn: &mut PgConnection,
) -> Result<BTreeMap<ChunkPk, Vec<WorkerPk>>> {
    let mut timer = PhaseTimer::new("run_visibility_cycle:fetch_confirmed_routing");
    let rows = sqlx::query(
        r#"
        SELECT ccw.chunk_pk, ccw.worker_ids
        FROM sched_confirmed_chunk_workers ccw
        JOIN sched_chunk_metadata s ON s.chunk_pk = ccw.chunk_pk
        WHERE s.applied_at_portal_assignment_id IS NOT NULL
          AND s.dropped_at_portal_assignment_id IS NULL
        "#,
    )
    .fetch_all(conn)
    .await?;
    timer.stmt(rows.len() as u64);
    let routing = rows
        .into_iter()
        .map(|row| {
            (
                row.get::<ChunkPk, _>("chunk_pk"),
                row.get::<Vec<WorkerPk>, _>("worker_ids"),
            )
        })
        .collect();
    Ok(routing)
}

pub(super) async fn fetch_portal_workers(
    conn: &mut PgConnection,
) -> Result<BTreeMap<WorkerPk, AssignmentWorker>> {
    let mut timer = PhaseTimer::new("run_visibility_cycle:fetch_portal_workers");
    let worker_rows: Vec<WorkerRow> =
        sqlx::query_as("SELECT id, peer_id, version, inactive_since FROM sched_workers")
            .fetch_all(conn)
            .await?;
    timer.stmt(worker_rows.len() as u64);

    worker_rows
        .iter()
        .map(assignment_worker_from_row)
        .collect::<Result<_>>()
}

pub(super) fn assemble_portal_assignment(
    id: AssignmentId,
    chunks: BTreeMap<ChunkPk, WorkerAssignmentChunk>,
    chunk_workers: BTreeMap<ChunkPk, Vec<WorkerPk>>,
    workers: BTreeMap<WorkerPk, AssignmentWorker>,
) -> PortalAssignment {
    PortalAssignment {
        id,
        chunk_workers,
        chunks,
        workers,
    }
}
