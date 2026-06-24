//! Phase helpers for [`PostgresStorage::run_scheduling_cycle`], run inside the
//! cycle's transaction, plus the post-commit [`build_worker_assignment`] read.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use anyhow::{Context, Result};
use semver::Version;
use sqlx::postgres::PgConnection;
use sqlx::{Postgres, Row, Transaction};

use crate::metrics::{PhaseTimer, Timer};
use crate::scheduler_storage::{
    AssignmentId, AssignmentWorker, ChunkPk, WorkerAssignment, WorkerPk,
};
use crate::types::{Chunk, Worker};

use super::rows::{
    ChunkRow, DatasetInterner, IdealChunkWorkersRow, StaleWorkerRow, WorkerRow,
    assignment_worker_from_row, chunk_from_row, tick_to_i64,
};

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

/// Tombstone chunks whose portal-drop landed at least `m_ticks` ago.
pub(super) async fn tombstone_expired_chunks(
    tx: &mut Transaction<'_, Postgres>,
    new_wa_id: AssignmentId,
    now: u64,
    m_ticks: u64,
) -> Result<()> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:tombstone_expired_chunks");
    let res = sqlx::query(
        r#"
        UPDATE sched_chunk_metadata
        SET dropped_at_worker_assignment_id = $1
        WHERE dropped_at_portal_assignment_id IS NOT NULL
          AND dropped_at_worker_assignment_id IS NULL
          AND dropped_at_portal_assignment_id IN (
              SELECT id FROM sched_portal_assignments
              WHERE created_at <= $2 - $3
          )
        "#,
    )
    .bind(new_wa_id)
    .bind(tick_to_i64(now))
    .bind(tick_to_i64(m_ticks))
    .execute(&mut **tx)
    .await
    .context("run_scheduling_cycle: tombstone chunks")?;
    timer.stmt(res.rows_affected());

    // Drop tombstoned chunks' stale mappings. A removed chunk's stale never drains (its
    // superseded_at_worker assignment is never confirmed, so it's never dropped_at_portal/expired);
    // left behind it counts in `ideal ∪ stale` and overcommits a worker. Mirrors the in-memory oracle.
    let res = sqlx::query(
        "DELETE FROM sched_stale_mappings WHERE chunk_pk IN \
         (SELECT chunk_pk FROM sched_chunk_metadata WHERE dropped_at_worker_assignment_id IS NOT NULL)",
    )
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

pub(super) async fn fetch_ideal_placement_rows(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Vec<IdealChunkWorkersRow>> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:fetch_ideal_placement_rows");
    let rows: Vec<IdealChunkWorkersRow> =
        sqlx::query_as("SELECT chunk_pk, worker_ids FROM sched_ideal_chunk_workers")
            .fetch_all(&mut **tx)
            .await
            .context("run_scheduling_cycle: fetch ideal placement")?;
    timer.stmt(rows.len() as u64);
    Ok(rows)
}

pub(super) async fn fetch_stale_placement_rows(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Vec<StaleWorkerRow>> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:fetch_stale_placement_rows");
    let rows: Vec<StaleWorkerRow> =
        sqlx::query_as("SELECT chunk_pk, worker_id FROM sched_stale_mappings")
            .fetch_all(&mut **tx)
            .await
            .context("run_scheduling_cycle: fetch stale placement")?;
    timer.stmt(rows.len() as u64);
    Ok(rows)
}

/// Union of ideal and stale holders per chunk — the placement handed to the algorithm.
pub(super) fn merge_current_placement(
    ideal_rows: &[IdealChunkWorkersRow],
    stale_rows: &[StaleWorkerRow],
) -> BTreeMap<ChunkPk, Vec<WorkerPk>> {
    let _timer = Timer::new("run_scheduling_cycle:merge_current_placement");
    let mut placement_set: BTreeMap<ChunkPk, BTreeSet<WorkerPk>> = BTreeMap::new();
    for row in ideal_rows {
        placement_set
            .entry(row.chunk_pk)
            .or_default()
            .extend(row.worker_ids.iter().copied());
    }
    for row in stale_rows {
        placement_set
            .entry(row.chunk_pk)
            .or_default()
            .insert(row.worker_id);
    }
    placement_set
        .into_iter()
        .map(|(chunk_pk, holders)| (chunk_pk, holders.into_iter().collect()))
        .collect()
}

/// Active = not tombstoned.
pub(super) async fn fetch_active_chunks(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Vec<ChunkRow>> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:fetch_active_chunks");
    let rows: Vec<ChunkRow> = sqlx::query_as(
        r#"
        SELECT c.chunk_pk, d.name AS dataset_name, c.chunk_id, c.size, c.files,
               c.first_block, c.last_block_delta
        FROM chunks c
        JOIN datasets d ON d.id = c.dataset_id
        JOIN sched_chunk_metadata s ON s.chunk_pk = c.chunk_pk
        WHERE s.dropped_at_worker_assignment_id IS NULL
          AND NOT s.rejected
        "#,
    )
    .fetch_all(&mut **tx)
    .await
    .context("run_scheduling_cycle: fetch active chunks")?;
    timer.stmt(rows.len() as u64);
    Ok(rows)
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

/// Chunks with a pending portal-drop; skipped when minting stale mappings.
pub(super) async fn fetch_being_removed_chunks(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<HashSet<ChunkPk>> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:fetch_being_removed_chunks");
    let being_removed_rows = sqlx::query(
        "SELECT chunk_pk FROM sched_chunk_metadata WHERE dropped_at_portal_assignment_id IS NOT NULL",
    )
    .fetch_all(&mut **tx)
    .await
    .context("run_scheduling_cycle: fetch being-removed chunks")?;
    timer.stmt(being_removed_rows.len() as u64);
    Ok(being_removed_rows
        .iter()
        .map(|row| row.get::<ChunkPk, _>("chunk_pk"))
        .collect())
}

/// Mint pending stale mappings for `(chunk, worker)` pairs dropped from the
/// ideal but still holding a draining copy.
pub(super) async fn mint_superseded_stale_mappings(
    tx: &mut Transaction<'_, Postgres>,
    new_wa_id: AssignmentId,
    prev_ideal: &BTreeMap<ChunkPk, HashSet<WorkerPk>>,
    ideal_mappings: &BTreeMap<ChunkPk, HashSet<WorkerPk>>,
    workers_map: &BTreeMap<WorkerPk, AssignmentWorker>,
    being_removed: &HashSet<ChunkPk>,
    batch_size: usize,
) -> Result<()> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:mint_superseded_stale_mappings");
    // Survivors: a chunk's previous holders that are still a known worker and dropped from the new
    // ideal (a worker GC'd from sched_workers holds no draining copy, and a stale row for it would
    // break the worker_id FK and abort the cycle). Chunks on their way out are skipped.
    let mut entries: Vec<(ChunkPk, Vec<WorkerPk>)> = Vec::new();
    for (chunk_pk, prev_workers) in prev_ideal {
        if being_removed.contains(chunk_pk) {
            continue;
        }
        let new_workers = ideal_mappings.get(chunk_pk);
        let survivors: Vec<WorkerPk> = prev_workers
            .iter()
            .copied()
            .filter(|w| workers_map.contains_key(w))
            .filter(|w| !new_workers.is_some_and(|holders| holders.contains(w)))
            .collect();
        if !survivors.is_empty() {
            entries.push((*chunk_pk, survivors));
        }
    }
    for window in entries.chunks(batch_size) {
        let pairs: usize = window.iter().map(|(_, w)| w.len()).sum();
        let mut chunk_pks: Vec<ChunkPk> = Vec::with_capacity(pairs);
        let mut worker_ids: Vec<WorkerPk> = Vec::with_capacity(pairs);
        for (chunk_pk, holders) in window {
            for &worker_id in holders {
                chunk_pks.push(*chunk_pk);
                worker_ids.push(worker_id);
            }
        }
        let res = sqlx::query(
            r#"
            INSERT INTO sched_stale_mappings (chunk_pk, worker_id, superseded_at_worker_assignment_id)
            SELECT chunk_pk, worker_id, $3
            FROM UNNEST($1::bigint[], $2::bigint[]) AS p(chunk_pk, worker_id)
            ON CONFLICT (chunk_pk, worker_id) DO NOTHING
            "#,
        )
        .bind(&chunk_pks)
        .bind(&worker_ids)
        .bind(new_wa_id)
        .execute(&mut **tx)
        .await
        .context("run_scheduling_cycle: mint stale mappings")?;
        timer.stmt(res.rows_affected());
    }
    Ok(())
}

/// Record routing diffs: chunks whose holder set changed, plus chunks removed
/// entirely (empty worker list).
pub(super) async fn record_routing_diffs(
    tx: &mut Transaction<'_, Postgres>,
    new_wa_id: AssignmentId,
    prev_ideal: &BTreeMap<ChunkPk, HashSet<WorkerPk>>,
    ideal_mappings: &BTreeMap<ChunkPk, HashSet<WorkerPk>>,
    batch_size: usize,
) -> Result<()> {
    // Diff'd chunks: holder set changed, or removed entirely (recorded as an empty set). `pair_*`
    // carries the flattened holders, re-aggregated server-side.
    let mut timer = PhaseTimer::new("run_scheduling_cycle:record_routing_diffs");
    let empty: HashSet<WorkerPk> = HashSet::new();
    let mut diffs: Vec<(ChunkPk, &HashSet<WorkerPk>)> = Vec::new();
    for (chunk_pk, new_workers_set) in ideal_mappings {
        let changed = prev_ideal.get(chunk_pk) != Some(new_workers_set);
        if changed {
            diffs.push((*chunk_pk, new_workers_set));
        }
    }
    for chunk_pk in prev_ideal.keys() {
        if !ideal_mappings.contains_key(chunk_pk) {
            diffs.push((*chunk_pk, &empty));
        }
    }
    for window in diffs.chunks(batch_size) {
        let (diff_pks, pair_chunk_pks, pair_worker_ids) = flatten_holders(window);
        let res = sqlx::query(
            r#"
            INSERT INTO sched_worker_assignment_diffs (worker_assignment_id, chunk_pk, worker_ids)
            SELECT $1, d.chunk_pk, COALESCE(w.worker_ids, '{}'::bigint[])
            FROM UNNEST($2::bigint[]) AS d(chunk_pk)
            LEFT JOIN (
                SELECT chunk_pk, array_agg(worker_id ORDER BY worker_id) AS worker_ids
                FROM UNNEST($3::bigint[], $4::bigint[]) AS p(chunk_pk, worker_id)
                GROUP BY chunk_pk
            ) w ON w.chunk_pk = d.chunk_pk
            "#,
        )
        .bind(new_wa_id)
        .bind(&diff_pks)
        .bind(&pair_chunk_pks)
        .bind(&pair_worker_ids)
        .execute(&mut **tx)
        .await
        .context("run_scheduling_cycle: insert routing diffs")?;
        timer.stmt(res.rows_affected());
    }
    Ok(())
}

/// Commit the new ideal: upsert present chunks, delete the ones that left.
pub(super) async fn commit_new_ideal(
    tx: &mut Transaction<'_, Postgres>,
    prev_ideal: &BTreeMap<ChunkPk, HashSet<WorkerPk>>,
    ideal_mappings: &BTreeMap<ChunkPk, HashSet<WorkerPk>>,
    batch_size: usize,
) -> Result<()> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:commit_new_ideal");

    // Upsert in chunk windows. `chunk_pks` drives the LEFT JOIN so a chunk with an empty holder set
    // still upserts '{}'. `IS DISTINCT FROM` skips the UPDATE when the holder set is unchanged — no
    // dead tuple/WAL for stable chunks; correct because both sides are canonically ordered
    // (array_agg ORDER BY worker_id), so an unchanged set compares equal.
    let entries: Vec<(ChunkPk, &HashSet<WorkerPk>)> =
        ideal_mappings.iter().map(|(k, v)| (*k, v)).collect();
    for window in entries.chunks(batch_size) {
        let (chunk_pks, pair_chunk_pks, pair_worker_ids) = flatten_holders(window);
        let res = sqlx::query(
            r#"
            INSERT INTO sched_ideal_chunk_workers (chunk_pk, worker_ids)
            SELECT c.chunk_pk, COALESCE(w.worker_ids, '{}'::bigint[])
            FROM UNNEST($1::bigint[]) AS c(chunk_pk)
            LEFT JOIN (
                SELECT chunk_pk, array_agg(worker_id ORDER BY worker_id) AS worker_ids
                FROM UNNEST($2::bigint[], $3::bigint[]) AS p(chunk_pk, worker_id)
                GROUP BY chunk_pk
            ) w ON w.chunk_pk = c.chunk_pk
            ON CONFLICT (chunk_pk) DO UPDATE SET worker_ids = EXCLUDED.worker_ids
            WHERE sched_ideal_chunk_workers.worker_ids IS DISTINCT FROM EXCLUDED.worker_ids
            "#,
        )
        .bind(&chunk_pks)
        .bind(&pair_chunk_pks)
        .bind(&pair_worker_ids)
        .execute(&mut **tx)
        .await
        .context("run_scheduling_cycle: upsert ideal")?;
        timer.stmt(res.rows_affected());
    }

    // The table held the previous ideal; delete exactly the chunks that left it, by PK (sargable,
    // churn-sized) — far cheaper than a `chunk_pk != ALL(<whole new ideal>)` anti-scan.
    let removed: Vec<ChunkPk> = prev_ideal
        .keys()
        .filter(|pk| !ideal_mappings.contains_key(pk))
        .copied()
        .collect();
    for batch in removed.chunks(batch_size) {
        let res = sqlx::query("DELETE FROM sched_ideal_chunk_workers WHERE chunk_pk = ANY($1)")
            .bind(batch)
            .execute(&mut **tx)
            .await
            .context("run_scheduling_cycle: delete obsolete ideal")?;
        timer.stmt(res.rows_affected());
    }
    Ok(())
}

/// A pair re-added to the ideal is no longer stale, so clear any stale row for
/// it (resolves flip-flops).
pub(super) async fn resolve_flip_flops(
    tx: &mut Transaction<'_, Postgres>,
    ideal_mappings: &BTreeMap<ChunkPk, HashSet<WorkerPk>>,
    batch_size: usize,
) -> Result<()> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:resolve_flip_flops");
    let entries: Vec<(ChunkPk, &HashSet<WorkerPk>)> =
        ideal_mappings.iter().map(|(k, v)| (*k, v)).collect();
    for window in entries.chunks(batch_size) {
        // Only the flattened pairs matter here (the DELETE matches on (chunk_pk, worker_id)).
        let (_, pair_chunk_pks, pair_worker_ids) = flatten_holders(window);
        if pair_chunk_pks.is_empty() {
            continue;
        }
        let res = sqlx::query(
            r#"
            DELETE FROM sched_stale_mappings s
            USING UNNEST($1::bigint[], $2::bigint[]) AS p(chunk_pk, worker_id)
            WHERE s.chunk_pk = p.chunk_pk
              AND s.worker_id = p.worker_id
            "#,
        )
        .bind(&pair_chunk_pks)
        .bind(&pair_worker_ids)
        .execute(&mut **tx)
        .await
        .context("run_scheduling_cycle: resolve flip-flops")?;
        timer.stmt(res.rows_affected());
    }
    Ok(())
}

/// Stamp newly entered chunks with this assignment.
pub(super) async fn stamp_entered_chunks(
    tx: &mut Transaction<'_, Postgres>,
    new_wa_id: AssignmentId,
    ideal_mappings: &BTreeMap<ChunkPk, HashSet<WorkerPk>>,
) -> Result<()> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:stamp_entered_chunks");
    let ideal_pks: Vec<ChunkPk> = ideal_mappings.keys().copied().collect();
    let res = sqlx::query(
        r#"
        UPDATE sched_chunk_metadata
        SET applied_at_worker_assignment_id = $1
        WHERE chunk_pk = ANY($2)
          AND applied_at_worker_assignment_id IS NULL
        "#,
    )
    .bind(new_wa_id)
    .bind(&ideal_pks)
    .execute(&mut **tx)
    .await
    .context("run_scheduling_cycle: stamp entered chunks")?;
    timer.stmt(res.rows_affected());
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

/// Post-commit read building the published WorkerAssignment: ideal ∪ stale,
/// excluding tombstoned chunks.
pub(super) async fn build_worker_assignment(
    conn: &mut PgConnection,
    id: AssignmentId,
    workers: &BTreeMap<WorkerPk, AssignmentWorker>,
    replication_by_weight: BTreeMap<u16, u16>,
) -> Result<WorkerAssignment> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:build_worker_assignment");
    let ideal_rows: Vec<IdealChunkWorkersRow> = sqlx::query_as(
        r#"
        SELECT i.chunk_pk, i.worker_ids
        FROM sched_ideal_chunk_workers i
        JOIN sched_chunk_metadata m ON m.chunk_pk = i.chunk_pk
        WHERE m.dropped_at_worker_assignment_id IS NULL
          AND NOT m.rejected
        "#,
    )
    .fetch_all(&mut *conn)
    .await
    .context("build_worker_assignment: fetch ideal")?;
    timer.stmt(ideal_rows.len() as u64);

    let stale_rows: Vec<StaleWorkerRow> = sqlx::query_as(
        r#"
        SELECT s.chunk_pk, s.worker_id
        FROM sched_stale_mappings s
        JOIN sched_chunk_metadata m ON m.chunk_pk = s.chunk_pk
        WHERE m.dropped_at_worker_assignment_id IS NULL
          AND NOT m.rejected
        "#,
    )
    .fetch_all(&mut *conn)
    .await
    .context("build_worker_assignment: fetch stale")?;
    timer.stmt(stale_rows.len() as u64);

    let chunk_workers = merge_current_placement(&ideal_rows, &stale_rows);

    // Re-fetch only the placed chunks (bounded by the assignment, not the full active set).
    let placed_pks: Vec<ChunkPk> = chunk_workers.keys().copied().collect();
    let chunk_rows: Vec<ChunkRow> = sqlx::query_as(
        r#"
        SELECT c.chunk_pk, d.name AS dataset_name, c.chunk_id, c.size, c.files,
               c.first_block, c.last_block_delta
        FROM chunks c
        JOIN datasets d ON d.id = c.dataset_id
        WHERE c.chunk_pk = ANY($1)
        "#,
    )
    .bind(&placed_pks)
    .fetch_all(&mut *conn)
    .await
    .context("build_worker_assignment: fetch placed chunks")?;
    timer.stmt(chunk_rows.len() as u64);

    let mut datasets = DatasetInterner::new();
    let mut chunks_out: BTreeMap<ChunkPk, Chunk> = BTreeMap::new();
    for row in &chunk_rows {
        let chunk = chunk_from_row(row, datasets.intern(&row.dataset_name))?;
        chunks_out.insert(row.chunk_pk, chunk);
    }

    Ok(WorkerAssignment {
        id,
        chunk_workers,
        chunks: chunks_out,
        workers: workers.clone(),
        replication_by_weight,
    })
}

/// Flatten one window of `(chunk_pk, holders)` into the parallel arrays the full-ideal write phases
/// bind: the distinct chunk pks, plus the `(chunk_pk, worker_id)` pairs (each worker repeated under
/// its chunk).
fn flatten_holders(
    window: &[(ChunkPk, &HashSet<WorkerPk>)],
) -> (Vec<ChunkPk>, Vec<ChunkPk>, Vec<WorkerPk>) {
    let pairs: usize = window.iter().map(|(_, holders)| holders.len()).sum();
    let mut chunk_pks = Vec::with_capacity(window.len());
    let mut pair_chunk_pks = Vec::with_capacity(pairs);
    let mut pair_worker_ids = Vec::with_capacity(pairs);
    for &(chunk_pk, holders) in window {
        chunk_pks.push(chunk_pk);
        for &worker_id in holders {
            pair_chunk_pks.push(chunk_pk);
            pair_worker_ids.push(worker_id);
        }
    }
    (chunk_pks, pair_chunk_pks, pair_worker_ids)
}
