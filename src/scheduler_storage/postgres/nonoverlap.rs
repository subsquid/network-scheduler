//! Postgres-native non-overlap enforcement, shared by registration and promotion.
//!
//! Both ask the same question — does this chunk's block range overlap one already in the dataset? —
//! and answer it without loading the dataset's whole set:
//!
//! 1. an indexed SQL probe rejects candidates that overlap an existing chunk, then
//! 2. a small Rust pass settles overlaps *within* the batch (two new chunks covering the same range).
//!
//! The probe rides the `chunks(dataset_id, (first_block + last_block_delta))` index, so it touches
//! only the few chunks whose range reaches up to a candidate — not the dataset's whole history.
//! See `docs/nonoverlap-promotion-gate.md`.

use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use sqlx::postgres::PgRow;
use sqlx::{PgExecutor, Row};

use crate::scheduler_storage::{ChunkPk, DatasetId};

/// A chunk being considered for admission or promotion, with its inclusive block range.
pub(super) struct Candidate {
    pub pk: ChunkPk,
    pub dataset_id: DatasetId,
    pub dataset_name: String,
    pub first_block: i64,
    pub last_block: i64,
}

impl Candidate {
    /// From a row exposing `chunk_pk, dataset_id, dataset_name, first_block, last_block_delta`.
    pub(super) fn from_row(row: &PgRow) -> Self {
        let first_block: i64 = row.get("first_block");
        let last_block_delta: i32 = row.get("last_block_delta");
        Candidate {
            pk: row.get("chunk_pk"),
            dataset_id: row.get("dataset_id"),
            dataset_name: row.get("dataset_name"),
            first_block,
            last_block: first_block + i64::from(last_block_delta),
        }
    }
}

/// The chunks a *new* candidate must not overlap: admitted, not rejected, not leaving. No
/// `applied_at_worker` gate — a chunk claims its range from admission, so a not-yet-scheduled (or
/// shortage-stuck) one must still block an overlapping newcomer.
const LIVE_ADMITTED: &str = "NOT s.rejected \
     AND s.marked_for_removal IS NULL \
     AND s.dropped_at_portal_assignment_id IS NULL \
     AND s.dropped_at_worker_assignment_id IS NULL";

/// The chunks a *promotable* candidate must not overlap: visible now and staying visible. Excluding
/// `marked_for_removal` lets a same-range correction replacement promote past the chunk it supersedes.
const STILL_VISIBLE: &str = "s.applied_at_portal_assignment_id IS NOT NULL \
     AND s.dropped_at_portal_assignment_id IS NULL \
     AND s.marked_for_removal IS NULL";

/// pks of candidates whose range overlaps a live chunk in the same dataset — to reject at registration.
pub(super) async fn overlapping_live<'e>(
    executor: impl PgExecutor<'e>,
    candidates: &[Candidate],
) -> Result<HashSet<ChunkPk>> {
    overlapping(executor, candidates, LIVE_ADMITTED).await
}

/// pks of candidates whose range overlaps a still-visible chunk in the same dataset — to hold back at
/// promotion.
pub(super) async fn overlapping_visible<'e>(
    executor: impl PgExecutor<'e>,
    candidates: &[Candidate],
) -> Result<HashSet<ChunkPk>> {
    overlapping(executor, candidates, STILL_VISIBLE).await
}

/// From candidates that already cleared the existing-chunk probe, keep the ones that don't overlap
/// *each other*; lower `(first_block, chunk_pk)` wins. Input is batch-sized, so one sorted pass does
/// it. Returns `(accepted pks, rejected candidates)`.
pub(super) fn settle_within_batch(mut clear: Vec<Candidate>) -> (Vec<ChunkPk>, Vec<Candidate>) {
    clear.sort_unstable_by_key(|c| (c.dataset_id, c.first_block, c.pk));
    let mut accepted = Vec::new();
    let mut rejected = Vec::new();
    // Sorted by start within a dataset, accepted ranges are non-overlapping with rising ends, so a
    // candidate need only be checked against the last accepted chunk in its dataset.
    let mut last: Option<(DatasetId, i64)> = None; // (dataset_id, last_block of last accepted)
    for c in clear {
        match last {
            Some((dataset_id, end)) if dataset_id == c.dataset_id && c.first_block <= end => {
                rejected.push(c);
            }
            _ => {
                last = Some((c.dataset_id, c.last_block));
                accepted.push(c.pk);
            }
        }
    }
    (accepted, rejected)
}

/// Log + count chunks refused at registration. Call every cycle so the gauge resets on a clean one.
pub(super) fn report_registration_rejected(rejected: &[Candidate]) {
    report(
        rejected,
        "chunks rejected at registration: block range overlaps a live chunk in the dataset",
        "rejected at registration",
        crate::metrics::report_registration_rejected,
    );
}

/// Log + count chunks held back at promotion (should never fire — registration removed overlaps).
/// Call every cycle so the gauge resets on a clean one.
pub(super) fn report_promotion_held_back(held: &[Candidate]) {
    report(
        held,
        "chunks held back from portal promotion: block range overlaps a visible chunk in the dataset",
        "held back from promotion",
        crate::metrics::report_promotion_held_back,
    );
}

fn report(
    chunks: &[Candidate],
    summary: &str,
    per_chunk: &'static str,
    emit: impl FnOnce(BTreeMap<String, i64>),
) {
    if !chunks.is_empty() {
        tracing::warn!(count = chunks.len(), "{summary}");
        for c in chunks {
            tracing::debug!(
                chunk = c.pk.0,
                dataset_id = c.dataset_id.0,
                first_block = c.first_block,
                last_block = c.last_block,
                "{per_chunk}"
            );
        }
    }
    emit(counts_by_dataset(chunks));
}

/// For each candidate (passed as parallel arrays), is there an existing chunk in its dataset that
/// matches `state` and whose range overlaps it?
async fn overlapping<'e>(
    executor: impl PgExecutor<'e>,
    candidates: &[Candidate],
    state: &str,
) -> Result<HashSet<ChunkPk>> {
    if candidates.is_empty() {
        return Ok(HashSet::new());
    }
    let mut timer = crate::metrics::PhaseTimer::new("nonoverlap:overlap_probe");
    let pks: Vec<ChunkPk> = candidates.iter().map(|c| c.pk).collect();
    let datasets: Vec<DatasetId> = candidates.iter().map(|c| c.dataset_id).collect();
    let firsts: Vec<i64> = candidates.iter().map(|c| c.first_block).collect();
    let lasts: Vec<i64> = candidates.iter().map(|c| c.last_block).collect();

    // Inclusive-range overlap: an existing chunk overlaps us if it ends at/after our start and
    // starts at/before our end (same predicate as the in-memory `ranges_overlap`).
    // `state` is one of two in-crate const fragments (LIVE_ADMITTED / STILL_VISIBLE), never user
    // input — safe to interpolate, hence `AssertSqlSafe` on the built string (sqlx 0.9 gates
    // dynamic SQL behind `SqlSafeStr`).
    let sql = format!(
        r#"
        SELECT cand.chunk_pk
        FROM UNNEST($1::bigint[], $2::smallint[], $3::bigint[], $4::bigint[])
               AS cand(chunk_pk, dataset_id, first_block, last_block)
        WHERE EXISTS (
            SELECT 1
            FROM sched_chunk_metadata s
            JOIN chunks c ON c.chunk_pk = s.chunk_pk
            WHERE c.dataset_id = cand.dataset_id
              AND c.first_block + c.last_block_delta >= cand.first_block
              AND c.first_block <= cand.last_block
              AND {state}
        )
        "#
    );
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(&pks)
        .bind(&datasets)
        .bind(&firsts)
        .bind(&lasts)
        .fetch_all(executor)
        .await
        .context("non-overlap: existing-chunk probe")?;
    timer.stmt(rows.len() as u64);
    Ok(rows
        .iter()
        .map(|r| r.get::<ChunkPk, _>("chunk_pk"))
        .collect())
}

fn counts_by_dataset(chunks: &[Candidate]) -> BTreeMap<String, i64> {
    let mut counts: BTreeMap<String, i64> = BTreeMap::new();
    for c in chunks {
        *counts.entry(c.dataset_name.clone()).or_default() += 1;
    }
    counts
}
