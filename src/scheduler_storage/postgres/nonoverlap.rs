//! Postgres-native non-overlap enforcement, shared by registration and promotion.
//!
//! Both ask the same question — does this chunk's block range overlap one already in the dataset? —
//! and answer it without loading the dataset's whole set:
//!
//! 1. an indexed SQL probe rejects candidates that overlap an existing chunk, then
//! 2. a small Rust pass settles overlaps *within* the batch (two new chunks covering the same range).
//!
//! The probe rides the `chunks_dataset_range_gist` GiST index, which covers both range endpoints,
//! so each candidate touches only the chunks that actually intersect it — not the dataset's whole
//! history, and (unlike a btree walk) not the candidate's own not-yet-live batch-mates.
//! See `docs/nonoverlap-promotion-gate.md`.

use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use sqlx::Row;
use sqlx::postgres::{PgConnection, PgRow};

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
pub(super) const LIVE_ADMITTED: &str = "NOT s.rejected \
     AND s.marked_for_removal IS NULL \
     AND s.dropped_at_portal_assignment_id IS NULL \
     AND s.dropped_from_worker_assignment_at IS NULL";

/// pks of candidates whose range overlaps a live chunk in the same dataset — to reject at registration.
pub(super) async fn overlapping_live(
    conn: &mut PgConnection,
    candidates: &[Candidate],
) -> Result<HashSet<ChunkPk>> {
    overlapping(conn, candidates, LIVE_ADMITTED).await
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

/// Returns the pks of the candidates that overlap an existing chunk in their dataset matching
/// `chunk_state` (those to reject); candidates with no such overlap are absent from the result.
async fn overlapping(
    conn: &mut PgConnection,
    candidates: &[Candidate],
    chunk_state: &str,
) -> Result<HashSet<ChunkPk>> {
    if candidates.is_empty() {
        return Ok(HashSet::new());
    }
    let mut timer = crate::metrics::PhaseTimer::new("nonoverlap:overlap_probe");
    let n = candidates.len();
    let mut pks: Vec<ChunkPk> = Vec::with_capacity(n);
    let mut datasets: Vec<DatasetId> = Vec::with_capacity(n);
    let mut firsts: Vec<i64> = Vec::with_capacity(n);
    let mut lasts: Vec<i64> = Vec::with_capacity(n);
    for c in candidates {
        pks.push(c.pk);
        datasets.push(c.dataset_id);
        firsts.push(c.first_block);
        lasts.push(c.last_block);
    }

    // Per candidate, one search of the chunks_dataset_range_gist index: keep the candidate iff some
    // live chunk's range intersects its own (`&&`, built with the index's exact expression). The
    // nearest-left btree walk this replaces had to liveness-check every row it skipped; a dataset
    // registered whole in one batch has no live row to stop at, so that walk was O(N²) in the
    // batch. `LIMIT 1` also pins the plan: an un-fenced EXISTS in WHERE may flatten into a semi
    // join, and a plan that sees the array cardinality picks a merge join sorting every live chunk
    // — minutes at production scale. A LATERAL with LIMIT can't be flattened, so the probe stays on
    // the index regardless of stats or plan-cache state.
    // `chunk_state` is a trusted in-crate const (LIVE_ADMITTED), never user input.
    let sql = format!(
        r#"
        SELECT cand.chunk_pk
        FROM UNNEST($1::bigint[], $2::smallint[], $3::bigint[], $4::bigint[])
               AS cand(chunk_pk, dataset_id, first_block, last_block)
        CROSS JOIN LATERAL (
            SELECT 1
            FROM sched_chunk_metadata s
            JOIN chunks c ON c.chunk_pk = s.chunk_pk
            WHERE c.dataset_id = cand.dataset_id
              AND int8range(c.first_block, c.first_block + c.last_block_delta, '[]')
                  && int8range(cand.first_block, cand.last_block, '[]')
              AND {chunk_state}
            LIMIT 1
        ) hit
        "#
    );
    let rows = super::debug::with_explain(conn, async |c| {
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(&pks)
            .bind(&datasets)
            .bind(&firsts)
            .bind(&lasts)
            .fetch_all(c)
            .await
    })
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
