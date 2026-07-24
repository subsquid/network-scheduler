//! The admission gate's liveness predicate plus the ingest-side overlap probe. The root scheduler
//! sweep (`scheduler_storage::postgres::nonoverlap`) re-exports these consts verbatim, so "live"
//! and the index-matching range test can't drift.
//!
//! The ingest probe (`overlapping_incoming`) is **authoritative**: taken under the per-dataset
//! advisory lock ([`super::lock::lock_dataset`]), it treats a committed *pending* chunk (one with no
//! `sched_chunk_metadata` row yet) as a claimant, so two concurrent overlapping inserts can never
//! both commit. The async admission gate's overlap-reject half is then a should-never-fire backstop.

use anyhow::Context;

use crate::error::StorageError;

/// Live chunks a new candidate must not overlap. No `applied_at_worker` gate: a chunk claims its
/// range at admission. Trusted in-crate const, never user input.
pub const LIVE_ADMITTED: &str = "NOT s.rejected \
     AND s.marked_for_removal IS NULL \
     AND s.dropped_at_portal_assignment_id IS NULL \
     AND s.dropped_from_worker_assignment_at IS NULL";

/// Range-overlap of live chunk `c` against candidate `cand` (whose `UNNEST` alias exposes
/// `first_block`/`last_block`). Must stay byte-identical to the `chunks_dataset_range_gist` index
/// expression, or the probe falls off the index into an O(N²) scan.
pub const OVERLAP_RANGE_PRED: &str = "int8range(c.first_block, c.first_block + c.last_block_delta, '[]') \
         && int8range(cand.first_block, cand.last_block, '[]')";

/// The first incoming `(chunk_id, first_block, last_block)` with a claimant chunk overlapping it
/// in `dataset_id`, as (0-based candidate index, claimant id) — `None` when the whole batch is
/// clear. The batch aborts on the first hit, so the outer `LIMIT 1` stops the scan there; which
/// candidate is reported is arbitrary. CROSS JOIN LATERAL yields no row for non-overlapping
/// candidates. Rides `chunks_dataset_range_gist`.
///
/// A candidate never conflicts with its **own** committed row (`c.chunk_id <> cand.chunk_id`): a
/// re-sent chunk falls through to the insert's `ON CONFLICT DO NOTHING` and is reported as a
/// duplicate, id-only — a same-id re-send with a changed range is still a duplicate and the stored
/// range wins.
///
/// **Authoritative**: it drives from `chunks` (LEFT JOIN `sched_chunk_metadata`), so a committed
/// *pending* chunk — no metadata row yet — is a claimant too (`s.chunk_pk IS NULL`), not just an
/// admitted one. Only truly gone chunks (rejected/dropped, via `LIVE_ADMITTED`) are ignored. Sound
/// only under the per-dataset lock (unlocked, concurrent inserts can't see each other's
/// uncommitted rows) — hence the [`LockedDataset`](super::lock::LockedDataset) parameter, which
/// also supplies the dataset.
pub(crate) async fn overlapping_incoming(
    locked: &mut super::lock::LockedDataset<'_>,
    chunk_ids: &[&str],
    first_blocks: &[i64],
    last_blocks: &[i64],
) -> Result<Option<(usize, String)>, StorageError> {
    let dataset_id = locked.dataset_id();
    if first_blocks.is_empty() {
        return Ok(None);
    }
    // `LIVE_ADMITTED` is a trusted in-crate const (never user input) — hence `AssertSqlSafe`.
    // `s.chunk_pk IS NULL` = a pending chunk (no metadata row): a claimant the async gate hasn't
    // reached yet. Judging with-metadata rows by the identical `LIVE_ADMITTED` keeps the sync gate
    // and the async sweep from drifting.
    let sql = format!(
        r#"
        SELECT cand.idx, hit.chunk_id
        FROM UNNEST($2::text[], $3::bigint[], $4::bigint[]) WITH ORDINALITY
             AS cand(chunk_id, first_block, last_block, idx)
        CROSS JOIN LATERAL (
            SELECT c.chunk_id
            FROM chunks c
            LEFT JOIN sched_chunk_metadata s ON s.chunk_pk = c.chunk_pk
            WHERE c.dataset_id = $1
              AND {OVERLAP_RANGE_PRED}
              AND c.chunk_id <> cand.chunk_id
              AND (s.chunk_pk IS NULL OR ({LIVE_ADMITTED}))
            LIMIT 1
        ) hit
        LIMIT 1
        "#
    );
    let hit: Option<(i64, String)> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
        .bind(dataset_id)
        .bind(chunk_ids)
        .bind(first_blocks)
        .bind(last_blocks)
        .fetch_optional(locked.conn())
        .await
        .context("ingest: overlap probe")?;
    Ok(hit.map(|(idx, chunk_id)| {
        let i = usize::try_from(idx - 1).expect("WITH ORDINALITY is 1-based");
        (i, chunk_id)
    }))
}

#[cfg(test)]
mod tests {
    use super::OVERLAP_RANGE_PRED;

    /// If `OVERLAP_RANGE_PRED` drifts from the index expression in the migration, answers stay
    /// correct but the probe stops using the index — a slowdown no other test catches.
    #[test]
    fn overlap_pred_matches_gist_index_expression() {
        fn normalize(s: &str) -> String {
            s.split_whitespace().collect::<Vec<_>>().join(" ")
        }
        let live_side = OVERLAP_RANGE_PRED
            .split("&&")
            .next()
            .expect("predicate has a && operator")
            .replace("c.", "");
        let migration = include_str!("../../migrations/0001_sched_tables.sql");
        assert!(
            normalize(migration).contains(normalize(&live_side).trim_end()),
            "OVERLAP_RANGE_PRED's live-chunk side is not byte-equivalent to the \
             chunks_dataset_range_gist expression in 0001_sched_tables.sql"
        );
    }
}
