//! Brute-force non-overlap resolver: the in-memory backend's reference for the rule Postgres
//! implements in SQL (`postgres::nonoverlap`). O(n²) and obviously correct — this backend is
//! test-only, so clarity beats speed. See `docs/nonoverlap-promotion-gate.md`.

use std::collections::{BTreeMap, HashMap};
use std::ops::RangeInclusive;

use super::{Dataset, InMemoryStorage, WorkerAssignmentId};
use crate::scheduler_storage::ChunkPk;
use crate::types::BlockNumber;

/// A chunk's identity and inclusive block range within its dataset.
struct RangedChunk {
    dataset: Dataset,
    pk: ChunkPk,
    blocks: RangeInclusive<BlockNumber>,
}

/// The pks to promote this cycle: confirmed chunks that overlap neither a still-visible chunk nor
/// each other. Held-back chunks are reported as a side effect.
pub(super) fn chunks_to_promote(
    store: &InMemoryStorage,
    confirmed_up_to: WorkerAssignmentId,
) -> Vec<ChunkPk> {
    let candidates = promotable_chunks(store, confirmed_up_to);
    let retained_visible = retained_visible_chunks(store);
    let (to_promote, held_back) = select_non_overlapping(candidates, retained_visible);
    report_promotion_held_back(&held_back);
    to_promote
}

/// The admission verdict for newly-ingested chunks: which to admit (caller writes a default row) and
/// which to reject (caller writes a terminal `rejected` row). Rejections are reported here.
pub(super) struct AdmissionDecision {
    pub admitted: Vec<ChunkPk>,
    pub rejected: Vec<ChunkPk>,
}

pub(super) fn admission_decision(store: &InMemoryStorage) -> AdmissionDecision {
    // New chunks (no metadata row yet) split two ways. A correction's replacement is admitted only
    // if it still covers its predecessor's exact range — a same-range replacement overlaps only the
    // chunk it supersedes, so it is overlap-safe; a range-changing one is rejected (a backstop:
    // register_correction already refuses it). Every other new chunk is overlap-checked against the
    // dataset's live chunks.
    let predecessors = correction_predecessors(store);
    let mut admitted: Vec<ChunkPk> = Vec::new();
    let mut range_rejected: Vec<RangedChunk> = Vec::new();
    let mut to_check: Vec<RangedChunk> = Vec::new();
    for pk in store.chunks.keys().copied() {
        if store.sched_chunk_metadata.contains_key(&pk) {
            continue;
        }
        let Some(cand) = ranged_chunk(store, &pk) else {
            continue;
        };
        match predecessors.get(&pk) {
            Some(old_pk) => {
                let same_range = store
                    .chunks
                    .get(old_pk)
                    .is_some_and(|old| old.blocks == cand.blocks);
                if same_range {
                    admitted.push(pk);
                } else {
                    range_rejected.push(cand);
                }
            }
            None => to_check.push(cand),
        }
    }

    let live_chunks = live_admitted_chunks(store);
    let (admitted_after_check, overlap_rejected) = select_non_overlapping(to_check, live_chunks);
    admitted.extend(admitted_after_check);
    report_registration_rejected(&overlap_rejected);
    report_correction_range_rejected(&range_rejected);

    AdmissionDecision {
        admitted,
        rejected: overlap_rejected
            .into_iter()
            .chain(range_rejected)
            .map(|r| r.pk)
            .collect(),
    }
}

/// Promotion candidates: confirmed, not yet visible, not dropped, not marked_for_removal, and not a
/// correction's replacement (those promote via the correction path).
fn promotable_chunks(
    store: &InMemoryStorage,
    confirmed_up_to: WorkerAssignmentId,
) -> Vec<RangedChunk> {
    let predecessors = correction_predecessors(store);
    store
        .sched_chunk_metadata
        .iter()
        .filter(|(pk, meta)| {
            // Confirmed (threshold) but not yet promoted (threshold) and not in the removal tail;
            // and not a correction replacement (those promote via the correction path).
            meta.confirmed_by(confirmed_up_to)
                && !meta.entered_portal_assignment()
                && !meta.in_removal()
                && !predecessors.contains_key(pk)
        })
        .filter_map(|(pk, _)| ranged_chunk(store, pk))
        .collect()
}

/// The promotion comparison set: chunks that stay portal-visible after this cycle's drops — visible
/// now and not being removed. Excluding marked_for_removal is what lets a same-range correction
/// replacement promote past the old chunk it supersedes.
fn retained_visible_chunks(store: &InMemoryStorage) -> Vec<RangedChunk> {
    store
        .sched_chunk_metadata
        .iter()
        .filter(|(_, meta)| meta.portal_visible() && !meta.marked_for_removal)
        .filter_map(|(pk, _)| ranged_chunk(store, pk))
        .collect()
}

/// The registration comparison set: the dataset's live chunks a new chunk must not overlap —
/// admitted, not rejected, not leaving. Ignores scheduling state (`applied_at_worker`): a chunk
/// claims its range from admission, so a not-yet-scheduled (or shortage-stuck) one must still block
/// an overlapping newcomer.
fn live_admitted_chunks(store: &InMemoryStorage) -> Vec<RangedChunk> {
    store
        .sched_chunk_metadata
        .iter()
        .filter(|(_, meta)| meta.live())
        .filter_map(|(pk, _)| ranged_chunk(store, pk))
        .collect()
}

/// New-side pk -> old-side pk for each correction not yet applied at the portal. The replacement
/// legitimately covers the old chunk's range, so both gates route it specially: registration admits
/// it only while it still covers exactly that range, and promotion skips it until the swap fires.
fn correction_predecessors(store: &InMemoryStorage) -> HashMap<ChunkPk, ChunkPk> {
    store
        .chunk_corrections
        .iter()
        .filter(|(_, c)| c.applied_at_portal_assignment_id.is_none())
        .map(|(old_pk, c)| (c.new_chunk_pk, *old_pk))
        .collect()
}

/// Map a chunk pk to a [`RangedChunk`].
fn ranged_chunk(store: &InMemoryStorage, pk: &ChunkPk) -> Option<RangedChunk> {
    store.chunks.get(pk).map(|chunk| RangedChunk {
        dataset: (*chunk.dataset).clone(),
        pk: *pk,
        blocks: chunk.blocks.clone(),
    })
}

/// Accept the candidates that overlap neither an `existing` chunk nor an already-accepted candidate
/// in the same dataset; lower `(first_block, chunk_pk)` wins. Returns `(accepted pks, rejected)`.
fn select_non_overlapping(
    mut candidates: Vec<RangedChunk>,
    existing: Vec<RangedChunk>,
) -> (Vec<ChunkPk>, Vec<RangedChunk>) {
    candidates.sort_unstable_by_key(|c| (*c.blocks.start(), c.pk));
    let mut accepted = existing; // existing chunks, grown with each accepted candidate
    let mut accepted_pks = Vec::new();
    let mut rejected = Vec::new();
    for cand in candidates {
        let overlaps = accepted
            .iter()
            .any(|e| e.dataset == cand.dataset && ranges_overlap(&e.blocks, &cand.blocks));
        if overlaps {
            rejected.push(cand);
        } else {
            accepted_pks.push(cand.pk);
            accepted.push(cand);
        }
    }
    (accepted_pks, rejected)
}

/// Two inclusive ranges overlap iff each starts no later than the other ends.
/// `[10,20]` and `[20,30]` overlap (share block 20); `[0,10]` and `[11,20]` do not.
fn ranges_overlap(a: &RangeInclusive<BlockNumber>, b: &RangeInclusive<BlockNumber>) -> bool {
    a.start() <= b.end() && b.start() <= a.end()
}

/// Log + count chunks held back at promotion (should never fire — registration removed overlaps);
/// resets the gauge each call.
fn report_promotion_held_back(held: &[RangedChunk]) {
    if !held.is_empty() {
        tracing::warn!(
            count = held.len(),
            "chunks held back from portal promotion: block range overlaps a visible chunk in the dataset"
        );
    }
    crate::metrics::report_promotion_held_back(counts_by_dataset(held));
}

/// Log + count chunks refused at registration for overlapping a live chunk; resets the gauge each
/// call.
fn report_registration_rejected(rejected: &[RangedChunk]) {
    if !rejected.is_empty() {
        tracing::warn!(
            count = rejected.len(),
            "chunks rejected at registration: block range overlaps a live chunk in the dataset"
        );
    }
    crate::metrics::report_registration_rejected(counts_by_dataset(rejected));
}

/// Warn on correction replacements refused at registration for changing range — a should-never-fire
/// backstop (`register_correction` already rejects these), so it is logged, not metered.
fn report_correction_range_rejected(rejected: &[RangedChunk]) {
    if !rejected.is_empty() {
        tracing::warn!(
            count = rejected.len(),
            "correction replacements rejected at registration: range differs from predecessor \
             (register_correction should have refused these)"
        );
    }
}

fn counts_by_dataset(chunks: &[RangedChunk]) -> BTreeMap<Dataset, i64> {
    let mut counts = BTreeMap::new();
    for c in chunks {
        *counts.entry(c.dataset.clone()).or_default() += 1;
    }
    counts
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rc(dataset: &str, pk: i64, start: u64, end: u64) -> RangedChunk {
        RangedChunk {
            dataset: dataset.to_string(),
            pk: ChunkPk(pk),
            blocks: start..=end,
        }
    }

    #[test]
    fn overlap_predicate_is_inclusive() {
        assert!(ranges_overlap(&(10..=20), &(20..=30))); // shared endpoint
        assert!(ranges_overlap(&(5..=5), &(5..=5))); // single block
        assert!(ranges_overlap(&(10..=20), &(12..=15))); // nested
        assert!(!ranges_overlap(&(0..=10), &(11..=20))); // adjacent, no overlap
    }

    #[test]
    fn candidate_overlapping_existing_is_rejected() {
        let (accepted, rejected) =
            select_non_overlapping(vec![rc("a", 2, 15, 25)], vec![rc("a", 1, 10, 20)]);
        assert!(accepted.is_empty());
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0].pk, ChunkPk(2));
    }

    #[test]
    fn non_overlapping_candidate_accepted() {
        let (accepted, rejected) =
            select_non_overlapping(vec![rc("a", 2, 21, 30)], vec![rc("a", 1, 10, 20)]);
        assert_eq!(accepted, vec![ChunkPk(2)]);
        assert!(rejected.is_empty());
    }

    #[test]
    fn other_dataset_never_conflicts() {
        let (accepted, _) =
            select_non_overlapping(vec![rc("b", 2, 10, 20)], vec![rc("a", 1, 10, 20)]);
        assert_eq!(accepted, vec![ChunkPk(2)]);
    }

    #[test]
    fn intra_batch_lower_first_block_then_pk_wins() {
        // Two new overlapping candidates, no existing set: deterministic winner by (start, pk).
        let (accepted, rejected) =
            select_non_overlapping(vec![rc("a", 9, 10, 20), rc("a", 3, 10, 20)], vec![]);
        assert_eq!(accepted, vec![ChunkPk(3)]);
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0].pk, ChunkPk(9));
    }

    #[test]
    fn intra_batch_non_overlapping_all_accepted() {
        let (mut accepted, _) =
            select_non_overlapping(vec![rc("a", 1, 0, 9), rc("a", 2, 10, 19)], vec![]);
        accepted.sort();
        assert_eq!(accepted, vec![ChunkPk(1), ChunkPk(2)]);
    }
}
