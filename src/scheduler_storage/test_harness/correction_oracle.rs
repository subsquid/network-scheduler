//! Backend-generic invariant oracles for chunk corrections, over the
//! [`StorageInspect`](crate::scheduler_storage::test_harness::inspect::StorageInspect) views. Pure detection:
//! each returns the first violation as a message and never panics. Shared by the storage
//! correction tests (both backends) and the multistep simulation.
//!
//! Visibility is judged at **gate A** — chunk lifecycle state (`promoted(pk)` =
//! `applied_at_portal_assignment_id` set, `dropped_at_portal_assignment_id` unset) — deliberately
//! *not* by routing presence: a promoted chunk whose confirmed holders all departed drops out of
//! routing (gate B, `portal_consistency`'s domain) without any swap-atomicity violation.
//!
//! Invariants from `docs/mvcc-storage.md` ("Corrections" / invariant 5):
//!
//! - **O1 — exclusivity and no gap.** Old and new are never promoted simultaneously, and a
//!   completed correction leaves the replacement promoted unless it was swapped onward or is
//!   being removed.
//! - **O2 — pending replacements stay hidden.** A pending correction's `new_chunk_pk` is never
//!   promoted.
//! - **O3 — dependency ordering.** A correction completes only after the correction that produces
//!   its old chunk (a chain link `X → old`) has completed — the structural dependency, not
//!   `created_at` order; independent corrections may complete in any order.
//!
//! Alongside those three, [`visibility_monotonicity`] asserts that a chunk visible in one published
//! portal assignment stays visible in the next, until a removal or completed correction — checked on
//! the published artifact, generalizing what O1 asserts only at correction swaps.

use std::collections::HashSet;

use crate::scheduler_storage::ChunkPk;
use crate::scheduler_storage::test_harness::inspect::CorrectionView;

/// Run all three oracles. `promoted(pk)` is the gate-A visibility predicate (see module docs);
/// `removing(pk)` reports a removal mark (`marked_for_removal` or either `dropped_*` stamp) —
/// the no-gap excusal for replacements that are themselves on the way out.
pub fn corrections_safety(
    corrections: &[CorrectionView],
    promoted: impl Fn(&ChunkPk) -> bool,
    removing: impl Fn(&ChunkPk) -> bool,
) -> Result<(), String> {
    exclusivity_and_no_gap(corrections, &promoted, removing)?;
    pending_new_excluded(corrections, &promoted)?;
    dependency_ordering(corrections)
}

/// O1: old and new never promoted simultaneously; once completed, the old chunk is dropped and
/// the replacement promoted unless swapped onward by a completed chain link or being removed.
pub fn exclusivity_and_no_gap(
    corrections: &[CorrectionView],
    promoted: impl Fn(&ChunkPk) -> bool,
    removing: impl Fn(&ChunkPk) -> bool,
) -> Result<(), String> {
    for correction in corrections {
        let old_visible = promoted(&correction.old_chunk_pk);
        let new_visible = promoted(&correction.new_chunk_pk);
        if old_visible && new_visible {
            return Err(format!(
                "correction overlap: old {:?} and new {:?} are both promoted (gate A) — \
                 invariant 5 requires the swap to be atomic",
                correction.old_chunk_pk, correction.new_chunk_pk,
            ));
        }
        if !correction.is_completed() {
            continue;
        }
        if old_visible {
            return Err(format!(
                "correction fired but old chunk {:?} is still promoted (gate A) \
                 (applied at portal assignment {:?})",
                correction.old_chunk_pk, correction.applied_at_portal_assignment_id,
            ));
        }
        // An onward correction excuses an invisible replacement only once it has *completed*.
        let swapped_onward = corrections
            .iter()
            .any(|other| other.old_chunk_pk == correction.new_chunk_pk && other.is_completed());
        if !new_visible && !swapped_onward && !removing(&correction.new_chunk_pk) {
            return Err(format!(
                "correction gap: correction {:?} → {:?} fired, but the replacement was never \
                 promoted (and is neither swapped onward nor being removed) — the block range \
                 is unqueryable",
                correction.old_chunk_pk, correction.new_chunk_pk,
            ));
        }
    }
    Ok(())
}

/// O2: a pending correction's `new_chunk_pk` is never promoted.
pub fn pending_new_excluded(
    corrections: &[CorrectionView],
    promoted: impl Fn(&ChunkPk) -> bool,
) -> Result<(), String> {
    for correction in corrections {
        if !correction.is_completed() && promoted(&correction.new_chunk_pk) {
            return Err(format!(
                "pending replacement leaked: {:?} (replacing {:?}) is promoted while its \
                 correction is still pending",
                correction.new_chunk_pk, correction.old_chunk_pk,
            ));
        }
    }
    Ok(())
}

/// O3: a correction completes only after the correction that produces its old chunk (a chain link
/// `producer.new_chunk_pk == this.old_chunk_pk`) has completed. This is the structural dependency;
/// `created_at` order is not required, so independent corrections may complete in any order.
pub fn dependency_ordering(corrections: &[CorrectionView]) -> Result<(), String> {
    for this in corrections {
        if !this.is_completed() {
            continue;
        }
        for producer in corrections {
            if producer.new_chunk_pk == this.old_chunk_pk && !producer.is_completed() {
                return Err(format!(
                    "dependency ordering violated: correction {:?} → {:?} completed while the \
                     correction producing its old chunk ({:?} → {:?}) is still pending",
                    this.old_chunk_pk,
                    this.new_chunk_pk,
                    producer.old_chunk_pk,
                    producer.new_chunk_pk,
                ));
            }
        }
    }
    Ok(())
}

/// Gate-A visibility monotonicity: a chunk in one published portal assignment must still appear in
/// the next, unless the model removed it or a completed correction swapped it out. `previously_visible`
/// is the prior assignment's chunk set, `visible` tests the new one. Checked on the published artifact
/// rather than the promote/drop stamps, so it asserts the contract clients consume. The excuses key on
/// model intent — `removal_requested`, or a *completed* correction's old pk; a pending one excuses
/// nothing (whether a swap may fire is O1/O2/O3's job).
pub fn visibility_monotonicity(
    previously_visible: &HashSet<ChunkPk>,
    visible: impl Fn(&ChunkPk) -> bool,
    removal_requested: impl Fn(&ChunkPk) -> bool,
    corrections: &[CorrectionView],
) -> Result<(), String> {
    let swapped_out: HashSet<ChunkPk> = corrections
        .iter()
        .filter(|correction| correction.is_completed())
        .map(|correction| correction.old_chunk_pk)
        .collect();
    for pk in previously_visible {
        if visible(pk) || removal_requested(pk) || swapped_out.contains(pk) {
            continue;
        }
        return Err(format!(
            "chunk {pk:?} lost gate-A visibility — it was in the published portal assignment and has \
             dropped out with neither a model-requested removal nor a completed correction; \
             visibility must be monotonic until removal or correction",
        ));
    }
    Ok(())
}

/// Fire-direction self-tests: an oracle that never fires would turn the suites it guards into
/// green noise.
#[cfg(test)]
mod tests {
    use super::*;
    use maplit::hashset;
    type PK = ChunkPk;

    fn correction(
        old: PK,
        new: PK,
        dataset: &str,
        created_at: u64,
        applied: Option<u64>,
    ) -> CorrectionView {
        CorrectionView {
            old_chunk_pk: old,
            new_chunk_pk: new,
            dataset: dataset.to_string(),
            created_at,
            applied_at_portal_assignment_id: applied,
        }
    }

    /// A gate-A visibility predicate: promoted exactly for the listed pks.
    fn promoted(visible: &[PK]) -> impl Fn(&ChunkPk) -> bool {
        let set: std::collections::BTreeSet<PK> = visible.iter().copied().collect();
        move |pk| set.contains(pk)
    }

    #[test]
    fn overlap_fires_when_old_and_new_both_visible() {
        let corrections = [correction(ChunkPk(1), ChunkPk(2), "a", 1, None)];
        assert!(
            exclusivity_and_no_gap(&corrections, promoted(&[ChunkPk(1), ChunkPk(2)]), |_| false)
                .is_err()
        );
    }

    #[test]
    fn completed_correction_with_visible_old_fires() {
        let corrections = [correction(ChunkPk(1), ChunkPk(2), "a", 1, Some(7))];
        assert!(exclusivity_and_no_gap(&corrections, promoted(&[ChunkPk(1)]), |_| false).is_err());
    }

    #[test]
    fn gap_fires_when_replacement_vanished() {
        let corrections = [correction(ChunkPk(1), ChunkPk(2), "a", 1, Some(7))];
        assert!(exclusivity_and_no_gap(&corrections, promoted(&[]), |_| false).is_err());
    }

    #[test]
    fn gap_excused_by_completed_chain_link() {
        // A→B and B→C both fired: B invisible is fine (swapped onward).
        let corrections = [
            correction(ChunkPk(1), ChunkPk(2), "a", 1, Some(7)),
            correction(ChunkPk(2), ChunkPk(3), "a", 2, Some(7)),
        ];
        assert!(exclusivity_and_no_gap(&corrections, promoted(&[ChunkPk(3)]), |_| false).is_ok());
    }

    #[test]
    fn gap_not_excused_by_pending_chain_link() {
        // A→B fired but B→C is pending: a pending link excuses nothing.
        let corrections = [
            correction(ChunkPk(1), ChunkPk(2), "a", 1, Some(7)),
            correction(ChunkPk(2), ChunkPk(3), "a", 2, None),
        ];
        assert!(exclusivity_and_no_gap(&corrections, promoted(&[]), |_| false).is_err());
    }

    #[test]
    fn gap_excused_by_removal_mark() {
        let corrections = [correction(ChunkPk(1), ChunkPk(2), "a", 1, Some(7))];
        assert!(
            exclusivity_and_no_gap(&corrections, promoted(&[]), |pk| *pk == ChunkPk(2)).is_ok()
        );
    }

    #[test]
    fn completed_swap_passes() {
        let corrections = [correction(ChunkPk(1), ChunkPk(2), "a", 1, Some(7))];
        assert!(exclusivity_and_no_gap(&corrections, promoted(&[ChunkPk(2)]), |_| false).is_ok());
    }

    #[test]
    fn pending_new_excluded_fires_on_leak() {
        let corrections = [correction(ChunkPk(1), ChunkPk(2), "a", 1, None)];
        assert!(pending_new_excluded(&corrections, promoted(&[ChunkPk(2)])).is_err());
    }

    #[test]
    fn pending_new_excluded_passes_once_completed() {
        let corrections = [correction(ChunkPk(1), ChunkPk(2), "a", 1, Some(7))];
        assert!(pending_new_excluded(&corrections, promoted(&[ChunkPk(2)])).is_ok());
    }

    #[test]
    fn dependency_ordering_fires_when_chain_link_completes_before_producer() {
        // A→B pending, B→C completed: B→C consumed B while the correction producing B (A→B) is
        // still pending — its old chunk was swapped before it was ever produced.
        let corrections = [
            correction(ChunkPk(1), ChunkPk(2), "a", 1, None),
            correction(ChunkPk(2), ChunkPk(3), "a", 2, Some(7)),
        ];
        assert!(dependency_ordering(&corrections).is_err());
    }

    #[test]
    fn dependency_ordering_allows_independent_same_dataset() {
        // 1→2 pending, 3→4 completed, same dataset but no chain link between them: independent,
        // so completing in any order is fine (this is the over-conservative case the temporal
        // rule wrongly rejected).
        let corrections = [
            correction(ChunkPk(1), ChunkPk(2), "a", 1, None),
            correction(ChunkPk(3), ChunkPk(4), "a", 2, Some(7)),
        ];
        assert!(dependency_ordering(&corrections).is_ok());
    }

    #[test]
    fn dependency_ordering_passes_when_chain_completes_in_order() {
        // A→B then B→C both completed: the producer completed before (or with) the consumer.
        let corrections = [
            correction(ChunkPk(1), ChunkPk(2), "a", 1, Some(6)),
            correction(ChunkPk(2), ChunkPk(3), "a", 2, Some(7)),
        ];
        assert!(dependency_ordering(&corrections).is_ok());
    }

    #[test]
    fn monotonicity_fires_on_disappearance_without_cause() {
        let verdict =
            visibility_monotonicity(&hashset! {ChunkPk(1)}, promoted(&[]), |_| false, &[]);
        assert!(verdict.is_err());
    }

    #[test]
    fn monotonicity_passes_when_still_visible() {
        let verdict = visibility_monotonicity(
            &hashset! {ChunkPk(1)},
            promoted(&[ChunkPk(1)]),
            |_| false,
            &[],
        );
        assert!(verdict.is_ok());
    }

    #[test]
    fn monotonicity_excused_by_requested_removal() {
        let verdict = visibility_monotonicity(
            &hashset! {ChunkPk(1)},
            promoted(&[]),
            |pk| *pk == ChunkPk(1),
            &[],
        );
        assert!(verdict.is_ok());
    }

    #[test]
    fn monotonicity_excused_by_completed_correction_old_side() {
        let corrections = [correction(ChunkPk(1), ChunkPk(2), "a", 1, Some(7))];
        let verdict = visibility_monotonicity(
            &hashset! {ChunkPk(1)},
            promoted(&[]),
            |_| false,
            &corrections,
        );
        assert!(verdict.is_ok());
    }

    #[test]
    fn monotonicity_not_excused_by_pending_correction() {
        let corrections = [correction(ChunkPk(1), ChunkPk(2), "a", 1, None)];
        let verdict = visibility_monotonicity(
            &hashset! {ChunkPk(1)},
            promoted(&[]),
            |_| false,
            &corrections,
        );
        assert!(verdict.is_err());
    }
}
