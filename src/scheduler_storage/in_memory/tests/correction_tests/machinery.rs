//! Correction machinery driven directly through the static scheduling stub.
//!
//! The stub lets each test decide exactly which chunks land in a cycle, so cases that need
//! selective placement (a chunk left unscheduled, A dropped from the ideal) live here rather than
//! with the real scheduler, which places every visible chunk every cycle.

use super::*;
use crate::scheduler_storage::algorithm::IdealMapping;
use crate::scheduler_storage::test_harness::assert_portal_chunks_exact;
use crate::scheduler_storage::test_harness::inspect::StorageInspect;
use claims::assert_matches;
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn run_cycle(
    storage: &mut InMemoryStorage,
    chunk_pk: &ChunkPk,
    worker_ids: Vec<WorkerPk>,
    at: TimeUnit,
) -> WorkerAssignment {
    run_cycle_multi(storage, [(*chunk_pk, worker_ids)], at)
}

fn run_cycle_multi(
    storage: &mut InMemoryStorage,
    mappings: impl IntoIterator<Item = (ChunkPk, Vec<WorkerPk>)>,
    at: TimeUnit,
) -> WorkerAssignment {
    let mapping: IdealMapping = mappings.into_iter().collect();
    let algorithm = StaticSchedulingAlgorithm { mapping };
    storage
        .run_scheduling_cycle(&algorithm, &(), at, GRACE_PERIOD)
        .expect("scheduling succeeds")
}

// ---------------------------------------------------------------------------
// Registration guards
// ---------------------------------------------------------------------------

#[test]
fn register_correction_unknown_old_chunk() {
    let a = chunk("a", 1, 100);
    let mut storage = storage_with(vec![a.clone()]);

    let result = storage.register_correction_int(ChunkPk(9999), chunk("a", 2, 100), 1);
    assert_matches!(result, Err(CorrectionRejected::OldChunkNotFound { .. }));
}

#[test]
fn register_correction_replacement_already_exists() {
    let a = chunk("a", 1, 100);
    // Same-range as `a` (2..=3) so the range guard passes and the insert-level
    // ReplacementChunkExists check is the one that fires; distinct id keeps it a separate chunk.
    let existing = chunk_with_blocks("a", 2, 100, 2..=3);
    let mut storage = storage_with(vec![a.clone(), existing.clone()]);
    let a_pk = storage.pk_of(&a);

    // `existing` is already a known chunk in the dataset, so it cannot be minted as a replacement.
    let result = storage.register_correction_int(a_pk, existing, 1);
    assert_matches!(
        result,
        Err(CorrectionRejected::ReplacementChunkExists { .. })
    );
}

#[test]
fn register_correction_rejects_cross_dataset_replacement() {
    let a = chunk("a", 1, 100);
    let mut storage = storage_with(vec![a.clone(), chunk("b", 1, 100)]);
    let a_pk = storage.pk_of(&a);

    // Replacement in dataset "b" for an old chunk in dataset "a" — must be rejected, not coerced.
    let result = storage.register_correction_int(a_pk, chunk("b", 2, 100), 1);
    assert_matches!(result, Err(CorrectionRejected::DatasetMismatch { .. }));
}

#[test]
fn register_correction_rejects_range_change() {
    // A correction must be a 1:1 same-range swap. A replacement whose block range differs from the
    // old chunk's range is rejected, even for an otherwise healthy old chunk.
    let a = chunk("a", 1, 100); // range 2..=3
    let mut storage = storage_with(vec![a.clone()]);
    let a_pk = storage.pk_of(&a);

    // Distinct id, same dataset, but a DIFFERENT block range (4..=5) than `a`.
    let result = storage.register_correction_int(a_pk, chunk("a", 2, 100), 1);
    assert_matches!(result, Err(CorrectionRejected::RangeChanged { .. }));
}

#[test]
fn register_correction_duplicate_pending() -> anyhow::Result<()> {
    let a = chunk("a", 1, 100);
    let mut storage = storage_with(vec![a.clone()]);
    let a_pk = storage.pk_of(&a);

    storage.register_correction(a_pk, chunk_with_blocks("a", 2, 100, 2..=3), 1)?;

    let result = storage.register_correction_int(a_pk, chunk("a", 3, 100), 2);
    assert_matches!(result, Err(CorrectionRejected::DuplicatePending { .. }));
    Ok(())
}

#[test]
fn register_correction_duplicate_completed() -> anyhow::Result<()> {
    let a = chunk("a", 1, 100);
    let mut storage = storage_with(vec![a.clone()]);
    let a_pk = storage.pk_of(&a);

    let w = worker(1, None);
    storage.update_worker_set(&[w], 0, 1000)?;
    let wid = workers(&storage)[0].worker_id;

    let wa1 = run_cycle(&mut storage, &a_pk, vec![wid], CYCLE_INTERVAL);
    storage.confirm_worker_assignment(wa1.id, CYCLE_INTERVAL)?;
    storage.run_visibility_cycle(CYCLE_INTERVAL + DELTA)?;

    let b_pk = storage.register_correction(a_pk, chunk_with_blocks("a", 2, 100, 2..=3), 200)?;
    let wa2 = run_cycle_multi(
        &mut storage,
        [(a_pk, vec![wid]), (b_pk, vec![wid])],
        2 * CYCLE_INTERVAL,
    );
    storage.confirm_worker_assignment(wa2.id, 2 * CYCLE_INTERVAL)?;
    storage.run_visibility_cycle(300)?;

    // Completed audit row must still block re-registration.
    let result = storage.register_correction_int(a_pk, chunk("a", 3, 100), 400);
    assert_matches!(result, Err(CorrectionRejected::DuplicatePending { .. }));
    Ok(())
}

#[test]
fn register_correction_old_chunk_marked_for_removal() {
    let a = chunk("a", 1, 100);
    let mut storage = storage_with(vec![a.clone()]);
    let a_pk = storage.pk_of(&a);
    storage.register_new_chunks().unwrap();

    storage
        .sched_chunk_metadata
        .get_mut(&a_pk)
        .unwrap()
        .marked_for_removal = true;

    let result = storage.register_correction_int(a_pk, chunk("a", 2, 100), 1);
    assert_matches!(result, Err(CorrectionRejected::OldChunkBeingRemoved { .. }));
}

#[test]
fn register_correction_old_chunk_dropped_at_portal() {
    let a = chunk("a", 1, 100);
    let mut storage = storage_with(vec![a.clone()]);
    let a_pk = storage.pk_of(&a);
    storage.register_new_chunks().unwrap();

    storage
        .sched_chunk_metadata
        .get_mut(&a_pk)
        .unwrap()
        .dropped_at_portal_assignment_id = Some(1);

    let result = storage.register_correction_int(a_pk, chunk("a", 2, 100), 1);
    assert_matches!(result, Err(CorrectionRejected::OldChunkBeingRemoved { .. }));
}

#[test]
fn register_correction_old_chunk_dropped_at_worker() {
    let a = chunk("a", 1, 100);
    let mut storage = storage_with(vec![a.clone()]);
    let a_pk = storage.pk_of(&a);
    storage.register_new_chunks().unwrap();

    storage
        .sched_chunk_metadata
        .get_mut(&a_pk)
        .unwrap()
        .dropped_from_worker_assignment_at = Some(1);

    let result = storage.register_correction_int(a_pk, chunk("a", 2, 100), 1);
    assert_matches!(result, Err(CorrectionRejected::OldChunkBeingRemoved { .. }));
}

#[test]
fn register_correction_old_chunk_rejected() {
    // Two overlapping chunks: the higher-(first_block, pk) one is rejected at registration. A
    // rejected chunk is terminal (never admitted, nothing to supersede), so correcting it is refused.
    let lower = chunk_with_blocks("a", 1, 100, 10..=20);
    let higher = chunk_with_blocks("a", 2, 100, 15..=25); // overlaps lower → rejected
    let mut storage = storage_with(vec![lower, higher.clone()]);
    let higher_pk = storage.pk_of(&higher);
    storage.register_new_chunks().unwrap();

    let result =
        storage.register_correction_int(higher_pk, chunk_with_blocks("a", 3, 100, 15..=25), 1);
    assert_matches!(result, Err(CorrectionRejected::OldChunkRejected { .. }));
}

// ---------------------------------------------------------------------------
// Firing / visibility-cycle integration
// ---------------------------------------------------------------------------

#[test]
fn correction_chain_link_held_until_producer_fires() -> anyhow::Result<()> {
    // A correction can only fire once the correction that produced its old chunk has fired.
    // Here the chain is A→B→C: A→B produces B, and B is the old chunk of B→C. So B→C depends on
    // A→B. We confirm C but leave B unconfirmed. A→B is held (B unconfirmed), and that in turn
    // holds B→C even though C itself is confirmed. Only A stays visible — neither B nor C is
    // promoted. (Readiness is structural — does the producing correction depend on it — not "which
    // correction was registered earlier in the dataset".)
    //
    // Kept on the static stub: "C confirmed while B is not" needs scheduling A and C but not B,
    // which the real scheduler (it places every visible chunk each cycle) cannot do.
    let dataset = "ds";
    let chunk_a = chunk(dataset, 1, 100);
    let mut storage = storage_with(vec![chunk_a.clone()]);
    let pk_a = storage.pk_of(&chunk_a);

    let single_worker = worker(1, None);
    storage.update_worker_set(&[single_worker], 0, 1000)?;
    let worker_id = workers(&storage)[0].worker_id;

    // Make A visible at the portal so the chain has something to swap out of.
    let wa_a = run_cycle(&mut storage, &pk_a, vec![worker_id], CYCLE_INTERVAL);
    storage.confirm_worker_assignment(wa_a.id, CYCLE_INTERVAL)?;
    storage.run_visibility_cycle(CYCLE_INTERVAL + DELTA)?;

    // Chain A→B→C: every link inherits A's range (2..=3).
    let pk_b = storage.register_correction(pk_a, chunk_with_blocks(dataset, 2, 100, 2..=3), 1)?;
    let pk_c = storage.register_correction(pk_b, chunk_with_blocks(dataset, 3, 100, 2..=3), 2)?;

    // Confirm an assignment covering A and C but NOT B: this is what makes C confirmed while B
    // stays unconfirmed, so A→B remains pending.
    let wa_ac = run_cycle_multi(
        &mut storage,
        [(pk_a, vec![worker_id]), (pk_c, vec![worker_id])],
        2 * CYCLE_INTERVAL,
    );
    storage.confirm_worker_assignment(wa_ac.id, 2 * CYCLE_INTERVAL)?;

    // Now schedule B too, but never confirm this assignment — B remains unconfirmed.
    run_cycle_multi(
        &mut storage,
        [
            (pk_a, vec![worker_id]),
            (pk_b, vec![worker_id]),
            (pk_c, vec![worker_id]),
        ],
        3 * CYCLE_INTERVAL,
    );

    let portal = storage.run_visibility_cycle(3 * CYCLE_INTERVAL + DELTA)?;

    assert!(
        !storage.get_chunk_metadata_by_pk(pk_a).marked_for_removal,
        "A→B has not fired, so A must not be marked for removal"
    );
    // B and C are both hidden: A→B is held by B, and B→C is held by the unfired A→B.
    assert_portal_chunks_exact(&portal, &[pk_a], "A→B held by B blocks B→C; only A visible");
    Ok(())
}

#[test]
fn correction_independent_same_dataset_fires_without_waiting() -> anyhow::Result<()> {
    // Structural readiness: A→B and C→D are independent (no chain link) in the same dataset, so
    // C→D fires as soon as D is confirmed — it does NOT wait for the unrelated A→B. A→B is held
    // only by its own confirmation gate (B unconfirmed), not by C→D.
    //
    // Kept on the static stub: holding A→B while C→D fires needs B left unscheduled — selective
    // control the real scheduler lacks. The both-fire case is `correction_independent_corrections_fire_together`.
    let a = chunk("a", 1, 100);
    let c = chunk("a", 3, 100);
    let mut storage = storage_with(vec![a.clone(), c.clone()]);
    let a_pk = storage.pk_of(&a);
    let c_pk = storage.pk_of(&c);

    let w = worker(1, None);
    storage.update_worker_set(&[w], 0, 1000)?;
    let wid = workers(&storage)[0].worker_id;

    let wa1 = run_cycle_multi(
        &mut storage,
        [(a_pk, vec![wid]), (c_pk, vec![wid])],
        CYCLE_INTERVAL,
    );
    storage.confirm_worker_assignment(wa1.id, CYCLE_INTERVAL)?;
    storage.run_visibility_cycle(CYCLE_INTERVAL + DELTA)?;

    // B inherits A's range (2..=3); D inherits C's range (6..=7).
    let b_pk = storage.register_correction(a_pk, chunk_with_blocks("a", 2, 100, 2..=3), 1)?;
    let d_pk = storage.register_correction(c_pk, chunk_with_blocks("a", 4, 100, 6..=7), 2)?;

    // D scheduled and confirmed; B left unscheduled (hence unconfirmed) so A→B stays pending.
    let wa2 = run_cycle_multi(
        &mut storage,
        [(a_pk, vec![wid]), (c_pk, vec![wid]), (d_pk, vec![wid])],
        2 * CYCLE_INTERVAL,
    );
    storage.confirm_worker_assignment(wa2.id, 2 * CYCLE_INTERVAL)?;

    let portal1 = storage.run_visibility_cycle(3 * CYCLE_INTERVAL)?;
    // C→D fires independently; A→B held (B unconfirmed). Exactly A (still visible) and D (promoted).
    assert_portal_chunks_exact(
        &portal1,
        &[a_pk, d_pk],
        "C→D fired independently, A→B held: exactly A and D visible",
    );

    let wa3 = run_cycle_multi(
        &mut storage,
        [(a_pk, vec![wid]), (b_pk, vec![wid]), (d_pk, vec![wid])],
        4 * CYCLE_INTERVAL,
    );
    storage.confirm_worker_assignment(wa3.id, 4 * CYCLE_INTERVAL)?;

    // Once B is confirmed, A→B fires too; C→D already completed in the earlier cycle.
    let portal2 = storage.run_visibility_cycle(4 * CYCLE_INTERVAL + DELTA)?;
    // A→B now fires too (C→D completed earlier). Exactly B (promoted) and D (still promoted).
    assert_portal_chunks_exact(
        &portal2,
        &[b_pk, d_pk],
        "A→B fired too: exactly B and D visible (A dropped)",
    );
    Ok(())
}

#[test]
fn correction_audit_row_retained() -> anyhow::Result<()> {
    let a = chunk("a", 1, 100);
    let mut storage = storage_with(vec![a.clone()]);
    let a_pk = storage.pk_of(&a);

    let w = worker(1, None);
    storage.update_worker_set(&[w], 0, 1000)?;
    let wid = workers(&storage)[0].worker_id;

    let wa1 = run_cycle(&mut storage, &a_pk, vec![wid], CYCLE_INTERVAL);
    storage.confirm_worker_assignment(wa1.id, CYCLE_INTERVAL)?;
    storage.run_visibility_cycle(CYCLE_INTERVAL + DELTA)?;

    let b_pk = storage.register_correction(a_pk, chunk_with_blocks("a", 2, 100, 2..=3), 200)?;
    let wa2 = run_cycle_multi(
        &mut storage,
        [(a_pk, vec![wid]), (b_pk, vec![wid])],
        2 * CYCLE_INTERVAL,
    );
    storage.confirm_worker_assignment(wa2.id, 2 * CYCLE_INTERVAL)?;

    let portal = storage.run_visibility_cycle(300)?;

    let entry = storage
        .get_corrections(|c| c.old_chunk_pk == a_pk)
        .into_iter()
        .next()
        .expect("correction row must be retained after completion");
    assert!(
        entry.applied_at_portal_assignment_id.is_some(),
        "audit row must have applied_at_portal_assignment_id set"
    );
    assert_eq!(
        entry.applied_at_portal_assignment_id,
        Some(portal.id as u64),
        "audit row must reference the portal assignment that applied the correction"
    );

    // Completed audit row must still block re-registration.
    let result = storage.register_correction_int(a_pk, chunk("a", 3, 100), 400);
    assert_matches!(result, Err(CorrectionRejected::DuplicatePending { .. }));
    Ok(())
}

#[test]
fn correction_old_chunk_removed_from_worker_after_m_ticks() -> anyhow::Result<()> {
    // Kept on the static stub: the worker-drain tail needs A dropped from the ideal (scheduling
    // only B), which the real scheduler cannot express.
    let a = chunk("a", 1, 100);
    let mut storage = storage_with(vec![a.clone()]);
    let a_pk = storage.pk_of(&a);

    let w = worker(1, None);
    storage.update_worker_set(&[w], 0, 1000)?;
    let wid = workers(&storage)[0].worker_id;

    let wa1 = run_cycle(&mut storage, &a_pk, vec![wid], CYCLE_INTERVAL);
    storage.confirm_worker_assignment(wa1.id, CYCLE_INTERVAL)?;
    storage.run_visibility_cycle(CYCLE_INTERVAL + DELTA)?;

    let b_pk = storage.register_correction(a_pk, chunk_with_blocks("a", 2, 100, 2..=3), 250)?;
    let wa2 = run_cycle_multi(
        &mut storage,
        [(a_pk, vec![wid]), (b_pk, vec![wid])],
        2 * CYCLE_INTERVAL,
    );
    storage.confirm_worker_assignment(wa2.id, 2 * CYCLE_INTERVAL)?;

    // A is portal-dropped at t = 3 * CYCLE_INTERVAL.
    storage.run_visibility_cycle(3 * CYCLE_INTERVAL)?;

    let early_time = 3 * CYCLE_INTERVAL + DELTA;
    run_cycle_multi(&mut storage, [(b_pk, vec![wid])], early_time);
    assert!(
        storage
            .get_chunk_metadata_by_pk(a_pk)
            .dropped_from_worker_assignment_at
            .is_none(),
        "A must NOT be tombstoned yet (< GRACE_PERIOD since portal drop)"
    );

    let late_time = 3 * CYCLE_INTERVAL + GRACE_PERIOD + 1;
    let wa_late = run_cycle_multi(&mut storage, [(b_pk, vec![wid])], late_time);
    assert_eq!(
        wa_late.chunks.keys().copied().collect::<HashSet<_>>(),
        HashSet::from([b_pk]),
        "worker assignment must hold exactly B after A is tombstoned"
    );
    assert!(
        storage
            .get_chunk_metadata_by_pk(a_pk)
            .dropped_from_worker_assignment_at
            .is_some(),
        "A metadata must have dropped_from_worker_assignment_at set after M_TICKS"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// P1: Basic scenarios — timing variations (confirm before/after first visibility)
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    #[test]
    fn prop_corrections_safety(
        n_workers in 2u8..=5u8,
        confirm_before_visibility in proptest::bool::ANY,
    ) {
        let orig_chunk = chunk("a", 1, 100);

        let worker_list: Vec<Worker> = (0..n_workers).map(|i| worker(i + 1, None)).collect();
        let mut storage = storage_with(vec![orig_chunk.clone()]);
        let orig_pk = storage.pk_of(&orig_chunk);
        storage.update_worker_set(&worker_list, 0, 1000).unwrap();
        let all_wids: Vec<WorkerPk> = workers(&storage).iter().map(|v| v.worker_id).collect();
        let wid = all_wids[0];

        let wa1 = run_cycle(&mut storage, &orig_pk, vec![wid], CYCLE_INTERVAL);
        storage.confirm_worker_assignment(wa1.id, CYCLE_INTERVAL).unwrap();
        storage.run_visibility_cycle(CYCLE_INTERVAL + DELTA).unwrap();

        let repl_pk = storage
            .register_correction(orig_pk, chunk_with_blocks("a", 2, 100, 2..=3), 200)
            .unwrap();
        let wa2 = run_cycle_multi(
            &mut storage,
            [(orig_pk, vec![wid]), (repl_pk, vec![wid])],
            2 * CYCLE_INTERVAL,
        );

        // The replacement is created at registration, so it can only be confirmed afterwards;
        // the remaining timing freedom is whether the confirmation lands before or after the
        // first visibility cycle. Safety must hold either way.
        let mut confirmed = confirm_before_visibility;
        if confirmed {
            storage.confirm_worker_assignment(wa2.id, 2 * CYCLE_INTERVAL).unwrap();
        }

        for t in [250u64, 300, 350] {
            let _portal = storage.run_visibility_cycle(t).unwrap();
            if !confirmed {
                storage.confirm_worker_assignment(wa2.id, 2 * CYCLE_INTERVAL).unwrap();
                confirmed = true;
            }
            if let Err(violation) = corrections_safety_ok(&storage) {
                prop_assert!(false, "{violation} (at t={t})");
            }
        }
    }
}
