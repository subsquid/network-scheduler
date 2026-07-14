//! Correction machinery driven through the real multistep scheduler, run against **both** backends.
//!
//! Each scenario is written once, generic over a [`TestStorage`] backend, and the `backend_cases!`
//! macro stamps it out as a `#[test]` under `in_memory` and `pg`. Unlike the static stub, the real
//! scheduler decides placement; one worker with a floor of 1 keeps it deterministic, so
//! confirmation and visibility — not placement — drive the tests.

use super::*;
use crate::multistep_scheduler::SchedulingConfig;
use crate::scheduler_storage::algorithm::MultistepAlgorithm;
use crate::scheduler_storage::postgres::PostgresStorage;
use crate::scheduler_storage::test_harness::assert_portal_chunks_exact;
use crate::scheduler_storage::test_harness::pg_harness::fresh_db;
use crate::types::ChunkWeight;
use crate::types::DatasetSchema;
use crate::weight::{SchedulingChunk, WeightStrategy};
use proptest::prelude::*;
use semver::Version;
use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Backend fixture: a fresh, empty storage per scenario.
// ---------------------------------------------------------------------------

trait TestStorage: SchedulerStorage + StorageInspect {
    fn fresh() -> Self;
}

impl TestStorage for InMemoryStorage {
    fn fresh() -> Self {
        Self::default()
    }
}

impl TestStorage for PostgresStorage {
    fn fresh() -> Self {
        // Unique database per instance — `CREATE DATABASE` of a duplicate name panics.
        static DB_ID: AtomicU64 = AtomicU64::new(0);
        fresh_db(
            "correction_multistep",
            DB_ID.fetch_add(1, Ordering::Relaxed),
        )
    }
}

/// Fresh storage seeded with `chunks` (and their datasets); metadata is materialised by the first
/// `run_real_cycle`, mirroring the real scheduler loop.
fn make_storage<S: TestStorage>(chunks: Vec<NewChunk>) -> S {
    let mut storage = S::fresh();
    let datasets: Vec<(String, DatasetSchema)> = chunks
        .iter()
        .map(|chunk| (*chunk.dataset).clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .map(|name| (name, DatasetSchema::default()))
        .collect();
    storage.insert_new_datasets(datasets).unwrap();
    storage.insert_new_chunks(chunks).unwrap();
    storage
}

// ---------------------------------------------------------------------------
// Real multistep-scheduler driver
// ---------------------------------------------------------------------------

/// Weight 1 per chunk. `DatasetsConfig` and the sim's weight table both panic on chunks they don't
/// know, so neither works here.
struct UniformWeight;

impl WeightStrategy for UniformWeight {
    fn prepare<T: SchedulingChunk>(
        &self,
        chunks: Vec<T>,
    ) -> Vec<(T, ChunkWeight, Option<Version>)> {
        chunks.into_iter().map(|chunk| (chunk, 1, None)).collect()
    }
}

fn real_config() -> SchedulingConfig {
    SchedulingConfig {
        worker_capacity: 1_000_000,
        saturation: 0.9,
        min_replication: 1,
        ignore_reliability: true,
    }
}

/// Discover new chunks (incl. correction replacements, whose metadata is created here, not at
/// registration) then run one real scheduling cycle.
fn run_real_cycle<S: SchedulerStorage>(storage: &mut S, at: TimeUnit) -> WorkerAssignment {
    storage.register_new_chunks().expect("register new chunks");
    storage
        .run_scheduling_cycle(
            &MultistepAlgorithm::new(UniformWeight),
            &real_config(),
            at,
            GRACE_PERIOD,
        )
        .expect("scheduling succeeds")
}

// ---------------------------------------------------------------------------
// Scenarios — written once, generic over the backend.
// ---------------------------------------------------------------------------

fn correction_held_until_new_chunk_confirmed<S: TestStorage>() -> anyhow::Result<()> {
    // Held until the replacement confirms, then swapped atomically. The post-confirm assertion
    // (exactly B) is what the removed atomic_swap test checked.
    let dataset = "a";
    let chunk_a = chunk(dataset, 1, 100);
    let mut storage = make_storage::<S>(vec![chunk_a.clone()]);
    let pk_a = storage.pk_of(&chunk_a);

    storage.update_worker_set(&[worker(1, None)], 0, 1000)?;

    let assignment_a = run_real_cycle(&mut storage, CYCLE_INTERVAL);
    storage.confirm_worker_assignment(assignment_a.id, CYCLE_INTERVAL)?;
    storage.run_visibility_cycle(CYCLE_INTERVAL + DELTA)?;

    let pk_b = storage.register_correction(pk_a, chunk_with_blocks(dataset, 2, 100, 2..=3), 150)?;

    // Left unconfirmed: the correction must stay held.
    let assignment_ab = run_real_cycle(&mut storage, 2 * CYCLE_INTERVAL);

    let held = storage.run_visibility_cycle(3 * CYCLE_INTERVAL)?;
    assert_portal_chunks_exact(&held, &[pk_a], "B unconfirmed: only A visible");

    storage.confirm_worker_assignment(assignment_ab.id, 2 * CYCLE_INTERVAL)?;
    let fired = storage.run_visibility_cycle(3 * CYCLE_INTERVAL + DELTA)?;
    assert_portal_chunks_exact(&fired, &[pk_b], "fired: exactly B, A swapped out");
    Ok(())
}

fn correction_new_chunk_not_promoted_until_correction_fires<S: TestStorage>() -> anyhow::Result<()>
{
    // B is registered before A is ever visible and confirmed in the same cycle as A, yet the
    // pending-correction guard keeps it out of the promote pass until the correction fires.
    let dataset = "a";
    let chunk_a = chunk(dataset, 1, 100);
    let mut storage = make_storage::<S>(vec![chunk_a.clone()]);
    let pk_a = storage.pk_of(&chunk_a);

    storage.update_worker_set(&[worker(1, None)], 0, 1000)?;

    let pk_b = storage.register_correction(pk_a, chunk_with_blocks(dataset, 2, 100, 2..=3), 150)?;

    let assignment_ab = run_real_cycle(&mut storage, CYCLE_INTERVAL);
    storage.confirm_worker_assignment(assignment_ab.id, CYCLE_INTERVAL)?;

    let portal = storage.run_visibility_cycle(CYCLE_INTERVAL + DELTA)?;
    assert_portal_chunks_exact(&portal, &[pk_b], "A dropped, B promoted, never both");
    Ok(())
}

fn correction_chain_collapses_in_one_cycle<S: TestStorage>() -> anyhow::Result<()> {
    let dataset = "a";
    let chunk_a = chunk(dataset, 1, 100);
    let mut storage = make_storage::<S>(vec![chunk_a.clone()]);
    let pk_a = storage.pk_of(&chunk_a);

    storage.update_worker_set(&[worker(1, None)], 0, 1000)?;

    let assignment_a = run_real_cycle(&mut storage, CYCLE_INTERVAL);
    storage.confirm_worker_assignment(assignment_a.id, CYCLE_INTERVAL)?;
    storage.run_visibility_cycle(CYCLE_INTERVAL + DELTA)?;

    let pk_b = storage.register_correction(pk_a, chunk_with_blocks(dataset, 2, 100, 2..=3), 1)?;
    let pk_c = storage.register_correction(pk_b, chunk_with_blocks(dataset, 3, 100, 2..=3), 2)?;

    let assignment_abc = run_real_cycle(&mut storage, 2 * CYCLE_INTERVAL);
    storage.confirm_worker_assignment(assignment_abc.id, 2 * CYCLE_INTERVAL)?;

    let portal = storage.run_visibility_cycle(3 * CYCLE_INTERVAL)?;
    assert_portal_chunks_exact(
        &portal,
        &[pk_c],
        "A→B→C collapsed in one cycle: only C visible",
    );
    Ok(())
}

fn correction_independent_corrections_fire_together<S: TestStorage>() -> anyhow::Result<()> {
    // Independent A→B and C→D both fire in one cycle once confirmed; neither waits on the other.
    let dataset = "a";
    let chunk_a = chunk(dataset, 1, 100);
    let chunk_c = chunk(dataset, 3, 100);
    let mut storage = make_storage::<S>(vec![chunk_a.clone(), chunk_c.clone()]);
    let pk_a = storage.pk_of(&chunk_a);
    let pk_c = storage.pk_of(&chunk_c);

    storage.update_worker_set(&[worker(1, None)], 0, 1000)?;

    let assignment_ac = run_real_cycle(&mut storage, CYCLE_INTERVAL);
    storage.confirm_worker_assignment(assignment_ac.id, CYCLE_INTERVAL)?;
    storage.run_visibility_cycle(CYCLE_INTERVAL + DELTA)?;

    let pk_b = storage.register_correction(pk_a, chunk_with_blocks(dataset, 2, 100, 2..=3), 1)?;
    let pk_d = storage.register_correction(pk_c, chunk_with_blocks(dataset, 4, 100, 6..=7), 2)?;

    let assignment_abcd = run_real_cycle(&mut storage, 2 * CYCLE_INTERVAL);
    storage.confirm_worker_assignment(assignment_abcd.id, 2 * CYCLE_INTERVAL)?;

    let portal = storage.run_visibility_cycle(3 * CYCLE_INTERVAL)?;
    assert_portal_chunks_exact(
        &portal,
        &[pk_b, pk_d],
        "both corrections fired: exactly B and D",
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Non-overlap enforcement (docs/nonoverlap-promotion-gate.md)
// ---------------------------------------------------------------------------

fn overlapping_duplicate_rejected_at_registration<S: TestStorage>() -> anyhow::Result<()> {
    // Two distinct chunks in one dataset with overlapping ranges, no correction linking them. The
    // lower-first_block one is admitted; the other is rejected at registration (terminal), so it is
    // never scheduled and only the winner reaches the portal.
    let dataset = "a";
    let lower = chunk_with_blocks(dataset, 1, 100, 10..=20);
    let higher = chunk_with_blocks(dataset, 2, 100, 15..=25); // overlaps lower at [15,20]
    let mut storage = make_storage::<S>(vec![lower.clone(), higher.clone()]);
    let lower_pk = storage.pk_of(&lower);

    storage.update_worker_set(&[worker(1, None)], 0, 1000)?;
    let assignment = run_real_cycle(&mut storage, CYCLE_INTERVAL);
    storage.confirm_worker_assignment(assignment.id, CYCLE_INTERVAL)?;

    let portal = storage.run_visibility_cycle(CYCLE_INTERVAL + DELTA)?;
    assert_portal_chunks_exact(
        &portal,
        &[lower_pk],
        "overlap: only the admitted (lower-first_block) chunk is visible",
    );
    Ok(())
}

fn rejected_duplicate_does_not_self_heal<S: TestStorage>() -> anyhow::Result<()> {
    // The registration loser is rejected terminally — it never self-heals, even after the winner
    // leaves the portal. The freed range can only be filled by a fresh registration.
    let dataset = "a";
    let lower = chunk_with_blocks(dataset, 1, 100, 10..=20);
    let higher = chunk_with_blocks(dataset, 2, 100, 15..=25);
    let mut storage = make_storage::<S>(vec![lower.clone(), higher.clone()]);
    let lower_pk = storage.pk_of(&lower);

    storage.update_worker_set(&[worker(1, None)], 0, 1000)?;
    let assignment = run_real_cycle(&mut storage, CYCLE_INTERVAL);
    storage.confirm_worker_assignment(assignment.id, CYCLE_INTERVAL)?;
    let portal = storage.run_visibility_cycle(CYCLE_INTERVAL + DELTA)?;
    assert_portal_chunks_exact(&portal, &[lower_pk], "only the admitted chunk is visible");

    // Remove the winner; the rejected chunk does NOT take over the freed range.
    storage.mark_for_removal(lower_pk, 2 * CYCLE_INTERVAL)?;
    let portal = storage.run_visibility_cycle(2 * CYCLE_INTERVAL + DELTA)?;
    assert_portal_chunks_exact(&portal, &[], "rejected duplicate never self-heals");
    Ok(())
}

fn correction_replacement_changing_range_is_rejected<S: TestStorage>() -> anyhow::Result<()> {
    // A correction is a 1:1 same-range swap. register_correction refuses a replacement whose range
    // differs from the old chunk's — here [25,35] vs the original's [10,20] — so the correction is
    // never created and the portal is untouched. (Range-changing re-partitions are out of scope.)
    let dataset = "a";
    let original = chunk_with_blocks(dataset, 1, 100, 10..=20);
    let neighbor = chunk_with_blocks(dataset, 5, 100, 30..=40);
    let mut storage = make_storage::<S>(vec![original.clone(), neighbor.clone()]);
    let original_pk = storage.pk_of(&original);
    let neighbor_pk = storage.pk_of(&neighbor);

    storage.update_worker_set(&[worker(1, None)], 0, 1000)?;
    let assignment = run_real_cycle(&mut storage, CYCLE_INTERVAL);
    storage.confirm_worker_assignment(assignment.id, CYCLE_INTERVAL)?;
    let portal = storage.run_visibility_cycle(CYCLE_INTERVAL + DELTA)?;
    assert_portal_chunks_exact(
        &portal,
        &[original_pk, neighbor_pk],
        "original and neighbor both visible",
    );

    // Range-changing replacement [25,35] != original [10,20] → rejected at registration.
    let err = storage
        .register_correction(
            original_pk,
            chunk_with_blocks(dataset, 2, 100, 25..=35),
            150,
        )
        .expect_err("range-changing correction must be rejected");
    assert!(
        matches!(
            err,
            crate::scheduler_storage::StorageError::CorrectionRejected { .. }
        ),
        "expected CorrectionRejected, got {err:?}"
    );

    // The correction was never created: the portal is unchanged.
    let portal = storage.run_visibility_cycle(2 * CYCLE_INTERVAL)?;
    assert_portal_chunks_exact(
        &portal,
        &[original_pk, neighbor_pk],
        "rejected correction leaves the portal untouched",
    );
    Ok(())
}

/// Stamp each generic scenario out as a `#[test]` against both backends, so the suite stays in
/// lock-step without hand-duplicating bodies.
macro_rules! backend_cases {
    ($($scenario:ident),+ $(,)?) => {
        mod in_memory {
            use super::*;
            $(
                #[test]
                fn $scenario() -> anyhow::Result<()> {
                    super::$scenario::<InMemoryStorage>()
                }
            )+
        }

        mod pg {
            use super::*;
            $(
                #[test]
                fn $scenario() -> anyhow::Result<()> {
                    super::$scenario::<PostgresStorage>()
                }
            )+
        }
    };
}

backend_cases!(
    correction_held_until_new_chunk_confirmed,
    correction_new_chunk_not_promoted_until_correction_fires,
    correction_chain_collapses_in_one_cycle,
    correction_independent_corrections_fire_together,
    overlapping_duplicate_rejected_at_registration,
    rejected_duplicate_does_not_self_heal,
    correction_replacement_changing_range_is_rejected,
);

// ---------------------------------------------------------------------------
// P2: Chain scenarios — entire chain collapses in one cycle.
//
// In-memory only: a fresh database per proptest case would make this prohibitively slow on
// Postgres (the PG sim PBT already covers the real-scheduler chain there).
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

    #[test]
    fn prop_correction_chain(
        chain_depth in 2u8..=4u8,
    ) {
        // Real scheduler: one visibility cycle must collapse C0→…→Cn to only Cn.
        let dataset = "a";
        let c0 = chunk(dataset, 1, 100);
        let mut storage = make_storage::<InMemoryStorage>(vec![c0.clone()]);
        let c0_pk = storage.pk_of(&c0);
        storage.update_worker_set(&[worker(1, None)], 0, 1000).unwrap();

        let seed_assignment = run_real_cycle(&mut storage, CYCLE_INTERVAL);
        storage.confirm_worker_assignment(seed_assignment.id, CYCLE_INTERVAL).unwrap();
        storage.run_visibility_cycle(CYCLE_INTERVAL + DELTA).unwrap();

        let mut pks = vec![c0_pk];
        for i in 0..chain_depth {
            // Every link inherits the chain root's range (2..=3): corrections are same-range.
            let repl = chunk_with_blocks(dataset, u32::from(i) + 2, 100, 2..=3);
            let new_pk = storage
                .register_correction(pks[i as usize], repl, (u64::from(i) + 1) * 10)
                .unwrap();
            pks.push(new_pk);
        }

        let chain_assignment = run_real_cycle(&mut storage, 2 * CYCLE_INTERVAL);
        storage.confirm_worker_assignment(chain_assignment.id, 2 * CYCLE_INTERVAL).unwrap();

        let portal = storage.run_visibility_cycle(3 * CYCLE_INTERVAL).unwrap();
        if let Err(violation) = corrections_safety_ok(&storage) {
            prop_assert!(false, "{violation} (chain depth {chain_depth})");
        }

        let last_pk = pks.last().unwrap();
        prop_assert!(
            portal.chunks.contains_key(last_pk),
            "Last chunk in chain must be portal-visible"
        );
    }
}
