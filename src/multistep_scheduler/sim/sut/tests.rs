//! Deterministic unit tests driving `SimUnderTest` directly (no runner), plus the
//! `convergence_under_shortage` drain proptest.

use super::super::utils::{chunk_pk, new_chunk};
use super::*;

#[cfg(test)]
impl<D: SimStorage> SimUnderTest<D> {
    /// Insert chunks into the storage without running a cycle.
    fn seed_chunks(&mut self, new: &[NewChunk]) {
        self.insert_and_register(new);
    }
}

/// `count` uniform chunks keyed from `base`; shared with the Postgres parity tests.
pub(super) fn uniform_chunks(base: u64, count: u64) -> Vec<NewChunk> {
    (0..count)
        .map(|i| {
            new_chunk((
                mint_key(base + i),
                CHUNK_SIZE,
                WEIGHTS[0],
                DATASETS[0].to_string(),
            ))
        })
        .collect()
}

/// All [`WEIGHTS`] values coexist in one dataset: `prepare` hands every chunk back with its
/// recorded weight, nothing dropped.
#[test]
fn multiple_weights_coexist_in_one_dataset() {
    let weights = WeightTable::default();
    let chunks: Vec<NewChunk> = (0u64..3)
        .flat_map(|rep| {
            WEIGHTS.iter().map(move |&weight| {
                new_chunk((
                    mint_key(rep * 100 + u64::from(weight)),
                    CHUNK_SIZE,
                    weight,
                    DATASETS[0].to_string(),
                ))
            })
        })
        .collect();
    let expected: HashMap<(String, String), u16> =
        chunks.iter().map(|c| (chunk_pk(c), c.weight)).collect();

    // pk sort order mirrors a real cycle.
    record_weights(&weights, &chunks);
    let mut storage_chunks: Vec<Chunk> = chunks.iter().map(storage_chunk).collect();
    storage_chunks.sort_by_key(|c| ((*c.dataset).clone(), (*c.id).clone()));
    let prepared = weights.prepare(storage_chunks);

    assert_eq!(prepared.len(), chunks.len(), "a chunk lost its weight");
    for (chunk, weight, _) in prepared {
        let pk = ((*chunk.dataset).clone(), (*chunk.id).clone());
        assert_eq!(weight, expected[&pk], "wrong weight for {pk:?}");
    }
}

/// With stale accumulated and never cleared, repeated frozen-clock rescheduling must reach a
/// fixed point — no oscillation.
#[test]
fn settle_without_clearing_reaches_fixed_point() {
    let mut sim = SimUnderTest::new();
    // Each burst lowers the replication factor and sheds copies into stale.
    for burst in [8u64, 8, 6] {
        let base = sim.total_chunk_count() as u64;
        sim.seed_chunks(&uniform_chunks(base, burst));
        assert!(
            sim.with_step_safety(|s| s.run_scheduler().is_some()),
            "setup reschedule should stay feasible"
        );
    }

    let mut converged = false;
    for _ in 0..40 {
        let before = sim.storage.snapshot().ideal;
        assert!(sim.with_step_safety(|s| s.run_scheduler().is_some()));
        if sim.storage.snapshot().ideal == before {
            converged = true;
            break;
        }
    }
    assert!(
        converged,
        "pure settle (no clearing) did not reach a fixed point"
    );
}

/// `AddOutcome` distinguishes a feasible add from a recorded shortage; an over-subscribing
/// burst is inserted either way.
#[test]
fn add_outcome_distinguishes_progress_from_shortage() {
    let mut sim = SimUnderTest::new();
    let base = sim.total_chunk_count() as u64;
    assert_eq!(
        sim.do_add_chunks(uniform_chunks(base, 4)),
        AddOutcome::AllPlaced
    );

    let before = sim.total_chunk_count();
    let base = sim.total_chunk_count() as u64;
    assert_eq!(
        sim.do_add_chunks(uniform_chunks(base, 255)),
        AddOutcome::Infeasible
    );
    assert_eq!(
        sim.total_chunk_count(),
        before + 255,
        "an over-subscribing burst is still inserted, not rejected"
    );
}

/// A join adds a worker; a feasible departure re-replicates lost copies so every chunk holds
/// the floor at convergence.
#[test]
fn worker_join_and_feasible_departure_preserve_floors() {
    let config = SimConfig::fixed(); // 6 workers, min_replication 2
    let mut sim = SimUnderTest::from_config(&config);
    let min_replication = usize::from(sim.config.min_replication);
    let base = sim.total_chunk_count() as u64;
    let chunks = uniform_chunks(base, 5);
    let keys: Vec<ChunkKey> = chunks.iter().map(|c| c.key.clone()).collect();
    assert_eq!(sim.do_add_chunks(chunks), AddOutcome::AllPlaced);

    let fresh = sim
        .workers
        .iter()
        .position(|w| !w.active)
        .expect("the pool has spare workers to join");
    assert!(sim.do_worker_joined(fresh), "a fresh worker joins");
    assert_eq!(sim.active_workers_count(), 7);
    assert!(
        !sim.do_worker_joined(fresh),
        "re-adding the same worker is a no-op"
    );
    assert_eq!(sim.active_workers_count(), 7);

    assert!(sim.do_worker_left(0), "a present worker departs");
    assert_eq!(sim.active_workers_count(), 6);
    for key in &keys {
        assert!(
            sim.converged_replication_of(key) >= min_replication,
            "chunk {key} fell below the floor after a feasible departure",
        );
    }
}

/// A departure that strands chunks below the floor records the shortage rather than panicking.
#[test]
fn removing_a_needed_worker_records_a_shortage() {
    let config = SimConfig {
        worker_count: 2,
        worker_capacity: WORKER_CAPACITY,
        min_replication: 2,
        saturation: 0.9,
        converge_is_terminal: false,
        chunk_cap: None,
        datasets: sim_datasets(),
        confirm_threshold_pct: 100,
    };
    let mut sim = SimUnderTest::from_config(&config);
    let key = sim.total_chunk_count() as u64;
    assert_eq!(
        sim.do_add_chunks(uniform_chunks(key, 1)),
        AddOutcome::AllPlaced,
    );
    assert!(
        !sim.is_infeasible,
        "feasible while two workers hold the chunk"
    );

    assert!(
        sim.do_worker_left(1),
        "the worker leaves even though it strands the chunk"
    );
    assert_eq!(sim.active_workers_count(), 1);
    assert!(sim.is_infeasible, "the shortage is recorded, not panicked");
}

/// An over-subscribing burst is still inserted (a real client always registers its chunks); the
/// shortage is recorded, and the set still drains to a fixed point.
#[test]
fn over_subscribing_burst_is_inserted_and_recorded() {
    let mut sim = SimUnderTest::new(); // 6 workers × 10 MiB, min_replication 2 ⇒ ~27 chunks feasible
    let base = sim.total_chunk_count() as u64;
    let burst = uniform_chunks(base, 255);
    let n = burst.len();

    sim.do_add_chunks(burst);
    assert_eq!(
        sim.total_chunk_count(),
        n,
        "an over-subscribing burst is still inserted into storage"
    );
    assert!(
        sim.is_infeasible,
        "the shortage is recorded on the SUT, not rejected or panicked"
    );

    // The infeasible set still drains and asserts gracefully (no panic).
    sim.check_converged(ConvergenceCheck::FloorsPreemptBonuses);
}

/// An over-subscribed fleet yields the typed `Err(StorageError::Shortage)` at the
/// `SchedulerStorage` trait surface — a first-class outcome callers can match on.
#[test]
fn over_subscription_yields_typed_storage_error_shortage() {
    let mut sim = SimUnderTest::new(); // 6 workers × 10 MiB, min_replication 2
    let base = sim.total_chunk_count() as u64;
    // `InMemoryStorage` has inherent methods of the same name, so drive the trait explicitly.
    let burst = uniform_chunks(base, 255);
    record_weights(&sim.weights, &burst); // the scheduler reads each chunk's weight back
    let storage_chunks: Vec<Chunk> = burst.iter().map(storage_chunk).collect();
    SchedulerStorage::insert_new_chunks(&mut sim.storage, storage_chunks)
        .expect("chunks name registered datasets");
    SchedulerStorage::register_new_chunks(&mut sim.storage)
        .expect("chunks are inserted before registration");

    let result = SchedulerStorage::run_scheduling_cycle(
        &mut sim.storage,
        &sim.algo,
        &sim.config,
        sim.now,
        M_TICKS,
    );
    assert!(
        matches!(result, Err(StorageError::Shortage)),
        "an over-subscribed fleet yields a typed Err(StorageError::Shortage)",
    );
}

/// Lowering `min_replication` frees capacity: a fleet saturated at a high floor holds strictly
/// more chunks once the floor drops.
#[test]
fn dropping_min_replication_frees_capacity_for_more_chunks() {
    let config = SimConfig {
        worker_count: 5,
        worker_capacity: WORKER_CAPACITY,
        min_replication: 5, // each chunk replicated onto every worker — costly
        saturation: 0.9,
        converge_is_terminal: false,
        chunk_cap: None,
        datasets: sim_datasets(),
        confirm_threshold_pct: 100,
    };
    let mut sim = SimUnderTest::from_config(&config);

    // Add one chunk at a time until the fleet first records a shortage.
    fn saturate(sim: &mut SimUnderTest<InMemoryStorage>) {
        for _ in 0..1000 {
            if sim.is_infeasible() {
                return;
            }
            let base = sim.total_chunk_count() as u64;
            sim.do_add_chunks(uniform_chunks(base, 1));
        }
        panic!("never saturated within 1000 adds");
    }

    saturate(&mut sim);
    let at_high_floor = sim.total_chunk_count();

    sim.do_set_min_replication(2);
    saturate(&mut sim);
    let at_low_floor = sim.total_chunk_count();

    assert!(
        at_low_floor > at_high_floor,
        "lowering min_replication 5→2 should free capacity for more chunks: \
         {at_low_floor} at floor 2 vs {at_high_floor} at floor 5"
    );

    // The recorded shortage must be a justified refusal, not wasted room.
    sim.check_converged(ConvergenceCheck::FloorLocallyFeasible);
}

/// A successful WorkerFetch replaces a worker's holdings with exactly its slice of the latest
/// published assignment.
#[test]
fn worker_fetch_replaces_holdings() {
    let mut sim = SimUnderTest::new();
    let base = sim.total_chunk_count() as u64;
    sim.do_add_chunks(uniform_chunks(base, 4));
    sim.do_worker_fetch(0, true);
    let worker = sim.worker_pk_of_index(0).expect("worker 0 is active");
    let wa = sim
        .latest_worker_assignment
        .as_ref()
        .expect("a scheduling cycle ran");
    for (chunk, workers) in &wa.chunk_workers {
        assert_eq!(
            sim.workers_state.holds_chunk(worker, chunk),
            workers.contains(&worker),
            "worker 0's holdings must equal its slice of the published assignment for {chunk:?}",
        );
    }
}

/// At 100% quorum one never-fetched worker pins the watermark at 0; once every worker fetches,
/// it advances.
#[test]
fn full_quorum_requires_every_worker_to_fetch() {
    let mut sim = SimUnderTest::new(); // 6 workers, threshold 100
    let base = sim.total_chunk_count() as u64;
    sim.do_add_chunks(uniform_chunks(base, 3));
    let watermark = |sim: &SimUnderTest<InMemoryStorage>| {
        let active = sim.active_worker_pks();
        sim.workers_state
            .confirmation_watermark(sim.confirm_threshold_pct, &active)
    };
    for index in 1..sim.active_workers_count() {
        sim.do_worker_fetch(index, true);
    }
    assert_eq!(
        watermark(&sim),
        0,
        "one un-fetched worker pins the 100% watermark"
    );
    sim.do_worker_fetch(0, true);
    assert!(
        watermark(&sim) > 0,
        "all workers fetched — watermark advances"
    );
}

/// The consistency oracle fires when a within-M portal routes a chunk that no active routed
/// worker holds (premature deletion).
#[test]
#[should_panic(expected = "no active routed worker holds it")]
fn consistency_oracle_fires_on_premature_deletion() {
    let mut sim = SimUnderTest::new();
    let base = sim.total_chunk_count() as u64;
    sim.do_add_chunks(uniform_chunks(base, 3));
    // Chunks must be confirmed to become portal-visible.
    sim.catch_up_and_confirm();
    let pa = SchedulerStorage::run_visibility_cycle(&mut sim.storage, sim.now)
        .expect("visibility cycle succeeds");
    sim.publish_portal_assignment(pa);
    sim.do_portal_fetch(true);
    assert!(
        sim.latest_portal_assignment
            .as_ref()
            .is_some_and(|pa| !pa.chunk_workers.is_empty()),
        "precondition: the portal must route at least one chunk for this test to be meaningful",
    );
    wipe_fleet_holdings(&mut sim);
    sim.assert_portal_consistency(); // panics
}

/// Beyond M the same zero-holder state is tolerated (the portal is too stale).
#[test]
fn consistency_oracle_tolerates_beyond_m() {
    let mut sim = SimUnderTest::new();
    let base = sim.total_chunk_count() as u64;
    sim.do_add_chunks(uniform_chunks(base, 3));
    sim.catch_up_and_confirm();
    let pa = SchedulerStorage::run_visibility_cycle(&mut sim.storage, sim.now)
        .expect("visibility cycle succeeds");
    sim.publish_portal_assignment(pa);
    sim.do_portal_fetch(true);
    wipe_fleet_holdings(&mut sim);
    sim.now += M_TICKS; // push the snapshot age to >= M
    sim.assert_portal_consistency(); // no panic
}

/// Wipe every active worker's holdings while they stay active — a stand-in for premature deletion.
#[cfg(test)]
fn wipe_fleet_holdings(sim: &mut SimUnderTest<InMemoryStorage>) {
    let active = sim.active_worker_pks();
    let empty = WorkerAssignment {
        id: 0,
        chunk_workers: BTreeMap::new(),
        chunks: BTreeMap::new(),
        workers: BTreeMap::new(),
        replication_by_weight: BTreeMap::new(),
    };
    sim.workers_state.catch_up_all(&active, &empty, sim.now);
}

#[cfg(test)]
mod convergence_under_shortage {
    use super::*;
    use proptest::prelude::*;
    use proptest::statistics::{self, CoverageConfidence};

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 400,
            // Sequential statistical test (QuickCheck's checkCoverage) for the `cover`
            // requirements below: a run can't fail by an unlucky sample, and a generator
            // regression that kills a regime fails loudly.
            coverage_confidence: Some(CoverageConfidence {
                max_cases: 10_000,
                ..CoverageConfidence::default()
            }),
            ..ProptestConfig::default()
        })]
        /// Build a placement the real way (add bursts + optional departure), then drain: the
        /// *ideal* placement must reach a fixed point even over-subscribed with a permanently
        /// stale orphan. Regression for the drain-termination bug — it used to require stale to
        /// fully drain, which a sustained shortage prevents.
        #[test]
        fn draining_after_churn_reaches_a_fixed_point(
            worker_count in 3u16..=6,
            // At the higher floors demand regularly exceeds the post-departure budget, keeping
            // the sustained-shortage regime (see `cover` below) common.
            min_replication in 1u16..=3,
            bursts in prop::collection::vec(
                prop::collection::vec(
                    (prop::sample::select(WEIGHTS.to_vec()), 0usize..DATASETS.len()),
                    1..=6usize,
                ),
                1..=4usize,
            ),
            leave in prop::option::of(0usize..6usize),
        ) {
            let datasets = sim_datasets();
            let config = SimConfig {
                worker_count,
                worker_capacity: WORKER_CAPACITY,
                min_replication,
                saturation: 0.9,
                converge_is_terminal: false,
                chunk_cap: None,
                datasets: datasets.clone(),
                confirm_threshold_pct: 100,
            };
            let mut sim = SimUnderTest::from_config(&config);
            let mut key = 0u64;
            for burst in &bursts {
                let chunks: Vec<NewChunk> = burst
                    .iter()
                    .map(|&(weight, ds)| {
                        let chunk =
                            new_chunk((mint_key(key), CHUNK_SIZE, weight, datasets[ds].clone()));
                        key += 1;
                        chunk
                    })
                    .collect();
                sim.do_add_chunks(chunks); // real path: insert + cycle
            }
            let departed = match leave {
                Some(idx) => sim.do_worker_left(idx % usize::from(worker_count)),
                None => false,
            };
            // Panics if no fixed point within MAX_SETTLE (the bug).
            sim.drain_to_fixed_point();
            // Observed rates are ~50% / ~14%; the requirements sit well below. The run fails if
            // either regime stops occurring.
            statistics::cover(departed, 30.0, "a worker departed mid-run");
            statistics::cover(sim.is_infeasible(), 5.0, "drained under a sustained shortage");
        }
    }
}
