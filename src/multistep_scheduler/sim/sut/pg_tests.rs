//! Postgres parity tests mirroring the in-memory shortage and drain scenarios.
//! `PostgresStorage` has no `Default`, so the SUT is built via `from_config_with`
//! on a fresh migrated database.

use super::super::utils::new_chunk;
use super::tests::uniform_chunks;
use super::*;
use crate::scheduler_storage::postgres::PostgresStorage;
use crate::scheduler_storage::test_harness::pg_harness::fresh_db;

/// Postgres parity for the `AddOutcome` half of [`over_subscribing_burst_is_inserted_and_recorded`].
#[test]
fn pg_add_outcome_distinguishes_progress_from_shortage() {
    let storage = fresh_db("pg_add_outcome", 1);
    let mut sim = SimUnderTest::<PostgresStorage>::from_config_with(&SimConfig::fixed(), storage);

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

use proptest::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};

/// Unique database id per proptest case, including shrink retries — `CREATE DATABASE` of a
/// duplicate name panics.
static G_CASE: AtomicU64 = AtomicU64::new(0);

proptest! {
    // Postgres is far slower, so run far fewer cases.
    #![proptest_config(ProptestConfig { cases: 16, ..ProptestConfig::default() })]
    /// Postgres parity for
    /// [`convergence_under_shortage::draining_after_churn_reaches_a_fixed_point`], on a fresh
    /// database per case.
    #[test]
    fn pg_draining_after_churn_reaches_a_fixed_point(
        worker_count in 3u16..=6,
        bursts in prop::collection::vec(
            prop::collection::vec(
                (prop::sample::select(WEIGHTS.to_vec()), 0usize..DATASETS.len()),
                1..=6usize,
            ),
            1..=4usize,
        ),
        leave in prop::option::of(0usize..6usize),
    ) {
        let id = G_CASE.fetch_add(1, Ordering::Relaxed);
        let datasets = sim_datasets();
        let config = SimConfig {
            worker_count,
            worker_capacity: WORKER_CAPACITY,
            min_replication: 1,
            saturation: 0.9,
            converge_is_terminal: false,
            chunk_cap: None,
            datasets: datasets.clone(),
            confirm_threshold_pct: 100,
            gc_ticks: 0,
        };
        let storage = fresh_db("pg_drain_churn", id);
        let mut sim = SimUnderTest::<PostgresStorage>::from_config_with(&config, storage);
        let mut key = 0u64;
        for burst in &bursts {
            let chunks: Vec<NewChunk> = burst
                .iter()
                .map(|&(weight, ds)| {
                    let chunk = new_chunk((mint_key(key), CHUNK_SIZE, weight, datasets[ds].clone()));
                    key += 1;
                    chunk
                })
                .collect();
            sim.do_add_chunks(chunks); // real path: insert + cycle
        }
        if let Some(idx) = leave {
            sim.do_worker_left(idx % usize::from(worker_count));
        }
        // Panics if no fixed point within MAX_SETTLE.
        sim.drain_to_fixed_point();
    }
}
