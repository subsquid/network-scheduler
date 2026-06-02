//! Property-based tests for the scheduler. It discovers the replication factor
//! dynamically (placing replicas round by round up to the byte-budget cap), so a
//! fragmenting placement backs off to a feasible factor instead of panicking.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Once;

use proptest::prelude::*;

use crate::{
    replication::ReplicationError,
    scheduling::{ScheduledChunk, SchedulingConfig, schedule},
    types::{Assignment, Worker, WorkerStatus},
};

/// The counterexample that made the old single-pass scatter panic.
#[test]
fn schedule_backs_off_instead_of_panicking() {
    let scenario = Scenario {
        num_workers: 6,
        chunk_sizes: vec![284, 717],
        saturation: 0.94,
        min_replication: 1,
    };
    let assignment = run(&scenario).expect("schedule should place the fragmenting scenario");
    assert_eq!(
        assignment.replication_by_weight.get(&1),
        Some(&3),
        "should back off to R=3, the largest factor that fits",
    );
}

proptest! {
    /// `schedule` never panics: it returns a placement or a clean `NotEnoughCapacity`.
    /// Whenever it places, every chunk keeps at least `min_replication` replicas.
    #[test]
    fn schedule_never_panics(scenario in strategy_scenario()) {
        let outcome = no_panic(|| run(&scenario));
        prop_assert!(outcome.is_some(), "schedule must never panic");
        if let Some(Ok(assignment)) = outcome {
            prop_assert!(
                assignment
                    .replication_by_weight
                    .values()
                    .all(|&r| r >= scenario.min_replication),
                "placed chunks must keep >= min_replication replicas, got {:?}",
                assignment.replication_by_weight,
            );
            // Invariant: a worker never holds two replicas of the same chunk.
            for (worker, chunks) in &assignment.worker_chunks {
                let unique: std::collections::BTreeSet<_> = chunks.iter().collect();
                prop_assert_eq!(
                    unique.len(),
                    chunks.len(),
                    "worker {:?} holds a duplicate chunk replica: {:?}",
                    worker,
                    chunks,
                );
            }
        }
    }
}

// ---- helpers ----

const WORKER_CAPACITY: u64 = 1_000;
const DATASET: &str = "ds";
const CHUNK_PREFIX: char = 'c';

#[derive(Debug, Clone)]
struct Scenario {
    num_workers: usize,
    chunk_sizes: Vec<u32>,
    saturation: f64,
    min_replication: u16,
}

prop_compose! {
    // Single weight keeps replication a scalar. Coarse chunk sizes (up to a full worker)
    // make per-worker fragmentation bite. `min_replication` depends on `num_workers`,
    // hence the two-stage generator.
    fn strategy_scenario()(num_workers in 2usize..=8)(
        num_workers in Just(num_workers),
        min_replication in 1u16..=num_workers as u16,
        chunk_sizes in proptest::collection::vec(1u32..=WORKER_CAPACITY as u32, 1..=40),
        saturation in 0.5f64..=0.99,
    ) -> Scenario {
        Scenario { num_workers, chunk_sizes, saturation, min_replication }
    }
}

/// Build the workers/chunks for a `Scenario` and run `schedule`.
fn run(scenario: &Scenario) -> Result<Assignment, ReplicationError> {
    setup();
    let workers: Vec<Worker> = (0..scenario.num_workers as u64).map(test_worker).collect();
    let ids = chunk_ids(CHUNK_PREFIX, scenario.chunk_sizes.len());
    let chunks = chunks_from(DATASET, &ids, &scenario.chunk_sizes);
    let config = SchedulingConfig {
        worker_capacity: WORKER_CAPACITY,
        saturation: scenario.saturation,
        min_replication: scenario.min_replication,
        ignore_reliability: true,
    };
    schedule(&chunks, &workers, config)
}

/// Idempotent per-test setup: silence the panic printer (the intentional `catch_unwind`s
/// would otherwise flood stderr) and route `RUST_LOG` to the test writer.
fn setup() {
    static HOOK: Once = Once::new();
    HOOK.call_once(|| std::panic::set_hook(Box::new(|_| {})));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

/// Run `f`, returning `None` if it panicked.
fn no_panic<T>(f: impl FnOnce() -> T) -> Option<T> {
    catch_unwind(AssertUnwindSafe(f)).ok()
}

/// Deterministic, distinct worker per index (stable proptest shrinking).
fn test_worker(index: u64) -> Worker {
    let mut seed = [0u8; 32];
    seed[..8].copy_from_slice(&index.to_le_bytes());
    let keypair = libp2p_identity::Keypair::ed25519_from_bytes(seed).unwrap();
    Worker {
        id: keypair.public().to_peer_id(),
        status: WorkerStatus::Online,
        version: None,
    }
}

/// `count` distinct chunk ids prefixed with `prefix`. Owned so callers keep them alive
/// for the borrowed `ScheduledChunk`s.
fn chunk_ids(prefix: char, count: usize) -> Vec<String> {
    (0..count).map(|i| format!("{prefix}{i}")).collect()
}

/// Weight-1 `ScheduledChunk`s pairing `ids` with `sizes`.
fn chunks_from<'a>(dataset: &'a str, ids: &'a [String], sizes: &[u32]) -> Vec<ScheduledChunk<'a>> {
    sizes
        .iter()
        .zip(ids)
        .map(|(&size, id)| ScheduledChunk {
            dataset,
            chunk_id: id.as_str(),
            size,
            weight: 1,
            minimum_worker_version: None,
        })
        .collect()
}
