//! Property-based tests for the scheduling/scatter algorithm, kept separate from
//! the example-based tests in `scheduling.rs`. They drive the public `schedule`
//! plus the crate-internal `distribute` scatter (via the `placeable_at` oracle).
//!
//! Order: regression tests, then the property tests they came from, then helpers.

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Once;

use libp2p_identity::PeerId;
use proptest::prelude::*;
use semver::Version;

use crate::{
    replication::{ReplicationError, calc_replication_factors},
    scheduling::{ReplicatedChunk, ScheduledChunk, SchedulingConfig, distribute, schedule},
    types::{ChunkIndex, Worker, WorkerStatus},
};

// ---- regression tests ----
// Hardcoded counterexamples from the property tests below, each named after the
// proptest it pins down.

/// Regression for `schedule_picks_a_placeable_replication`
/// The minimum replication places fine, but `schedule` fills capacity to a higher factor that fragments
/// across workers and panics instead of backing off.
#[test]
#[should_panic(expected = "schedule should place when a feasible replication exists")]
fn regression_schedule_picks_a_placeable_replication() {
    let scenario = Scenario {
        num_workers: 6,
        chunk_sizes: vec![284, 717],
        saturation: 0.94,
        min_replication: 1,
    };
    let Outcome {
        max_feasible,
        derived,
        scheduled_ok,
    } = evaluate(&scenario);

    assert!(
        max_feasible.is_some(),
        "min_replication should be placeable in this scenario"
    );
    assert!(
        scheduled_ok,
        "schedule should place when a feasible replication exists \
         (max feasible = {max_feasible:?}, derived = {derived:?})",
    );
}

// ---- property tests ----

#[derive(Debug, Clone)]
struct Scenario {
    num_workers: usize,
    chunk_sizes: Vec<u32>,
    saturation: f64,
    min_replication: u16,
}

prop_compose! {
    // Single weight keeps replication a scalar. Coarse chunk sizes (up to a full
    // worker) make per-worker fragmentation bite. `min_replication` depends on
    // `num_workers`, hence the two-stage generator.
    fn strategy_scenario()(num_workers in 2usize..=8)(
        num_workers in Just(num_workers),
        min_replication in 1u16..=num_workers as u16,
        chunk_sizes in proptest::collection::vec(1u32..=WORKER_CAPACITY as u32, 1..=40),
        saturation in 0.5f64..=0.99,
    ) -> Scenario {
        Scenario { num_workers, chunk_sizes, saturation, min_replication }
    }
}

struct Outcome {
    /// Largest uniform replication (>= min) that fits, or `None` if even `min` fragments.
    max_feasible: Option<u16>,
    /// The factor `schedule` derives for the whole set (diagnostics only).
    derived: Result<BTreeMap<u16, u16>, ReplicationError>,
    /// Whether `schedule` placed everything without panicking.
    scheduled_ok: bool,
}

proptest! {
    /// `schedule` fills capacity at a replication factor whose placement
    /// (consistent-hashing first-fit) can fragment and fail — even though a lower
    /// factor (still >= `min_replication`) would have placed everything. It never
    /// backs off, so it panics.
    ///
    /// Property: if any factor `>= min_replication` places all chunks, `schedule` must too.
    ///
    /// `#[should_panic]`: a counterexample currently exists; remove the marker once fixed.
    #[test]
    #[should_panic(expected = "a placeable replication exists")]
    fn schedule_picks_a_placeable_replication(scenario in strategy_scenario()) {
        let Outcome { max_feasible, derived, scheduled_ok } = evaluate(&scenario);
        prop_assert!(
            max_feasible.is_none() || scheduled_ok,
            "a placeable replication exists (max feasible = {max_feasible:?}, min = {min}), \
             but schedule derived {derived:?} and failed to place — it should have backed off \
             to the largest feasible replication (which may be above min). \
             workers={workers}, saturation={saturation}, chunk_sizes={sizes:?}",
            min = scenario.min_replication,
            workers = scenario.num_workers,
            saturation = scenario.saturation,
            sizes = scenario.chunk_sizes,
        );
    }
}

// ---- helpers ----

const WORKER_CAPACITY: u64 = 1_000;

/// Dataset name shared by every test chunk.
const DATASET: &str = "ds";

// Chunk-id prefixes, kept distinct per role so ids never collide across chunk sets.
const SCENARIO_CHUNK_PREFIX: char = 'c';

/// Idempotent per-test setup: silence the panic printer (the intentional
/// `catch_unwind`s would otherwise flood stderr) and route `RUST_LOG` to the test writer.
fn setup() {
    static HOOK: Once = Once::new();
    HOOK.call_once(|| std::panic::set_hook(Box::new(|_| {})));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

/// Run `f`, returning `None` if it panicked — placement panics on capacity/version
/// failures and we catch it rather than abort the run.
fn no_panic<T>(f: impl FnOnce() -> T) -> Option<T> {
    catch_unwind(AssertUnwindSafe(f)).ok()
}

/// Deterministic, distinct worker per index (stable proptest shrinking).
fn worker_with_version(index: u64, version: Option<&str>) -> Worker {
    let mut seed = [0u8; 32];
    seed[..8].copy_from_slice(&index.to_le_bytes());
    let keypair = libp2p_identity::Keypair::ed25519_from_bytes(seed).unwrap();
    Worker {
        id: keypair.public().to_peer_id(),
        status: WorkerStatus::Online,
        version: version.map(|v| Version::parse(v).unwrap()),
    }
}

fn test_worker(index: u64) -> Worker {
    worker_with_version(index, None)
}

/// `count` distinct chunk ids prefixed with `prefix`. Owned so callers keep them
/// alive for the borrowed `ScheduledChunk`s.
fn chunk_ids(prefix: char, count: usize) -> Vec<String> {
    (0..count).map(|i| format!("{prefix}{i}")).collect()
}

/// Weight-1 `ScheduledChunk`s pairing `ids` with `sizes`, all sharing `min_ver`.
fn chunks_from<'a>(
    dataset: &'a str,
    ids: &'a [String],
    sizes: &[u32],
    min_ver: Option<&'a Version>,
) -> Vec<ScheduledChunk<'a>> {
    sizes
        .iter()
        .zip(ids)
        .map(|(&size, id)| ScheduledChunk {
            dataset,
            chunk_id: id.as_str(),
            size,
            weight: 1,
            minimum_worker_version: min_ver,
        })
        .collect()
}

/// Oracle: the scatter placement (worker -> chunk indices) at a uniform `replication`,
/// or `None` if `distribute` panics (a chunk found no worker with free capacity).
fn placement_at(
    chunks: &[ScheduledChunk],
    workers: &[&Worker],
    replication: u16,
) -> Option<BTreeMap<PeerId, Vec<ChunkIndex>>> {
    let replicated: Vec<_> = chunks
        .iter()
        .map(|chunk| ReplicatedChunk {
            dataset: chunk.dataset,
            chunk_id: chunk.chunk_id,
            size: chunk.size,
            replication,
            minimum_worker_version: None,
        })
        .collect();
    no_panic(|| distribute(&replicated, workers, WORKER_CAPACITY))
}

fn placeable_at(chunks: &[ScheduledChunk], workers: &[&Worker], replication: u16) -> bool {
    placement_at(chunks, workers, replication).is_some()
}

/// Render a scatter placement as `worker N: id(size), …` rows, sorted by worker index.
fn format_placement(
    placement: &BTreeMap<PeerId, Vec<ChunkIndex>>,
    workers: &[&Worker],
    chunks: &[ScheduledChunk],
) -> String {
    let mut rows: Vec<(usize, String)> = placement
        .iter()
        .map(|(id, indices)| {
            let worker = workers.iter().position(|w| w.id == *id).unwrap();
            let held = if indices.is_empty() {
                "(empty)".to_string()
            } else {
                indices
                    .iter()
                    .map(|&i| {
                        let c = &chunks[i as usize];
                        format!("{}({})", c.chunk_id, c.size)
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            (worker, format!("  worker {worker}: {held}"))
        })
        .collect();
    rows.sort();
    rows.into_iter()
        .map(|(_, row)| row)
        .collect::<Vec<_>>()
        .join("\n")
}

/// Run the scenario through both the feasibility oracle and `schedule`.
fn evaluate(scenario: &Scenario) -> Outcome {
    setup();

    let workers: Vec<Worker> = (0..scenario.num_workers as u64).map(test_worker).collect();
    let worker_refs: Vec<&Worker> = workers.iter().collect();

    let ids = chunk_ids(SCENARIO_CHUNK_PREFIX, scenario.chunk_sizes.len());
    let chunks = chunks_from(DATASET, &ids, &scenario.chunk_sizes, None);

    let config = SchedulingConfig {
        worker_capacity: WORKER_CAPACITY,
        saturation: scenario.saturation,
        min_replication: scenario.min_replication,
        ignore_reliability: true,
    };

    // Feasibility is monotone in replication, so the feasible factors form a prefix:
    // `take_while` collects it and `last` is the maximum.
    let max_feasible = (config.min_replication..=scenario.num_workers as u16)
        .take_while(|&replication| placeable_at(&chunks, &worker_refs, replication))
        .last();

    // What `schedule` derives (mirrors `schedule_to_workers`).
    let total_size: u64 = scenario.chunk_sizes.iter().map(|&size| size as u64).sum();
    let capacity_budget =
        (WORKER_CAPACITY * scenario.num_workers as u64) as f64 * scenario.saturation;
    let derived = calc_replication_factors(
        BTreeMap::from([(1u16, total_size)]),
        capacity_budget as u64,
        config.min_replication,
        scenario.num_workers as u16,
    );

    let scheduled_ok = no_panic(|| schedule(&chunks, &workers, config.clone())).is_some();

    tracing::info!(
        "derived={derived:?}, largest feasible (>= min {}) = {max_feasible:?}, placed_ok={scheduled_ok}",
        scenario.min_replication,
    );

    // `schedule` failed but a lower replication is feasible: print the eligible
    // placement the oracle found — the schedule `schedule` should have produced.
    if !scheduled_ok {
        if let Some(feasible) = max_feasible {
            if let Some(placement) = placement_at(&chunks, &worker_refs, feasible) {
                tracing::info!(
                    "eligible schedule at R={feasible}:\n{}",
                    format_placement(&placement, &worker_refs, &chunks),
                );
            }
        }
    }

    Outcome {
        max_feasible,
        derived,
        scheduled_ok,
    }
}
