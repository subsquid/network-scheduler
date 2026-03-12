use std::collections::BTreeMap;

use itertools::Itertools;
use libp2p_identity::PeerId;
use semver::Version;

use super::{
    input::{generate_chunks, generate_input, generate_workers},
    utils::{Stats, compare_intersection},
};
use crate::{
    scheduling::{SchedulingConfig, schedule},
    types::{Assignment, Worker, WorkerIndex, WorkerStatus},
};

#[test]
fn test_scheduling_stable() {
    let (chunks, mut workers, total_size) = generate_input(100, 100_000, &[1]);
    let capacity = (total_size as f64 / workers.len() as f64 * 30.) as u64;
    let config = SchedulingConfig {
        worker_capacity: capacity,
        saturation: 0.95,
        min_replication: 1,
        ignore_reliability: false,
    };

    let assignment1 = schedule(&chunks, &workers, config.clone()).unwrap();
    workers.reverse();
    let assignment2 = schedule(&chunks, &workers, config).unwrap();

    assert_eq!(assignment1, assignment2);
}

#[test]
fn test_scheduling_sorted() {
    let (chunks, workers, total_size) = generate_input(100, 100_000, &[1]);
    let capacity = (total_size as f64 / workers.len() as f64 * 30.) as u64;
    let assignment = schedule(
        &chunks,
        &workers,
        SchedulingConfig {
            worker_capacity: capacity,
            saturation: 0.999,
            min_replication: 1,
            ignore_reliability: false,
        },
    )
    .unwrap();
    for chunks in assignment.worker_chunks.into_values() {
        assert!(chunks.into_iter().is_sorted());
    }
}

#[test]
fn test_scheduling_uniform() {
    let (chunks, workers, total_size) = generate_input(100, 50_000, &[1]);
    let capacity = (total_size as f64 / workers.len() as f64 * 30.) as u64;
    let assignment = schedule(
        &chunks,
        &workers,
        SchedulingConfig {
            worker_capacity: capacity,
            saturation: 0.95,
            min_replication: 1,
            ignore_reliability: false,
        },
    )
    .unwrap();

    let mut sizes: Vec<u64> = vec![0; workers.len()];
    for (worker_index, chunk_indexes) in assignment.worker_chunks.into_values().enumerate() {
        for chunk_index in chunk_indexes {
            sizes[worker_index as usize] += chunks[chunk_index as usize].size as u64;
        }
    }
    let stats = Stats::new(sizes.iter().copied());
    println!(
        "Capacity: {}GB, {}",
        capacity / (1 << 30),
        stats.format("GB", 1 << 30)
    );

    // The test is randomized, but these ranges are very likely to be satisfied
    assert!(stats.std < stats.avg * 0.02);
    assert!(stats.min as f64 > stats.avg * 0.95);
    assert!((stats.max as f64) < stats.avg * 1.05);
}

#[test]
fn test_rescheduling_workers_left_strict() {
    const WORKERS_BEFORE: WorkerIndex = 100;
    const WORKERS_AFTER: WorkerIndex = 90;

    let (chunks, mut workers, total_size) = generate_input(WORKERS_BEFORE, 50_000, &[1]);
    let total_capacity = 30 * total_size;
    let assignment1 = schedule(
        &chunks,
        &workers,
        SchedulingConfig {
            worker_capacity: total_capacity / WORKERS_BEFORE as u64,
            saturation: 0.99,
            min_replication: 1,
            ignore_reliability: false,
        },
    )
    .unwrap();

    workers.truncate(WORKERS_AFTER as usize);
    // Capacity grows to accomodate all the chunks
    let assignment2 = schedule(
        &chunks,
        &workers,
        SchedulingConfig {
            worker_capacity: total_capacity / WORKERS_AFTER as u64,
            saturation: 0.99,
            min_replication: 1,
            ignore_reliability: false,
        },
    )
    .unwrap();

    let compare_result = compare_intersection(&chunks, &assignment1, &assignment2);
    compare_result.display_stats("GB", 1 << 30);
    assert!(*compare_result.removed.values().max().unwrap() < 5 * (1 << 30));
}

#[test]
fn test_rescheduling_workers_became_reliable() {
    const WORKERS: WorkerIndex = 100;
    const UNRELIABLE: WorkerIndex = 30;

    let (chunks, workers, total_size) = generate_input(WORKERS, 50_000, &[1]);
    let workers_with_unreliable = {
        let mut workers = workers.clone();
        for i in 0..UNRELIABLE {
            workers[i as usize].status = WorkerStatus::Offline;
        }
        workers
    };

    let worker_capacity = 30 * total_size / WORKERS as u64;
    let config = SchedulingConfig {
        worker_capacity,
        saturation: 0.99,
        min_replication: 1,
        ignore_reliability: false,
    };

    let assignment1 = schedule(&chunks, &workers_with_unreliable, config.clone()).unwrap();
    let assignment2 = schedule(&chunks, &workers, config).unwrap();

    let is_reliable = workers_with_unreliable
        .into_iter()
        .map(|w| (w.id, w.reliable()))
        .collect::<BTreeMap<_, _>>();
    let (assignment1_reliable, assignment1_unreliable) =
        split_by_workers(assignment1, |worker| is_reliable[worker]);
    let (assignment2_reliable, assignment2_unreliable) =
        split_by_workers(assignment2, |worker| is_reliable[worker]);

    println!(
        "Reliable workers: {}",
        assignment1_reliable.worker_chunks.len()
    );
    let compare_result =
        compare_intersection(&chunks, &assignment1_reliable, &assignment2_reliable);
    compare_result.display_stats("GB", 1 << 30);
    assert!(
        compare_result
            .removed
            .values()
            .all(|size| (*size as f64) < 0.35 * worker_capacity as f64)
    );

    println!(
        "Unreliable workers: {}",
        assignment1_unreliable.worker_chunks.len()
    );
    let compare_result =
        compare_intersection(&chunks, &assignment1_unreliable, &assignment2_unreliable);
    compare_result.display_stats("GB", 1 << 30);

    assert!(compare_result.removed.values().all(|size| *size == 0));
    assert!(compare_result.added.values().all(|size| *size == 0));
}

fn split_by_workers(
    assignment: Assignment,
    mut predicate: impl FnMut(&PeerId) -> bool,
) -> (Assignment, Assignment) {
    let (first, second) = assignment
        .worker_chunks
        .into_iter()
        .partition(|(worker, _)| predicate(worker));
    (
        Assignment {
            worker_chunks: first,
            replication_by_weight: assignment.replication_by_weight.clone(),
        },
        Assignment {
            worker_chunks: second,
            replication_by_weight: assignment.replication_by_weight,
        },
    )
}

/// Verifies that version-restricted chunks are only assigned to eligible workers.
/// Scenario: 100 workers, 20% eligible, 99% saturation, ~4% of chunks version-restricted.
///
/// Two constraints must hold (both are stressed close to their limits):
/// 1. R ≤ E: replication factor must not exceed eligible worker count.
///    R = floor(C * N * s / S) = floor(20 * 0.99) = 19. E = 20 ≥ 19 ✓
/// 2. f ≤ 0.2 * E / (N * s): versioned fraction fits in per-worker caps.
///    f = 0.04 ≤ 0.2 * 20 / (100 * 0.99) = 0.0404 (99% utilized) ✓
/// Regular chunks are unrestricted and spill freely to ineligible workers
/// when eligible workers' remaining capacity is insufficient.
#[test]
fn test_minimum_worker_version_filtering() {
    let min_version = Version::new(2, 8, 0);
    const N_WORKERS: WorkerIndex = 100;
    const N_ELIGIBLE: WorkerIndex = 20;
    const N_CHUNKS: u32 = 50_000;
    const N_VERSIONED: u32 = 2_000; // ~4% of chunks

    let mut chunks = generate_chunks(N_CHUNKS, &[1]);
    for chunk in &mut chunks[..N_VERSIONED as usize] {
        chunk.minimum_worker_version = Some(min_version.clone());
    }

    let workers = generate_workers(N_WORKERS)
        .into_iter()
        .enumerate()
        .map(|(i, id)| Worker {
            id,
            status: WorkerStatus::Online,
            version: if (i as WorkerIndex) < N_ELIGIBLE {
                Some(Version::new(2, 8, 0))
            } else {
                Some(Version::new(2, 7, 0))
            },
        })
        .collect_vec();

    let eligible_ids = workers[..N_ELIGIBLE as usize]
        .iter()
        .map(|w| w.id)
        .collect::<std::collections::BTreeSet<_>>();

    let total_size: u64 = chunks.iter().map(|c| c.size as u64).sum();
    // C * N / S = 20, so R = floor(20 * 0.99) = 19
    let worker_capacity = 20 * total_size / N_WORKERS as u64;

    let assignment = schedule(
        &chunks,
        &workers,
        SchedulingConfig {
            worker_capacity,
            saturation: 0.99,
            min_replication: 1,
            ignore_reliability: false,
        },
    )
    .unwrap();

    // Versioned chunks must only be assigned to eligible workers
    for (worker_id, chunk_indexes) in &assignment.worker_chunks {
        if !eligible_ids.contains(worker_id) {
            let has_versioned = chunk_indexes.iter().any(|&i| (i as u32) < N_VERSIONED);
            assert!(
                !has_versioned,
                "Ineligible worker {:?} was assigned version-restricted chunks",
                worker_id,
            );
        }
    }

    // All eligible workers should have some versioned chunks
    for worker_id in &eligible_ids {
        let versioned_count = assignment
            .worker_chunks
            .get(worker_id)
            .map(|idxs| idxs.iter().filter(|&&i| (i as u32) < N_VERSIONED).count())
            .unwrap_or(0);
        assert!(
            versioned_count > 0,
            "Eligible worker {:?} has no versioned chunks",
            worker_id
        );
    }
}

/// Measures reassignment impact when workers gradually upgrade.
/// Scenario: 100 workers, 99% saturation, ~4% versioned chunks,
/// workers upgrade in batches: 20 -> 35 -> 50 -> 75 -> 100.
/// R = floor(20 * 0.99) = 19, so we need ≥ 19 eligible workers in each step.
///
/// At E=20 (first step), f = 0.04 ≈ 0.2 * 20 / (100 * 0.99) = 0.0404 (99% of limit).
/// When the pool grows from 20→35, each original worker loses up to ~43%
/// of its restricted data. With restricted data at ~19% of capacity,
/// per-worker reassignment is bounded by 0.43 * 0.19 ≈ 8% of capacity.
/// We use a 15% threshold to account for hash ring variance.
#[test]
fn test_minimum_worker_version_gradual_upgrade() {
    let min_version = Version::new(2, 8, 0);
    const N_WORKERS: WorkerIndex = 100;
    const N_CHUNKS: u32 = 50_000;
    const N_VERSIONED: u32 = 2_000; // ~4%

    let mut chunks = generate_chunks(N_CHUNKS, &[1]);
    for chunk in &mut chunks[..N_VERSIONED as usize] {
        chunk.minimum_worker_version = Some(min_version.clone());
    }

    let total_size: u64 = chunks.iter().map(|c| c.size as u64).sum();
    let worker_capacity = 20 * total_size / N_WORKERS as u64;
    let config = SchedulingConfig {
        worker_capacity,
        saturation: 0.99,
        min_replication: 1,
        ignore_reliability: false,
    };

    let worker_ids = generate_workers(N_WORKERS);

    let upgrade_steps: &[WorkerIndex] = &[20, 35, 50, 75, 100];
    let mut prev_assignment: Option<Assignment> = None;

    for &n_upgraded in upgrade_steps {
        let workers = worker_ids
            .iter()
            .enumerate()
            .map(|(i, &id)| Worker {
                id,
                status: WorkerStatus::Online,
                version: if (i as WorkerIndex) < n_upgraded {
                    Some(Version::new(2, 8, 0))
                } else {
                    Some(Version::new(2, 7, 0))
                },
            })
            .collect_vec();

        let assignment = schedule(&chunks, &workers, config.clone()).unwrap();

        if let Some(prev) = &prev_assignment {
            let compare_result = compare_intersection(&chunks, prev, &assignment);
            let max_removed = *compare_result.removed.values().max().unwrap();
            println!(
                "Upgrade to {} workers: max removed per worker: {:.2}% of capacity",
                n_upgraded,
                max_removed as f64 / worker_capacity as f64 * 100.0
            );
            assert!(
                compare_result
                    .removed
                    .values()
                    .all(|size| (*size as f64) < 0.15 * worker_capacity as f64),
                "Too much reassignment when upgrading to {} workers: max removed = {:.2}% of capacity (limit = 15%)",
                n_upgraded,
                max_removed as f64 / worker_capacity as f64 * 100.0,
            );
        }

        prev_assignment = Some(assignment);
    }
}

/// Verifies that scheduling panics when R > E (replication factor exceeds
/// eligible worker count), even though the per-worker capacity constraint
/// is satisfied.
/// Scenario: 100 workers, 18 eligible, 99% saturation, 2% versioned chunks.
///
/// R = floor(20 * 0.99) = 19 > E = 18. Panics (can't place 19 replicas on 18 workers).
/// f = 0.02 ≤ 0.2 * 18 / (100 * 0.99) = 0.036 ✓ (capacity would suffice).
#[test]
#[should_panic(expected = "No worker found for chunk")]
fn test_minimum_worker_version_insufficient_eligible_workers() {
    let min_version = Version::new(2, 8, 0);
    const N_WORKERS: WorkerIndex = 100;
    const N_ELIGIBLE: WorkerIndex = 18;
    const N_CHUNKS: u32 = 50_000;
    const N_VERSIONED: u32 = 1_000; // 2% of chunks

    let mut chunks = generate_chunks(N_CHUNKS, &[1]);
    for chunk in &mut chunks[..N_VERSIONED as usize] {
        chunk.minimum_worker_version = Some(min_version.clone());
    }

    let total_size: u64 = chunks.iter().map(|c| c.size as u64).sum();
    let worker_capacity = 20 * total_size / N_WORKERS as u64;

    let workers = generate_workers(N_WORKERS)
        .into_iter()
        .enumerate()
        .map(|(i, id)| Worker {
            id,
            status: WorkerStatus::Online,
            version: if (i as WorkerIndex) < N_ELIGIBLE {
                Some(Version::new(2, 8, 0))
            } else {
                Some(Version::new(2, 7, 0))
            },
        })
        .collect_vec();

    let _ = schedule(
        &chunks,
        &workers,
        SchedulingConfig {
            worker_capacity,
            saturation: 0.99,
            min_replication: 1,
            ignore_reliability: false,
        },
    );
}

/// Verifies that scheduling panics when versioned data exceeds the per-worker
/// version-restricted cap (even though R ≤ E and total eligible capacity
/// would suffice without the cap).
/// Scenario: 100 workers, 20 eligible, 99% saturation, 5% versioned chunks.
/// Same framework as test_minimum_worker_version_filtering but with versioned
/// fraction just above the threshold.
///
/// R = floor(20 * 0.99) = 19 ≤ 20 ✓
/// f = 0.05 > 0.2 * 20 / (100 * 0.99) = 0.0404 (~24% over limit). Panics.
#[test]
#[should_panic(expected = "No worker found for chunk")]
fn test_minimum_worker_version_eligible_capacity_exceeded() {
    let min_version = Version::new(2, 8, 0);
    const N_WORKERS: WorkerIndex = 100;
    const N_ELIGIBLE: WorkerIndex = 20;
    const N_CHUNKS: u32 = 50_000;
    const N_VERSIONED: u32 = 2_500; // 5% of chunks, just above the ~4.2% threshold

    let mut chunks = generate_chunks(N_CHUNKS, &[1]);
    for chunk in &mut chunks[..N_VERSIONED as usize] {
        chunk.minimum_worker_version = Some(min_version.clone());
    }

    let total_size: u64 = chunks.iter().map(|c| c.size as u64).sum();
    let worker_capacity = 20 * total_size / N_WORKERS as u64;

    let workers = generate_workers(N_WORKERS)
        .into_iter()
        .enumerate()
        .map(|(i, id)| Worker {
            id,
            status: WorkerStatus::Online,
            version: if (i as WorkerIndex) < N_ELIGIBLE {
                Some(Version::new(2, 8, 0))
            } else {
                Some(Version::new(2, 7, 0))
            },
        })
        .collect_vec();

    let _ = schedule(
        &chunks,
        &workers,
        SchedulingConfig {
            worker_capacity,
            saturation: 0.99,
            min_replication: 1,
            ignore_reliability: false,
        },
    );
}

fn ring_iter<'l, T: Ord>(v: &'l [T], from: &T) -> impl Iterator<Item = &'l T> {
    let i = v.partition_point(|x| x < from);
    v[i..].iter().chain(v[..i].iter())
}

#[test]
fn test_ring_iterator() {
    let v = vec![1, 3, 5, 7];

    assert_eq!(ring_iter(&v, &0).copied().collect_vec(), vec![1, 3, 5, 7]);
    assert_eq!(ring_iter(&v, &2).copied().collect_vec(), vec![3, 5, 7, 1]);
    assert_eq!(ring_iter(&v, &3).copied().collect_vec(), vec![3, 5, 7, 1]);
    assert_eq!(ring_iter(&v, &4).copied().collect_vec(), vec![5, 7, 1, 3]);
    assert_eq!(ring_iter(&v, &6).copied().collect_vec(), vec![7, 1, 3, 5]);
    assert_eq!(ring_iter(&v, &7).copied().collect_vec(), vec![7, 1, 3, 5]);
    assert_eq!(ring_iter(&v, &8).copied().collect_vec(), vec![1, 3, 5, 7]);
}
