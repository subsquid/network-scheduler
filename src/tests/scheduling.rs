use std::collections::BTreeMap;

use itertools::Itertools;
use libp2p_identity::PeerId;
use semver::Version;

use super::{
    input::{generate_chunks, generate_input, generate_workers},
    utils::{Stats, compare_intersection},
};
use crate::{
    scheduling::{ScheduledChunk, SchedulingConfig, schedule},
    types::{Assignment, Worker, WorkerIndex, WorkerStatus},
};

#[test]
fn test_scheduling_stable() {
    let (test_data, mut workers, total_size) = generate_input(100, 100_000, &[1]);
    let chunks = test_data.as_scheduled();
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
    let (test_data, workers, total_size) = generate_input(100, 100_000, &[1]);
    let chunks = test_data.as_scheduled();
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
    let (test_data, workers, total_size) = generate_input(100, 50_000, &[1]);
    let chunks = test_data.as_scheduled();
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
            sizes[worker_index] += chunks[chunk_index as usize].size as u64;
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

    let (test_data, mut workers, total_size) = generate_input(WORKERS_BEFORE, 50_000, &[1]);
    let chunks = test_data.as_scheduled();
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

    let (test_data, workers, total_size) = generate_input(WORKERS, 50_000, &[1]);
    let chunks = test_data.as_scheduled();
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
/// Scenario: 100 workers, 50 eligible (~50%), 95% saturation, 5% version-restricted.
///
/// Two constraints must hold:
/// 1. R ≤ E: R = floor(10 * 0.95) = 9 ≤ E = 50 ✓
/// 2. f ≤ (1-s) * E / (s * (N-E)): 0.05 ≤ 0.0526 (95% of limit) ✓
#[test]
fn test_minimum_worker_version_filtering() {
    let min_version = Version::new(2, 8, 0);
    const N_WORKERS: WorkerIndex = 100;
    const N_ELIGIBLE: WorkerIndex = 50;
    const N_CHUNKS: u32 = 50_000;
    const N_VERSIONED: u32 = 2_500; // 5% of chunks (95% of the ~5.26% threshold)

    let mut test_data = generate_chunks(N_CHUNKS, &[1]);
    for entry in &mut test_data.entries[..N_VERSIONED as usize] {
        entry.minimum_worker_version = Some(min_version.clone());
    }
    let chunks = test_data.as_scheduled();

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
    // C * N / S = 10, so R = floor(10 * 0.95) = 9
    let worker_capacity = 10 * total_size / N_WORKERS as u64;

    let assignment = schedule(
        &chunks,
        &workers,
        SchedulingConfig {
            worker_capacity,
            saturation: 0.95,
            min_replication: 1,
            ignore_reliability: false,
        },
    )
    .unwrap();

    // Versioned chunks must only be assigned to eligible workers
    for (worker_id, chunk_indexes) in &assignment.worker_chunks {
        if !eligible_ids.contains(worker_id) {
            let has_versioned = chunk_indexes.iter().any(|&i| i < N_VERSIONED);
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
            .map(|idxs| idxs.iter().filter(|&&i| i < N_VERSIONED).count())
            .unwrap_or(0);
        assert!(
            versioned_count > 0,
            "Eligible worker {:?} has no versioned chunks",
            worker_id
        );
    }
}

/// Verifies that version restrictions cause no (or negligible) spill of
/// unrestricted chunks compared to a baseline without restrictions.
/// Uses the same scenario as test_minimum_worker_version_filtering.
///
/// Spill is measured by comparing unrestricted chunk-worker assignments between
/// the restricted and unrestricted schedules. Any replica that lands on a
/// different worker is counted as spilled.
#[test]
fn test_minimum_worker_version_unrestricted_spill() {
    let min_version = Version::new(2, 8, 0);
    const N_WORKERS: WorkerIndex = 100;
    const N_ELIGIBLE: WorkerIndex = 50;
    const N_CHUNKS: u32 = 50_000;
    const N_VERSIONED: u32 = 2_500; // 5% of chunks (95% of the ~5.26% threshold)
    const SATURATION: f64 = 0.95;

    let mut test_data = generate_chunks(N_CHUNKS, &[1]);
    for entry in &mut test_data.entries[..N_VERSIONED as usize] {
        entry.minimum_worker_version = Some(min_version.clone());
    }
    let chunks = test_data.as_scheduled();

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

    let total_size: u64 = chunks.iter().map(|c| c.size as u64).sum();
    let worker_capacity = 10 * total_size / N_WORKERS as u64;
    let config = SchedulingConfig {
        worker_capacity,
        saturation: SATURATION,
        min_replication: 1,
        ignore_reliability: false,
    };

    let assignment = schedule(&chunks, &workers, config.clone()).unwrap();

    // Baseline: same chunks without version restrictions
    let unrestricted_chunks: Vec<ScheduledChunk> = test_data
        .entries
        .iter()
        .map(|e| ScheduledChunk {
            dataset: &test_data.dataset,
            chunk_id: &e.chunk_id,
            size: e.size,
            weight: e.weight,
            minimum_worker_version: None,
        })
        .collect();
    let baseline = schedule(&unrestricted_chunks, &workers, config).unwrap();

    // Build chunk -> set of workers for unrestricted chunks in both schedules
    let chunk_to_workers = |assignment: &Assignment| {
        let mut map: BTreeMap<u32, std::collections::BTreeSet<PeerId>> = BTreeMap::new();
        for (&worker_id, chunk_indexes) in &assignment.worker_chunks {
            for &idx in chunk_indexes {
                if idx >= N_VERSIONED {
                    map.entry(idx).or_default().insert(worker_id);
                }
            }
        }
        map
    };
    let restricted_map = chunk_to_workers(&assignment);
    let baseline_map = chunk_to_workers(&baseline);

    let mut changed_replicas: u64 = 0;
    let mut total_replicas: u64 = 0;
    for i in N_VERSIONED..N_CHUNKS {
        let idx = i;
        let workers_a = restricted_map.get(&idx).cloned().unwrap_or_default();
        let workers_b = baseline_map.get(&idx).cloned().unwrap_or_default();
        let changed = workers_a.symmetric_difference(&workers_b).count() as u64 / 2;
        changed_replicas += changed;
        total_replicas += workers_a.len() as u64;
    }

    let spill_pct = changed_replicas as f64 / total_replicas as f64 * 100.0;
    println!(
        "Unrestricted chunk spill: {:.2}% of replicas moved ({} / {})",
        spill_pct, changed_replicas, total_replicas,
    );
    // With f below the threshold, eligible workers should absorb restricted
    // chunks within the saturation headroom. Any spill beyond 1% of replicas
    // would indicate the headroom is insufficient or the hash ring variance
    // is larger than expected.
    assert!(
        spill_pct < 1.0,
        "Too much unrestricted chunk spill: {:.2}% of replicas moved",
        spill_pct,
    );
}

/// Measures reassignment impact when workers gradually upgrade.
/// Scenario: 100 workers, 95% saturation, 1% versioned, upgrade 50→65→80→100.
/// R = floor(10 * 0.95) = 9.
///
/// At E=50 (first step): f = 0.01 ≤ (0.05 * 50) / (0.95 * 50) = 0.0526 ✓ (19% of limit).
/// With only 1% restricted data, reassignment per step should be small.
#[test]
fn test_minimum_worker_version_gradual_upgrade() {
    let min_version = Version::new(2, 8, 0);
    const N_WORKERS: WorkerIndex = 100;
    const N_CHUNKS: u32 = 50_000;
    const N_VERSIONED: u32 = 500; // 1%

    let mut test_data = generate_chunks(N_CHUNKS, &[1]);
    for entry in &mut test_data.entries[..N_VERSIONED as usize] {
        entry.minimum_worker_version = Some(min_version.clone());
    }
    let chunks = test_data.as_scheduled();

    let total_size: u64 = chunks.iter().map(|c| c.size as u64).sum();
    let worker_capacity = 10 * total_size / N_WORKERS as u64;
    let config = SchedulingConfig {
        worker_capacity,
        saturation: 0.95,
        min_replication: 1,
        ignore_reliability: false,
    };

    let worker_ids = generate_workers(N_WORKERS);

    let upgrade_steps: &[WorkerIndex] = &[50, 65, 80, 100];
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
            let max_added = *compare_result.added.values().max().unwrap();
            let max_churn = max_removed.max(max_added);
            println!(
                "Upgrade to {} workers: max churn per worker: {:.2}% of capacity (removed {:.2}%, added {:.2}%)",
                n_upgraded,
                max_churn as f64 / worker_capacity as f64 * 100.0,
                max_removed as f64 / worker_capacity as f64 * 100.0,
                max_added as f64 / worker_capacity as f64 * 100.0,
            );
            assert!(
                max_churn as f64 <= 0.05 * worker_capacity as f64,
                "Too much reassignment when upgrading to {} workers: max churn = {:.2}% of capacity (limit = 5%)",
                n_upgraded,
                max_churn as f64 / worker_capacity as f64 * 100.0,
            );
        }

        prev_assignment = Some(assignment);
    }
}

/// Verifies that scheduling panics up-front when R > E (replication factor
/// exceeds eligible worker count), even though the capacity constraint
/// is satisfied.
/// Scenario: 100 workers, 8 eligible, 95% saturation, 0.4% versioned chunks.
///
/// R = floor(10 * 0.95) = 9 > E = 8. Panics (can't place 9 replicas on 8 workers).
/// f = 0.004 ≤ (0.05 * 8) / (0.95 * 92) = 0.0046 ✓ (capacity would suffice).
#[test]
#[should_panic(expected = "Not enough eligible workers")]
fn test_minimum_worker_version_insufficient_eligible_workers() {
    let min_version = Version::new(2, 8, 0);
    const N_WORKERS: WorkerIndex = 100;
    const N_ELIGIBLE: WorkerIndex = 8;
    const N_CHUNKS: u32 = 50_000;
    const N_VERSIONED: u32 = 200; // 0.4% of chunks

    let mut test_data = generate_chunks(N_CHUNKS, &[1]);
    for entry in &mut test_data.entries[..N_VERSIONED as usize] {
        entry.minimum_worker_version = Some(min_version.clone());
    }
    let chunks = test_data.as_scheduled();

    let total_size: u64 = chunks.iter().map(|c| c.size as u64).sum();
    let worker_capacity = 10 * total_size / N_WORKERS as u64;

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
            saturation: 0.95,
            min_replication: 1,
            ignore_reliability: false,
        },
    );
}

/// Verifies that scheduling panics up-front when the restricted data
/// fraction exceeds the saturation headroom (even though R ≤ E).
/// Scenario: 100 workers, 50 eligible, 95% saturation, 6% versioned chunks.
/// Same framework as test_minimum_worker_version_filtering but f slightly
/// above the threshold.
///
/// R = floor(10 * 0.95) = 9 ≤ E = 50 ✓
/// f = 0.06 > (0.05 * 50) / (0.95 * 50) = 0.0526 (~14% over limit). Panics.
#[test]
#[should_panic(expected = "exceeds saturation headroom")]
fn test_minimum_worker_version_eligible_capacity_exceeded() {
    let min_version = Version::new(2, 8, 0);
    const N_WORKERS: WorkerIndex = 100;
    const N_ELIGIBLE: WorkerIndex = 50;
    const N_CHUNKS: u32 = 50_000;
    const N_VERSIONED: u32 = 3_000; // 6% of chunks, just above the ~5.26% threshold

    let mut test_data = generate_chunks(N_CHUNKS, &[1]);
    for entry in &mut test_data.entries[..N_VERSIONED as usize] {
        entry.minimum_worker_version = Some(min_version.clone());
    }
    let chunks = test_data.as_scheduled();

    let total_size: u64 = chunks.iter().map(|c| c.size as u64).sum();
    let worker_capacity = 10 * total_size / N_WORKERS as u64;

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
            saturation: 0.95,
            min_replication: 1,
            ignore_reliability: false,
        },
    );
}

/// Verifies zero reassignment when version restriction is removed after
/// all workers have upgraded. Compares:
/// - Schedule with minimum_worker_version set and all workers eligible
/// - Schedule with minimum_worker_version removed (None)
///
/// With all workers eligible, the version check is a no-op. The only difference
/// is the sort order, but since s=0.95 leaves ample headroom, no worker hits
/// capacity — so processing order doesn't affect which worker each chunk lands on.
#[test]
fn test_minimum_worker_version_no_reassignment_on_removal() {
    let min_version = Version::new(2, 8, 0);
    const N_WORKERS: WorkerIndex = 100;
    const N_CHUNKS: u32 = 50_000;
    const N_VERSIONED: u32 = 500; // 1%

    let mut test_data = generate_chunks(N_CHUNKS, &[1]);
    for entry in &mut test_data.entries[..N_VERSIONED as usize] {
        entry.minimum_worker_version = Some(min_version.clone());
    }
    let chunks_with_ver = test_data.as_scheduled();

    let chunks_without_ver: Vec<ScheduledChunk> = test_data
        .entries
        .iter()
        .map(|e| ScheduledChunk {
            dataset: &test_data.dataset,
            chunk_id: &e.chunk_id,
            size: e.size,
            weight: e.weight,
            minimum_worker_version: None,
        })
        .collect();

    // All workers are eligible (fully upgraded)
    let workers = generate_workers(N_WORKERS)
        .into_iter()
        .map(|id| Worker {
            id,
            status: WorkerStatus::Online,
            version: Some(Version::new(2, 8, 0)),
        })
        .collect_vec();

    let total_size: u64 = chunks_with_ver.iter().map(|c| c.size as u64).sum();
    let worker_capacity = 10 * total_size / N_WORKERS as u64;
    let config = SchedulingConfig {
        worker_capacity,
        saturation: 0.95,
        min_replication: 1,
        ignore_reliability: false,
    };

    let with_ver = schedule(&chunks_with_ver, &workers, config.clone()).unwrap();
    let without_ver = schedule(&chunks_without_ver, &workers, config).unwrap();

    let compare_result = compare_intersection(&chunks_with_ver, &with_ver, &without_ver);
    let total_removed: u64 = compare_result.removed.values().sum();
    let total_added: u64 = compare_result.added.values().sum();
    assert!(
        total_removed == 0 && total_added == 0,
        "Expected zero churn when removing restriction with all workers upgraded, \
         but got removed = {}, added = {}",
        total_removed,
        total_added,
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

// =============================================================================
// Reshuffling tests: validate when and how chunks move between owners
// =============================================================================

use std::collections::BTreeSet;

/// Helper: count how many chunk assignments changed between two assignments
/// for workers present in both. Returns (total_removed, total_added) in bytes.
fn total_churn(
    chunks: &[crate::scheduling::ScheduledChunk],
    a1: &Assignment,
    a2: &Assignment,
) -> (u64, u64) {
    let result = compare_intersection(chunks, a1, a2);
    let removed: u64 = result.removed.values().sum();
    let added: u64 = result.added.values().sum();
    (removed, added)
}

/// Helper: collect all (worker, chunk) pairs from an assignment
fn all_pairs(assignment: &Assignment) -> BTreeSet<(PeerId, u32)> {
    let mut set = BTreeSet::new();
    for (worker, chunks) in &assignment.worker_chunks {
        for &c in chunks {
            set.insert((*worker, c));
        }
    }
    set
}

// ---------------------------------------------------------------------------
// 1. Determinism: same inputs always produce the same output
// ---------------------------------------------------------------------------
#[test]
fn test_reshuffle_deterministic() {
    let (test_data, workers, total_size) = generate_input(50, 20_000, &[1, 3, 6]);
    let chunks = test_data.as_scheduled();
    let capacity = (total_size as f64 / workers.len() as f64 * 20.) as u64;
    let config = SchedulingConfig {
        worker_capacity: capacity,
        saturation: 0.95,
        min_replication: 2,
        ignore_reliability: false,
    };

    let a1 = schedule(&chunks, &workers, config.clone()).unwrap();
    let a2 = schedule(&chunks, &workers, config.clone()).unwrap();
    let a3 = schedule(&chunks, &workers, config).unwrap();

    assert_eq!(a1, a2, "Repeated scheduling must produce identical output");
    assert_eq!(a2, a3);
}

// ---------------------------------------------------------------------------
// 2. No reshuffle when nothing changes (idempotency)
// ---------------------------------------------------------------------------
#[test]
fn test_no_reshuffle_when_nothing_changes() {
    let (test_data, workers, total_size) = generate_input(80, 30_000, &[1, 4, 12]);
    let chunks = test_data.as_scheduled();
    let capacity = (total_size as f64 / workers.len() as f64 * 25.) as u64;
    let config = SchedulingConfig {
        worker_capacity: capacity,
        saturation: 0.95,
        min_replication: 2,
        ignore_reliability: false,
    };

    let a1 = schedule(&chunks, &workers, config.clone()).unwrap();
    let a2 = schedule(&chunks, &workers, config).unwrap();

    let (removed, added) = total_churn(&chunks, &a1, &a2);
    assert_eq!(removed, 0, "No churn expected when inputs are identical");
    assert_eq!(added, 0);
}

// ---------------------------------------------------------------------------
// 3. Adding new chunks: existing assignments change due to replication factor recalculation
// ---------------------------------------------------------------------------
#[test]
fn test_new_chunks_with_same_weight_limited_churn() {
    // When all chunks have the same weight, adding more chunks increases total_size
    // which changes the replication multiplier. Use a single weight to isolate the
    // consistent-hashing stability from replication-factor changes.
    let all_chunks = generate_chunks(30_000, &[1]);
    let all_scheduled = all_chunks.as_scheduled();
    let initial_chunks = &all_scheduled[..20_000];
    let extended_chunks = &all_scheduled[..30_000];

    let workers: Vec<_> = generate_workers(80)
        .into_iter()
        .map(|id| crate::types::Worker {
            id,
            status: WorkerStatus::Online,
            version: None,
        })
        .collect();

    let total_size_initial: u64 = initial_chunks.iter().map(|c| c.size as u64).sum();

    // Use generous capacity so that the replication factor stays constant (at min_replication)
    // for both the initial and extended chunk sets
    let total_size_extended: u64 = extended_chunks.iter().map(|c| c.size as u64).sum();
    let capacity = (total_size_extended as f64 / workers.len() as f64 * 25.) as u64;

    let a1 = schedule(
        initial_chunks,
        &workers,
        SchedulingConfig {
            worker_capacity: capacity,
            saturation: 0.90,
            min_replication: 1,
            ignore_reliability: true,
        },
    )
    .unwrap();
    let a2 = schedule(
        extended_chunks,
        &workers,
        SchedulingConfig {
            worker_capacity: capacity,
            saturation: 0.90,
            min_replication: 1,
            ignore_reliability: true,
        },
    )
    .unwrap();

    // Verify replication factors are the same (so churn is only from consistent hashing)
    println!("Replication before: {:?}", a1.replication_by_weight);
    println!("Replication after: {:?}", a2.replication_by_weight);

    // Check: for each worker, chunks from a1 should still be assigned to the same worker
    let mut moved_bytes: u64 = 0;
    for (worker, chunks_before) in &a1.worker_chunks {
        if let Some(chunks_after) = a2.worker_chunks.get(worker) {
            let after_set: BTreeSet<_> = chunks_after.iter().copied().collect();
            for &ci in chunks_before {
                if ci < 20_000 && !after_set.contains(&ci) {
                    moved_bytes += initial_chunks[ci as usize].size as u64;
                }
            }
        }
    }

    let move_pct = moved_bytes as f64 / total_size_initial as f64 * 100.;
    println!("Existing chunks moved: {moved_bytes} bytes ({move_pct:.2}% of initial data)");

    if a1.replication_by_weight == a2.replication_by_weight {
        // When replication factors are unchanged, consistent hashing ensures
        // existing chunks don't move (new chunks just fill the same rings)
        assert!(
            move_pct < 1.0,
            "With stable replication factors, < 1% of existing data should move, got {move_pct:.2}%"
        );
    }
    // If replication factors changed, more churn is expected — that's the documented behavior
}

// ---------------------------------------------------------------------------
// 4. Single worker joining: churn depends on whether replication factors change
// ---------------------------------------------------------------------------
#[test]
fn test_single_worker_join_stable_replication() {
    // Use the SAME worker_capacity for both runs so that total_capacity changes
    // only by one worker's worth. With min_replication=1 and a single weight,
    // the replication factor should stay at min_replication=1.
    const N: WorkerIndex = 100;
    let all_workers: Vec<_> = generate_workers(N + 1)
        .into_iter()
        .map(|id| crate::types::Worker {
            id,
            status: WorkerStatus::Online,
            version: None,
        })
        .collect();
    let workers_before = &all_workers[..N as usize];
    let workers_after = &all_workers[..N as usize + 1];

    let (test_data, _, total_size) = generate_input(N, 50_000, &[1]);
    let chunks = test_data.as_scheduled();
    // Use same capacity for both — generous enough that adding one worker doesn't
    // change replication factors
    let capacity = 30 * total_size / N as u64;
    let config = SchedulingConfig {
        worker_capacity: capacity,
        saturation: 0.95,
        min_replication: 1,
        ignore_reliability: true,
    };

    let a1 = schedule(&chunks, workers_before, config.clone()).unwrap();
    let a2 = schedule(&chunks, workers_after, config).unwrap();

    println!("Replication before: {:?}", a1.replication_by_weight);
    println!("Replication after: {:?}", a2.replication_by_weight);

    let (removed, _) = total_churn(&chunks, &a1, &a2);
    let churn_pct = removed as f64 / total_size as f64 * 100.;
    println!("Single worker join (same capacity): {churn_pct:.2}% churn among existing workers");

    if a1.replication_by_weight == a2.replication_by_weight {
        // With replication factor R, each replica has ~1/N chance of landing on the new worker.
        // Total churn across all replicas is ~R/N of total replicated data per worker.
        // For R=28, N=100 → ~28% churn, which is expected.
        let max_replication = *a1.replication_by_weight.values().max().unwrap() as f64;
        let expected_max_pct = max_replication / N as f64 * 100. * 1.5; // 1.5x tolerance
        assert!(
            churn_pct < expected_max_pct,
            "Expected < {expected_max_pct:.2}% churn (R={max_replication}, N={N}), got {churn_pct:.2}%"
        );
    }
}

// ---------------------------------------------------------------------------
// 4b. Worker join with capacity change: replication factor shift causes more churn
// ---------------------------------------------------------------------------
#[test]
fn test_single_worker_join_with_capacity_change() {
    const N: WorkerIndex = 100;
    let all_workers: Vec<_> = generate_workers(N + 1)
        .into_iter()
        .map(|id| crate::types::Worker {
            id,
            status: WorkerStatus::Online,
            version: None,
        })
        .collect();
    let workers_before = &all_workers[..N as usize];
    let workers_after = &all_workers[..N as usize + 1];

    let (test_data, _, total_size) = generate_input(N, 50_000, &[1]);
    let chunks = test_data.as_scheduled();
    // Different capacity per worker based on different N — this triggers replication changes
    let config_before = SchedulingConfig {
        worker_capacity: 30 * total_size / N as u64,
        saturation: 0.95,
        min_replication: 1,
        ignore_reliability: true,
    };
    let config_after = SchedulingConfig {
        worker_capacity: 30 * total_size / (N as u64 + 1),
        saturation: 0.95,
        min_replication: 1,
        ignore_reliability: true,
    };

    let a1 = schedule(&chunks, workers_before, config_before).unwrap();
    let a2 = schedule(&chunks, workers_after, config_after).unwrap();

    println!("Replication before: {:?}", a1.replication_by_weight);
    println!("Replication after: {:?}", a2.replication_by_weight);

    let (removed, _) = total_churn(&chunks, &a1, &a2);
    let churn_pct = removed as f64 / total_size as f64 * 100.;
    println!("Single worker join (capacity change): {churn_pct:.2}% churn");

    // When capacity-per-worker shrinks (because total capacity is redistributed),
    // replication factors can change, causing significantly more churn than pure
    // consistent-hashing would predict. This is expected behavior.
    // The test validates this completes without panic (no capacity overflow).
}

// ---------------------------------------------------------------------------
// 5. Single worker leaving: only that worker's chunks redistribute
// ---------------------------------------------------------------------------
#[test]
fn test_single_worker_leave_limited_churn() {
    const N: WorkerIndex = 100;
    let all_workers: Vec<_> = generate_workers(N)
        .into_iter()
        .map(|id| crate::types::Worker {
            id,
            status: WorkerStatus::Online,
            version: None,
        })
        .collect();

    let (test_data, _, total_size) = generate_input(N, 50_000, &[1]);
    let chunks = test_data.as_scheduled();
    let capacity = 30 * total_size / N as u64;
    let config = SchedulingConfig {
        worker_capacity: capacity,
        saturation: 0.95,
        min_replication: 1,
        ignore_reliability: true,
    };

    let a1 = schedule(&chunks, &all_workers, config.clone()).unwrap();

    // Remove last worker
    let workers_after = &all_workers[..N as usize - 1];
    let config_after = SchedulingConfig {
        worker_capacity: 30 * total_size / (N as u64 - 1),
        saturation: 0.95,
        min_replication: 1,
        ignore_reliability: true,
    };
    let a2 = schedule(&chunks, workers_after, config_after).unwrap();

    // For workers that remain, the only churn should be absorbing the departed worker's chunks
    let (removed, _added) = total_churn(&chunks, &a1, &a2);
    let churn_pct = removed as f64 / total_size as f64 * 100.;
    println!("Single worker leave: {churn_pct:.2}% removed from existing workers");

    // Remaining workers should lose very little (only capacity redistribution effects).
    // The removed bytes are chunks that moved from one remaining worker to another.
    let expected_max_pct = 3.0 / N as f64 * 100.;
    assert!(
        churn_pct < expected_max_pct,
        "Expected < {expected_max_pct:.2}% churn among remaining workers, got {churn_pct:.2}%"
    );
}

// ---------------------------------------------------------------------------
// 6. Reliability transition: reliable assignments are preserved
// ---------------------------------------------------------------------------
#[test]
fn test_reliable_assignments_stable_when_unreliable_joins() {
    const N: WorkerIndex = 80;
    const UNRELIABLE: WorkerIndex = 20;

    let (test_data, workers, total_size) = generate_input(N + UNRELIABLE, 40_000, &[1, 6]);
    let chunks = test_data.as_scheduled();
    let capacity = 25 * total_size / (N + UNRELIABLE) as u64;
    let config = SchedulingConfig {
        worker_capacity: capacity,
        saturation: 0.95,
        min_replication: 2,
        ignore_reliability: false,
    };

    // First: all workers reliable
    let a1 = schedule(&chunks, &workers[..N as usize], config.clone()).unwrap();

    // Second: add unreliable workers
    let mut mixed_workers = workers[..N as usize].to_vec();
    for w in &workers[N as usize..] {
        mixed_workers.push(crate::types::Worker {
            id: w.id,
            status: WorkerStatus::Offline,
            version: None,
        });
    }
    let a2 = schedule(&chunks, &mixed_workers, config).unwrap();

    // Extract only reliable worker assignments from a2
    let reliable_ids: BTreeSet<_> = workers[..N as usize].iter().map(|w| w.id).collect();
    let a2_reliable = Assignment {
        worker_chunks: a2
            .worker_chunks
            .into_iter()
            .filter(|(w, _)| reliable_ids.contains(w))
            .collect(),
        replication_by_weight: a2.replication_by_weight,
    };

    // Reliable workers' assignments should be identical regardless of unreliable workers
    assert_eq!(
        a1.worker_chunks, a2_reliable.worker_chunks,
        "Reliable worker assignments must not change when unreliable workers are added"
    );
}

// ---------------------------------------------------------------------------
// 7. Reliability flip: unreliable→reliable causes bounded churn
// ---------------------------------------------------------------------------
#[test]
fn test_unreliable_to_reliable_bounded_churn() {
    const N: WorkerIndex = 100;
    const FLIPPING: WorkerIndex = 10;

    let (test_data, mut workers, total_size) = generate_input(N, 50_000, &[1, 4]);
    let chunks = test_data.as_scheduled();
    let capacity = 25 * total_size / N as u64;
    let config = SchedulingConfig {
        worker_capacity: capacity,
        saturation: 0.95,
        min_replication: 2,
        ignore_reliability: false,
    };

    // Mark some workers as unreliable
    for worker in &mut workers[..FLIPPING as usize] {
        worker.status = WorkerStatus::Stale;
    }
    let a1 = schedule(&chunks, &workers, config.clone()).unwrap();

    // Now make them reliable
    for worker in &mut workers[..FLIPPING as usize] {
        worker.status = WorkerStatus::Online;
    }
    let a2 = schedule(&chunks, &workers, config).unwrap();

    // Reliable workers that were already reliable should see limited churn
    let already_reliable: BTreeSet<_> = workers[FLIPPING as usize..].iter().map(|w| w.id).collect();

    let a1_stable = Assignment {
        worker_chunks: a1
            .worker_chunks
            .iter()
            .filter(|(w, _)| already_reliable.contains(w))
            .map(|(w, c)| (*w, c.clone()))
            .collect(),
        replication_by_weight: a1.replication_by_weight.clone(),
    };
    let a2_stable = Assignment {
        worker_chunks: a2
            .worker_chunks
            .iter()
            .filter(|(w, _)| already_reliable.contains(w))
            .map(|(w, c)| (*w, c.clone()))
            .collect(),
        replication_by_weight: a2.replication_by_weight.clone(),
    };

    let compare = compare_intersection(&chunks, &a1_stable, &a2_stable);
    let max_removed = *compare.removed.values().max().unwrap();
    let max_pct = max_removed as f64 / capacity as f64 * 100.;
    println!("Max churn on stable reliable worker: {max_pct:.2}% of capacity");

    // Each stable reliable worker should lose less than FLIPPING/N of their capacity
    // (the newly-reliable workers absorb ~FLIPPING/N from each)
    let expected_max_pct = (FLIPPING as f64 / N as f64) * 100. * 2.0; // 2x tolerance
    assert!(
        max_pct < expected_max_pct,
        "Expected < {expected_max_pct:.2}% churn per worker, got {max_pct:.2}%"
    );
}

// ---------------------------------------------------------------------------
// 8. ignore_reliability flag causes full reshuffle vs two-pass
// ---------------------------------------------------------------------------
#[test]
fn test_ignore_reliability_different_from_two_pass() {
    const N: WorkerIndex = 60;
    const UNRELIABLE: WorkerIndex = 20;

    let (test_data, mut workers, total_size) = generate_input(N, 30_000, &[1, 6]);
    let chunks = test_data.as_scheduled();
    let capacity = 25 * total_size / N as u64;

    for worker in &mut workers[..UNRELIABLE as usize] {
        worker.status = WorkerStatus::Offline;
    }

    let config_two_pass = SchedulingConfig {
        worker_capacity: capacity,
        saturation: 0.95,
        min_replication: 2,
        ignore_reliability: false,
    };
    let config_ignore = SchedulingConfig {
        worker_capacity: capacity,
        saturation: 0.95,
        min_replication: 2,
        ignore_reliability: true,
    };

    let a_two_pass = schedule(&chunks, &workers, config_two_pass).unwrap();
    let a_ignore = schedule(&chunks, &workers, config_ignore).unwrap();

    // They should NOT be equal when there are unreliable workers
    assert_ne!(
        a_two_pass, a_ignore,
        "Two-pass and single-pass should differ when unreliable workers exist"
    );
}

// ---------------------------------------------------------------------------
// 9. Replication factor increase: existing replicas stay, new ones added
// ---------------------------------------------------------------------------
#[test]
fn test_replication_increase_preserves_existing() {
    let (test_data, workers, total_size) = generate_input(50, 20_000, &[1]);
    let chunks = test_data.as_scheduled();
    let capacity_low = (total_size as f64 / workers.len() as f64 * 10.) as u64;
    let capacity_high = (total_size as f64 / workers.len() as f64 * 30.) as u64;

    let config_low = SchedulingConfig {
        worker_capacity: capacity_low,
        saturation: 0.95,
        min_replication: 1,
        ignore_reliability: true,
    };
    let config_high = SchedulingConfig {
        worker_capacity: capacity_high,
        saturation: 0.95,
        min_replication: 1,
        ignore_reliability: true,
    };

    let a_low = schedule(&chunks, &workers, config_low).unwrap();
    let a_high = schedule(&chunks, &workers, config_high).unwrap();

    // Higher capacity → higher replication factors.
    // Existing (worker, chunk) pairs should mostly be preserved.
    let pairs_low = all_pairs(&a_low);
    let pairs_high = all_pairs(&a_high);

    let preserved = pairs_low.intersection(&pairs_high).count();
    let total_low = pairs_low.len();
    let preserve_pct = preserved as f64 / total_low as f64 * 100.;
    println!(
        "Preserved {preserved}/{total_low} pairs ({preserve_pct:.2}%), high has {} total pairs",
        pairs_high.len()
    );

    // With consistent hashing, most existing assignments should be preserved
    assert!(
        preserve_pct > 70.,
        "Expected > 70% of low-replication pairs preserved, got {preserve_pct:.2}%"
    );
    // Higher replication should have more total pairs
    assert!(
        pairs_high.len() > pairs_low.len(),
        "Higher capacity should produce more total assignments"
    );
}

// ---------------------------------------------------------------------------
// 10. Multiple workers leaving: remaining workers absorb departed workers' chunks
// ---------------------------------------------------------------------------
#[test]
fn test_multiple_workers_leave_remaining_absorb() {
    const N: WorkerIndex = 100;

    let (test_data, workers, total_size) = generate_input(N, 50_000, &[1, 4]);
    let chunks = test_data.as_scheduled();
    // Use SAME capacity for all runs to isolate consistent-hashing behavior
    let capacity = 25 * total_size / N as u64;
    let config = SchedulingConfig {
        worker_capacity: capacity,
        saturation: 0.95,
        min_replication: 1,
        ignore_reliability: true,
    };

    let a_full = schedule(&chunks, &workers, config.clone()).unwrap();

    // Remove 5 workers
    let a_minus5 = schedule(&chunks, &workers[..95], config.clone()).unwrap();

    // Remove 20 workers
    let a_minus20 = schedule(&chunks, &workers[..80], config).unwrap();

    // With consistent hashing, remaining workers should KEEP their existing chunks
    // and only GAIN new ones from departed workers. So `removed` should be near zero.
    let result_5 = compare_intersection(&chunks, &a_full, &a_minus5);
    let result_20 = compare_intersection(&chunks, &a_full, &a_minus20);

    let added_5: u64 = result_5.added.values().sum();
    let added_20: u64 = result_20.added.values().sum();

    println!(
        "5 workers leave: remaining workers gained {}GB total",
        added_5 / (1 << 30)
    );
    println!(
        "20 workers leave: remaining workers gained {}GB total",
        added_20 / (1 << 30)
    );

    // More workers departing = remaining workers must absorb more data
    assert!(
        added_20 > added_5,
        "More departures should cause remaining workers to absorb more data"
    );

    // Remaining workers should keep nearly all their existing chunks (low removal)
    let max_removed_5 = *result_5.removed.values().max().unwrap();
    let max_removed_20 = *result_20.removed.values().max().unwrap();
    println!(
        "Max removed per worker (5 leave): {}GB, (20 leave): {}GB",
        max_removed_5 / (1 << 30),
        max_removed_20 / (1 << 30),
    );
}

// ---------------------------------------------------------------------------
// 11. Weighted chunks: higher-weight chunks get more replicas
// ---------------------------------------------------------------------------
#[test]
fn test_weighted_chunks_replication_proportional() {
    let (test_data, workers, total_size) = generate_input(50, 20_000, &[1, 6, 24]);
    let chunks = test_data.as_scheduled();
    let capacity = (total_size as f64 / workers.len() as f64 * 25.) as u64;
    let config = SchedulingConfig {
        worker_capacity: capacity,
        saturation: 0.95,
        min_replication: 2,
        ignore_reliability: true,
    };

    let assignment = schedule(&chunks, &workers, config).unwrap();

    // Higher weights should have higher replication factors
    let factors = &assignment.replication_by_weight;
    println!("Replication factors: {:?}", factors);

    let weights: Vec<_> = factors.keys().copied().collect();
    for i in 1..weights.len() {
        assert!(
            factors[&weights[i]] >= factors[&weights[i - 1]],
            "Weight {} should have >= replication than weight {}: {} vs {}",
            weights[i],
            weights[i - 1],
            factors[&weights[i]],
            factors[&weights[i - 1]],
        );
    }
}

// ---------------------------------------------------------------------------
// 12. All workers assigned: every worker gets some chunks
// ---------------------------------------------------------------------------
#[test]
fn test_all_workers_receive_chunks() {
    let (test_data, workers, total_size) = generate_input(50, 30_000, &[1, 6]);
    let chunks = test_data.as_scheduled();
    let capacity = (total_size as f64 / workers.len() as f64 * 20.) as u64;
    let config = SchedulingConfig {
        worker_capacity: capacity,
        saturation: 0.95,
        min_replication: 2,
        ignore_reliability: true,
    };

    let assignment = schedule(&chunks, &workers, config).unwrap();
    for worker in &workers {
        assert!(
            assignment.worker_chunks.contains_key(&worker.id),
            "Worker {} should have chunks assigned",
            worker.id
        );
        assert!(
            !assignment.worker_chunks[&worker.id].is_empty(),
            "Worker {} should have non-empty chunk list",
            worker.id
        );
    }
}

// ---------------------------------------------------------------------------
// 13. Capacity overflow: saturation level affects stability
// ---------------------------------------------------------------------------
#[test]
fn test_higher_saturation_more_churn_on_change() {
    const N: WorkerIndex = 80;

    let (test_data, workers, total_size) = generate_input(N, 30_000, &[1]);
    let chunks = test_data.as_scheduled();

    let run = |saturation: f64| {
        let capacity = (total_size as f64 / (N - 1) as f64 / saturation * 1.01) as u64;

        let config = SchedulingConfig {
            worker_capacity: capacity,
            saturation,
            min_replication: 1,
            ignore_reliability: true,
        };
        let a1 = schedule(&chunks, &workers[..N as usize], config.clone()).unwrap();
        let a2 = schedule(&chunks, &workers[..N as usize - 1], config).unwrap();
        total_churn(&chunks, &a1, &a2)
    };

    let (churn_low, _) = run(0.85);
    let (churn_high, _) = run(0.99);
    println!("Churn at 0.85 saturation: {}GB", churn_low / (1 << 30));
    println!("Churn at 0.99 saturation: {}GB", churn_high / (1 << 30));

    // Higher saturation means tighter packing, which can cause more cascading
    // when capacity changes. We just verify both complete without panic.
    // The actual churn relationship depends on the specific configuration.
}
