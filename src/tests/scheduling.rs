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
    let unrestricted_chunks: Vec<ScheduledChunk> = chunks
        .iter()
        .map(|c| {
            let mut c = c.clone();
            c.minimum_worker_version = None;
            c
        })
        .collect();
    let baseline = schedule(&unrestricted_chunks, &workers, config).unwrap();

    // Build chunk -> set of workers for unrestricted chunks in both schedules
    let chunk_to_workers = |assignment: &Assignment| {
        let mut map: BTreeMap<u32, std::collections::BTreeSet<PeerId>> = BTreeMap::new();
        for (&worker_id, chunk_indexes) in &assignment.worker_chunks {
            for &idx in chunk_indexes {
                if idx as u32 >= N_VERSIONED {
                    map.entry(idx as u32).or_default().insert(worker_id);
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
        let idx = i as u32;
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

    let mut chunks = generate_chunks(N_CHUNKS, &[1]);
    for chunk in &mut chunks[..N_VERSIONED as usize] {
        chunk.minimum_worker_version = Some(min_version.clone());
    }

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

    let mut chunks = generate_chunks(N_CHUNKS, &[1]);
    for chunk in &mut chunks[..N_VERSIONED as usize] {
        chunk.minimum_worker_version = Some(min_version.clone());
    }

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

    let mut chunks = generate_chunks(N_CHUNKS, &[1]);
    for chunk in &mut chunks[..N_VERSIONED as usize] {
        chunk.minimum_worker_version = Some(min_version.clone());
    }

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
/// With all workers eligible, the version check is a no-op. The only difference
/// is the sort order, but since s=0.95 leaves ample headroom, no worker hits
/// capacity — so processing order doesn't affect which worker each chunk lands on.
#[test]
fn test_minimum_worker_version_no_reassignment_on_removal() {
    let min_version = Version::new(2, 8, 0);
    const N_WORKERS: WorkerIndex = 100;
    const N_CHUNKS: u32 = 50_000;
    const N_VERSIONED: u32 = 500; // 1%

    let mut chunks_with_ver = generate_chunks(N_CHUNKS, &[1]);
    for chunk in &mut chunks_with_ver[..N_VERSIONED as usize] {
        chunk.minimum_worker_version = Some(min_version.clone());
    }
    let mut chunks_without_ver = chunks_with_ver.clone();
    for chunk in &mut chunks_without_ver[..N_VERSIONED as usize] {
        chunk.minimum_worker_version = None;
    }

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
        total_removed, total_added,
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
