use std::collections::BTreeMap;

use itertools::Itertools;
use libp2p_identity::PeerId;

use super::{
    input::generate_input,
    utils::{Stats, compare_intersection},
};
use crate::{
    scheduling::{SchedulingConfig, schedule},
    types::{Assignment, WorkerIndex, WorkerStatus},
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
