use itertools::Itertools;

use super::{
    input::generate_input,
    utils::{Stats, compare_intersection},
};
use crate::{
    scheduling::{SchedulingConfig, schedule},
    types::WorkerIndex,
};

#[test]
fn test_scheduling_stable() {
    let (mut chunks, mut workers, total_size) = generate_input(100, 100_000, &[1]);
    let capacity = (total_size as f64 / workers.len() as f64 * 30.) as u64;
    let config = SchedulingConfig {
        worker_capacity: capacity,
        saturation: 0.95,
        min_replication: 1,
    };

    let assignment1 = schedule(&chunks, &workers, config.clone()).unwrap();
    chunks.reverse();
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
        },
    )
    .unwrap();
    for chunks in assignment.workers.into_values() {
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
        },
    )
    .unwrap();

    let mut sizes: Vec<u64> = vec![0; workers.len()];
    for (worker_index, chunk_indexes) in assignment.workers.into_values().enumerate() {
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
        },
    )
    .unwrap();

    let compare_result = compare_intersection(&chunks, &assignment1, &assignment2);
    compare_result.display_stats("GB", 1 << 30);
    assert!(compare_result.removed.values().all(|size| *size == 0));
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
