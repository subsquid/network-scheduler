//! Scenario measurements: print before/after reshuffling statistics for representative cluster
//! changes. Assertions are loose; the printed output documents scheduler behaviour.

use std::collections::{BTreeMap, BTreeSet};

use bytesize::ByteSize;
use maplit::btreemap;

use super::input::{TestChunks, generate_chunks_fixed_size, generate_workers};
use crate::{
    scheduling::{SchedulingConfig, schedule},
    types::{Assignment, WorkerIndex, WorkerStatus},
};

/// 600 GB per worker × 2,000 workers × 0.95 saturation = ~1,140 TB total capacity.
const SCENARIO_WORKER_CAPACITY: ByteSize = ByteSize::gib(600);

const SCENARIO_CHUNK_SIZE: ByteSize = ByteSize::mib(100);

const SCENARIO_WORKERS: u16 = 2_000;

const SCENARIO_CHUNKS: u32 = 500_000;

fn generate_scenario_input(
    n_workers: WorkerIndex,
    n_chunks: u32,
    weights: &[u16],
) -> (TestChunks, Vec<crate::types::Worker>, ByteSize) {
    println!("Generating scenario input");
    let chunks = generate_chunks_fixed_size(n_chunks, weights, SCENARIO_CHUNK_SIZE);
    let workers: Vec<_> = generate_workers(n_workers)
        .into_iter()
        .map(|id| crate::types::Worker {
            id,
            status: WorkerStatus::Online,
            version: None,
        })
        .collect();
    let total_size = ByteSize(chunks.entries.iter().map(|c| c.size as u64).sum());
    println!(
        "Total chunks: {}, total size: {:.0}, chunk size: {:.0}, per worker: {:.0}",
        chunks.entries.len(),
        total_size,
        SCENARIO_CHUNK_SIZE,
        ByteSize(total_size.as_u64() / workers.len() as u64),
    );
    (chunks, workers, total_size)
}

/// Detailed per-worker analysis of a before/after scheduling comparison.
fn measure_reshuffle(
    chunks: &[crate::scheduling::ScheduledChunk],
    a1: &Assignment,
    a2: &Assignment,
) {
    println!("\nMeasure reshuffle:");

    let workers_1: BTreeSet<_> = a1.worker_chunks.keys().collect();
    let workers_2: BTreeSet<_> = a2.worker_chunks.keys().collect();
    let common: Vec<_> = workers_1.intersection(&workers_2).copied().collect();
    let departed: Vec<_> = workers_1.difference(&workers_2).copied().collect();
    let new_workers: Vec<_> = workers_2.difference(&workers_1).copied().collect();
    let only_in_1 = departed.len();
    let only_in_2 = new_workers.len();

    let mut removed_percentages = Vec::new();
    let mut added_percantages = Vec::new();
    let mut workers_with_removal = 0usize;
    let mut workers_with_addition = 0usize;
    let mut total_removed_bytes = ByteSize(0);
    let mut total_added_bytes = ByteSize(0);
    let mut total_before_bytes = ByteSize(0);

    let chunk_bytes = |i: u32| chunks[i as usize].size as u64;

    for &worker in &common {
        let c1: BTreeSet<_> = a1.worker_chunks[worker].iter().copied().collect();
        let c2: BTreeSet<_> = a2.worker_chunks[worker].iter().copied().collect();

        let before = ByteSize(c1.iter().map(|&i| chunk_bytes(i)).sum());
        let removed = ByteSize(c1.difference(&c2).map(|&i| chunk_bytes(i)).sum());
        let added = ByteSize(c2.difference(&c1).map(|&i| chunk_bytes(i)).sum());

        total_before_bytes += before;
        total_removed_bytes += removed;
        total_added_bytes += added;

        if before.as_u64() > 0 {
            removed_percentages.push(removed.as_u64() as f64 / before.as_u64() as f64 * 100.);
            added_percantages.push(added.as_u64() as f64 / before.as_u64() as f64 * 100.);
        }
        if removed.as_u64() > 0 {
            workers_with_removal += 1;
        }
        if added.as_u64() > 0 {
            workers_with_addition += 1;
        }
    }

    removed_percentages.sort_unstable_by(f64::total_cmp);
    added_percantages.sort_unstable_by(f64::total_cmp);

    let avg = |v: &[f64]| {
        if v.is_empty() {
            0.
        } else {
            v.iter().sum::<f64>() / v.len() as f64
        }
    };
    let median = |v: &[f64]| if v.is_empty() { 0. } else { v[v.len() / 2] };
    let max = |v: &[f64]| v.iter().cloned().fold(0f64, f64::max);

    let departed_bytes = ByteSize(
        departed
            .iter()
            .flat_map(|w| a1.worker_chunks[w].iter())
            .map(|&i| chunk_bytes(i))
            .sum(),
    );
    let new_worker_bytes = ByteSize(
        new_workers
            .iter()
            .flat_map(|w| a2.worker_chunks[w].iter())
            .map(|&i| chunk_bytes(i))
            .sum(),
    );

    println!(
        "  Workers: {} common, {} departed, {} new",
        common.len(),
        only_in_1,
        only_in_2
    );
    if only_in_1 > 0 {
        println!("  Departed workers held: {:.0}", departed_bytes);
    }
    if only_in_2 > 0 {
        println!("  New workers received: {:.0}", new_worker_bytes);
    }
    println!(
        "  Retained workers that lost chunks:   {}/{} ({:.0}%)",
        workers_with_removal,
        common.len(),
        workers_with_removal as f64 / common.len() as f64 * 100.
    );
    println!(
        "  Retained workers that gained chunks: {}/{} ({:.0}%)",
        workers_with_addition,
        common.len(),
        workers_with_addition as f64 / common.len() as f64 * 100.
    );
    println!(
        "  Data removed per worker: avg {:.1}%, median {:.1}%, max {:.1}%",
        avg(&removed_percentages),
        median(&removed_percentages),
        max(&removed_percentages)
    );
    println!(
        "  Data added per worker:   avg {:.1}%, median {:.1}%, max {:.1}%",
        avg(&added_percantages),
        median(&added_percantages),
        max(&added_percantages)
    );
    println!(
        "  Aggregate: {:.0} removed, {:.0} added out of {:.0} total",
        total_removed_bytes, total_added_bytes, total_before_bytes
    );
}

#[test]
fn scenario_worker_joins() {
    println!("\n========== SCENARIO: Worker Joins the Network ==========");
    println!("  Worker capacity: {:.0}\n", SCENARIO_WORKER_CAPACITY);

    // (n_workers, n_chunks, weights, min_replication)
    let inputs: &[(u16, u32, &[u16], u16)] = &[
        (SCENARIO_WORKERS, SCENARIO_CHUNKS, &[23], 1),
        (SCENARIO_WORKERS, SCENARIO_CHUNKS, &[1, 6, 24], 2),
    ];
    // Per-input weight -> replication regression guard; deterministic because 500k fixed-size
    // chunks make the weight split converge.
    let expected_weights: [BTreeMap<u16, u16>; 2] = [
        btreemap! { 23 => 23 },
        btreemap! { 1 => 2, 6 => 13, 24 => 54 },
    ];

    for (&(n_workers, n_chunks, weights, min_rep), expected_replication) in
        inputs.iter().zip(&expected_weights)
    {
        let all_workers: Vec<_> = generate_workers(n_workers + 1)
            .into_iter()
            .map(|id| crate::types::Worker {
                id,
                status: WorkerStatus::Online,
                version: None,
            })
            .collect();
        let (test_data, _, _total_size) = generate_scenario_input(n_workers, n_chunks, weights);
        let chunks = test_data.as_scheduled();
        let config = SchedulingConfig {
            worker_capacity: SCENARIO_WORKER_CAPACITY.as_u64(),
            saturation: 0.95,
            min_replication: min_rep,
            ignore_reliability: true,
        };

        let before = schedule(&chunks, &all_workers[..n_workers as usize], config.clone()).unwrap();
        let after = schedule(&chunks, &all_workers[..n_workers as usize + 1], config).unwrap();

        println!(
            "--- {n_workers} workers + 1 joins, {n_chunks} chunks, weights={weights:?}, min_rep={min_rep} ---"
        );

        assert_eq!(before.replication_by_weight, after.replication_by_weight);
        assert_eq!(&before.replication_by_weight, expected_replication);

        println!(
            "  Replication factors (before == after): {:?}",
            before.replication_by_weight
        );

        measure_reshuffle(&chunks, &before, &after);
        println!();
    }
}

#[test]
fn scenario_worker_leaves() {
    println!("\n========== SCENARIO: Worker Leaves the Network ==========");
    println!("  Worker capacity: {:.0}\n", SCENARIO_WORKER_CAPACITY);

    // (n_workers, n_chunks, weights, min_replication)
    let inputs: &[(u16, u32, &[u16], u16)] = &[
        (SCENARIO_WORKERS, SCENARIO_CHUNKS, &[1], 1),
        (SCENARIO_WORKERS, SCENARIO_CHUNKS, &[1, 6, 24], 2),
    ];
    // Per-input weight -> replication regression guard; deterministic because 500k fixed-size
    // chunks make the weight split converge.
    let expected_weights: [BTreeMap<u16, u16>; 2] = [
        btreemap! { 1 => 23 },
        btreemap! { 1 => 2, 6 => 13, 24 => 54 },
    ];

    for (&(n_workers, n_chunks, weights, min_rep), expected_replication) in
        inputs.iter().zip(&expected_weights)
    {
        let all_workers: Vec<_> = generate_workers(n_workers)
            .into_iter()
            .map(|id| crate::types::Worker {
                id,
                status: WorkerStatus::Online,
                version: None,
            })
            .collect();
        let (test_data, _, _total_size) = generate_scenario_input(n_workers, n_chunks, weights);
        let chunks = test_data.as_scheduled();
        let config = SchedulingConfig {
            worker_capacity: SCENARIO_WORKER_CAPACITY.as_u64(),
            saturation: 0.95,
            min_replication: min_rep,
            ignore_reliability: true,
        };

        let before = schedule(&chunks, &all_workers, config.clone()).unwrap();
        let after = schedule(&chunks, &all_workers[..n_workers as usize - 1], config).unwrap();

        println!(
            "--- {n_workers} workers, 1 leaves, {n_chunks} chunks, weights={weights:?}, min_rep={min_rep} ---"
        );

        assert_eq!(before.replication_by_weight, after.replication_by_weight);
        assert_eq!(&before.replication_by_weight, expected_replication);

        println!(
            "  Replication factors (before == after): {:?}",
            before.replication_by_weight
        );

        measure_reshuffle(&chunks, &before, &after);
        println!();
    }
}

#[test]
fn scenario_many_workers_leave() {
    // Edge case: enough departures to force replication down to min_replication.
    println!("\n========== SCENARIO: Many Workers Leave (min_replication=22) ==========");
    println!("  Worker capacity: {:.0}\n", SCENARIO_WORKER_CAPACITY);

    let (test_data, all_workers, total_size) =
        generate_scenario_input(SCENARIO_WORKERS, SCENARIO_CHUNKS, &[1]);
    let chunks = test_data.as_scheduled();

    let config = SchedulingConfig {
        worker_capacity: SCENARIO_WORKER_CAPACITY.as_u64(),
        saturation: 0.95,
        min_replication: 22,
        ignore_reliability: true,
    };

    // Min workers for min_replication=22 = ceil(22 * total_size / (600 GB * 0.95)).
    let effective_per_worker = ByteSize((SCENARIO_WORKER_CAPACITY.as_u64() as f64 * 0.95) as u64);
    let min_workers_needed = (22 * total_size.as_u64()).div_ceil(effective_per_worker.as_u64());
    println!(
        "  Effective capacity per worker: {:.0}",
        effective_per_worker
    );
    println!(
        "  Minimum workers needed for min_replication=22: {} (required capacity: {:.0})\n",
        min_workers_needed,
        ByteSize(22 * total_size.as_u64())
    );

    let baseline = schedule(&chunks, &all_workers, config.clone()).unwrap();

    let worker_scenarios = [
        SCENARIO_WORKERS,
        1990,
        1980,
        1970,
        1950,
        1900,
        1885,
        1884,
        1880,
    ];
    for &remaining in &worker_scenarios {
        let Ok(assignment) = schedule(&chunks, &all_workers[..remaining as usize], config.clone())
            .inspect_err(|e| println!("  {remaining} workers remain -> FAILED: {e}"))
        else {
            continue;
        };

        let total_added = ByteSize(
            assignment
                .worker_chunks
                .iter()
                .map(|(worker, chunks_after)| {
                    let before_set: BTreeSet<_> = baseline
                        .worker_chunks
                        .get(worker)
                        .map(|v| v.iter().copied().collect())
                        .unwrap_or_default();
                    let after_set: BTreeSet<_> = chunks_after.iter().copied().collect();
                    after_set
                        .difference(&before_set)
                        .map(|&i| chunks[i as usize].size as u64)
                        .sum::<u64>()
                })
                .sum::<u64>(),
        );

        let pct_dataset = total_added.as_u64() as f64 / total_size.as_u64() as f64 * 100.0;
        println!(
            "  {remaining} workers remain -> OK, replication: {:?}, S3 download: {:.1} ({:.1}% of dataset)",
            assignment.replication_by_weight, total_added, pct_dataset,
        );
    }
}

#[test]
fn scenario_new_chunks_added() {
    println!("\n========== SCENARIO: New Chunks Added ==========");
    println!("  Worker capacity: {:.0}\n", SCENARIO_WORKER_CAPACITY);

    let workers: Vec<_> = generate_workers(SCENARIO_WORKERS)
        .into_iter()
        .map(|id| crate::types::Worker {
            id,
            status: WorkerStatus::Online,
            version: None,
        })
        .collect();

    for &(initial, added, weights) in &[
        (SCENARIO_CHUNKS, 2_500u32, &[1u16][..]),    // 0.5% growth
        (SCENARIO_CHUNKS, 5_000, &[1][..]),          // 1% growth
        (SCENARIO_CHUNKS, 10_000, &[1][..]),         // 2% growth
        (SCENARIO_CHUNKS, 15_000, &[1][..]),         // 3% growth
        (SCENARIO_CHUNKS, 20_000, &[1][..]),         // 4% growth
        (SCENARIO_CHUNKS, 25_000, &[1][..]),         // 5% growth
        (SCENARIO_CHUNKS, 50_000, &[1][..]),         // 10% growth
        (SCENARIO_CHUNKS, 250_000, &[1][..]),        // 50% growth
        (SCENARIO_CHUNKS, 2_500, &[1, 6, 24][..]),   // 0.5% growth, multi-weight
        (SCENARIO_CHUNKS, 25_000, &[1, 6, 24][..]),  // 5% growth, multi-weight
        (SCENARIO_CHUNKS, 50_000, &[1, 6, 24][..]),  // 10% growth, multi-weight
        (SCENARIO_CHUNKS, 250_000, &[1, 6, 24][..]), // 50% growth, multi-weight
    ] {
        let all_chunks = generate_chunks_fixed_size(initial + added, weights, SCENARIO_CHUNK_SIZE);
        let all_scheduled = all_chunks.as_scheduled();
        let chunks_before = &all_scheduled[..initial as usize];
        let chunks_after = &all_scheduled[..(initial + added) as usize];

        let total_before = ByteSize(chunks_before.iter().map(|c| c.size as u64).sum());
        let total_after = ByteSize(chunks_after.iter().map(|c| c.size as u64).sum());
        let config = SchedulingConfig {
            worker_capacity: SCENARIO_WORKER_CAPACITY.as_u64(),
            saturation: 0.95,
            min_replication: 2,
            ignore_reliability: true,
        };

        let a1 = schedule(chunks_before, &workers, config.clone()).unwrap();
        let a2 = schedule(chunks_after, &workers, config).unwrap();

        println!(
            "--- {initial} + {added} chunks ({:.0}% growth), weights={weights:?} ---",
            added as f64 / initial as f64 * 100.
        );
        println!("  Total data: {:.0} -> {:.0}", total_before, total_after);
        println!(
            "  Replication factors before: {:?}",
            a1.replication_by_weight
        );
        println!(
            "  Replication factors after:  {:?}",
            a2.replication_by_weight
        );
        // Use full chunk set for index lookups (a2 has indices up to initial+added)
        measure_reshuffle(chunks_after, &a1, &a2);

        // Full chunk count: a2 carries indices up to initial + added.
        let owners_before = a1.chunk_holders(chunks_after.len());
        let owners_after = a2.chunk_holders(chunks_after.len());

        let mut existing_changed = 0u32;
        let mut existing_unchanged = 0u32;
        for ci in 0..initial as usize {
            if owners_before[ci] != owners_after[ci] {
                existing_changed += 1;
            } else {
                existing_unchanged += 1;
            }
        }

        let new_chunks = added;
        let total_affected = existing_changed + new_chunks;

        println!("  Chunk ownership:");
        println!(
            "    Existing chunks unchanged: {existing_unchanged}/{initial} ({:.1}%)",
            existing_unchanged as f64 / initial as f64 * 100.
        );
        println!(
            "    Existing chunks changed owners: {existing_changed}/{initial} ({:.1}%)",
            existing_changed as f64 / initial as f64 * 100.
        );
        println!("    New chunks added: {new_chunks}");
        println!(
            "    Total chunks added or changed: {total_affected}/{} ({:.1}%)",
            initial + added,
            total_affected as f64 / (initial + added) as f64 * 100.
        );
        println!();
    }
}

#[test]
fn scenario_reliability_flip() {
    println!(
        "\n========== SCENARIO: Worker Reliability Changes (Online -> Offline -> Online) =========="
    );
    println!("  Worker capacity: {:.0}\n", SCENARIO_WORKER_CAPACITY);

    for &(n_workers, flipping, n_chunks) in &[
        (SCENARIO_WORKERS, 1u16, SCENARIO_CHUNKS),
        (SCENARIO_WORKERS, 10, SCENARIO_CHUNKS),
        (SCENARIO_WORKERS, 50, SCENARIO_CHUNKS),
        (SCENARIO_WORKERS, 200, SCENARIO_CHUNKS),
    ] {
        let (test_data, mut workers, _total_size) =
            generate_scenario_input(n_workers, n_chunks, &[1, 6]);
        let chunks = test_data.as_scheduled();
        let config = SchedulingConfig {
            worker_capacity: SCENARIO_WORKER_CAPACITY.as_u64(),
            saturation: 0.95,
            min_replication: 2,
            ignore_reliability: false,
        };

        let flipping_ids: BTreeSet<_> = workers[..flipping as usize].iter().map(|w| w.id).collect();

        // State A: all workers Online
        let state_a = schedule(&chunks, &workers, config.clone()).unwrap();

        // State B: some workers go Offline
        for worker in &mut workers[..flipping as usize] {
            worker.status = WorkerStatus::Offline;
        }
        let state_b = schedule(&chunks, &workers, config.clone()).unwrap();

        // State C: those workers come back Online
        for worker in &mut workers[..flipping as usize] {
            worker.status = WorkerStatus::Online;
        }
        let state_c = schedule(&chunks, &workers, config).unwrap();

        println!("--- {n_workers} workers, {flipping} go Online->Offline->Online ---");
        println!(
            "  Replication factors State A (all Online): {:?}",
            state_a.replication_by_weight
        );
        println!(
            "  Replication factors State B ({flipping} Offline):  {:?}",
            state_b.replication_by_weight
        );
        println!(
            "  Replication factors State C (all Online): {:?}",
            state_c.replication_by_weight
        );

        // Step 1 (A -> B): "added" = chunks a worker must download from S3.
        let mut step1_stable_download = ByteSize(0);
        let mut step1_flipping_download = ByteSize(0);
        for (worker, chunks_b) in &state_b.worker_chunks {
            let chunks_a_set: BTreeSet<_> = state_a
                .worker_chunks
                .get(worker)
                .map(|v| v.iter().copied().collect())
                .unwrap_or_default();
            let chunks_b_set: BTreeSet<_> = chunks_b.iter().copied().collect();
            let added = ByteSize(
                chunks_b_set
                    .difference(&chunks_a_set)
                    .map(|&i| chunks[i as usize].size as u64)
                    .sum::<u64>(),
            );
            if flipping_ids.contains(worker) {
                step1_flipping_download += added;
            } else {
                step1_stable_download += added;
            }
        }

        // Step 2 (B -> C).
        let mut step2_stable_download = ByteSize(0);
        let mut step2_flipping_download = ByteSize(0);
        for (worker, chunks_c) in &state_c.worker_chunks {
            let chunks_b_set: BTreeSet<_> = state_b
                .worker_chunks
                .get(worker)
                .map(|v| v.iter().copied().collect())
                .unwrap_or_default();
            // Returning workers still hold their State A data locally — they only download
            // chunks in C they have from neither A nor B.
            let chunks_c_set: BTreeSet<_> = chunks_c.iter().copied().collect();
            if flipping_ids.contains(worker) {
                let chunks_a_set: BTreeSet<_> = state_a
                    .worker_chunks
                    .get(worker)
                    .map(|v| v.iter().copied().collect())
                    .unwrap_or_default();
                let local_data: BTreeSet<_> = chunks_a_set.union(&chunks_b_set).copied().collect();
                step2_flipping_download += ByteSize(
                    chunks_c_set
                        .difference(&local_data)
                        .map(|&i| chunks[i as usize].size as u64)
                        .sum::<u64>(),
                );
            } else {
                step2_stable_download += ByteSize(
                    chunks_c_set
                        .difference(&chunks_b_set)
                        .map(|&i| chunks[i as usize].size as u64)
                        .sum::<u64>(),
                );
            }
        }

        let total_download = step1_stable_download
            + step1_flipping_download
            + step2_stable_download
            + step2_flipping_download;

        println!("\n  Step 1: Online -> Offline ({flipping} workers go down)");
        println!(
            "    Stable workers S3 download:   {:.1}",
            step1_stable_download
        );
        println!(
            "    Flipping workers S3 download:  {:.1}",
            step1_flipping_download
        );

        println!("\n  Step 2: Offline -> Online ({flipping} workers come back)");
        println!(
            "    Stable workers S3 download:    {:.1}",
            step2_stable_download
        );
        println!(
            "    Returning workers S3 download: {:.1} (only chunks not in State A or B)",
            step2_flipping_download
        );

        println!(
            "\n  Total S3 download across full cycle: {:.1}",
            total_download
        );

        if state_a == state_c {
            println!("  >> IDENTICAL: assignments fully restored after round-trip");
        } else {
            println!("  >> DIFFERENT: assignments changed after round-trip");
        }
        println!();
    }
}

#[test]
fn scenario_capacity_changes() {
    println!("\n========== SCENARIO: Worker Capacity or Saturation Changes ==========");

    let dataset_size = ByteSize(SCENARIO_CHUNKS as u64 * 100 * bytesize::MB);

    let n_workers: u16 = SCENARIO_WORKERS;
    let (test_data, workers, _) = generate_scenario_input(n_workers, SCENARIO_CHUNKS, &[1, 6, 24]);
    let chunks = test_data.as_scheduled();

    println!("\n--- Worker capacity changes (saturation fixed at 0.95) ---\n");

    let base_capacity = ByteSize::gb(600);
    let base = schedule(
        &chunks,
        &workers,
        SchedulingConfig {
            worker_capacity: base_capacity.as_u64(),
            saturation: 0.95,
            min_replication: 2,
            ignore_reliability: true,
        },
    )
    .unwrap();
    println!(
        "  Baseline: capacity={:.0}, replication={:?}",
        base_capacity, base.replication_by_weight
    );

    for &(new_cap_gb, label) in &[
        (550u64, "600GB -> 550GB (-8.3%)"),
        (500, "600GB -> 500GB (-16.7%)"),
        (650, "600GB -> 650GB (+8.3%)"),
        (700, "600GB -> 700GB (+16.7%)"),
    ] {
        let new_capacity = ByteSize::gb(new_cap_gb);
        let after = schedule(
            &chunks,
            &workers,
            SchedulingConfig {
                worker_capacity: new_capacity.as_u64(),
                saturation: 0.95,
                min_replication: 2,
                ignore_reliability: true,
            },
        )
        .unwrap();

        let mut total_added = ByteSize(0);
        let mut workers_affected = 0usize;
        for (worker, chunks_after) in &after.worker_chunks {
            let before_set: BTreeSet<_> = base.worker_chunks[worker].iter().copied().collect();
            let after_set: BTreeSet<_> = chunks_after.iter().copied().collect();
            let added = ByteSize(
                after_set
                    .difference(&before_set)
                    .map(|&i| chunks[i as usize].size as u64)
                    .sum::<u64>(),
            );
            total_added += added;
            if added.as_u64() > 0 {
                workers_affected += 1;
            }
        }

        let pct_dataset = total_added.as_u64() as f64 / dataset_size.as_u64() as f64 * 100.0;
        println!("  {label}:");
        println!("    Replication after: {:?}", after.replication_by_weight);
        println!("    Workers that gained new chunks: {workers_affected}/{n_workers}");
        println!(
            "    Data fetched from S3: {:.1} ({:.1}% of dataset)",
            total_added, pct_dataset
        );
    }

    println!("\n--- Saturation changes (capacity fixed at 600GB) ---\n");

    let base_sat = schedule(
        &chunks,
        &workers,
        SchedulingConfig {
            worker_capacity: base_capacity.as_u64(),
            saturation: 0.95,
            min_replication: 2,
            ignore_reliability: true,
        },
    )
    .unwrap();
    println!(
        "  Baseline: saturation=0.95, replication={:?}",
        base_sat.replication_by_weight
    );

    for &(new_sat, label) in &[
        (0.90, "0.95 -> 0.90 (-5.3%)"),
        (0.85, "0.95 -> 0.85 (-10.5%)"),
        (0.99, "0.95 -> 0.99 (+4.2%)"),
    ] {
        let after = schedule(
            &chunks,
            &workers,
            SchedulingConfig {
                worker_capacity: base_capacity.as_u64(),
                saturation: new_sat,
                min_replication: 2,
                ignore_reliability: true,
            },
        )
        .unwrap();

        let mut total_added = ByteSize(0);
        let mut workers_affected = 0usize;
        for (worker, chunks_after) in &after.worker_chunks {
            let before_set: BTreeSet<_> = base_sat.worker_chunks[worker].iter().copied().collect();
            let after_set: BTreeSet<_> = chunks_after.iter().copied().collect();
            let added = ByteSize(
                after_set
                    .difference(&before_set)
                    .map(|&i| chunks[i as usize].size as u64)
                    .sum::<u64>(),
            );
            total_added += added;
            if added.as_u64() > 0 {
                workers_affected += 1;
            }
        }

        let pct_dataset = total_added.as_u64() as f64 / dataset_size.as_u64() as f64 * 100.0;
        println!("  {label}:");
        println!("    Replication after: {:?}", after.replication_by_weight);
        println!("    Workers that gained new chunks: {workers_affected}/{n_workers}");
        println!(
            "    Data fetched from S3: {:.1} ({:.1}% of dataset)",
            total_added, pct_dataset
        );
    }
    println!();
}
