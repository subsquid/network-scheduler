//! Asserts the design's invariants directly on the published placement.

use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use libp2p_identity::PeerId;
use semver::Version;

use super::{ScheduledChunk, SchedulingConfig, schedule};
use crate::rings::WorkerRingCache;
use crate::{
    tests::input::{TestChunks, generate_input, generate_workers},
    types::{Assignment, Worker, WorkerIndex, WorkerStatus},
};

/// `TestChunks::as_scheduled` returns `crate::scheduling::ScheduledChunk`; convert into this
/// module's own `ScheduledChunk`.
fn as_scheduled(td: &TestChunks) -> Vec<ScheduledChunk<'_>> {
    td.entries
        .iter()
        .map(|e| ScheduledChunk {
            dataset: &td.dataset,
            chunk_id: &e.chunk_id,
            size: e.size,
            weight: e.weight,
            minimum_worker_version: e.minimum_worker_version.as_ref(),
        })
        .collect()
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

fn online_workers(n: u16) -> Vec<Worker> {
    generate_workers(n)
        .into_iter()
        .map(|id| Worker {
            id,
            status: WorkerStatus::Online,
            version: None,
        })
        .collect()
}

/// Empty current placement (every chunk brand-new).
fn empty(n: usize) -> Vec<Vec<WorkerIndex>> {
    vec![Vec::new(); n]
}

/// Per-chunk holders as positions into `workers` (the form `schedule` takes). A holder not in
/// `workers` is dropped — its copy is a shortfall.
fn to_positions(current: &[Vec<PeerId>], workers: &[Worker]) -> Vec<Vec<WorkerIndex>> {
    let index: std::collections::HashMap<PeerId, WorkerIndex> = workers
        .iter()
        .enumerate()
        .map(|(i, w)| (w.id, i as WorkerIndex))
        .collect();
    current
        .iter()
        .map(|holders| {
            holders
                .iter()
                .filter_map(|p| index.get(p).copied())
                .collect()
        })
        .collect()
}

/// Invert an assignment into `chunk -> holders`.
fn chunk_to_peers(a: &Assignment, n_chunks: usize) -> Vec<Vec<PeerId>> {
    let mut out = vec![Vec::new(); n_chunks];
    for (peer, chunks) in &a.worker_chunks {
        for &ci in chunks {
            out[ci as usize].push(*peer);
        }
    }
    out
}

/// Published bytes on each worker.
fn worker_bytes(a: &Assignment, chunks: &[ScheduledChunk]) -> BTreeMap<PeerId, u64> {
    a.worker_chunks
        .iter()
        .map(|(p, cs)| {
            (
                *p,
                cs.iter()
                    .map(|&i| chunks[i as usize].size as u64)
                    .sum::<u64>(),
            )
        })
        .collect()
}

// Feeding the output back as the current placement is a fixed point.
#[test]
fn idempotent_when_fed_its_own_output() {
    let (td, workers, total) = generate_input(60, 20_000, &[1, 3, 6]);
    let chunks = as_scheduled(&td);
    let cap = (total as f64 / workers.len() as f64 * 20.) as u64;
    let config = SchedulingConfig {
        worker_capacity: cap,
        saturation: 0.95,
        min_replication: 2,
        ignore_reliability: true,
    };
    let n = chunks.len();

    let a = schedule(
        &chunks,
        &workers,
        &config,
        &empty(n),
        &mut WorkerRingCache::default(),
    )
    .unwrap();
    let cur = chunk_to_peers(&a, n);
    let b = schedule(
        &chunks,
        &workers,
        &config,
        &to_positions(&cur, &workers),
        &mut WorkerRingCache::default(),
    )
    .unwrap();

    assert_eq!(
        a, b,
        "feeding the output back as the current placement must be a fixed point"
    );
}

// Worker input order must not affect the result.
#[test]
fn deterministic_across_worker_order() {
    let (td, mut workers, total) = generate_input(50, 15_000, &[1, 4]);
    let chunks = as_scheduled(&td);
    let cap = (total as f64 / workers.len() as f64 * 20.) as u64;
    let config = SchedulingConfig {
        worker_capacity: cap,
        saturation: 0.95,
        min_replication: 2,
        ignore_reliability: true,
    };
    let n = chunks.len();

    let a = schedule(
        &chunks,
        &workers,
        &config,
        &empty(n),
        &mut WorkerRingCache::default(),
    )
    .unwrap();
    workers.reverse();
    let b = schedule(
        &chunks,
        &workers,
        &config,
        &empty(n),
        &mut WorkerRingCache::default(),
    )
    .unwrap();

    assert_eq!(
        a, b,
        "reversed worker order must produce identical placement"
    );
}

// Adding workers shifts the ideal; existing copies must survive >= floor, without overcommit.
#[test]
fn migration_keeps_floor_and_never_overcommits() {
    const MIN: u16 = 3;
    let all = online_workers(80);
    let (td, _, total) = generate_input(60, 20_000, &[1, 4]);
    let chunks = as_scheduled(&td);
    let n = chunks.len();
    let cap = (total as f64 / 60. * 20.) as u64;
    let config = SchedulingConfig {
        worker_capacity: cap,
        saturation: 0.95,
        min_replication: MIN,
        ignore_reliability: true,
    };

    let a = schedule(
        &chunks,
        &all[..60],
        &config,
        &empty(n),
        &mut WorkerRingCache::default(),
    )
    .unwrap();
    let cur = chunk_to_peers(&a, n);

    // 20 fresh workers join, shifting the ideal.
    let b = schedule(
        &chunks,
        &all[..80],
        &config,
        &to_positions(&cur, &all[..80]),
        &mut WorkerRingCache::default(),
    )
    .unwrap();

    // Invariant 4: every existing chunk keeps >= min(MIN, |current|) current copies.
    let b_holders = b.chunk_holders(n);
    for ci in 0..n {
        let current: BTreeSet<PeerId> = cur[ci].iter().copied().collect();
        let kept = current.intersection(&b_holders[ci]).count();
        let floor = (MIN as usize).min(current.len());
        assert!(
            kept >= floor,
            "chunk {ci}: kept {kept} current copies, expected >= {floor}",
        );
    }

    for (peer, bytes) in worker_bytes(&b, &chunks) {
        assert!(
            bytes <= cap,
            "worker {peer} published {bytes} bytes > capacity {cap}",
        );
    }
}

// Iterating the cycle reaches a fixed point while never dropping a chunk below the floor.
#[test]
fn converges_to_a_fixed_point_staying_above_floor() {
    const MIN: u16 = 2;
    let all = online_workers(70);
    let (td, _, total) = generate_input(50, 12_000, &[1, 6]);
    let chunks = as_scheduled(&td);
    let n = chunks.len();
    let cap = (total as f64 / 50. * 25.) as u64;
    let config = SchedulingConfig {
        worker_capacity: cap,
        saturation: 0.9,
        min_replication: MIN,
        ignore_reliability: true,
    };

    // Settle on 50 workers, then 20 join — sustained migration.
    let settled = schedule(
        &chunks,
        &all[..50],
        &config,
        &empty(n),
        &mut WorkerRingCache::default(),
    )
    .unwrap();
    let mut cur = chunk_to_peers(&settled, n);

    let mut prev: Option<Assignment> = None;
    let mut converged_at = None;
    for cycle in 0..8 {
        let next = schedule(
            &chunks,
            &all[..70],
            &config,
            &to_positions(&cur, &all[..70]),
            &mut WorkerRingCache::default(),
        )
        .unwrap();

        let h = next.chunk_holders(n);
        for (ci, copies) in h.iter().enumerate() {
            assert!(
                copies.len() >= MIN as usize,
                "cycle {cycle}, chunk {ci}: only {} copies (< floor {MIN})",
                copies.len(),
            );
        }
        if prev.as_ref() == Some(&next) {
            converged_at = Some(cycle);
            break;
        }
        prev = Some(next.clone());
        cur = chunk_to_peers(&next, n);
    }

    assert!(
        converged_at.is_some(),
        "did not reach a fixed point within 8 cycles",
    );
}

// A new chunk that cannot reach the floor is excluded, never published under-replicated;
// existing chunks keep their copies.
#[test]
fn new_chunk_excluded_when_floor_unreachable() {
    let w = generate_workers(4);
    let workers = w
        .iter()
        .map(|&id| Worker {
            id,
            status: WorkerStatus::Online,
            version: None,
        })
        .collect_vec();

    // 4 chunks of equal size; capacity = exactly 3 chunk-slots per worker.
    let dataset = "s3://ds".to_string();
    let ids = ["c0", "c1", "c2", "c3"];
    let chunks: Vec<ScheduledChunk> = ids
        .iter()
        .map(|id| ScheduledChunk {
            dataset: &dataset,
            chunk_id: id,
            size: 1000,
            weight: 1,
            minimum_worker_version: None,
        })
        .collect();
    let n = chunks.len();

    // c0..c2 each sit on all four workers, filling every worker to its 3000-byte cap;
    // c3 is brand-new.
    let mut current = vec![Vec::new(); n];
    for slot in current.iter_mut().take(3) {
        *slot = vec![w[0], w[1], w[2], w[3]];
    }

    let config = SchedulingConfig {
        worker_capacity: 3000,
        saturation: 0.9,
        min_replication: 2,
        ignore_reliability: true,
    };

    let a = schedule(
        &chunks,
        &workers,
        &config,
        &to_positions(&current, &workers),
        &mut WorkerRingCache::default(),
    )
    .unwrap();
    let h = a.chunk_holders(n);

    assert_eq!(
        h[3].len(),
        0,
        "new chunk c3 should be excluded, not under-replicated"
    );
    for (ci, copies) in h.iter().enumerate().take(3) {
        assert!(
            copies.len() >= 2,
            "existing chunk c{ci} fell below the floor"
        );
    }
}

// A draining copy is revived in place of an ideal copy or released — never kept on top of
// the target replica count as a permanent extra.
#[test]
fn draining_copies_do_not_accumulate() {
    let (td, workers, total) = generate_input(40, 8_000, &[1, 3]);
    let chunks = as_scheduled(&td);
    let n = chunks.len();
    let cap = (total as f64 / workers.len() as f64 * 30.) as u64;
    let config = SchedulingConfig {
        worker_capacity: cap,
        saturation: 0.9,
        min_replication: 2,
        ignore_reliability: true,
    };

    let ideal = schedule(
        &chunks,
        &workers,
        &config,
        &empty(n),
        &mut WorkerRingCache::default(),
    )
    .unwrap();
    let ideal_holders = ideal.chunk_holders(n);

    // `current` = ideal plus one extra (draining) copy per chunk on a non-holder.
    let mut current = chunk_to_peers(&ideal, n);
    for ci in 0..n {
        if let Some(extra) = workers
            .iter()
            .find(|wk| !ideal_holders[ci].contains(&wk.id))
        {
            current[ci].push(extra.id);
        }
    }

    let out = schedule(
        &chunks,
        &workers,
        &config,
        &to_positions(&current, &workers),
        &mut WorkerRingCache::default(),
    )
    .unwrap();
    let out_holders = out.chunk_holders(n);

    for ci in 0..n {
        assert_eq!(
            out_holders[ci].len(),
            ideal_holders[ci].len(),
            "chunk {ci}: replica count changed ({} -> {}) — draining copy accumulated",
            ideal_holders[ci].len(),
            out_holders[ci].len(),
        );
        assert!(out_holders[ci].len() >= 2, "chunk {ci} below the floor");
    }

    for (peer, bytes) in worker_bytes(&out, &chunks) {
        assert!(
            bytes <= cap,
            "worker {peer} published {bytes} bytes > capacity {cap}"
        );
    }
}

// A chunk below the floor (lost holder) is repaired best-effort, never rolled back.
#[test]
fn degraded_chunk_repaired_not_rolled_back() {
    let workers = online_workers(6);
    let dataset = "s3://ds".to_string();
    let chunk = ScheduledChunk {
        dataset: &dataset,
        chunk_id: "only",
        size: 1000,
        weight: 1,
        minimum_worker_version: None,
    };
    let chunks = vec![chunk];

    // Single copy on workers[0], below the floor of 2.
    let current = vec![vec![workers[0].id]];

    let config = SchedulingConfig {
        worker_capacity: 1_000_000,
        saturation: 0.9,
        min_replication: 2,
        ignore_reliability: true,
    };

    let out = schedule(
        &chunks,
        &workers,
        &config,
        &to_positions(&current, &workers),
        &mut WorkerRingCache::default(),
    )
    .unwrap();
    let h = out.chunk_holders(1);

    assert!(
        h[0].contains(&workers[0].id),
        "degraded chunk must keep its existing copy (never rolled back)",
    );
    assert!(
        h[0].len() >= 2,
        "degraded chunk should be repaired back to the floor when capacity allows, got {}",
        h[0].len(),
    );
}

// Version-pinned chunks land only on eligible workers.
#[test]
fn version_restriction_preserved() {
    let min_version = Version::new(2, 8, 0);
    const N_WORKERS: u16 = 40;
    const N_ELIGIBLE: u16 = 20;
    const N_CHUNKS: u32 = 10_000;
    const N_VERSIONED: u32 = 300;

    let (mut td, _, _) = generate_input(N_WORKERS, N_CHUNKS, &[1]);
    for entry in &mut td.entries[..N_VERSIONED as usize] {
        entry.minimum_worker_version = Some(min_version.clone());
    }
    let chunks = as_scheduled(&td);
    let n = chunks.len();

    let workers = generate_workers(N_WORKERS)
        .into_iter()
        .enumerate()
        .map(|(i, id)| Worker {
            id,
            status: WorkerStatus::Online,
            version: if (i as u16) < N_ELIGIBLE {
                Some(Version::new(2, 8, 0))
            } else {
                Some(Version::new(2, 7, 0))
            },
        })
        .collect_vec();

    let eligible: BTreeSet<PeerId> = workers[..N_ELIGIBLE as usize]
        .iter()
        .map(|w| w.id)
        .collect();

    let total_size: u64 = chunks.iter().map(|c| c.size as u64).sum();
    let cap = 10 * total_size / N_WORKERS as u64;
    let config = SchedulingConfig {
        worker_capacity: cap,
        saturation: 0.95,
        min_replication: 1,
        ignore_reliability: true,
    };

    let out = schedule(
        &chunks,
        &workers,
        &config,
        &empty(n),
        &mut WorkerRingCache::default(),
    )
    .unwrap();

    for (worker_id, chunk_indexes) in &out.worker_chunks {
        if !eligible.contains(worker_id) {
            assert!(
                !chunk_indexes.iter().any(|&i| i < N_VERSIONED),
                "ineligible worker {worker_id} was assigned a version-restricted chunk",
            );
        }
    }
}
