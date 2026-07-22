//! Asserts the design's invariants directly on the published placement.

use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use libp2p_identity::PeerId;
use semver::Version;

use super::{ScheduledChunk, SchedulingConfig};
use crate::replication::ReplicationError;
use crate::rings::WorkerRingCache;
use crate::{
    tests::input::{TestChunks, generate_input, generate_workers},
    types::{Assignment, ChunkIndex, Worker, WorkerIndex, WorkerStatus},
};

/// Thin wrapper over [`super::schedule`] for the high-level tests: they feed the previous cycle's
/// published placement back as `current`, which is entirely committed ideal (no separately-modelled
/// drains), so `committed` and `routed` both equal `current`. The focused reclaim-decision tests
/// drive [`super::Reconcile`] directly instead, where they pin `committed`/`routed` precisely.
fn schedule(
    chunks: &[ScheduledChunk<'_>],
    workers: &[Worker],
    config: &SchedulingConfig,
    current: &[Vec<WorkerIndex>],
    cache: &mut WorkerRingCache,
) -> Result<(Assignment, Vec<(ChunkIndex, PeerId)>), ReplicationError> {
    super::schedule(chunks, workers, config, current, current, current, cache)
}

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
            is_portal_visible: false,
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

    let (a, _) = schedule(
        &chunks,
        &workers,
        &config,
        &empty(n),
        &mut WorkerRingCache::default(),
    )
    .unwrap();
    let cur = chunk_to_peers(&a, n);
    let (b, _) = schedule(
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

    let (a, _) = schedule(
        &chunks,
        &workers,
        &config,
        &empty(n),
        &mut WorkerRingCache::default(),
    )
    .unwrap();
    workers.reverse();
    let (b, _) = schedule(
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

    let (a, _) = schedule(
        &chunks,
        &all[..60],
        &config,
        &empty(n),
        &mut WorkerRingCache::default(),
    )
    .unwrap();
    let cur = chunk_to_peers(&a, n);

    // 20 fresh workers join, shifting the ideal.
    let (b, _) = schedule(
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
    let (settled, _) = schedule(
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
        let (next, _) = schedule(
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

// Part 1 (pipeline): a new chunk never evicts a routed copy to seat itself. Here every full worker's
// spare (bonus) copies are all still routed (committed == routed == current), so the only reclaimable
// space is routed — off-limits to a new chunk. The new chunk's atomic floor can't be met, so it
// defers to zero (backpressure) rather than sacrificing a live chunk's routed read; donors keep every
// copy. (The positive path — a new chunk reclaiming NON-routed space — is
// `new_chunk_reclaims_a_nonrouted_drain`; a portal-visible chunk in this same layout would preempt, cf.
// `a_portal_visible_chunk_that_lost_every_copy_keeps_what_fits`.)
#[test]
fn new_chunk_defers_rather_than_evicting_routed_copies() {
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
            is_portal_visible: false,
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

    let (a, _) = schedule(
        &chunks,
        &workers,
        &config,
        &to_positions(&current, &workers),
        &mut WorkerRingCache::default(),
    )
    .unwrap();
    let h = a.chunk_holders(n);

    assert!(
        h[3].is_empty(),
        "new chunk c3 must defer (not evict a routed copy) when the only reclaimable space is \
         routed; got holders {:?}",
        h[3],
    );
    for (ci, copies) in h.iter().enumerate().take(3) {
        assert!(
            copies.len() >= 2,
            "donor chunk c{ci} must stay at or above its own floor"
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

    let (ideal, _) = schedule(
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

    let (out, _) = schedule(
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
        is_portal_visible: false,
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

    let (out, _) = schedule(
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

    let (out, _) = schedule(
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

/// The reliable-first partition shifts every position behind a non-Online worker, so
/// [`translate`](super::translate) must remap `current` with the partition's `new_pos` —
/// every translated position has to resolve to the worker the caller meant.
#[test]
fn partition_reliable_translates_current_positions() {
    let mut workers = online_workers(4);
    workers[1].status = WorkerStatus::Offline;

    // chunk 0 held at caller positions {1, 3}; chunk 1 at {0}; chunk 2 unheld.
    let current: Vec<Vec<WorkerIndex>> = vec![vec![1, 3], vec![0], vec![]];
    let (sorted, reliable, new_pos) = super::partition_reliable(&workers);
    let translated = super::translate(&current, &new_pos);

    assert_eq!(reliable, 3);
    assert_eq!(
        sorted[3].id, workers[1].id,
        "the unreliable worker moves to the back"
    );
    for (chunk, held) in current.iter().enumerate() {
        assert_eq!(held.len(), translated[chunk].len());
        for (&old, &new) in held.iter().zip(&translated[chunk]) {
            assert_eq!(
                sorted[new as usize].id, workers[old as usize].id,
                "chunk {chunk}: caller position {old} resolves to a different worker after translation",
            );
        }
    }

    // An all-reliable fleet partitions to the identity: nothing moves, nothing translates.
    let all_online = online_workers(3);
    let current: Vec<Vec<WorkerIndex>> = vec![vec![2, 0]];
    let (_, reliable, new_pos) = super::partition_reliable(&all_online);
    let translated = super::translate(&current, &new_pos);
    assert_eq!(reliable, 3);
    assert_eq!(translated, current);
}

/// The Part-1 differentiation, at the pipeline level. The SAME starved layout is scheduled twice,
/// differing only in the starved chunk's portal visibility, with every full worker's spare copy still
/// routed:
///   - **portal-visible** (an existing degraded chunk portals already route to): routing is soft, so
///     same-cycle preemption reclaims a still-routed above-floor copy and the chunk reaches its floor.
///   - **not visible** (a new chunk nobody reads yet): the only reclaimable space is routed, so
///     eviction is vetoed; the atomic floor can't be met and the chunk defers to zero (backpressure)
///     rather than sacrificing a live chunk's routed read. It regains a copy once space frees.
#[test]
fn a_portal_visible_chunk_that_lost_every_copy_keeps_what_fits() {
    let w = generate_workers(4);
    let workers = w
        .iter()
        .map(|&id| Worker {
            id,
            status: WorkerStatus::Online,
            version: None,
        })
        .collect_vec();
    let dataset = "s3://ds".to_string();
    let ids = ["c0", "c1", "c2", "c3"];

    // c0..c2 fill workers 0-2 to their 3000-byte cap; worker 3 is empty, so c3 fits on exactly one
    // worker — short of its floor of 2.
    let placed = |routed: bool| {
        let chunks: Vec<ScheduledChunk> = ids
            .iter()
            .map(|id| ScheduledChunk {
                dataset: &dataset,
                chunk_id: id,
                size: 1000,
                weight: 1,
                minimum_worker_version: None,
                is_portal_visible: routed && *id == "c3",
            })
            .collect();
        let mut current = vec![Vec::new(); ids.len()];
        for slot in current.iter_mut().take(3) {
            *slot = vec![w[0], w[1], w[2]];
        }
        let config = SchedulingConfig {
            worker_capacity: 3000,
            saturation: 0.9,
            min_replication: 2,
            ignore_reliability: true,
        };
        let (a, _) = schedule(
            &chunks,
            &workers,
            &config,
            &to_positions(&current, &workers),
            &mut WorkerRingCache::default(),
        )
        .unwrap();
        a.chunk_holders(ids.len())[3].len()
    };

    assert!(
        placed(true) >= 2,
        "a portal-visible degraded chunk reaches its floor by preempting a still-routed above-floor \
         copy (routing is soft for a visible chunk)",
    );
    assert_eq!(
        placed(false),
        0,
        "Part 1: a new chunk must not evict a routed copy to seat itself — with only routed space \
         reclaimable it defers via the atomic floor exclusion, rather than sacrificing a live \
         chunk's read",
    );
}

/// Between entering a worker assignment and being promoted, a chunk holds copies but is not yet
/// routed to. It is not new — rolling it back would tell workers to delete data they already have —
/// so holding nothing is required for the atomic floor, not merely being unrouted.
#[test]
fn an_unrouted_chunk_with_copies_keeps_them_when_its_floor_is_unreachable() {
    let w = generate_workers(4);
    let workers = w
        .iter()
        .map(|&id| Worker {
            id,
            status: WorkerStatus::Online,
            version: None,
        })
        .collect_vec();
    let dataset = "s3://ds".to_string();
    let ids = ["c0", "c1", "c2", "c3"];

    // c0..c2 fill workers 0-2; c3 holds one copy on the empty worker 3 and is not yet promoted, so
    // its floor of 2 is out of reach.
    let chunks: Vec<ScheduledChunk> = ids
        .iter()
        .map(|id| ScheduledChunk {
            dataset: &dataset,
            chunk_id: id,
            size: 1000,
            weight: 1,
            minimum_worker_version: None,
            is_portal_visible: false,
        })
        .collect();
    let mut current = vec![Vec::new(); ids.len()];
    for slot in current.iter_mut().take(3) {
        *slot = vec![w[0], w[1], w[2]];
    }
    current[3] = vec![w[3]];

    let config = SchedulingConfig {
        worker_capacity: 3000,
        saturation: 0.9,
        min_replication: 2,
        ignore_reliability: true,
    };
    let (a, _) = schedule(
        &chunks,
        &workers,
        &config,
        &to_positions(&current, &workers),
        &mut WorkerRingCache::default(),
    )
    .unwrap();

    assert!(
        !a.chunk_holders(ids.len())[3].is_empty(),
        "an unrouted chunk that already has copies must keep them, not be rolled back to zero",
    );
}

// ADR 0001 example 6: a genuine capacity shortage — the fleet cannot hold every chunk even at
// `min_replication`, and there is nothing above any floor to reclaim — must surface as backpressure
// (`NotEnoughCapacity`), never as a placement that seats one chunk by taking another below its floor.
#[test]
fn genuine_shortage_signals_backpressure_not_a_sub_floor_placement() {
    let workers = online_workers(3);
    let dataset = "s3://ds".to_string();
    // 4 chunks, floor 2 each = 8 replicas; 3 workers hold 2 slots each = 6. Cannot fit even the floor.
    let ids = ["c0", "c1", "c2", "c3"];
    let chunks: Vec<ScheduledChunk> = ids
        .iter()
        .map(|id| ScheduledChunk {
            dataset: &dataset,
            chunk_id: id,
            size: 1000,
            weight: 1,
            minimum_worker_version: None,
            is_portal_visible: false,
        })
        .collect();
    let n = chunks.len();
    let config = SchedulingConfig {
        worker_capacity: 2000,
        saturation: 1.0,
        min_replication: 2,
        ignore_reliability: true,
    };

    let result = schedule(
        &chunks,
        &workers,
        &config,
        &empty(n),
        &mut WorkerRingCache::default(),
    );
    assert!(
        result.is_err(),
        "a set that cannot be floored on the fleet must return NotEnoughCapacity (backpressure), \
         not a partial placement that seats some chunks below the floor",
    );
}

// ADR 0001: under contention several starved floors preempt above-floor copies, but no donor is ever
// taken below `min_replication` — and the copies it keeps are PRESENT (were already on disk), not
// freshly-targeted downloads. Three donors fill the fleet; two starved, portal-visible chunks must
// seat their floors by reclaiming the donors' above-floor (drain) copies (routing is soft for a
// visible chunk, so a still-routed above-floor copy may be reclaimed as a last resort).
#[test]
fn preemption_never_takes_a_donor_below_its_present_floor() {
    let w = generate_workers(4);
    let workers = w
        .iter()
        .map(|&id| Worker {
            id,
            status: WorkerStatus::Online,
            version: None,
        })
        .collect_vec();

    let dataset = "s3://ds".to_string();
    let ids = ["d0", "d1", "d2", "n0", "n1"];
    let chunks: Vec<ScheduledChunk> = ids
        .iter()
        .map(|id| ScheduledChunk {
            dataset: &dataset,
            chunk_id: id,
            size: 1000,
            weight: 1,
            minimum_worker_version: None,
            // The starved chunks are portal-visible so preemption of the donors' still-routed
            // above-floor copies is permitted (a new chunk would defer instead).
            is_portal_visible: id.starts_with('n'),
        })
        .collect();
    let n = chunks.len();

    // d0..d2 each sit on all four workers — 3 present copies per worker fill every 3000-byte disk.
    // Each donor's floor is 2 (weight 1), so 1 of its 3+ held copies is above-floor and reclaimable.
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

    let (a, _) = schedule(
        &chunks,
        &workers,
        &config,
        &to_positions(&current, &workers),
        &mut WorkerRingCache::default(),
    )
    .unwrap();
    let h = a.chunk_holders(n);

    // Every donor keeps >= min_replication copies, and >= min_replication of them are PRESENT (were
    // in `current`) — never stranded onto not-yet-downloaded targets.
    for di in 0..3 {
        let present = h[di].iter().filter(|p| current[di].contains(p)).count();
        assert!(
            present >= 2,
            "donor d{di} kept only {present} present copies — floor breached or stranded",
        );
    }
    // At least one starved chunk seated its floor by preemption (the fleet started full).
    assert!(
        h[3].len() >= 2 || h[4].len() >= 2,
        "no starved chunk reached its floor — preemption did not fire",
    );
    // No worker over capacity in the published placement.
    for (peer, bytes) in worker_bytes(&a, &chunks) {
        assert!(bytes <= 3000, "worker {peer} overcommitted: {bytes} > 3000");
    }
}

// --------------------------------------------------------------------------
// Focused reclaim-decision tests (ADR 0001)
//
// These drive the stage-2 reconcile directly, with a fixed test ring
// (`crate::rings::Rings::test_identity`) and hand-written `ideal`/`held` layouts, so each test pins
// exactly which replica the reclaim rule may drop — the precision the sim/PBT can't give.
// --------------------------------------------------------------------------

/// One chunk's full layout for [`drive_reconcile`]. `hashes[t]` is the worker the `t`-th replica's
/// ring walk starts at (see `Rings::test_identity`); supply at least `max(min_replication, |ideal|)`.
///
/// `committed` is the pre-cycle committed ideal — the durability floor the eviction guard must not
/// breach (a subset of `held`; the drains are `held \ committed`). `routed` is the confirmed routing
/// (best-effort victim ordering: a still-routed copy is evicted last).
struct ChunkLayout {
    size: u32,
    ideal: Vec<WorkerIndex>,
    held: Vec<WorkerIndex>,
    committed: Vec<WorkerIndex>,
    routed: Vec<WorkerIndex>,
    hashes: Vec<u64>,
    is_portal_visible: bool,
}

/// Run one reconcile over `layout`; return each chunk's published holders (worker indices, sorted)
/// and the evicted `(chunk, worker)` pairs.
fn drive_reconcile(
    n_workers: usize,
    capacity: u64,
    min_replication: crate::types::ReplicationFactor,
    layout: &[ChunkLayout],
) -> (
    Vec<Vec<WorkerIndex>>,
    Vec<(crate::types::ChunkIndex, WorkerIndex)>,
) {
    let ids = generate_workers(n_workers as u16);
    let workers: Vec<Worker> = ids
        .iter()
        .map(|&id| Worker {
            id,
            status: WorkerStatus::Online,
            version: None,
        })
        .collect();
    let worker_refs: Vec<&Worker> = workers.iter().collect();
    let rings = crate::rings::Rings::test_identity(n_workers);

    let dataset = "s3://ds".to_string();
    let chunk_ids: Vec<String> = (0..layout.len()).map(|i| format!("c{i}")).collect();
    // Phase B may grow a chunk up to `|ideal|` replicas, indexing `hashes[tag]` for each, so pad
    // every chunk's hashes to `max(min_replication, |ideal|)` (extra starts don't affect the floor).
    let padded_hashes: Vec<Vec<u64>> = layout
        .iter()
        .map(|l| {
            let needed = (min_replication as usize).max(l.ideal.len());
            let mut h = l.hashes.clone();
            while h.len() < needed {
                h.push((h.len() % n_workers) as u64);
            }
            h
        })
        .collect();
    let chunks: Vec<super::ReplicatedChunk> = layout
        .iter()
        .enumerate()
        .map(|(i, l)| super::ReplicatedChunk {
            dataset: &dataset,
            chunk_id: &chunk_ids[i],
            size: l.size,
            replication: l.ideal.len() as crate::types::ReplicationFactor,
            minimum_worker_version: None,
            is_portal_visible: l.is_portal_visible,
            hashes: padded_hashes[i].as_slice(),
        })
        .collect();

    let ideal: Vec<Vec<WorkerIndex>> = layout.iter().map(|l| l.ideal.clone()).collect();
    let held: Vec<Vec<WorkerIndex>> = layout.iter().map(|l| l.held.clone()).collect();
    let committed: Vec<Vec<WorkerIndex>> = layout.iter().map(|l| l.committed.clone()).collect();
    let routed: Vec<Vec<WorkerIndex>> = layout.iter().map(|l| l.routed.clone()).collect();
    let placement_order: Vec<usize> = (0..layout.len()).collect();

    let mut reconcile = super::Reconcile::new(
        &chunks,
        &worker_refs,
        &rings,
        capacity,
        super::PlacementViews {
            ideal: &ideal,
            held: &held,
            committed: &committed,
            routed: &routed,
        },
    );
    let (worker_chunks, _achieved, evicted) = reconcile.run(min_replication, &placement_order);

    let pos: std::collections::HashMap<PeerId, WorkerIndex> = ids
        .iter()
        .enumerate()
        .map(|(i, &id)| (id, i as WorkerIndex))
        .collect();
    let mut holders = vec![Vec::new(); layout.len()];
    for (peer, cs) in &worker_chunks {
        let w = pos[peer];
        for &ci in cs {
            holders[ci as usize].push(w);
        }
    }
    for h in holders.iter_mut() {
        h.sort_unstable();
    }
    (holders, evicted)
}

// Durability-hard / routing-best-effort (ADR 0001 as revised): reclaim may take a donor's
// above-committed-floor copy — even one the confirmed routing still addresses — but never its last
// committed copy. B has one committed copy (w2) and one drain (w1), min_replication 1; its new ideal
// is w0 (downloading). A starved, portal-visible C (an existing degraded chunk — the case where
// routing is soft ordering, not a veto) reclaims the drain on w1, but B's committed w2 is off-limits,
// so B stays served (durability floor kept). The transient stale route to w1 is bounded routing-lag,
// not a strand — B keeps a covered holder.
#[test]
fn reclaim_takes_a_drain_but_never_the_last_committed_copy() {
    let (holders, evicted) = drive_reconcile(
        3,
        1000,
        1,
        &[
            // B: new ideal w0 (downloading); committed copy on w2, drain on w1; both still routed.
            ChunkLayout {
                size: 1000,
                ideal: vec![0],
                held: vec![1, 2],
                committed: vec![2],
                routed: vec![1, 2],
                hashes: vec![0],
                is_portal_visible: false,
            },
            // C: starved & portal-visible, ring order w2,w0,w1 — lands by reclaiming B's still-routed
            // drain (routing is soft for a visible chunk), never B's committed floor copy.
            ChunkLayout {
                size: 1000,
                ideal: vec![2, 0, 1],
                held: vec![],
                committed: vec![],
                routed: vec![],
                hashes: vec![2, 0, 1],
                is_portal_visible: true,
            },
        ],
    );
    assert_eq!(
        evicted,
        vec![(0, 1)],
        "reclaim must take B's above-floor drain on w1, never its committed copy on w2; got {evicted:?}",
    );
    assert!(
        holders[0].contains(&2),
        "B must keep its committed floor copy on w2"
    );
    assert!(
        holders[1].contains(&1),
        "starved C reaches its floor on the reclaimed worker w1"
    );
}

// ADR 0001 (the guarantee behind the cumulative-eviction fix): several starved floors preempting the
// SAME donor must never take it below its floor. Donor X (min_rep 1) has ideal on all 5 workers but
// is only PRESENT on w2,w3,w4 (w0,w1 downloading); its floor lands on w0. Three starved chunks target
// w2,w3,w4. Without subtracting same-cycle evictions from the kept-floor count, all three of X's
// present copies get evicted and X is stranded to zero present copies. The fix keeps one.
#[test]
fn cumulative_preemption_never_strands_the_same_donor() {
    let (holders, evicted) = drive_reconcile(
        5,
        1000,
        1,
        &[
            // X: ideal everywhere, present+committed on w2,w3,w4; floor walks to w0 (a fresh download).
            ChunkLayout {
                size: 1000,
                ideal: vec![0, 1, 2, 3, 4],
                held: vec![2, 3, 4],
                committed: vec![2, 3, 4],
                routed: vec![2, 3, 4],
                hashes: vec![0],
                is_portal_visible: false,
            },
            // F: filler pinning w1 so the starved chunks can't use it for free.
            ChunkLayout {
                size: 1000,
                ideal: vec![1],
                held: vec![1],
                committed: vec![1],
                routed: vec![1],
                hashes: vec![1],
                is_portal_visible: false,
            },
            // S1,S2,S3: starved & portal-visible (so they may preempt X's still-routed copies),
            // targeting X's three present copies.
            ChunkLayout {
                size: 1000,
                ideal: vec![2, 3, 4, 0, 1],
                held: vec![],
                committed: vec![],
                routed: vec![],
                hashes: vec![2],
                is_portal_visible: true,
            },
            ChunkLayout {
                size: 1000,
                ideal: vec![3, 4, 0, 1, 2],
                held: vec![],
                committed: vec![],
                routed: vec![],
                hashes: vec![3],
                is_portal_visible: true,
            },
            ChunkLayout {
                size: 1000,
                ideal: vec![4, 0, 1, 2, 3],
                held: vec![],
                committed: vec![],
                routed: vec![],
                hashes: vec![4],
                is_portal_visible: true,
            },
        ],
    );
    let present_kept = holders[0]
        .iter()
        .filter(|w| [2u16, 3, 4].contains(w))
        .count();
    assert!(
        present_kept >= 1,
        "donor X was stranded: it kept {present_kept} of its present copies (w2,w3,w4). \
         evicted={evicted:?}, X holders={:?}",
        holders[0],
    );
}

// ADR 0001, example 1 (bonus): a chunk over-replicated by weight may be dropped toward — but never
// below — its floor to seat a starved chunk. X wants 5 (weight) and holds 5; min_rep 2; a starved,
// portal-visible S reclaims two of X's still-routed bonus copies (routing is soft for a visible
// chunk). X must end at >= 2 present, S at its floor.
#[test]
fn reclaim_drops_bonus_copies_to_floor_a_starved_chunk_but_not_below_floor() {
    let (holders, _evicted) = drive_reconcile(
        5,
        1000,
        2,
        &[
            // X: weight target 5, all present and committed.
            ChunkLayout {
                size: 1000,
                ideal: vec![0, 1, 2, 3, 4],
                held: vec![0, 1, 2, 3, 4],
                committed: vec![0, 1, 2, 3, 4],
                routed: vec![0, 1, 2, 3, 4],
                hashes: vec![0, 1, 2, 3, 4],
                is_portal_visible: false,
            },
            // S: starved & portal-visible, needing floor 2, targeting X's bonus copies on w3,w4.
            ChunkLayout {
                size: 1000,
                ideal: vec![3, 4, 0, 1, 2],
                held: vec![],
                committed: vec![],
                routed: vec![],
                hashes: vec![3, 4],
                is_portal_visible: true,
            },
        ],
    );
    let x_present = holders[0]
        .iter()
        .filter(|w| [0u16, 1, 2, 3, 4].contains(w))
        .count();
    assert!(
        x_present >= 2,
        "donor X dropped below its floor: only {x_present} copies left"
    );
    assert!(
        holders[1].len() >= 2,
        "starved S must reach its floor by reclaiming X's bonus copies"
    );
}

// ADR 0001, example 4 (at the floor): a chunk sitting at exactly `min_replication` is never robbed to
// seat another — the starved chunk stays short instead. Y holds exactly its floor of 2; a new C that
// can only land where Y sits must be excluded, and none of Y's copies evicted.
#[test]
fn reclaim_never_robs_a_donor_sitting_at_its_floor() {
    let (holders, evicted) = drive_reconcile(
        2,
        1000,
        2,
        &[
            // Y: at exactly its floor of 2, both copies present, committed and needed.
            ChunkLayout {
                size: 1000,
                ideal: vec![0, 1],
                held: vec![0, 1],
                committed: vec![0, 1],
                routed: vec![0, 1],
                hashes: vec![0, 1],
                is_portal_visible: false,
            },
            // C: starved new chunk — the fleet is full of Y's floor copies, nothing above a floor to take.
            ChunkLayout {
                size: 1000,
                ideal: vec![0, 1],
                held: vec![],
                committed: vec![],
                routed: vec![],
                hashes: vec![0, 1],
                is_portal_visible: false,
            },
        ],
    );
    assert!(
        evicted.is_empty(),
        "a floor copy must never be evicted; got {evicted:?}"
    );
    assert_eq!(
        holders[0],
        vec![0, 1],
        "donor Y must keep both floor copies"
    );
    assert!(
        holders[1].is_empty(),
        "starved C stays short rather than robbing a floor"
    );
}

// ADR 0001, example 3 (being removed): a chunk whose ideal is empty carries no floor obligation, so
// its leftover copies are freely reclaimable. Z is being removed (empty ideal) with a copy lingering
// on w1; a starved C reclaims it.
#[test]
fn reclaim_freely_takes_a_copy_of_a_chunk_being_removed() {
    let (holders, evicted) = drive_reconcile(
        2,
        1000,
        1,
        &[
            // Z: being removed — empty ideal and no committed floor, copies linger on w0,w1.
            ChunkLayout {
                size: 1000,
                ideal: vec![],
                held: vec![0, 1],
                committed: vec![],
                routed: vec![],
                hashes: vec![0],
                is_portal_visible: false,
            },
            // C: starved, wants w1 — reclaims Z's lingering copy there.
            ChunkLayout {
                size: 1000,
                ideal: vec![1],
                held: vec![],
                committed: vec![],
                routed: vec![],
                hashes: vec![1],
                is_portal_visible: false,
            },
        ],
    );
    assert!(
        evicted.contains(&(0, 1)),
        "a being-removed chunk's lingering copy must be reclaimable; evicted={evicted:?}"
    );
    assert!(
        holders[1].contains(&1),
        "starved C should seat its floor on the reclaimed worker"
    );
}

// Part 1 (focused, positive): a NEW (not-yet-portal-visible) chunk reclaims only NON-routed space.
// Donor X is committed+routed on w0 and carries a leftover drain on w1 the routing has already moved
// off. The fleet is full, so a starved new chunk C must reclaim the drain on w1 — permitted because
// w1 ∉ routed[X] — while X's routed floor copy on w0 stays untouched.
#[test]
fn new_chunk_reclaims_a_nonrouted_drain() {
    let (holders, evicted) = drive_reconcile(
        2,
        1000,
        1,
        &[
            // X: committed+routed floor on w0; a non-routed drain lingers on w1.
            ChunkLayout {
                size: 1000,
                ideal: vec![0],
                held: vec![0, 1],
                committed: vec![0],
                routed: vec![0],
                hashes: vec![0],
                is_portal_visible: false,
            },
            // C: starved NEW chunk, wants w1 — reclaims X's non-routed drain there.
            ChunkLayout {
                size: 1000,
                ideal: vec![1],
                held: vec![],
                committed: vec![],
                routed: vec![],
                hashes: vec![1],
                is_portal_visible: false,
            },
        ],
    );
    assert_eq!(
        evicted,
        vec![(0, 1)],
        "a new chunk must reclaim X's non-routed drain on w1; got {evicted:?}"
    );
    assert!(
        holders[0].contains(&0),
        "X keeps its committed, routed floor copy on w0"
    );
    assert!(
        holders[1].contains(&1),
        "starved new C seats its floor on the reclaimed worker w1"
    );
}

// Part 1 (focused, veto): the SAME layout but with X's copy on w1 still routed. A new chunk must NOT
// evict it (routed read of a live chunk is protected), so C stays short and nothing is evicted — while
// a portal-visible C in the same layout would preempt it (routing is soft). The visibility flag is the
// only difference.
#[test]
fn new_chunk_defers_from_a_routed_copy_but_visible_preempts() {
    let layout = |c_visible: bool| {
        [
            // X: committed floor on w0; a copy on w1 that the routing STILL addresses.
            ChunkLayout {
                size: 1000,
                ideal: vec![0],
                held: vec![0, 1],
                committed: vec![0],
                routed: vec![0, 1],
                hashes: vec![0],
                is_portal_visible: false,
            },
            // C: starved, wants w1 — its visibility decides whether the routed copy may be reclaimed.
            ChunkLayout {
                size: 1000,
                ideal: vec![1],
                held: vec![],
                committed: vec![],
                routed: vec![],
                hashes: vec![1],
                is_portal_visible: c_visible,
            },
        ]
    };

    let (holders_new, evicted_new) = drive_reconcile(2, 1000, 1, &layout(false));
    // Nothing force-evicted: the routed copy on w1 is not deleted (it may still drain normally over M,
    // so the read stays served), and C stays short rather than robbing it.
    assert!(
        evicted_new.is_empty(),
        "a new chunk must not evict a routed copy; got {evicted_new:?}"
    );
    assert!(
        holders_new[1].is_empty(),
        "starved new C stays short rather than robbing a routed read"
    );
    assert!(
        holders_new[0].contains(&0),
        "X keeps its committed floor copy on w0"
    );

    let (holders_vis, evicted_vis) = drive_reconcile(2, 1000, 1, &layout(true));
    assert_eq!(
        evicted_vis,
        vec![(0, 1)],
        "a portal-visible C may preempt the routed copy (soft); got {evicted_vis:?}"
    );
    assert!(
        holders_vis[1].contains(&1),
        "portal-visible C seats its floor on w1"
    );
    assert!(
        holders_vis[0].contains(&0),
        "X keeps its committed floor copy on w0"
    );
}
