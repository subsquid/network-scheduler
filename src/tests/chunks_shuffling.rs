// All tests use 50 workers and 20,000 chunks of 100 MiB each (total ≈ 1.9 TiB),
// with min_replication=1 unless noted otherwise.

use std::collections::{BTreeMap, BTreeSet};

use bytesize::ByteSize;
use semver::Version;

use super::input::{generate_chunks_fixed_size, generate_workers};
use crate::{
    scheduling::{ScheduledChunk, SchedulingConfig, schedule},
    types::{Assignment, Worker, WorkerStatus},
};

// 1. One of 50 workers has its PeerId rotated (equivalently: replaced; count unchanged). The new
//    PeerId downloads its share from S3; hash-ring redistribution cascades onto existing workers.
#[test]
fn test_shuffle_worker_replaced() {
    const NUM_WORKERS: u16 = 50;
    let worker_ids = generate_workers(NUM_WORKERS + 1);
    let original_workers = make_workers(&worker_ids[..NUM_WORKERS as usize]);
    let workers_with_replacement = {
        let mut ws = make_workers(&worker_ids[..NUM_WORKERS as usize]);
        ws[NUM_WORKERS as usize - 1] = Worker {
            id: worker_ids[NUM_WORKERS as usize],
            status: WorkerStatus::Online,
            version: None,
        };
        ws
    };

    let test_data = generate_chunks_fixed_size(20_000, &[1, 4], ByteSize::mib(100));
    let chunks = test_data.as_scheduled();
    let config = r1_config(total_chunks_size(&chunks), NUM_WORKERS);

    let before_replacement = schedule(&chunks, &original_workers, config.clone()).unwrap();
    let after_replacement = schedule(&chunks, &workers_with_replacement, config).unwrap();

    assert_eq!(
        before_replacement.replication_by_weight, after_replacement.replication_by_weight,
        "Replication factors must stay the same"
    );

    let breakdown =
        download_breakdown_same_chunks(&chunks, &before_replacement, &after_replacement);
    assert_eq!(breakdown.new_chunk_bytes, ByteSize(0));
    assert_eq!(breakdown.increased_replication_bytes, ByteSize(0));
    assert_eq!(as_gib(breakdown.shuffled_bytes), 75);
    assert_eq!(breakdown.shuffled_count, 775);

    let replacement_worker_id = worker_ids[NUM_WORKERS as usize];
    let downloads_per_worker =
        per_worker_downloads(&chunks, &before_replacement, &after_replacement);
    let replacement_worker_downloads = downloads_per_worker
        .get(&replacement_worker_id)
        .copied()
        .unwrap_or(ByteSize(0));
    let existing_workers_downloads = ByteSize(
        downloads_per_worker
            .iter()
            .filter(|(id, _)| **id != replacement_worker_id)
            .map(|(_, b)| b.as_u64())
            .sum(),
    );
    assert_eq!(as_gib(replacement_worker_downloads), 38);
    assert_eq!(as_gib(existing_workers_downloads), 36);
}

// 2a. 100 chunks removed from the middle: hash positions are ID-based, so no existing chunk
//     changes owner.
#[test]
fn test_shuffle_chunk_removed_from_middle() {
    const NUM_WORKERS: u16 = 50;
    let worker_ids = generate_workers(NUM_WORKERS);
    let workers = make_workers(&worker_ids);

    let test_data = generate_chunks_fixed_size(20_000, &[1], ByteSize::mib(100));
    let original_chunks = test_data.as_scheduled();
    let chunks_with_removal: Vec<ScheduledChunk> = original_chunks
        .iter()
        .enumerate()
        .filter(|(i, _)| !(10_000..10_100).contains(i))
        .map(|(_, c)| c.clone())
        .collect();

    let config = r1_config(total_chunks_size(&original_chunks), NUM_WORKERS);

    let before_removal = schedule(&original_chunks, &workers, config.clone()).unwrap();
    let after_removal = schedule(&chunks_with_removal, &workers, config).unwrap();

    assert_eq!(
        before_removal.replication_by_weight, after_removal.replication_by_weight,
        "Replication factors must stay the same"
    );

    let breakdown = download_breakdown_different_chunks(
        &original_chunks,
        &chunks_with_removal,
        &before_removal,
        &after_removal,
    );
    assert_eq!(breakdown.total(), ByteSize(0));
    assert_eq!(breakdown.shuffled_count, 0);
}

// 2b. 100 chunks inserted in the middle: only the inserted chunks need downloading.
#[test]
fn test_shuffle_chunk_added_in_middle() {
    const NUM_WORKERS: u16 = 50;
    let worker_ids = generate_workers(NUM_WORKERS);
    let workers = make_workers(&worker_ids);

    let all_chunks = generate_chunks_fixed_size(20_100, &[1], ByteSize::mib(100));
    let all_scheduled = all_chunks.as_scheduled();
    let original_chunks: Vec<ScheduledChunk> = all_scheduled[..20_000].to_vec();
    let mut chunks_with_insertion = Vec::with_capacity(20_100);
    chunks_with_insertion.extend_from_slice(&all_scheduled[..10_000]);
    chunks_with_insertion.extend_from_slice(&all_scheduled[20_000..20_100]);
    chunks_with_insertion.extend_from_slice(&all_scheduled[10_000..20_000]);

    let inserted_size = total_chunks_size(&all_scheduled[20_000..20_100]);
    let config = r1_config(total_chunks_size(&chunks_with_insertion), NUM_WORKERS);

    let before_insertion = schedule(&original_chunks, &workers, config.clone()).unwrap();
    let after_insertion = schedule(&chunks_with_insertion, &workers, config).unwrap();

    assert_eq!(
        before_insertion.replication_by_weight, after_insertion.replication_by_weight,
        "Replication factors must stay the same"
    );

    let breakdown = download_breakdown_different_chunks(
        &original_chunks,
        &chunks_with_insertion,
        &before_insertion,
        &after_insertion,
    );
    assert_eq!(breakdown.shuffled_bytes, ByteSize(0));
    assert_eq!(breakdown.increased_replication_bytes, ByteSize(0));
    assert_eq!(breakdown.shuffled_count, 0);
    assert_eq!(breakdown.new_chunk_bytes, inserted_size);
    assert_eq!(breakdown.new_chunk_bytes, ByteSize::mib(100) * 100u64);
}

// 2c. Relative-weight boundary shift (production-style `from: -N, weight: 12` rule): appending
//     100 chunks slides the "last TAIL_SIZE" boundary, so chunks [15000..15100] drop from
//     weight 12 to 1. With R(1)=1 and R(12)=3, consistent hashing keeps every existing
//     placement stable — the only downloads are the 100 new tail chunks at 3 replicas each.
#[test]
fn test_shuffle_chunk_appended_with_relative_weight_boundary() {
    const NUM_WORKERS: u16 = 50;
    const TAIL_SIZE: usize = 5000;
    let worker_ids = generate_workers(NUM_WORKERS);
    let workers = make_workers(&worker_ids);

    let all_chunks = generate_chunks_fixed_size(20_100, &[1], ByteSize::mib(100));
    let all_scheduled = all_chunks.as_scheduled();
    let mut original_chunks: Vec<ScheduledChunk> = all_scheduled[..20_000].to_vec();
    for c in &mut original_chunks[20_000 - TAIL_SIZE..] {
        c.weight = 12;
    }
    let mut appended_chunks: Vec<ScheduledChunk> = all_scheduled.to_vec();
    for c in &mut appended_chunks[20_100 - TAIL_SIZE..] {
        c.weight = 12;
    }

    // Capacity tuned so R(1)=1 and R(12)=3.
    let total_size = total_chunks_size(&appended_chunks);
    let config = SchedulingConfig {
        worker_capacity: 3 * total_size.as_u64() / NUM_WORKERS as u64,
        saturation: 0.5,
        min_replication: 1,
        ignore_reliability: true,
    };

    let before = schedule(&original_chunks, &workers, config.clone()).unwrap();
    let after = schedule(&appended_chunks, &workers, config).unwrap();

    assert_eq!(before.replication_by_weight, after.replication_by_weight);
    assert_eq!(after.replication_by_weight[&1], 1);
    assert_eq!(after.replication_by_weight[&12], 3);

    let breakdown =
        download_breakdown_different_chunks(&original_chunks, &appended_chunks, &before, &after);
    assert_eq!(breakdown.shuffled_bytes, ByteSize(0));
    assert_eq!(breakdown.increased_replication_bytes, ByteSize(0));
    assert_eq!(breakdown.shuffled_count, 0);
    // 100 new tail chunks * 100 MiB * R(w=12)=3 replicas.
    assert_eq!(
        breakdown.new_chunk_bytes,
        ByteSize::mib(100) * 100u64 * 3u64
    );
}

// 2d. Same boundary shift as 2c but with workers ~98% full: the freed load reshapes the
//     bounded-load landscape enough that some replicas snap to different workers — cascading
//     shuffle even though replication factors are stable.
#[test]
fn test_shuffle_boundary_shift_tight_capacity_causes_cascade() {
    const NUM_WORKERS: u16 = 50;
    const TAIL_SIZE: usize = 5000;
    let worker_ids = generate_workers(NUM_WORKERS);
    let workers = make_workers(&worker_ids);

    let all_chunks = generate_chunks_fixed_size(20_100, &[1], ByteSize::mib(100));
    let all_scheduled = all_chunks.as_scheduled();
    let mut original_chunks: Vec<ScheduledChunk> = all_scheduled[..20_000].to_vec();
    for c in &mut original_chunks[20_000 - TAIL_SIZE..] {
        c.weight = 12;
    }
    let mut appended_chunks: Vec<ScheduledChunk> = all_scheduled.to_vec();
    for c in &mut appended_chunks[20_100 - TAIL_SIZE..] {
        c.weight = 12;
    }

    // ≈2940 GiB replicated / 50 workers → 58.8 GiB avg vs a 60 GiB cap (~98% full) — hash
    // variance pushes some workers over, forcing bounded-load spillover.
    let config = SchedulingConfig {
        worker_capacity: ByteSize::gib(60).as_u64(),
        saturation: 1.0,
        min_replication: 1,
        ignore_reliability: true,
    };

    let before = schedule(&original_chunks, &workers, config.clone()).unwrap();
    let after = schedule(&appended_chunks, &workers, config).unwrap();

    assert_eq!(
        before.replication_by_weight, after.replication_by_weight,
        "Replication factors must stay the same"
    );
    assert_eq!(after.replication_by_weight[&1], 1);
    assert_eq!(after.replication_by_weight[&12], 3);

    let breakdown =
        download_breakdown_different_chunks(&original_chunks, &appended_chunks, &before, &after);

    assert_eq!(breakdown.increased_replication_bytes, ByteSize(0));
    // The replication-back-off distributor spills less than the previous algorithm under
    // the same near-full landscape, so the boundary shift still cascades but moves less.
    assert_eq!(as_gib(breakdown.shuffled_bytes), 4);
    assert_eq!(breakdown.shuffled_count, 51);
    // 100 new tail chunks * 100 MiB * R(w=12)=3 replicas.
    assert_eq!(
        breakdown.new_chunk_bytes,
        ByteSize::mib(100) * 100u64 * 3u64
    );
}

// 3. 5 workers upgrade v2.7 -> v2.8; 500 chunks require v2.8+, so some version-restricted
//    chunks migrate to the newly eligible workers.
#[test]
fn test_shuffle_worker_version_upgrade() {
    let min_ver = Version::new(2, 8, 0);
    const NUM_WORKERS: u16 = 50;
    const NUM_VERSIONED: usize = 500;

    let worker_ids = generate_workers(NUM_WORKERS);
    let scheduled = generate_chunks_fixed_size(20_000, &[1], ByteSize::mib(100));
    let mut chunks = scheduled.as_scheduled();
    for chunk in &mut chunks[..NUM_VERSIONED] {
        chunk.minimum_worker_version = Some(&min_ver);
    }

    let workers_before_upgrade: Vec<Worker> = worker_ids
        .iter()
        .enumerate()
        .map(|(i, &id)| Worker {
            id,
            status: WorkerStatus::Online,
            version: Some(if i < 40 {
                Version::new(2, 8, 0)
            } else {
                Version::new(2, 7, 0)
            }),
        })
        .collect();

    let workers_after_upgrade: Vec<Worker> = worker_ids
        .iter()
        .enumerate()
        .map(|(i, &id)| Worker {
            id,
            status: WorkerStatus::Online,
            version: Some(if i < 45 {
                Version::new(2, 8, 0)
            } else {
                Version::new(2, 7, 0)
            }),
        })
        .collect();

    let config = r1_config(total_chunks_size(&chunks), NUM_WORKERS);

    let before_upgrade = schedule(&chunks, &workers_before_upgrade, config.clone()).unwrap();
    let after_upgrade = schedule(&chunks, &workers_after_upgrade, config).unwrap();

    assert_eq!(
        before_upgrade.replication_by_weight, after_upgrade.replication_by_weight,
        "Replication factors must stay the same"
    );

    let breakdown = download_breakdown_same_chunks(&chunks, &before_upgrade, &after_upgrade);
    assert_eq!(breakdown.new_chunk_bytes, ByteSize(0));
    assert_eq!(breakdown.increased_replication_bytes, ByteSize(0));
    assert_eq!(as_gib(breakdown.shuffled_bytes), 5);
    assert_eq!(breakdown.shuffled_count, 57);
}

// 4. 5 workers flip Stale -> Online. The all-workers ring is identical in both runs, so zero
//    downloads: the reliable-only sub-ring reshuffle is invisible because the all-workers pass
//    already placed those chunks.
#[test]
fn test_shuffle_reliability_flip() {
    const NUM_WORKERS: u16 = 50;
    const NUM_FLIPPING: usize = 5;

    let worker_ids = generate_workers(NUM_WORKERS);
    let scheduled = generate_chunks_fixed_size(20_000, &[1], ByteSize::mib(100));
    let chunks = scheduled.as_scheduled();
    let total_size = total_chunks_size(&chunks);

    let mut workers_with_unreliable: Vec<Worker> = worker_ids
        .iter()
        .map(|&id| Worker {
            id,
            status: WorkerStatus::Online,
            version: None,
        })
        .collect();
    for w in &mut workers_with_unreliable[..NUM_FLIPPING] {
        w.status = WorkerStatus::Stale;
    }

    let mut workers_all_reliable = workers_with_unreliable.clone();
    for w in &mut workers_all_reliable[..NUM_FLIPPING] {
        w.status = WorkerStatus::Online;
    }

    // K_before = 3 * 45/50 * 0.4 = 1.08 → R=1
    // K_after  = 3 * 50/50 * 0.4 = 1.20 → R=1
    let config = SchedulingConfig {
        worker_capacity: 3 * total_size.as_u64() / NUM_WORKERS as u64,
        saturation: 0.4,
        min_replication: 1,
        ignore_reliability: false,
    };

    let before_flip = schedule(&chunks, &workers_with_unreliable, config.clone()).unwrap();
    let after_flip = schedule(&chunks, &workers_all_reliable, config).unwrap();

    assert_eq!(
        before_flip.replication_by_weight, after_flip.replication_by_weight,
        "Replication factors must stay the same"
    );

    let breakdown = download_breakdown_same_chunks(&chunks, &before_flip, &after_flip);
    assert_eq!(breakdown.total(), ByteSize(0));
    assert_eq!(breakdown.shuffled_count, 0);
}

// 5. A 51st worker joins; capacity keeps R=1. The new worker absorbs chunks from the ring,
//    causing downloads on it and migrations among existing workers.
#[test]
fn test_shuffle_worker_added_stable_replication() {
    const NUM_WORKERS: u16 = 50;
    let worker_ids = generate_workers(NUM_WORKERS + 1);
    let original_workers = make_workers(&worker_ids[..NUM_WORKERS as usize]);
    let workers_with_new = make_workers(&worker_ids[..(NUM_WORKERS + 1) as usize]);

    let scheduled = generate_chunks_fixed_size(20_000, &[1], ByteSize::mib(100));
    let chunks = scheduled.as_scheduled();
    // K(N=50) = 3 * 0.4 = 1.2 → R=1
    // K(N=51) = 3 * 51/50 * 0.4 = 1.224 → R=1
    let config = r1_config(total_chunks_size(&chunks), NUM_WORKERS);

    let before_addition = schedule(&chunks, &original_workers, config.clone()).unwrap();
    let after_addition = schedule(&chunks, &workers_with_new, config).unwrap();

    assert_eq!(
        before_addition.replication_by_weight, after_addition.replication_by_weight,
        "Replication factors must stay the same"
    );

    let breakdown = download_breakdown_same_chunks(&chunks, &before_addition, &after_addition);
    assert_eq!(breakdown.new_chunk_bytes, ByteSize(0));
    assert_eq!(breakdown.increased_replication_bytes, ByteSize(0));
    assert_eq!(as_gib(breakdown.shuffled_bytes), 37);
    assert_eq!(breakdown.shuffled_count, 388);
}

// 6a/6b. Test 1's replacement at different saturation levels: higher saturation leaves less
//        spare capacity, amplifying cascade when chunks spill over to ring neighbors.

#[test]
fn test_shuffle_worker_replaced_loose_saturation() {
    let breakdown = replace_one_worker_with_saturation(0.70);
    assert_eq!(breakdown.new_chunk_bytes, ByteSize(0));
    assert_eq!(breakdown.increased_replication_bytes, ByteSize(0));
    assert_eq!(as_gib(breakdown.shuffled_bytes), 75);
    assert_eq!(breakdown.shuffled_count, 775);
}

#[test]
fn test_shuffle_worker_replaced_tight_saturation() {
    let breakdown = replace_one_worker_with_saturation(0.99);
    assert_eq!(breakdown.new_chunk_bytes, ByteSize(0));
    assert_eq!(breakdown.increased_replication_bytes, ByteSize(0));
    assert_eq!(as_gib(breakdown.shuffled_bytes), 77);
    assert_eq!(breakdown.shuffled_count, 798);
}

// Utility functions

/// Breakdown of downloads by cause.
#[derive(Debug, Default)]
struct DownloadBreakdown {
    /// Bytes downloaded because a chunk moved from one worker to another.
    shuffled_bytes: ByteSize,
    /// Bytes downloaded for chunks that are new to the system.
    new_chunk_bytes: ByteSize,
    /// Bytes downloaded for additional replicas (R increased for existing chunks).
    increased_replication_bytes: ByteSize,
    /// Number of chunks that changed owner.
    shuffled_count: usize,
}

impl DownloadBreakdown {
    fn total(&self) -> ByteSize {
        ByteSize(
            self.shuffled_bytes.as_u64()
                + self.new_chunk_bytes.as_u64()
                + self.increased_replication_bytes.as_u64(),
        )
    }
}

/// Per-worker bytes to fetch (chunks in `after` not in `before`); chunk list must be the SAME
/// between runs.
fn per_worker_downloads(
    chunks: &[ScheduledChunk],
    before: &Assignment,
    after: &Assignment,
) -> BTreeMap<libp2p_identity::PeerId, ByteSize> {
    let mut result = BTreeMap::new();
    for (worker, idxs_after) in &after.worker_chunks {
        let empty = vec![];
        let idxs_before = before.worker_chunks.get(worker).unwrap_or(&empty);
        let set_before: BTreeSet<_> = idxs_before.iter().copied().collect();
        let mut worker_total = 0u64;
        for &idx in idxs_after {
            if !set_before.contains(&idx) {
                worker_total += chunks[idx as usize].size as u64;
            }
        }
        result.insert(*worker, ByteSize(worker_total));
    }
    result
}

/// Download breakdown when the chunk list is the SAME between runs: per chunk, gains matched by
/// losses count as shuffled, extra copies beyond the previous replica count as replication.
fn download_breakdown_same_chunks(
    chunks: &[ScheduledChunk],
    before: &Assignment,
    after: &Assignment,
) -> DownloadBreakdown {
    let chunk_workers_before = before.chunk_holders(chunks.len());
    let chunk_workers_after = after.chunk_holders(chunks.len());

    let mut result = DownloadBreakdown::default();
    for (idx, workers_after) in chunk_workers_after.iter().enumerate() {
        let size = chunks[idx].size as u64;
        let workers_before = &chunk_workers_before[idx];
        let gained: usize = workers_after.difference(workers_before).count();
        let lost: usize = workers_before.difference(workers_after).count();

        let shuffled = gained.min(lost);
        let replication = gained.saturating_sub(lost);
        if shuffled > 0 {
            result.shuffled_count += shuffled;
        }
        result.shuffled_bytes = ByteSize(result.shuffled_bytes.as_u64() + size * shuffled as u64);
        result.increased_replication_bytes =
            ByteSize(result.increased_replication_bytes.as_u64() + size * replication as u64);
    }
    result
}

/// Download breakdown when the chunk list DIFFERS between runs (insert/remove): compares by
/// chunk ID so index shifts don't matter.
fn download_breakdown_different_chunks(
    chunks_before: &[ScheduledChunk],
    chunks_after: &[ScheduledChunk],
    before: &Assignment,
    after: &Assignment,
) -> DownloadBreakdown {
    let mut chunk_workers_before: BTreeMap<(&str, &str), BTreeSet<libp2p_identity::PeerId>> =
        BTreeMap::new();
    for (&worker, idxs) in &before.worker_chunks {
        for &idx in idxs {
            let c = &chunks_before[idx as usize];
            chunk_workers_before
                .entry((c.dataset, c.chunk_id))
                .or_default()
                .insert(worker);
        }
    }

    let mut chunk_workers_after: BTreeMap<(&str, &str), BTreeSet<libp2p_identity::PeerId>> =
        BTreeMap::new();
    for (&worker, idxs) in &after.worker_chunks {
        for &idx in idxs {
            let c = &chunks_after[idx as usize];
            chunk_workers_after
                .entry((c.dataset, c.chunk_id))
                .or_default()
                .insert(worker);
        }
    }

    let size_lookup: BTreeMap<(&str, &str), u64> = chunks_after
        .iter()
        .map(|c| ((c.dataset, c.chunk_id), c.size as u64))
        .collect();

    let mut result = DownloadBreakdown::default();
    for (chunk_id, workers_after) in &chunk_workers_after {
        let size = size_lookup.get(chunk_id).copied().unwrap_or(0);

        if let Some(workers_before) = chunk_workers_before.get(chunk_id) {
            let gained: usize = workers_after.difference(workers_before).count();
            let lost: usize = workers_before.difference(workers_after).count();

            let shuffled = gained.min(lost);
            let replication = gained.saturating_sub(lost);
            if shuffled > 0 {
                result.shuffled_count += shuffled;
            }
            result.shuffled_bytes =
                ByteSize(result.shuffled_bytes.as_u64() + size * shuffled as u64);
            result.increased_replication_bytes =
                ByteSize(result.increased_replication_bytes.as_u64() + size * replication as u64);
        } else {
            result.new_chunk_bytes =
                ByteSize(result.new_chunk_bytes.as_u64() + size * workers_after.len() as u64);
        }
    }
    result
}

fn make_workers(ids: &[libp2p_identity::PeerId]) -> Vec<Worker> {
    ids.iter()
        .map(|&id| Worker {
            id,
            status: WorkerStatus::Online,
            version: None,
        })
        .collect()
}

/// Config that produces min_rep=1 for all weights.
/// K = worker_capacity * n * saturation / total_size = 3 * 0.4 = 1.2 → round(1.2) = 1.
/// Capacity check: total_capacity = 1.2 * total_size ≥ total_size ✓
fn r1_config(total_size: ByteSize, n_workers: u16) -> SchedulingConfig {
    SchedulingConfig {
        worker_capacity: 3 * total_size.as_u64() / n_workers as u64,
        saturation: 0.4,
        min_replication: 1,
        ignore_reliability: true,
    }
}

fn total_chunks_size(chunks: &[ScheduledChunk]) -> ByteSize {
    ByteSize(chunks.iter().map(|c| c.size as u64).sum())
}

fn as_gib(size: ByteSize) -> u64 {
    size.as_u64() / ByteSize::gib(1).as_u64()
}

fn replace_one_worker_with_saturation(saturation: f64) -> DownloadBreakdown {
    const NUM_WORKERS: u16 = 50;
    let worker_ids = generate_workers(NUM_WORKERS + 1);
    let original_workers = make_workers(&worker_ids[..NUM_WORKERS as usize]);
    let mut workers_with_replacement = original_workers.clone();
    workers_with_replacement[NUM_WORKERS as usize - 1] = Worker {
        id: worker_ids[NUM_WORKERS as usize],
        status: WorkerStatus::Online,
        version: None,
    };

    let test_data = generate_chunks_fixed_size(20_000, &[1], ByteSize::mib(100));
    let chunks = test_data.as_scheduled();
    let total_size = total_chunks_size(&chunks);

    let config = SchedulingConfig {
        worker_capacity: (total_size.as_u64() as f64 / NUM_WORKERS as f64 / saturation * 1.05)
            as u64,
        min_replication: 1,
        saturation,
        ignore_reliability: true,
    };

    let before_replacement = schedule(&chunks, &original_workers, config.clone()).unwrap();
    let after_replacement = schedule(&chunks, &workers_with_replacement, config).unwrap();
    assert_eq!(
        before_replacement.replication_by_weight, after_replacement.replication_by_weight,
        "Replication factors must stay the same"
    );

    download_breakdown_same_chunks(&chunks, &before_replacement, &after_replacement)
}
