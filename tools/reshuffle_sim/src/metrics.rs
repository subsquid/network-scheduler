//! Reshuffle metrics and the assignment diffing that produces them.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use crate::simulation::StepPlacement;
use crate::{ChunkId, ChunkOwners, ChunkSizeIndex, DrainedOwners, WorkerIdx};

/// Metrics for a single simulation step.
pub struct ReshuffleMetrics {
    pub step: u32,
    pub new_chunks_in_step: u32,
    /// How many of this step's new chunks are version-restricted.
    pub new_restricted_in_step: u32,
    pub total_chunks: usize,
    /// Cumulative count of version-restricted chunks.
    pub total_restricted_chunks: usize,
    pub replication_by_weight: BTreeMap<u16, u16>,
    pub data_movement: DataMovement,
    pub total_capacity_bytes: u64,
    pub used_capacity_bytes: u64,
    /// Of `used_capacity_bytes`, the bytes held by draining stale copies (0 on the stateless path).
    pub stale_capacity_bytes: u64,
    pub eligible_workers: usize,
    /// False when scheduling failed at this step; the run stops after.
    pub scheduled: bool,
    pub failure_reason: Option<String>,
    /// Data movement restricted to version-restricted chunks only.
    pub restricted_movement: DataMovement,
    /// Wall-clock time the scheduler spent on this step.
    pub schedule_duration: Duration,
}

/// Data movement between two assignments, classified by cause. Every byte counted here is a real S3
/// download or a real physical free: a copy that stays on disk while only flipping between the ideal
/// and stale ledgers contributes nothing.
#[derive(Default)]
pub struct DataMovement {
    pub new_chunk_bytes: u64,
    pub shuffled_bytes: u64,
    pub shuffled_count: u32,
    /// Bytes re-downloaded onto a worker that physically dropped the copy this cycle (drain→refetch),
    /// then had it re-placed. Distinct from `increased_replication_bytes`: replication did not grow
    /// (the worker leaves as a stale copy and returns as an ideal one), but the bytes are refetched.
    pub refetched_bytes: u64,
    pub refetched_count: u32,
    pub increased_replication_bytes: u64,
    pub decreased_replication_bytes: u64,
    pub workers_receiving_new: usize,
    pub workers_losing: usize,
    pub workers_shuffled: usize,
}

impl DataMovement {
    pub fn zero() -> Self {
        DataMovement::default()
    }

    /// Total S3 download this step: every copy a worker had to fetch — brand-new chunks, shuffles
    /// onto a new worker, net replication increase, and same-worker refetches after a drain.
    pub fn total_download(&self) -> u64 {
        self.new_chunk_bytes
            + self.shuffled_bytes
            + self.increased_replication_bytes
            + self.refetched_bytes
    }
}

/// Per-step counters shared by the success and failure metric builders.
pub struct StepStats {
    pub step: u32,
    pub new_chunks: u32,
    pub new_restricted: u32,
    pub eligible_workers: usize,
}

/// Computes a step's metrics by diffing the step's placement against the
/// previous one. Returns the current chunk owners for use as the next step's
/// previous.
pub fn measure_reshuffle(
    previous_owners: &ChunkOwners,
    placement: StepPlacement,
    restricted: &HashSet<ChunkId>,
    stats: StepStats,
    total_capacity_bytes: u64,
) -> (ReshuffleMetrics, ChunkOwners) {
    let StepPlacement {
        owners: current_owners,
        chunk_sizes,
        drained,
        replication_by_weight,
        used_capacity_bytes,
        stale_capacity_bytes,
        total_chunks,
        schedule_duration,
    } = placement;

    let data_movement =
        compute_data_movement(previous_owners, &current_owners, &chunk_sizes, &drained);
    let restricted_movement = compute_data_movement(
        &filter_owners(previous_owners, restricted),
        &filter_owners(&current_owners, restricted),
        &chunk_sizes,
        &filter_owners(&drained, restricted),
    );

    let metrics = ReshuffleMetrics {
        step: stats.step,
        new_chunks_in_step: stats.new_chunks,
        new_restricted_in_step: stats.new_restricted,
        total_chunks,
        total_restricted_chunks: restricted.len(),
        replication_by_weight,
        data_movement,
        total_capacity_bytes,
        used_capacity_bytes,
        stale_capacity_bytes,
        eligible_workers: stats.eligible_workers,
        scheduled: true,
        failure_reason: None,
        restricted_movement,
        schedule_duration,
    };

    (metrics, current_owners)
}

/// Metrics for the step at which scheduling failed: zero movement, and the run
/// stops afterwards.
pub fn failed_step_metrics(
    stats: StepStats,
    total_chunks: usize,
    total_restricted_chunks: usize,
    total_capacity_bytes: u64,
    reason: String,
) -> ReshuffleMetrics {
    ReshuffleMetrics {
        step: stats.step,
        new_chunks_in_step: stats.new_chunks,
        new_restricted_in_step: stats.new_restricted,
        total_chunks,
        total_restricted_chunks,
        replication_by_weight: BTreeMap::new(),
        data_movement: DataMovement::zero(),
        total_capacity_bytes,
        used_capacity_bytes: 0,
        stale_capacity_bytes: 0,
        eligible_workers: stats.eligible_workers,
        scheduled: false,
        failure_reason: Some(reason),
        restricted_movement: DataMovement::zero(),
        schedule_duration: Duration::ZERO,
    }
}

/// Keeps only the owner entries whose chunk id is in `restricted`.
fn filter_owners(owners: &ChunkOwners, restricted: &HashSet<ChunkId>) -> ChunkOwners {
    owners
        .iter()
        .filter(|(id, _)| restricted.contains(*id))
        .map(|(id, workers)| (id.clone(), workers.clone()))
        .collect()
}

/// Classifies data movement between two assignments by cause. `previous`/`current` are *physical*
/// holder sets (ideal ∪ stale); `drained` is the copies the cycle physically expired this step.
/// - a chunk only in `current` is new — all its replicas are downloads;
/// - a chunk in both: a worker only in `current` gained a copy (download), one only in `previous`
///   lost it (freed), and one in both that was `drained` this cycle was re-fetched onto the same
///   worker (a refetch download the raw set-diff can't see);
/// - a chunk only in `previous` was removed — its replicas are freed.
fn compute_data_movement(
    previous: &ChunkOwners,
    current: &ChunkOwners,
    chunk_sizes: &ChunkSizeIndex,
    drained: &DrainedOwners,
) -> DataMovement {
    let mut movement = Movement::default();
    let no_drain: Vec<WorkerIdx> = Vec::new();

    for (chunk_id, current_workers) in current {
        let size = chunk_size(chunk_sizes, chunk_id);
        match previous.get(chunk_id) {
            Some(previous_workers) => movement.record_existing(
                size,
                previous_workers,
                current_workers,
                drained.get(chunk_id).unwrap_or(&no_drain),
            ),
            None => movement.record_added(size, current_workers),
        }
    }
    for (chunk_id, previous_workers) in previous {
        if !current.contains_key(chunk_id) {
            movement.record_removed(chunk_size(chunk_sizes, chunk_id), previous_workers);
        }
    }

    movement.finish()
}

fn chunk_size(chunk_sizes: &ChunkSizeIndex, chunk_id: &ChunkId) -> u64 {
    *chunk_sizes.get(chunk_id).unwrap_or(&0) as u64
}

/// Accumulates data movement while iterating chunks, tracking distinct affected
/// workers as sets before collapsing to counts in [`Movement::finish`].
#[derive(Default)]
struct Movement {
    new_chunk_bytes: u64,
    shuffled_bytes: u64,
    shuffled_count: u32,
    refetched_bytes: u64,
    refetched_count: u32,
    increased_replication_bytes: u64,
    decreased_replication_bytes: u64,
    // Distinct affected workers; only their final `.len()` is read, so unordered.
    workers_receiving_new: HashSet<WorkerIdx>,
    workers_losing: HashSet<WorkerIdx>,
    workers_shuffled: HashSet<WorkerIdx>,
}

impl Movement {
    fn record_existing(
        &mut self,
        size: u64,
        previous: &[WorkerIdx],
        current: &[WorkerIdx],
        drained: &[WorkerIdx],
    ) {
        // Both holder lists are sorted, so walk them in lockstep: a worker only in
        // `current` gained this chunk, one only in `previous` lost it, matches are
        // unchanged.
        let (mut i, mut j) = (0, 0);
        let (mut gained, mut lost, mut refetched) = (0u64, 0u64, 0u64);
        while i < current.len() && j < previous.len() {
            match current[i].cmp(&previous[j]) {
                Ordering::Less => {
                    self.workers_shuffled.insert(current[i]);
                    gained += 1;
                    i += 1;
                }
                Ordering::Greater => {
                    self.workers_losing.insert(previous[j]);
                    lost += 1;
                    j += 1;
                }
                Ordering::Equal => {
                    // A holder in both prev and cur normally means "unchanged, already on disk".
                    // But if that copy was physically expired this cycle, the worker had to
                    // re-download it (the ideal re-placed it onto the same worker) — a real refetch.
                    if drained.binary_search(&current[i]).is_ok() {
                        self.workers_shuffled.insert(current[i]);
                        refetched += 1;
                    }
                    i += 1;
                    j += 1;
                }
            }
        }
        // `current`-only workers (gained) cannot be in `drained`: a drained copy was on disk at the
        // start of the cycle, so it is in `previous`. `previous`-only workers are lost regardless.
        for &w in &current[i..] {
            self.workers_shuffled.insert(w);
            gained += 1;
        }
        for &w in &previous[j..] {
            self.workers_losing.insert(w);
            lost += 1;
        }

        let shuffled = gained.min(lost);
        if shuffled > 0 {
            self.shuffled_count += 1;
            self.shuffled_bytes += size * shuffled;
        }
        if refetched > 0 {
            self.refetched_count += 1;
            self.refetched_bytes += size * refetched;
        }
        self.increased_replication_bytes += size * gained.saturating_sub(lost);
        self.decreased_replication_bytes += size * lost.saturating_sub(gained);
    }

    fn record_added(&mut self, size: u64, current: &[WorkerIdx]) {
        self.new_chunk_bytes += size * current.len() as u64;
        self.workers_receiving_new.extend(current.iter().copied());
    }

    fn record_removed(&mut self, size: u64, previous: &[WorkerIdx]) {
        self.decreased_replication_bytes += size * previous.len() as u64;
        self.workers_losing.extend(previous.iter().copied());
    }

    fn finish(self) -> DataMovement {
        DataMovement {
            new_chunk_bytes: self.new_chunk_bytes,
            shuffled_bytes: self.shuffled_bytes,
            shuffled_count: self.shuffled_count,
            refetched_bytes: self.refetched_bytes,
            refetched_count: self.refetched_count,
            increased_replication_bytes: self.increased_replication_bytes,
            decreased_replication_bytes: self.decreased_replication_bytes,
            workers_receiving_new: self.workers_receiving_new.len(),
            workers_losing: self.workers_losing.len(),
            workers_shuffled: self.workers_shuffled.len(),
        }
    }
}
